#[macro_use]
extern crate log;

use clap::Parser;
use polling::Event;
use polling::Events;
use polling::Poller;
use quiche::flexicast::congestion::FlexicastCongestion;
use quiche::flexicast::FcError;
use quiche::flexicast::FlexicastConnection;
use quiche::flexicast::McAnnounceData;
use quiche::flexicast::McClientStatus;
use quiche::flexicast::McConfig;
use quiche::flexicast::McRole;
use quiche::h3::NameValue;
use quiche::Connection;
use quiche::ConnectionId;
use quiche::Error;
#[cfg(feature = "qlog")]
use quiche_apps::common::make_qlog_writer;
use ring::rand::SecureRandom;
use ring::rand::SystemRandom;
use std::fs::File;
use std::io::Write;
use std::net;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::ToSocketAddrs;
use std::process::Command;
use std::time;
use std::time::Duration;
use std::time::SystemTime;

use quiche_apps::fc_app::rtp::RtpClient;
use quiche_apps::fc_app::msg_socket::MsgSocket;

const MAX_DATAGRAM_SIZE: usize = 1350;

#[derive(Parser)]
struct Args {
    /// Activate multicast extension.
    #[clap(long)]
    flexicast: bool,

    /// URL of the server to contact.
    url: url::Url,

    /// Unicast source port.
    #[clap(short = 'p', long = "port", default_value = "9999")]
    source_port: u16,

    /// Multicast local IP.
    #[clap(
        short = 'l',
        long = "local",
        default_value = "127.0.0.1",
        value_parser
    )]
    local_ip: Ipv4Addr,

    #[clap(long = "proxy")]
    /// Multicast packets are proxied using packet replication for this client.
    /// This argument is a trick to avoid out-of-band computation by the source
    /// of the proxies to the clients. If this value is true, instead of
    /// binding to the multicast address given in the MC_ANNOUNCE frame, the
    /// client will listen to its own address and the port advertised by the
    /// source.
    proxy_uc: bool,

    #[clap(
        short = 'o',
        long = "output",
        value_parser,
        default_value = "output.avi"
    )]
    output_file: String,

    /// Address of the RTP sink.
    #[clap(short = 'r', long = "rtp-addr", value_parser)]
    rtp_sink_addr: Option<SocketAddr>,

    /// Whether a system call is performed to kill the GStreamer sink when the
    /// connection is closed.
    #[clap(long = "kill-gst")]
    kill_gst_at_end: bool,

    /// Initial channel index to join.
    /// FC-TODO: this should be done by using a real heuristic, not the index
    /// because we could not know per se which channel to join.
    #[clap(short = 'i', long = "idx-chan", default_value = "0")]
    idx_fc_chan: usize,

    /// Use automatic migration using the builtin multicast congestion controller
    #[clap(long)]
    auto_migration: bool,

    /// Log frame count at transitions
    #[clap(long)]
    migration_log: Option<String>,
}

fn main() {
    env_logger::builder()
        .format_timestamp_nanos()
        .init();

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let args = Args::parse();

    // Whether the flexicast client leaves the channel and joins another after
    // some time. Time of start of reception of data.
    let mut start_recv: Option<time::Instant> = None;

    let mut mc_states: Option<Channels> = None;

    // Setup the event loop.
    let mut poll = polling::Poller::new().unwrap();
    let mut events = Events::new();

    // Resolve server address.
    let url = args.url.clone();
    let peer_addr = url.to_socket_addrs().unwrap().next().unwrap();

    // Bind to INADDR_ANY or IN6ADDR_ANY depending on the IP family of the
    // server address. This is needed on macOS and BSD variants that don't
    // support binding to IN6ADDR_ANY for both v4 and v6.
    let bind_addr = match peer_addr {
        std::net::SocketAddr::V4(_) => format!("0.0.0.0:{}", args.source_port),
        std::net::SocketAddr::V6(_) => format!("[::]:{}", args.source_port),
    };

    // Create the UDP socket backing the QUIC connection, and register it with
    // the event loop.
    let mut socket = MsgSocket::new(bind_addr.parse().unwrap(), false);
    socket.recv_ttl().unwrap();
    socket.recv_ecn().unwrap();
    unsafe{
        poll.add(&socket.socket, Event::readable(0)).unwrap()
    };

    // Create the configuration for the QUIC connection.
    let mut config = get_config(args.flexicast);

    // Generate a random source connection ID for the connection.
    let mut scid = [0; 16];
    let random = SystemRandom::new();
    random.fill(&mut scid[..]).unwrap();

    let scid = quiche::ConnectionId::from_ref(&scid);

    // Get local address.
    let local_addr: SocketAddr = socket.local_addr().unwrap().into();

    // Create a QUIC connection and initiate handshake.
    let mut conn =
        quiche::connect(None, &scid, local_addr, peer_addr, &mut config).unwrap();

    // Only bother with qlog if the user specified it.
    #[cfg(feature = "qlog")]
    {
        if let Some(dir) = std::env::var_os("QLOGDIR") {
            let id = format!("Client-{}", args.local_ip.to_string());
            let writer = make_qlog_writer(&dir, "client", &id);

            conn.set_qlog(
                std::boxed::Box::new(writer),
                "quiche-client qlog".to_string(),
                format!("{} id={}", "quiche-client qlog", id),
            );
        }
    }

    // Create the RTP application handler at the client.
    let mut rtp_client =
        RtpClient::new(&args.output_file, args.rtp_sink_addr).unwrap();

    info!(
        "connecting to {:} from {:} with scid {}",
        peer_addr,
        local_addr,
        hex_dump(&scid)
    );

    let (write, send_info) = conn.send(&mut out).expect("initial send failed");

    while let Err(e) = socket.send_to(&out[..write], &send_info.to.into()) {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            debug!("send() would block");
            continue;
        }

        panic!("send() failed: {:?}", e);
    }

    loop {
        // Compute (FC-)QUIC timeout.
        let now = std::time::Instant::now();
        /*
        if conn.get_flexicast_attributes().is_some() {
            conn.rmc_set_next_timeout(now, &random).unwrap();
        }
        */

        // Timer if the client changes its flexicast channel.
        /*
        let timer_change = start_recv.zip(args.change_fc_chan.as_ref()).map(
            |(start, change)| {
                change.time.saturating_sub(now.duration_since(start))
            },
        );
        */

        let timers = [
            conn.timeout(),        // QUIC timeout
            // conn.mc_timeout(now),  // FC-QUIC timeout
            // conn.rmc_timeout(now), // Reliable FC-QUIC timeout
            // timer_change,          // FC Channel change
        ];
        let timeout = timers.iter().flatten().min().copied();

        let timeout = timeout.or(Some(Duration::from_millis(100)));

        poll.wait(&mut events, timeout).unwrap();

        rearm_poll(&mut poll, &socket, &mc_states);

        // Read incoming UDP packets from the socket and feed them to quiche,
        // until there are no more packets to read.
        'uc_read: loop {
            // If the event loop reported no events, it means that the timeout
            // has expired, so handle it without attempting to read packets. We
            // will then proceed with the send loop.
            if events.is_empty() {
                conn.on_timeout();

                break 'uc_read;
            }

            let (len, from, ancillaries) = match socket.recv_from(&mut buf) {
                Ok(v) => v,

                Err(e) => {
                    // There are no more UDP packets to read, so end the read
                    // loop.
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // debug!("recv() would block");
                        break 'uc_read;
                    }

                    panic!("recv() failed: {:?}", e);
                },
            };
            debug!("Recv from socket unicast");

            println!("Ancillaries = {:?}", ancillaries);

            let recv_info = quiche::RecvInfo {
                to: socket.local_addr().unwrap(),
                from,
                from_mc: false,
            };

            // Process potentially coalesced packets.
            let _read = match conn.recv_with_ancillaries(&mut buf[..len], recv_info, ancillaries) {
                Ok(v) => v,

                Err(e) => {
                    error!("recv failed: {:?}", e);
                    continue 'uc_read;
                },
            };
        }

        if let Some(mc_states) = &mut mc_states{
            // Read incomming UDP packets from the multicast sockets and feed them to
            // flexicast quiche.
            for state in mc_states.channels.iter_mut(){
                if let Some(mc_socket) = state.mc_socket.as_mut() {
                    'mc_read: loop {
                        let (len, _, ancillaries) = match mc_socket.recv_from(&mut buf) {
                            Ok(v) => v,
                            Err(e) => {
                                // There are no more UDP packets to read, so end the read
                                // loop.
                                if e.kind() == std::io::ErrorKind::WouldBlock {
                                    // debug!("recv() would block");
                                    break 'mc_read;
                                }

                                panic!("recv() failed: {:?}", e);
                            },
                        };

                        let recv_info = quiche::RecvInfo {
                            to: state.bind_addr,
                            from: peer_addr,
                            from_mc: true,
                        };

                        // Only feed the packet to quiche if the client listens to the
                        // multicast channel.
                        let err_opt =
                            if conn.get_flexicast_attributes().unwrap().get_mc_role()
                                == McRole::Client(McClientStatus::ListenMcPath(true))
                            {
                                conn.recv_with_ancillaries(&mut buf[..len], recv_info, ancillaries)
                            } else {
                                conn.recv_with_ancillaries(&mut buf[..len], recv_info, ancillaries)
                            };

                        let _read = match err_opt {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Multicast failed: {:?}", e);
                                continue 'mc_read;
                            },
                        };
                        debug!("Recv from socket multicast processed");
                    }
                }
            }
        }

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
        }

        if let Some(mc_states) = &mut mc_states{
            check_migrate(&args, &mut conn, mc_states, peer_addr);
        }

        // Process Flexicast events.
        if let Some(flexicast) = conn.get_flexicast_attributes() {
            if mc_states.is_none(){
                // initialize mc_states
                if let Some(announce_data) = flexicast.get_mc_announce_data(args.idx_fc_chan){
                    let mut channels = Channels::new(args.migration_log.clone()); // use logs
                    channels.join_channel(args.idx_fc_chan, &announce_data);
                    mc_states = Some(channels);
                }
            }

            if let Some(mc_states) = &mut mc_states{
                // update the channel states
                for state in mc_states.channels.iter_mut(){

                    // Stop the socket if the client left the group and it was
                    // acknowledged.
                    if state.should_leave_multicast(){
                        state.leave_multicast(std::net::IpAddr::V4(args.local_ip), args.proxy_uc);
                    }

                    // Join the flexicast channel and creates the listening socket if not
                    // already done.
                    if matches!(
                        conn.get_flexicast_attributes().unwrap().get_mc_role(),
                        McRole::Client(McClientStatus::AwareUnjoined)
                            | McRole::Client(McClientStatus::Changing)
                    ) {
                        debug!("Client joins the flexicast channel.");

                        // Add the new connection ID for the announce data.
                        state.provide_cid(&mut conn);

                        // Create a second path.
                        state.probe(&mut conn, &mut poll, socket.local_addr().unwrap().ip(), peer_addr, args.proxy_uc);
                    }

                    // Join the multicast socket.
                    if state.should_join_multicast(&conn){
                        state.join_multicast(&mut conn, args.proxy_uc, IpAddr::V4(args.local_ip));
                    }
                }
            }
        }

        // Generate outgoing QUIC packets and send them on the UDP socket, until
        // quiche reports that there are no more packets to be sent.
        loop {
            let (write, send_info) = match conn.send(&mut out) {
                Ok(v) => v,

                Err(quiche::Error::Done) => {
                    break;
                },

                Err(Error::Flexicast(FcError::McPath)) => {
                    // migrated channel
                    break;
                }

                Err(e) => {
                    error!("send failed: {:?}", e);

                    conn.close(false, 0x1, b"fail").ok();
                    break;
                },
            };

            // Depending on `send_info`, use the appropriate socket.
            // The client may send packets on the multicast channel for the path
            // probing phase.
            let src_port = send_info.from.port();
            let out_socket = if src_port == socket.local_addr().unwrap().port() {
                &mut socket
            } else if let Some(socket) = mc_states.as_mut().map(|states| states.get_socket(src_port)).flatten(){
                socket
            } else {
                panic!("Unknown source addr to send packets: {:?}", send_info);
            };

            if let Err(e) = out_socket.send_to(&out[..write], &send_info.to) {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    break;
                }

                panic!("send() failed: {:?}", e);
            }
        }

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
            break;
        }

        if let Some(mc_states) = &mut mc_states{
            mc_states.clean();
        }

        // Process all readable streams.
        'streams: for stream_id in conn.readable() {
            if !conn.stream_fully_readable(stream_id) {
                continue 'streams;
            }

            if !conn.stream_readable(stream_id) {
                continue 'streams;
            }

            // We should be able to read the stream until its end.
            let mut total = 0;
            while let Ok((read, fin)) =
                conn.stream_recv(stream_id, &mut buf[..])
            {
                if start_recv.is_none() {
                    start_recv = Some(now);
                }

                total += read;

                let channels = mc_states.as_mut().expect("Should have an mc_state at this point");
                channels.recv += read as u64;

                rtp_client.on_sequential_stream_recv(&buf[..read]);

                if fin {
                    let now_st = SystemTime::now();
                    rtp_client.on_stream_complete(stream_id, now_st, total, None);
                }
            }
        }
    }

    // Kill the GStreamer sink.
    if args.kill_gst_at_end {
        Command::new("pkill")
            .arg("gst-launch")
            .output()
            .expect("Failed to kill GStreamer sink.");
    }
}

fn get_config(
    flexicast: bool,
) -> quiche::Config {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    config.verify_peer(false); // Not prodction-ready.

    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();

    if !flexicast {
        config.set_max_idle_timeout(100_000);
    }
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(100_000_000_000);
    config.set_initial_max_stream_data_bidi_local(100_000_000_000);
    config.set_initial_max_stream_data_bidi_remote(100_000_000_000);
    config.set_initial_max_stream_data_uni(100_000_000_000);
    config.set_initial_max_streams_bidi(100_000_000_000);
    config.set_initial_max_streams_uni(100_000_000_000);
    config.set_active_connection_id_limit(10);
    config.verify_peer(false);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    config.set_initial_max_path_id(10);

    if flexicast {
        config.set_initial_max_path_id(10);
        config.set_enable_flexicast(flexicast);
    }

    config
}

fn hex_dump(buf: &[u8]) -> String {
    let vec: Vec<String> = buf.iter().map(|b| format!("{b:02x}")).collect();

    vec.join("")
}

pub fn hdrs_to_strings(hdrs: &[quiche::h3::Header]) -> Vec<(String, String)> {
    hdrs.iter()
        .map(|h| {
            let name = String::from_utf8_lossy(h.name()).to_string();
            let value = String::from_utf8_lossy(h.value()).to_string();

            (name, value)
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
enum ChannelLifetime{
    ProvideCid,
    ProbePath,
    Bind,
    Joined,
    Leaving,
    Left
}

#[derive(Debug)]
struct ChannelState{
    mc_socket: Option<MsgSocket>,
    bind_addr: SocketAddr,
    group_addr: SocketAddr,
    lifetime: ChannelLifetime,
    fc_chan_idx: usize,
    from_fc_change_channel: bool
}

impl ChannelState{
    fn should_join_multicast(&self, conn: &Connection) -> bool{
        let flexicast = conn.get_flexicast_attributes().unwrap();

        self.lifetime == ChannelLifetime::Bind &&
        flexicast.get_mc_role() == McRole::Client(McClientStatus::ListenMcPath(true))
    }

    fn join_multicast(&mut self, conn: &mut Connection, proxy: bool, itf: IpAddr){
        let flexicast = conn.get_flexicast_attributes().unwrap();
        if !proxy {
            info!("Join MULTICAST on ip {:?}", flexicast
            .get_mc_announce_data(self.fc_chan_idx)
            .unwrap()
            .group_ip
            .to_owned());

            match (self.group_addr.ip(), itf){
                (IpAddr::V4(ipv4_addr), IpAddr::V4(itf_addr))  => {
                    self.mc_socket
                    .as_mut()
                    .unwrap()
                    .join_multicast_v4(
                        &ipv4_addr,
                        &itf_addr
                    )
                    .unwrap();
                },
                (IpAddr::V6(_ipv6_addr), IpAddr::V6(_itf_addr)) => todo!(),
                _ => error!("Incompatible type of addresses"),
            }
        }
        self.lifetime = ChannelLifetime::Joined;
    }

    fn get_group_ip(mc_announce_data: &McAnnounceData, local_ip: IpAddr, proxy: bool) -> SocketAddr{
        if mc_announce_data.probe_path || proxy {
            net::SocketAddr::new(
                local_ip,
                mc_announce_data.udp_port,
            )
        } else {
            SocketAddr::V4(net::SocketAddrV4::new(
                Ipv4Addr::from(
                    mc_announce_data.group_ip.to_owned(),
                ),
                mc_announce_data.udp_port,
            ))
        }
    }

    fn probe(&mut self, conn: &mut Connection, poll: &mut Poller, local_ip: IpAddr, server_addr: SocketAddr, proxy: bool){
        if self.lifetime != ChannelLifetime::ProbePath {
            return;
        }

        let mc_announce_data = self.get_mc_announce_data(conn);

        debug!("Create the second path. Client addr={:?}. Server addr={:?}", self.bind_addr, server_addr);
        let mc_space_id = conn.create_mc_path(
            self.bind_addr,
            server_addr,
            mc_announce_data.probe_path,
        );
        if let Ok(mc_space_id) = mc_space_id {
            conn.get_flexicast_attributes_mut().unwrap().set_fc_path_id(mc_space_id);

            // If soft-multicast is used by the source, the client
            // will receive multicast QUIC
            // packets with its unicast
            // address as destination of the IP packet. Bind the
            // socket to the local address
            // with the multicast destination
            // port.
            let mc_group_sockaddr: net::SocketAddr = Self::get_group_ip(&mc_announce_data, local_ip, proxy);

            if let Some(sock) = self.mc_socket.as_mut() {
                poll.delete(&sock.socket).unwrap();
            }

            let mc_socket = MsgSocket::new(mc_group_sockaddr, true);
            mc_socket.recv_ttl().unwrap();
            mc_socket.recv_ecn().unwrap();

            unsafe {
                poll.add(&mc_socket.socket, Event::readable(1)).unwrap()
            };
            debug!(
                "Multicast client binds on address: {:?}",
                mc_group_sockaddr
            );

            self.lifetime = ChannelLifetime::Bind;


            if !self.from_fc_change_channel{
                // must still join group
                conn.mc_join_channel(
                    false,
                    Some(&mc_announce_data.channel_id),
                )
                .unwrap();
            }

            self.mc_socket = Some(mc_socket);
        }
    }

    fn provide_cid(&mut self, conn: &mut Connection){
        if self.lifetime != ChannelLifetime::ProvideCid{
            return;
        }

        debug!("Add a new connection ID");
        let mc_announce_data = self.get_mc_announce_data(conn);
        let scid =
            ConnectionId::from_ref(&mc_announce_data.channel_id);
        conn.add_mc_cid(&scid).unwrap();
        self.lifetime = ChannelLifetime::ProbePath;
    }

    fn should_leave_multicast(&self) -> bool{
        self.mc_socket.is_some() && self.lifetime == ChannelLifetime::Leaving
    }

    fn leave_multicast(&mut self, itf_addr: IpAddr, proxy: bool){
        info!("Leave the multicast socket with ip={}, itf={} !", self.group_addr, itf_addr);
        if !proxy {
            match (self.group_addr.ip(), itf_addr){
                (IpAddr::V4(ipv4_addr), IpAddr::V4(itf_addr)) => {
                    self.mc_socket
                    .as_mut()
                    .unwrap()
                    .leave_multicast_v4(
                        &ipv4_addr,
                        &itf_addr,
                    )
                    .unwrap();
                },
                (IpAddr::V6(_ipv6_addr), IpAddr::V6(_itf_addr)) => todo!(),
                _ => error!("Inconsistency in ip addresses")
            }
        }
        self.lifetime = ChannelLifetime::Left;
    }

    fn get_mc_announce_data(&self, conn: &Connection) -> McAnnounceData{
        conn
            .get_flexicast_attributes()
            .unwrap()
            .get_mc_announce_data(self.fc_chan_idx)
            .unwrap()
            .to_owned()
    }
}

#[derive(Debug)]
struct Channels{
    channels: Vec<ChannelState>,
    changing_cid: Option<Vec<u8>>,
    recv: u64,
    count_frames: u64,
    migration_log: Option<File>
}

impl Channels {
    fn new(log: Option<String>) -> Channels{
        let migration_log = if let Some(filename) = log{
            let mut migrations = File::create(filename).expect("Failed to create file");
            migrations.write_all("group,rate,frame_idx\n".as_bytes()).expect("Failed to log");
            Some(migrations)
        }else{
            None
        };
        Channels{
            channels: vec![],
            changing_cid: None,
            recv: 0,
            count_frames: 0,
            migration_log
        }
    }

    fn join_channel(&mut self, channel_idx: usize, announce_data: &McAnnounceData){
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", announce_data.udp_port).parse().unwrap();
        let group_addr: SocketAddr = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from(announce_data.group_ip),
            announce_data.udp_port
        ));

        self.channels.push(ChannelState{
            mc_socket: None,
            bind_addr,
            group_addr,
            lifetime: ChannelLifetime::ProvideCid,
            fc_chan_idx: channel_idx,
            from_fc_change_channel: false
        });
    }

    fn leave_channel(&mut self, channel_idx: usize){
        for state in self.channels.iter_mut() {
            if state.fc_chan_idx == channel_idx && state.lifetime == ChannelLifetime::Joined{
                state.lifetime = ChannelLifetime::Leaving;
            }
        };
    }

    fn get_socket(&self, port: u16) -> Option<&MsgSocket>{
        self.channels.iter().find(|state| state.bind_addr.port() == port)?.mc_socket.as_ref()
    }

    fn clean(&mut self){
        self.channels.retain(|state| state.lifetime != ChannelLifetime::Left);
    }
}

fn check_migrate(args: &Args, conn: &mut Connection, mc_states: &mut Channels, server_addr: SocketAddr) -> Option<()>{
    if !args.auto_migration{
        return None;
    }
    let multicast = conn.get_flexicast_attributes_mut()?;

    if let None = mc_states.changing_cid{
        mc_states.changing_cid = multicast.should_change_channel();
    }

    let channel_id = mc_states.changing_cid.as_ref()?;

    info!("Should change to channel {:?}", channel_id);

    let new_idx = multicast.get_mc_announce_data_index(&channel_id).unwrap();
    let announce_data = multicast.get_mc_announce_data(new_idx).unwrap();

    if mc_states.channels.is_empty(){
        info!("Joining new channel");
        // left previous, can finally add the state,
        // rest of pipeline (provide cid, probe path, join group, ...)
        // will be handled in the loop
        mc_states.join_channel(new_idx, announce_data);
        mc_states.changing_cid = None;
    }else if !mc_states.channels.is_empty() && mc_states.channels[0].lifetime == ChannelLifetime::Joined{
        let curr_idx = multicast.get_mc_announce_data_index(
            &multicast.get_mc_announce_data_active().unwrap().channel_id
        ).unwrap();
        let curr_bitrate = multicast.get_mc_announce_data_active().unwrap().bitrate.expect("Only use channels with fixed bitrates");
        conn.mc_leave_channel().unwrap();
        conn.abandon_path(mc_states.channels[0].bind_addr, server_addr, 0).unwrap();
        mc_states.leave_channel(mc_states.channels[0].fc_chan_idx);

        if let Some(file) = &mut mc_states.migration_log{
            let fps = 30;
            // Simplifying assumption: all frames have same size (not case in reality)
            let frame_size = curr_bitrate / fps;
    
            let new_frames_count = mc_states.recv / frame_size;
            mc_states.count_frames = mc_states.count_frames + new_frames_count;
            mc_states.recv = 0;

            file.write(format!("{},{},{}", curr_idx, curr_bitrate, mc_states.count_frames).as_bytes()).expect("Failed to write");
        }
    }
    return None;
}

fn rearm_poll(poll: &mut Poller, unicast: &MsgSocket, multicast: &Option<Channels>){
    poll.modify(&unicast.socket, Event::readable(0)).unwrap();

    if let Some(multicast) = multicast{
        for channel in &multicast.channels{
            if let Some(socket) = &channel.mc_socket{
                poll.modify(&socket.socket, Event::readable(1)).unwrap();
            }
        }
    }
}
