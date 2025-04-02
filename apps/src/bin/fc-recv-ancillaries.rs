#[macro_use]
extern crate log;

use clap::Parser;
use polling::Event;
use polling::Events;
use polling::Poller;
use quiche::flexicast::congestion::config::FcCongestionConfig;
use quiche::flexicast::congestion::FlexicastCongestionConnection;
use quiche::flexicast::FcError;
use quiche::flexicast::FlexicastConnection;
use quiche::flexicast::McClientStatus;
use quiche::flexicast::McConfig;
use quiche::flexicast::McRole;
use quiche::h3::NameValue;
use quiche::Connection;
use quiche::Error;
#[cfg(feature = "qlog")]
use quiche_apps::common::make_qlog_writer;
use quiche_apps::fc_app::channels::Channels;
use quiche_apps::fc_app::rtp::RtpHeader;
use ring::rand::SecureRandom;
use ring::rand::SystemRandom;
use std::convert::TryInto;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::process::Command;
use std::time;
use std::time::Duration;
use std::time::Instant;
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

    /// Address of the RTP sink.
    #[clap(long = "debug-rtp", value_parser)]
    debug_rtp: bool,

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

    /// Duration of data reception
    #[clap(long, value_parser = humantime::parse_duration, default_value = "5s")]
    listen_duration: Duration,

    /// Migration timeout
    #[clap(long, value_parser = humantime::parse_duration)]
    migration_timeout: Option<Duration>,

    /// Loss threshold used by EXP3
    #[clap(long)]
    loss_threshold: Option<f64>,

    /// Throughput time window used by EXP3
    #[clap(long, value_parser = humantime::parse_duration)]
    throughput_time_window: Option<Duration>,

    /// K parameter used by EXP3
    #[clap(long)]
    k: Option<f64>,

    /// Gamma parameter used by EXP3
    #[clap(long)]
    gamma: Option<f64>,
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
    let mut config = get_config(&args);

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

    let mut rtp_debug_sinks: Option<Vec<RtpClient>> = None;
    if args.debug_rtp{
        rtp_debug_sinks = Some(
            (0..2).map(|idx|
                RtpClient::new("", Some(format!("127.0.0.1:{}", 9000 + idx + 1).parse().unwrap())).unwrap()
            ).collect()
        )
    }

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

    let start = Instant::now();

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

        if now.duration_since(start) > args.listen_duration{
            conn.close(false, 0x1, b"fail").ok();
            break;
        }

        let elapsed_since_start = now.duration_since(start);
        let timer_end = args.listen_duration - elapsed_since_start;

        let timers = [
            conn.timeout(),        // QUIC timeout
            // conn.mc_timeout(now),  // FC-QUIC timeout
            // conn.rmc_timeout(now), // Reliable FC-QUIC timeout
            // timer_change,          // FC Channel change
            Some(timer_end)
        ];
        let timeout = timers.iter().flatten().min().copied();

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
            debug!("Recv from socket unicast, ancillaries = {:?}", ancillaries);

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
            for state in mc_states.channels_mut(){
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
                    let mut channels = Channels::new();
                    channels.join_channel(args.idx_fc_chan, announce_data, &flexicast);
                    mc_states = Some(channels);
                }
            }

            debug!("Current role: {:?}", flexicast.get_mc_role());

            if let Some(mc_states) = &mut mc_states{
                // update the channel states
                for state in mc_states.channels_mut(){

                    // Stop the socket if the client left the group and it was
                    // acknowledged.
                    if state.should_leave_multicast(){
                        state.leave_multicast(std::net::IpAddr::V4(args.local_ip), peer_addr.ip(), args.proxy_uc);
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
                        state.join_multicast(&mut conn, args.proxy_uc, IpAddr::V4(args.local_ip), peer_addr.ip());
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
        //let recv = process_video_data_stream(&mut conn, &mut mc_states, &mut rtp_client);
        let recv = process_video_data_datagram(&mut conn, &mut mc_states, &mut rtp_client, &mut rtp_debug_sinks);
        if recv && start_recv.is_none() {
            start_recv = Some(now);
        }
    }

    // Kill the GStreamer sink.
    if args.kill_gst_at_end {
        Command::new("pkill")
            .arg("gst-launch")
            .output()
            .expect("Failed to kill GStreamer sink.");
    }

    if let Some(path) = args.migration_log{
        mc_states.unwrap().get_stats().write(&path).unwrap();
    }
}

fn get_config(
    args: &Args
) -> quiche::Config {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    config.verify_peer(false); // Not prodction-ready.

    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();

    if !args.flexicast {
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
    config.set_active_connection_id_limit(100);
    config.verify_peer(false);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    config.set_initial_max_path_id(100);
    config.enable_dgram(true, 10000, 10000);

    if args.flexicast {
        config.set_initial_max_path_id(100);
        config.set_enable_flexicast(args.flexicast);

        // configuration of exp3
        if let Some(gamma) = args.gamma{
            config.set_fc_exp3_gamma(Some(gamma));
        }
        if let Some(k) = args.k{
            config.set_fc_exp3_k(k);
        }
        if let Some(migration_timeout) = args.migration_timeout{
            config.set_fc_exp3_migration_timeout(migration_timeout);
        }
        if let Some(throughput_time_window) = args.throughput_time_window{
            config.set_fc_throughput_window(throughput_time_window);
        }
        if let Some(loss_threshold) = args.loss_threshold{
            config.set_fc_exp3_loss_threshold(loss_threshold);
        }
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

fn check_migrate(args: &Args, conn: &mut Connection, mc_states: &mut Channels, server_addr: SocketAddr) -> Option<()>{
    if !args.auto_migration{
        return None;
    }

    if let None = mc_states.changing_cid{
        mc_states.changing_cid = conn.fc_should_change_channel();
    }

    let channel_id = mc_states.changing_cid.as_ref()?;
    let multicast = conn.get_flexicast_attributes_mut()?;

    let new_idx = multicast.get_mc_announce_data_index(&channel_id).unwrap();
    let announce_data = multicast.get_mc_announce_data(new_idx).unwrap();

    if channel_id == &multicast.get_mc_announce_data_active().unwrap().channel_id{
        // no change, but we must reset the loss stats as this is a new Monitoring Interval
        mc_states.migration_done(new_idx, announce_data.bitrate.unwrap(), &multicast);
        conn.fc_did_change_channel();
        return None;
    }

    info!("Should change to channel {:?}", channel_id);

    if mc_states.channels().count() == 0{
        info!("Joining new channel");
        // left previous, can finally add the state,
        // rest of pipeline (provide cid, probe path, join group, ...)
        // will be handled in the loop
        mc_states.migration_done(new_idx, announce_data.bitrate.unwrap(), &multicast);
        mc_states.join_channel(new_idx, announce_data, &multicast);
    }else if let Some(channel) = mc_states.joined_channel(){
        conn.mc_leave_channel().unwrap();
        conn.abandon_path(channel.bind_addr, server_addr, 0).unwrap();
        mc_states.leave_channel(channel.fc_chan_idx);
    }
    return None;
}

fn rearm_poll(poll: &mut Poller, unicast: &MsgSocket, multicast: &Option<Channels>){
    poll.modify(&unicast.socket, Event::readable(0)).unwrap();

    if let Some(multicast) = multicast{
        for channel in multicast.channels(){
            if let Some(socket) = &channel.mc_socket{
                poll.modify(&socket.socket, Event::readable(1)).unwrap();
            }
        }
    }
}

fn process_video_data_datagram(conn: &mut Connection, mc_states: &mut Option<Channels>, rtp_client: &mut RtpClient, rtp_debug_sinks: &mut Option<Vec<RtpClient>>) -> bool{
    let now = SystemTime::now();
    let mut buf = [0; 65535];

    let mut recv = false;
    while let Ok(len) = conn.dgram_recv(&mut buf) {
        recv = true;

        let rtp = RtpHeader::from_bytes(buf[..12].try_into().unwrap());

        let channels = mc_states.as_mut().expect("Should have an mc_state at this point");
        debug!("Got {} bytes of DATAGRAM, loss rate {}%", len, (channels.get_loss_tracker().loss_rate() * 10000.0).round() / 100.0);

        if rtp.payload_type == 96{
            // not rtcp
            channels.get_loss_tracker_mut().on_header_recv(&rtp);
            // get the RTP header and store the timestamp if at start of stream and higher stream id
            channels.received_timestamp(rtp.timestamp);

            let loss_rate = channels.get_loss_tracker().loss_rate();
            let instant_loss_rate = channels.get_loss_tracker().instant_loss_rate();
            channels.get_stats().record_loss(rtp.timestamp, loss_rate);
            channels.get_stats().record_instant_loss(rtp.timestamp, instant_loss_rate);
            channels.get_stats().record_recv(rtp.timestamp, len, now);
        }

        conn.update_app_data_loss(channels.get_loss_tracker().loss_rate());

        rtp_client.on_sequential_stream_recv(&buf[..len]);

        if let Some(rtp_debug_sinks) = rtp_debug_sinks{
            let idx = mc_states.as_ref().unwrap().joined_channel().map(|c| c.fc_chan_idx);
            if let Some(idx) = idx{
                rtp_debug_sinks[idx].on_sequential_stream_recv(&buf[..len]);
            }
        }

        // let now_st = SystemTime::now();
        // rtp_client.on_stream_complete(stream_id, now_st, total, None);
    }

    recv
}
