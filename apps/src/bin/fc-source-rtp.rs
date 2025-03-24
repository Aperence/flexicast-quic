#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::net;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use std::usize;

use clap::Parser;
use quiche::flexicast;
use quiche::flexicast::congestion::config::FcCongestionConfig;
use quiche::flexicast::congestion::FcQos;
use quiche::flexicast::congestion::FlexicastCongestionConnection;
use quiche::flexicast::FlexicastChannelSource;
use quiche::flexicast::FlexicastConnection;
use quiche::flexicast::McAnnounceData;
use quiche::flexicast::McConfig;
use quiche::flexicast::FcConfig;
#[cfg(feature = "qlog")]
use quiche_apps::common::make_qlog_writer;
use quiche_apps::common::ClientIdMap;
use quiche_apps::fc_app::pacer::PacerType;
use quiche_apps::fc_app::rtp_async::RtpAsync;
use quiche_apps::fc_app::rtp_async::RtpAsyncHandler;
use quiche_apps::sendto::send_to;

use ring::rand::SecureRandom;
use ring::rand::SystemRandom;

const MAX_DATAGRAM_SIZE: usize = 1350;

struct Client {
    conn: quiche::Connection,
    client_id: u64,
    current_channel_idx: Option<usize>,

}

type ClientMap = HashMap<u64, Client>;

#[derive(Parser)]
struct Args {
    /// Activate flexicast extension.
    #[clap(long)]
    flexicast: bool,

    /// Keylog file for flexicast channel.
    #[clap(long = "keylog", value_parser, default_value = "/tmp/fc-server.txt")]
    fc_keylog_file: Box<Path>,

    /// Flexicast source authentication method.
    // #[clap(long = "auth", default_value = "none")]
    // authentication: FcAuthType,

    /// Wait that the indicated number of clients are ready to receive the data.
    /// If flexicast is enabled, waits for flexicast channel establishement.
    /// If unicast is used, waits for the connections to be established.
    #[clap(long = "wait", value_parser)]
    wait_clients: Option<u32>,

    /// Source address of the server.
    #[clap(long = "src", default_value = "127.0.0.1:4433")]
    src_addr: net::SocketAddr,

    /// Certificate path.
    #[clap(long = "cert-path", value_parser, default_value = "./src/bin")]
    cert_path: Box<Path>,

    /// Whether the multicast packet is proxied. In this case, the provided
    /// address will receive the multicast packet to transmit.
    #[clap(long = "proxy")]
    proxy_addr: Option<net::SocketAddr>,

    /// Multicast address.
    #[clap(
        long = "mc-addr",
        value_parser,
        default_value = "239.239.239.35:6000"
    )]
    mc_addr: net::SocketAddr,

    /// FEC max repair symbols within an expiration timer.
    #[clap(long = "max-fec-rs", value_parser)]
    max_fec_rs: Option<u32>,

    /// Expiration timer on the flexicast path.
    #[clap(long, value_parser, default_value = "600")]
    expiration_timer: u64,

    /// Sent video frames results (timestamps sent on the wire).
    #[clap(
        short = 'r',
        long,
        value_parser,
        default_value = "mc-server-result-wire.txt"
    )]
    result_wire_trace: String,

    /// Keylog file for multicast channel.
    #[clap(
        short = 'k',
        long,
        value_parser,
        default_value = "/tmp/mc-server.txt"
    )]
    mc_keylog_file: String,

    /// Sets the flexicast source congestion window to a fixed value.
    #[clap(long = "fc-cwnd")]
    fc_cwnd: Option<usize>,

    /// List of bitrates of the flexicast channels.
    /// If this parameter is used with `n` values, it means that `n` flexicast
    /// channels will be created, each with the values given as argument as the
    /// bitrate of the channel. Incomming clients will be aware of every
    /// channels through MC_ANNOUNCE frames and will be able to choose one of
    /// the channels depending on the advertised bitrate. The bitrate is in
    /// bits per second.
    #[clap(long = "bitrates", value_delimiter = ',', num_args=1..)]
    bitrates: Option<Vec<u64>>,

    /// Address of the RTP source.
    #[clap(long = "rtp-addr", value_parser)]
    rtp_src_addr: Vec<SocketAddr>,

    /// Loggers for the RTP source.
    #[clap(long = "rtp-loggers", value_delimiter = ',', num_args=1..)]
    rtp_loggers: Option<Vec<String>>,

    /// RTP message to indicate the end of the stream.
    #[clap(long = "rtp-stop", value_parser, default_value = "STOP RTP")]
    rtp_stop: String,

    /// Don't use path probing for multicast
    #[clap(long = "no-path-probing")]
    no_path_probing: bool,

    /// Don't use path probing for multicast
    #[clap(long = "pacer-type")]
    pacer_type: Option<PacerType>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(move ||{
        main_server();
    });
}

fn main_server() {
    env_logger::builder()
        .format_timestamp_nanos()
        .init();
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let args = Args::parse();

    // Setup the event loop.
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);

    // Create the UDP listening socket, and register it with the event loop.
    let mut socket = mio::net::UdpSocket::bind(args.src_addr).unwrap();
    socket.set_multicast_ttl_v4(32).unwrap();
    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)
        .unwrap();

    // Create the configuration for the QUIC connections.
    let mut config = get_config(&args);

    let rng = SystemRandom::new();
    let conn_id_seed =
        ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();

    let mut clients = ClientMap::new();
    let mut clients_ids = ClientIdMap::new();
    let mut next_client_id = 0;
    let local_addr = socket.local_addr().unwrap();

    // Sanity check: there are the same number of flexicast instances as RTP
    // sources, if flexicast is enabled.
    if args.flexicast &&
            args
            .bitrates
            .as_ref()
            .is_some_and(|b| b.len() != args.rtp_src_addr.len())
    {
        error!("If flexicast is enabled, the number of flexicast instances must be the same as the number of RTP sources!");
        std::process::exit(1);
    }

    // Sanity check: there are the same number of flexicast instances as RTP
    // sources, if flexicast is enabled.
    if args.flexicast &&
            args
            .rtp_loggers
            .as_ref()
            .is_some_and(|b| b.len() != args.rtp_src_addr.len())
    {
        error!("If flexicast is enabled, the number of loggers instances must be the same as the number of RTP sources!");
        std::process::exit(1);
    }

    // List of all flexicast channels with different bitrates.
    // If no bitrate is provided (i.e., the `bitrates` parameter is not used),
    // creates a single flexicast channel with the classical implemented
    // congestion control algorithm.
    let mut fc_channels = if args.flexicast {
        if let Some(bitrates) = args.bitrates.as_ref() {
            bitrates.iter().enumerate()
                .map(|(i, bitrate)| get_multicast_channel(&args, &rng, Some(i as u8), Some(*bitrate)))
                .collect()
        } else {
            (0..args.rtp_src_addr.len() as u8)
                .map(|i| get_multicast_channel(&args, &rng, Some(i), None))
                .collect()
        }
    } else {
        Vec::new() // Empty.
    };

    // Compute the mapping between Flexicast channel ID and index.
    let _fcid_to_idx: HashMap<Vec<u8>, usize> = fc_channels
        .iter()
        .enumerate()
        .map(|(i, fc_chan)| (fc_chan.mc_announce_data.channel_id.to_owned(), i))
        .collect();

    debug!("AFTER FLEXICAST CHANNELS SETUP.");

    // Register the flexicast sockets on the poll.
    // FC-TODO: is it really necessary as we only send data on it?
    for (i, fc_chan) in fc_channels.iter_mut().enumerate() {
        poll.registry()
            .register(&mut fc_chan.socket, mio::Token(i), mio::Interest::READABLE)
            .unwrap();
    }

    let now = Instant::now();

    let mut rtp_servers = vec![];

    for (idx, rtp_addr) in args.rtp_src_addr.iter().enumerate(){
        let logger = args.rtp_loggers.as_ref().map(|loggers| loggers[idx].clone());
        // max burst: 10 RTP packets
        let buffer = 1100 * 10;
        let bitrate = args.bitrates.as_ref().unwrap()[idx] as usize;
        let pacer = args.pacer_type.clone().map(|pacer_type| pacer_type.get_pacer(bitrate, buffer, now));
        rtp_servers.push(RtpAsync::run(*rtp_addr, pacer, logger).await);
    }

    // Stop RTP timer.
    // Once the RTP source sends a STOP RTP message, the source waits for 5 *
    // flexicast timer before closing the connection.
    let rtp_stop_timer =
        std::time::Duration::from_millis(args.expiration_timer) * 5;
    let mut start_rtp_timer: Option<std::time::Instant> = None;
    let mut can_close_conn_after_rtp = false;

    let mut no_receiver_timeout = None;

    loop {
        // Find the shorter timeout from all the active connections.
        //
        // TODO: use event loop that properly supports timers
        let now = std::time::Instant::now();
        let mut timeout = clients.values().filter_map(|c| c.conn.timeout()).min();

        // Timeout of all flexicast channels.
        /*
        let timeout_fc = fc_channels
            .iter()
            .map(|fc_chan| fc_chan.fc_chan.channel.fc_time(now))
            .flatten()
            .min();
        */

        // Timeout after the RTP source finished.
        let timeout_rtp = start_rtp_timer.map(|timer| {
            rtp_stop_timer.saturating_sub(now.duration_since(timer))
        });

        let rtp_read_timeout = Some(Duration::from_millis(300));

        // The RTP application has no timeout because we get data as soon as it
        // comes on the socket.
        //timeout = [timeout, timeout_fc, timeout_rtp]
        timeout = [timeout, timeout_rtp, rtp_read_timeout]
            .iter()
            .flatten()
            .min()
            .copied();

        //timeout = timeout.or(Some(Duration::from_millis(100)));

        //debug!("TIMEOUT: {:?}", timeout);

        poll.poll(&mut events, timeout).unwrap();

        // Read incoming UDP packets from the socket and feed them to quiche,
        // until there are no more packets to read.
        'uc_read: loop {
            // Received content on the RTP source socket.
            let contains_quic_socket_event = !events.is_empty();
            /* for event in events.iter() {
                contains_quic_socket_event = true;
            }*/

            // We can close the connection now.
            if let Some(timer) = start_rtp_timer {
                if rtp_stop_timer.saturating_sub(now.duration_since(timer)) ==
                    std::time::Duration::ZERO
                {
                    // Yes, we can close now...
                    can_close_conn_after_rtp = true;

                    // Empty the timer to avoid going over and over here.
                    start_rtp_timer = None;
                }
            }

            // If the event loop reported no events, it means that the timeout
            // has expired, so handle it without attempting to read packets. We
            // will then proceed with the send loop.
            if !contains_quic_socket_event {
                clients.values_mut().for_each(|c| c.conn.on_timeout());

                break 'uc_read;
            }

            let (len, from) = match socket.recv_from(&mut buf) {
                Ok(v) => v,

                Err(e) => {
                    // There are no more UDP packets to read, so send the read
                    // loop.
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        break 'uc_read;
                    }

                    panic!("recv() failed: {:?}", e);
                },
            };

            let pkt_buf = &mut buf[..len];

            // Parse the QUIC packet's header.
            let hdr = match quiche::Header::from_slice(pkt_buf, 16) {
                Ok(v) => v,

                Err(e) => {
                    error!("Parsing packet header failed: {:?}", e);
                    continue 'uc_read;
                },
            };

            trace!("got packet {:?}", hdr);

            let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
            let conn_id = &conn_id.as_ref()[..16];

            // Lookup a connection based on the packet's connection ID. If there
            // is no connection matching, create a new one.
            let client = if !clients_ids.contains_key(&hdr.dcid) &&
                !clients_ids.contains_key(&hdr.dcid)
            {
                if hdr.ty != quiche::Type::Initial {
                    error!("Packet is not Initial");
                    continue 'uc_read;
                }

                if !quiche::version_is_supported(hdr.version) {
                    warn!("Doing version negotiation");

                    let len =
                        quiche::negotiate_version(&hdr.scid, &hdr.dcid, &mut out)
                            .unwrap();

                    let out = &out[..len];

                    if let Err(e) = socket.send_to(out, from) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!("send() would block");
                            break;
                        }

                        panic!("send() failed: {:?}", e);
                    }
                    continue 'uc_read;
                }

                let mut scid = [0; 16];
                scid.copy_from_slice(conn_id);

                let scid = quiche::ConnectionId::from_ref(&scid);

                // Token is always present in Initial packets.
                let token = hdr.token.as_ref().unwrap();

                // Do stateless retry if the client didn't send a token.
                if token.is_empty() {
                    warn!("Doing stateless retry");

                    let new_token = mint_token(&hdr, &from);

                    let len = quiche::retry(
                        &hdr.scid,
                        &hdr.dcid,
                        &scid,
                        &new_token,
                        hdr.version,
                        &mut out,
                    )
                    .unwrap();

                    let out = &out[..len];

                    if let Err(e) = socket.send_to(out, from) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!("send() would block");
                            break;
                        }

                        panic!("send() failed: {:?}", e);
                    }
                    continue 'uc_read;
                }

                let odcid = validate_token(&from, token);

                // The token was not valid, meaning the retry failed, so
                // drop the packet.
                if odcid.is_none() {
                    error!("Invalid address validation token");
                    continue 'uc_read;
                }

                if scid.len() != hdr.dcid.len() {
                    error!("Invalid destination connection ID");
                    continue 'uc_read;
                }

                // Reuse the source connection ID we sent in the Retry packet,
                // instead of changing it again.
                let scid = hdr.dcid.clone();

                let conn = quiche::accept(
                    &scid,
                    odcid.as_ref(),
                    local_addr,
                    from,
                    &mut config,
                )
                .unwrap();

                let client_id = next_client_id;

                let client = Client {
                    conn,
                    client_id,
                    current_channel_idx: None,
                };

                next_client_id += 1;
                clients.insert(client_id, client);
                clients_ids.insert(scid.clone(), client_id);

                debug!(
                    "New connection: dcid={:?} scid={:?}. Client id: {}",
                    hdr.dcid, scid, client_id
                );

                let client = clients.get_mut(&client_id).unwrap();

                // Only bother with qlog if the user specified it.
                #[cfg(feature = "qlog")]
                {
                    if let Some(dir) = std::env::var_os("QLOGDIR") {
                        let id = format!("server-{:?}", client_id);
                        let writer = make_qlog_writer(&dir, "server", &id);

                        client.conn.set_qlog(
                            std::boxed::Box::new(writer),
                            "quiche-server qlog".to_string(),
                            format!("{} id={}", "quiche-server qlog", id),
                        );
                    }
                }

                for (i, fc_chan) in fc_channels.iter().enumerate() {
                    client
                        .conn
                        .fc_set_announce_data(&fc_chan.mc_announce_data)
                        .unwrap();
                    // FC-TODO: not sure that this will work if it
                    // replaces the decryption key everytime we
                    // add a new one :/. We should see this in the
                    // tests but it actually works... lol.
                    client
                        .conn
                        .mc_set_flexicast_receiver(
                            &fc_chan.fc_chan.master_secret,
                            fc_chan
                                .fc_chan
                                .channel
                                .get_flexicast_attributes()
                                .unwrap()
                                .get_fc_path_id()
                                .unwrap(),
                            fc_chan
                                .fc_chan
                                .channel
                                .get_flexicast_attributes()
                                .unwrap()
                                .get_decryption_key_algo(),
                            Some(i),
                        )
                        .unwrap();
                }

                client
            } else {
                let cid = match clients_ids.get(&hdr.dcid) {
                    Some(v) => v,

                    None => clients_ids.get(&hdr.scid).unwrap(),
                };

                clients.get_mut(cid).unwrap()
            };

            let recv_info = quiche::RecvInfo {
                to: socket.local_addr().unwrap(),
                from,
                from_mc: false,
            };

            // Process potentially coalesced packets.
            let _read = match client.conn.recv(pkt_buf, recv_info) {
                Ok(v) => v,

                Err(e) => {
                    error!("{} recv failed: {:?}", client.conn.trace_id(), e);
                    continue 'uc_read;
                },
            };

            update_receivers_counts(client, &mut fc_channels, &mut rtp_servers).await;

            handle_path_events(client);

            // Provides as many CIDs as possible.
            while client.conn.scids_left() > 0 {
                let (scid, reset_token) = {
                    let mut scid = [0; 16];
                    rng.fill(&mut scid).unwrap();
                    let scid = scid.to_vec().into();
                    let mut reset_token = [0; 16];
                    rng.fill(&mut reset_token).unwrap();
                    let reset_token = u128::from_be_bytes(reset_token);
                    (scid, reset_token)
                };
                if client
                    .conn
                    .new_scid(&scid, reset_token, false)
                    .is_err()
                {
                    break;
                }
                info!("add a new source cid: {:?}", scid.as_ref());
                clients_ids.insert(scid, client.client_id);
            }
        }

        // Handle time to live timeout of data of the multicast channel.
        let now = std::time::Instant::now();
        /*
        for (idx, fc_chan) in fc_channels.iter_mut().enumerate() {
            // Before expiring the data, deleguate to unicast connections if
            // reliable multicast is enabled.
            // let clients_conn = clients.iter_mut().filter(|client| client.1.conn.get_flexicast_attributes().map(|mc| mc.get_fc_chan_id().map(|(_, id)| *id)).flatten().is_some_and(|i| i == idx)).map(|c| &mut c.1.conn);
            // let _expired_pkt =
            //    fc_chan.fc_chan.channel.on_mc_timeout(now).unwrap();
        }
        */

        // Generate video content frames.
        // Repeat to ensure to dequeue all pending streams if needed.
        'app_data: loop {
            let mut send_at_least_once = false;
            for (i, rtp_server) in rtp_servers.iter_mut().enumerate() {
                if let Some(fc_chan) = fc_channels.get_mut(i) {
                    send_at_least_once |= send_rtp_data_datagram(rtp_server, fc_chan).await.is_some()
                }
            }
            if !send_at_least_once {
                break 'app_data;
            }
        }

        // Generate outgoing Flexicast QUIC packets for each flexicast channel.
        for fc_chan in fc_channels.iter_mut() {
            let fc_conn = &mut fc_chan.fc_chan;
            let fc_sock = &mut fc_chan.socket;
            let nb_active_fc_clients = fc_chan.number_receivers;
            'flexicast: loop {
                let (write, mut send_info) = match fc_conn.mc_send(&mut out) {
                    Ok(v) => v,

                    Err(quiche::Error::Done) => {
                        debug!("Mc_send done for {}", Ipv4Addr::from(fc_chan.mc_announce_data.group_ip));
                        break
                    },

                    Err(e) => {
                        error!("Flexicast out failed: {:?}", e);
                        break 'flexicast;
                    },
                };

                // The source may send to the proxy its content instead of
                // injecting in the multicast network.
                send_info.to = args.proxy_addr.unwrap_or(fc_conn.mc_send_addr);

                if nb_active_fc_clients >= 1 {
                    let err = send_to(
                        fc_sock,
                        &out[..write],
                        &send_info,
                        MAX_DATAGRAM_SIZE,
                        false,
                        false,
                    );
                    if let Err(e) = err {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!("mc_send() would block");
                            break 'flexicast;
                        }else{
                            debug!("mc_send() to {} failed: {:?}", send_info.to, e);
                        }

                        //panic!("mc_send() failed: {:?}", e);
                    }
                    debug!(
                        "Flexicast written {} bytes to {:?}",
                        write, send_info
                    );
                } else {
                    debug!("not actually sending on the wire for flexicast");
                }
            }
        }

        for (_, client) in clients.iter_mut(){
            client.conn.update_congestion_info(fc_channels.iter().map(|fc| &fc.fc_chan).collect());
        }

        // Generate outgoing QUIC packets for all active connections and
        // send them on the UDP socket, until quiche
        // reports that there are no more packets to be sent.
        for client in clients.values_mut() {
            // Close the connections with the clients whether RTP is finished and
            // we waited enough.
            if can_close_conn_after_rtp {
                info!("Closing connection {:?}", client.conn.trace_id());
                _ = client.conn.close(true, 1, &[1]);
            }

            'uc_send: loop {
                // Communication between the unicast and flexicast channels.
                // FC-TODO: here we assume that the index of the MC_ANNOUNCE_DATA
                // is the same as the index of the Flexicast
                // channel the client listens to.
                let fc_chan_idx = client
                    .conn
                    .get_flexicast_attributes()
                    .map(|mc| mc.get_fc_chan_id().map(|(_, idx)| *idx))
                    .flatten();
                if let Some(fc_chan) =
                    fc_chan_idx.map(|idx| fc_channels.get_mut(idx)).flatten()
                {
                    match client
                        .conn
                        .uc_to_fc_control(&mut fc_chan.fc_chan.channel, now)
                    {
                        Ok(()) => (),
                        Err(quiche::Error::Flexicast(
                            quiche::flexicast::FcError::McDisabled,
                        )) => debug!("uc_to_mc_control with flexicast disabled"),
                        Err(e) => panic!("error: {:?}", e),
                    }
                }

                let (write, send_info) = match client.conn.send(&mut out) {
                    Ok(v) => v,

                    Err(quiche::Error::Done) => break 'uc_send,

                    Err(e) => {
                        error!("{} send failed: {:}", client.conn.trace_id(), e);

                        client.conn.close(false, 0x1, b"fail").ok();
                        break 'uc_send;
                    },
                };

                if let Err(e) = socket.send_to(&out[..write], send_info.to) {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        debug!("send() would block");
                        break 'uc_send;
                    }

                    panic!("send() failed: {:?}", e);
                }

                debug!("{} written {} bytes", client.conn.trace_id(), write);
            }

            // Communication between the unicast and flexicast channels.
            // FC-TODO: here we assume that the index of the MC_ANNOUNCE_DATA is
            // the same as the index of the Flexicast channel the client listens
            // to.
            let fc_chan_idx = client
                .conn
                .get_flexicast_attributes()
                .map(|mc| mc.get_fc_chan_id().map(|(_, idx)| *idx))
                .flatten();
            if let Some(fc_chan) =
                fc_chan_idx.map(|idx| fc_channels.get_mut(idx)).flatten()
            {
                match client
                    .conn
                    .uc_to_fc_control(&mut fc_chan.fc_chan.channel, now)
                {
                    Ok(()) => (),
                    Err(quiche::Error::Flexicast(
                        quiche::flexicast::FcError::McDisabled,
                    )) => debug!("uc_to_mc_control with flexicast disabled"),
                    Err(e) => panic!("error: {:?}", e),
                }
            }
        }

        // Garbage collect closed connections.
        clients.retain(|_, ref mut c| {
            if c.conn.is_closed() {
                info!(
                    "{} connection collected {:?}",
                    c.conn.trace_id(),
                    c.conn.stats(),
                );
                if let Some(prev) = c.current_channel_idx{
                    fc_channels[prev].number_receivers -= 1;
                }
            }

            !c.conn.is_closed()
        });
        clients_ids.retain(|_, id| clients.contains_key(id));

        // Set the congestion window for each flexicast channel.
        for (_, fc_chan) in fc_channels.iter_mut().enumerate() {
            /*
            if let Some(ref bitrates) = args.bitrates {
                // Set the bitrate of each channel accordingly.
                fc_chan.fc_chan.channel.fc_set_flow_cwnd(
                    (bitrates[i] /
                        (8 * fc_chan.mc_announce_data.expiration_timer))
                        as usize,
                );


            } else {
                // Rely on the congestion control.
                let clients_conn = clients.iter_mut().map(|c| &mut c.1.conn);
                ucs_to_mc_cwnd!(
                    &mut fc_chan.fc_chan.channel,
                    clients_conn,
                    now,
                    None
                );

            }
            */
            // don't use a congestion window, application is paced
            // by RTP sources
            fc_chan.fc_chan.channel.fc_set_flow_cwnd(usize::MAX);
        }

        debug!("Number of receivers:");
        for fc_chan in &fc_channels{
            debug!("{}: {}", Ipv4Addr::from(fc_chan.mc_announce_data.group_ip), fc_chan.number_receivers);
        }

        // Stop sending data if all clients left the communication for more than 10s.
        if fc_channels.iter().map(|chan| chan.number_receivers).sum::<u64>() == 0 {
            match no_receiver_timeout{
                Some(timeout) if now.duration_since(timeout) > Duration::from_secs(10) => {
                    break;
                },
                Some(_) => (),
                None => {
                    no_receiver_timeout = Some(now)
                }
            };
        } else {
            no_receiver_timeout = None;
        }
    }
}

fn get_config(args: &Args) -> quiche::Config {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

    config
        .load_cert_chain_from_pem_file(
            Path::new(args.cert_path.as_ref())
                .join("cert.crt")
                .to_str()
                .unwrap(),
        )
        .unwrap();
    config
        .load_priv_key_from_pem_file(
            Path::new(args.cert_path.as_ref())
                .join("cert.key")
                .to_str()
                .unwrap(),
        )
        .unwrap();

    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();

    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(100_000_000_000);
    config.set_initial_max_stream_data_bidi_local(100_000_000_000);
    config.set_initial_max_stream_data_bidi_remote(100_000_000_000);
    config.set_initial_max_stream_data_uni(100_000_000_000);
    config.set_initial_max_streams_bidi(100_000_000_000);
    config.set_initial_max_streams_uni(100_000_000_000);
    config.set_disable_active_migration(true);
    config.set_active_connection_id_limit(10);
    config.enable_early_data();
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::DISABLED);
    config.enable_pacing(false);
    config.set_enable_flexicast(args.flexicast);
    config.set_initial_max_path_id(100);
    config.enable_dgram(true, 10000, 10000);

    config.set_fc_congestion_info_delay(Duration::from_secs(60 * 60 * 24 * 365)); // don't use congestion info

    config
}

struct FcChannelInfo {
    socket: mio::net::UdpSocket,
    fc_chan: FlexicastChannelSource,
    mc_announce_data: McAnnounceData,
    number_receivers: u64
}

fn get_multicast_channel(
    args: &Args, rng: &SystemRandom, fc_conn_idx: Option<u8>, bitrate: Option<u64>,
) -> FcChannelInfo {
    // Index of the flexicast channel.
    let idx_addr = fc_conn_idx.unwrap_or(0);

    // Source address.
    let mut src_addr = args.src_addr;
    src_addr.set_port(4434 + idx_addr as u16);

    // Multicast destination address.
    // We increase the address and port depending on the index of the channel.
    let mc_addr = args.mc_addr;
    let mc_addr_bytes = match mc_addr {
        net::SocketAddr::V4(ip) => {
            let mut bytes = ip.ip().octets();
            bytes[3] += idx_addr;
            bytes
        },
        _ => unreachable!("Only support IPv4 multicast addresses"),
    };
    let mc_addr = net::SocketAddr::V4(SocketAddrV4::new(
        mc_addr_bytes.into(),
        mc_addr.port() + idx_addr as u16,
    ));
    let mc_port = mc_addr.port();

    let socket = mio::net::UdpSocket::bind(src_addr).unwrap();
    socket.set_multicast_ttl_v4(56 + idx_addr as u32).unwrap();

    let mut server_config = get_mc_config(
        true,
        args.cert_path.as_ref().to_str().unwrap()
    );
    let mut client_config = get_mc_config(
        false,
        args.cert_path.as_ref().to_str().unwrap(),
    );

    // Generate a random source connection ID for the connection.
    let mut channel_id = [0; 16];
    rng.fill(&mut channel_id[..]).unwrap();

    let channel_id = quiche::ConnectionId::from_ref(&channel_id);
    let channel_id_vec = channel_id.as_ref().to_vec();

    let mc_path_info = flexicast::McPathInfo {
        local: src_addr,
        peer: src_addr,
        cid: channel_id,
    };

    let fc_config = FcConfig {
        probe_mc_path: !args.no_path_probing,
        ..Default::default()
    };

    let mut fc_chan = FlexicastChannelSource::new_with_tls(
        mc_path_info,
        &mut server_config,
        &mut client_config,
        mc_addr,
        args.fc_keylog_file.as_ref().to_str().unwrap(),
        &fc_config,
    )
    .unwrap();

    let mc_announce_data = McAnnounceData {
        channel_id: channel_id_vec,
        is_ipv6_addr: false,
        probe_path: !args.no_path_probing,
        reset_stream_on_join: true,
        source_ip: [127, 0, 0, 1],
        group_ip: mc_addr_bytes,
        udp_port: mc_port,
        public_key: None,
        expiration_timer: args.expiration_timer,
        is_processed: false,
        bitrate: bitrate,
        fc_channel_algo: None,
        fc_channel_secret: None,
        qos: FcQos::Throughput | FcQos::Delay,
    };

    fc_chan
        .channel
        .fc_set_announce_data(&mc_announce_data)
        .unwrap();

    FcChannelInfo {
        socket,
        fc_chan,
        mc_announce_data,
        number_receivers: 0
    }
}

pub fn get_mc_config(enable_fc: bool, cert_path: &str) -> quiche::Config {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    config
        .load_cert_chain_from_pem_file(
            Path::new(cert_path).join("cert.crt").to_str().unwrap(),
        )
        .unwrap();
    config
        .load_priv_key_from_pem_file(
            Path::new(cert_path).join("cert.key").to_str().unwrap(),
        )
        .unwrap();
    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();
    // let use_fec = false;
    config.set_max_recv_udp_payload_size(1350);
    config.set_max_send_udp_payload_size(1350);
    config.set_initial_max_data(100_000_000_000);
    config.set_initial_max_stream_data_bidi_local(100_000_000_000);
    config.set_initial_max_stream_data_bidi_remote(100_000_000_000);
    config.set_initial_max_stream_data_uni(100_000_000_000);
    config.set_initial_max_streams_bidi(100_000_000_000);
    config.set_initial_max_streams_uni(100_000_000_000);
    config.set_active_connection_id_limit(5);
    config.verify_peer(false);
    config.set_initial_max_path_id(100);
    config.set_enable_flexicast(enable_fc);
    config.enable_pacing(false);
    config.enable_dgram(true, 10000, 10000);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::DISABLED);
    config
}

/// Generate a stateless retry token.
///
/// The token includes the static string `"quiche"` followed by the IP address
/// of the client and by the original destination connection ID generated by the
/// client.
///
/// Note that this function is only an example and doesn't do any cryptographic
/// authenticate of the token. *It should not be used in production system*.
fn mint_token(hdr: &quiche::Header, src: &net::SocketAddr) -> Vec<u8> {
    let mut token = Vec::new();

    token.extend_from_slice(b"quiche");

    let addr = match src.ip() {
        std::net::IpAddr::V4(a) => a.octets().to_vec(),
        std::net::IpAddr::V6(a) => a.octets().to_vec(),
    };

    token.extend_from_slice(&addr);
    token.extend_from_slice(&hdr.dcid);

    token
}

/// Validates a stateless retry token.
///
/// This checks that the ticket includes the `"quiche"` static string, and that
/// the client IP address matches the address stored in the ticket.
///
/// Note that this function is only an example and doesn't do any cryptographic
/// authenticate of the token. *It should not be used in production system*.
fn validate_token<'a>(
    src: &net::SocketAddr, token: &'a [u8],
) -> Option<quiche::ConnectionId<'a>> {
    if token.len() < 6 {
        return None;
    }

    if &token[..6] != b"quiche" {
        return None;
    }

    let token = &token[6..];

    let addr = match src.ip() {
        std::net::IpAddr::V4(a) => a.octets().to_vec(),
        std::net::IpAddr::V6(a) => a.octets().to_vec(),
    };

    if token.len() < addr.len() || &token[..addr.len()] != addr.as_slice() {
        return None;
    }

    Some(quiche::ConnectionId::from_ref(&token[addr.len()..]))
}

fn handle_path_events(client: &mut Client) {
    while let Some((_, qe)) = client.conn.path_event_next() {
        match qe {
            quiche::PathEvent::New(local_addr, peer_addr) => {
                info!(
                    "{} Seen new path ({}, {})",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );

                // Directly probe the new path.
                client
                    .conn
                    .probe_path(local_addr, peer_addr)
                    .map_err(|e| error!("cannot probe: {}", e))
                    .ok();
            },

            quiche::PathEvent::Validated(local_addr, peer_addr) => {
                info!(
                    "{} Path ({}, {}) is now validated",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
                if client.conn.is_multipath_enabled() {
                    client
                        .conn
                        .set_active(local_addr, peer_addr, true)
                        .map_err(|e| error!("cannot set path active: {}", e))
                        .ok();
                }
            },

            quiche::PathEvent::FailedValidation(local_addr, peer_addr) => {
                info!(
                    "{} Path ({}, {}) failed validation",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            },

            quiche::PathEvent::Closed(local_addr, peer_addr, err) => {
                info!(
                    "{} Path ({}, {}) is now closed and unusable; err = {}",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr,
                    err,
                );
            },

            quiche::PathEvent::ReusedSourceConnectionId(cid_seq, old, new) => {
                info!(
                    "{} Peer reused cid seq {} (initially {:?}) on {:?}",
                    client.conn.trace_id(),
                    cid_seq,
                    old,
                    new
                );
            },

            quiche::PathEvent::PeerMigrated(local_addr, peer_addr) => {
                info!(
                    "{} Connection migrated to ({}, {})",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            },

            quiche::PathEvent::PeerPathStatus(addr, path_status) => {
                info!("Peer asks status {:?} for {:?}", path_status, addr,);
                client
                    .conn
                    .set_path_status(addr.0, addr.1, path_status, false)
                    .map_err(|e| error!("cannot follow status request: {}", e))
                    .ok();
            },
        }
    }
}

async fn send_rtp_data_datagram(rtp_server: &mut RtpAsyncHandler, fc_chan: &mut FcChannelInfo) -> Option<usize>{
    let data = rtp_server.recv().await?;

    // if at least 1 receiver, send datagram, otherwise drop packet
    if fc_chan.number_receivers > 0{
        match fc_chan
            .fc_chan
            .channel
            .dgram_send(&data)
        {
            Ok(v) => v,
            Err(quiche::Error::Done) => {
                debug!("{} couldn't write data", fc_chan.fc_chan.mc_send_addr);
                return None;
            },
            Err(e) => panic!("Other error: {:?}", e),
        };

        debug!("Written {} bytes on {}", data.len(), fc_chan.fc_chan.mc_send_addr)
    }
    Some(data.len())
}

async fn update_receivers_counts(client: &mut Client, sources: &mut Vec<FcChannelInfo>, rtps: &mut Vec<RtpAsyncHandler>) -> Option<()>{
    let flexicast = client.conn.get_flexicast_attributes()?;
    let curr_channel = flexicast.get_mc_announce_data_active();

    match curr_channel {
        Some(curr_channel) => {
            // change of channel, update new channel and prev
            let curr_channel_idx = flexicast.get_mc_announce_data_index(&curr_channel.channel_id).expect("Should find it");

            if Some(curr_channel_idx) != client.current_channel_idx{
                let prev_channel_idx = client.current_channel_idx;
                client.current_channel_idx = Some(curr_channel_idx);

                if let Some(prev) = prev_channel_idx{
                    sources[prev].number_receivers -= 1;
                }
                sources[curr_channel_idx].number_receivers += 1;
            }
        }
        None => {
            // may have left channel, may need to decrement count
            if let Some(prev_channel_idx) = client.current_channel_idx{
                sources[prev_channel_idx].number_receivers -= 1;
                client.current_channel_idx = None;
            }
        }
    }

    for (fc, rtp) in sources.iter().zip(rtps){
        rtp.set_number_receivers(fc.number_receivers as usize).await;
    }

    Some(())
}
