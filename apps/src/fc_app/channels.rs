use std::{net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4}, time::SystemTime};

use polling::{Event, Poller};
use quiche::{flexicast::{congestion::{FlexicastCongestion, FlexicastCongestionConnection}, FlexicastAttributes, FlexicastConnection, McAnnounceData, McClientStatus, McRole}, Connection, ConnectionId};

use crate::fc_app::ssm::SSM;

use super::{msg_socket::MsgSocket, recv_statistics::{Migration, MultiChannelRecvStats}, rtp::RtpLossTracker};

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
pub struct ChannelState{
    pub mc_socket: Option<MsgSocket>,
    pub bind_addr: SocketAddr,
    pub fc_chan_idx: usize,
    group_addr: SocketAddr,
    lifetime: ChannelLifetime,
    from_fc_change_channel: bool
}

impl ChannelState{
    pub fn provide_cid(&mut self, conn: &mut Connection){
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

    fn get_group_ip(mc_announce_data: &McAnnounceData, local_ip: IpAddr, proxy: bool) -> SocketAddr{
        if mc_announce_data.probe_path || proxy {
            SocketAddr::new(
                local_ip,
                mc_announce_data.udp_port,
            )
        } else {
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(
                    mc_announce_data.group_ip.to_owned(),
                ),
                mc_announce_data.udp_port,
            ))
        }
    }

    fn get_mc_announce_data(&self, conn: &Connection) -> McAnnounceData{
        conn
            .get_flexicast_attributes()
            .unwrap()
            .get_mc_announce_data(self.fc_chan_idx)
            .unwrap()
            .to_owned()
    }

    pub fn probe(&mut self, conn: &mut Connection, poll: &mut Poller, local_ip: IpAddr, server_addr: SocketAddr, proxy: bool){
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
            let mc_group_sockaddr: SocketAddr = Self::get_group_ip(&mc_announce_data, local_ip, proxy);

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
        }else{
            error!("Failed to create the second path: {:?}", mc_space_id);
        }
    }

    pub fn should_join_multicast(&self, conn: &Connection) -> bool{
        let flexicast = conn.get_flexicast_attributes().unwrap();

        self.lifetime == ChannelLifetime::Bind &&
        flexicast.get_mc_role() == McRole::Client(McClientStatus::ListenMcPath(true))
    }

    pub fn join_multicast(&mut self, conn: &mut Connection, proxy: bool, itf: IpAddr, source: IpAddr){
        let flexicast = conn.get_flexicast_attributes().unwrap();
        if !proxy {
            info!("Join MULTICAST on ip {:?}", flexicast
            .get_mc_announce_data(self.fc_chan_idx)
            .unwrap()
            .group_ip
            .to_owned());

            match (self.group_addr.ip(), itf, source){
                (IpAddr::V4(ipv4_addr), IpAddr::V4(itf_addr), IpAddr::V4(source))  => {
                    self.mc_socket
                    .as_mut()
                    .unwrap()
                    .join_ssm_multicast_v4(
                        &ipv4_addr,
                        &itf_addr,
                        &source
                    )
                    .unwrap();
                },
                (IpAddr::V6(_ipv6_addr), IpAddr::V6(_itf_addr), IpAddr::V4(_source)) => todo!(),
                _ => error!("Incompatible type of addresses"),
            }
        }
        self.lifetime = ChannelLifetime::Joined;

        // Inform flexicast that we effectively changed of channel
        conn.fc_did_change_channel();
    }

    pub fn should_leave_multicast(&self) -> bool{
        self.mc_socket.is_some() && self.lifetime == ChannelLifetime::Leaving
    }

    pub fn leave_multicast(&mut self, itf_addr: IpAddr, source: IpAddr, proxy: bool){
        info!("Leave the multicast socket with ip={}, itf={} !", self.group_addr, itf_addr);
        if !proxy {
            match (self.group_addr.ip(), itf_addr, source){
                (IpAddr::V4(ipv4_addr), IpAddr::V4(itf_addr), IpAddr::V4(source)) => {
                    self.mc_socket
                    .as_mut()
                    .unwrap()
                    .leave_ssm_multicast_v4(
                        &ipv4_addr,
                        &itf_addr,
                        &source
                    )
                    .unwrap();
                },
                (IpAddr::V6(_ipv6_addr), IpAddr::V6(_itf_addr), IpAddr::V6(_source)) => todo!(),
                _ => error!("Inconsistency in ip addresses")
            }
        }
        self.lifetime = ChannelLifetime::Left;
    }
}

#[derive(Debug)]
pub struct Channels{
    channels: Vec<ChannelState>,
    rtp_loss_tracker: RtpLossTracker,
    max_stream: u64,
    max_timestamp: u32,
    stats: MultiChannelRecvStats,
    pub changing_cid: Option<Vec<u8>>,
    initial_channel_joined: bool
}

impl Channels {
    pub fn new() -> Channels{
        Channels{
            channels: vec![],
            changing_cid: None,
            rtp_loss_tracker: RtpLossTracker::new(),
            max_stream: 0,
            max_timestamp: 0,
            stats: MultiChannelRecvStats::new(),
            initial_channel_joined: false
        }
    }

    pub fn migration_done(&mut self, channel_idx: usize, bitrate: u64, flexicast: &FlexicastAttributes){
        let now = SystemTime::now();
        // log now the migration, as socket is definitively closed
        self.stats.migrated(Migration {
            new_channel_idx: channel_idx,
            new_channel_bitrate: bitrate,
            last_recv_timestamp: self.max_timestamp as i64,
            time: now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
            exp3: flexicast.fc_get_exp3_state()
        });

        // and reset some metadata collected
        self.max_timestamp = 0;
        self.max_stream = 0;
        self.rtp_loss_tracker.reset();
        self.changing_cid = None;
    }

    pub fn join_channel(&mut self, channel_idx: usize, announce_data: &McAnnounceData, flexicast: &FlexicastAttributes){
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

        if !self.initial_channel_joined{
            let now = SystemTime::now();
            self.stats.migrated(Migration{
                new_channel_idx: channel_idx,
                new_channel_bitrate: announce_data.bitrate.unwrap(),
                last_recv_timestamp: -1,
                time: now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64(),
                exp3: flexicast.fc_get_exp3_state()
            });
            self.initial_channel_joined = true;
        }
    }

    pub fn leave_channel(&mut self, channel_idx: usize){
        for state in self.channels.iter_mut() {
            if state.fc_chan_idx == channel_idx && state.lifetime == ChannelLifetime::Joined{
                state.lifetime = ChannelLifetime::Leaving;
            }
        };
    }

    pub fn get_loss_tracker(&self) -> &RtpLossTracker{
        &self.rtp_loss_tracker
    }

    pub fn get_loss_tracker_mut(&mut self) -> &mut RtpLossTracker{
        &mut self.rtp_loss_tracker
    }

    pub fn get_stats(&mut self) -> &mut MultiChannelRecvStats{
        &mut self.stats
    }

    pub fn received_timestamp(&mut self, timestamp: u32){
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    pub fn joined_channel(&self) -> Option<&ChannelState>{
        self.channels.get(0).filter(|c| c.lifetime == ChannelLifetime::Joined)
    }

    pub fn channels(&self) -> impl Iterator<Item = &ChannelState>{
        self.channels.iter()
    }

    pub fn channels_mut(&mut self) -> impl Iterator<Item = &mut ChannelState>{
        self.channels.iter_mut()
    }

    pub fn get_socket(&self, port: u16) -> Option<&MsgSocket>{
        self.channels.iter().find(|state| state.bind_addr.port() == port)?.mc_socket.as_ref()
    }

    pub fn clean(&mut self){
        self.channels.retain(|state| state.lifetime != ChannelLifetime::Left);
    }
}
