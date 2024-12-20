//! Handles the congestion control for multicast
use std::time::{self, Duration, Instant};
use probe::ProbeState;

use crate::{ancillaries::{Ancillary, ECNValue}, Config, Connection};

use super::{FlexicastAttributes, FlexicastChannelSource, McRole};

const MAX_DELAY_MULTIPLIER: u32 = 16;

/// TODO
pub struct FcCongestionHeuristicOps {
    should_change_channel: fn(flexicast: &mut FlexicastAttributes) -> Option<Vec<u8>>
}

/// Heuristict used by Flexicast for automatic group migration
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum FcCongestionHeuristic {
    /// Join the group with closest rate
    KMEANS     = 0,
    /// Incrementally join groups with higher rates, if no congestion signal
    /// are detected
    PROBE      = 1,
}

impl From<FcCongestionHeuristic> for &'static FcCongestionHeuristicOps {
    fn from(heuristic: FcCongestionHeuristic) -> Self {
        match heuristic {
            FcCongestionHeuristic::KMEANS => &kmeans::KMEANS,
            FcCongestionHeuristic::PROBE => &probe::PROBE,
        }
    }
}

pub(crate) struct FcCongestionState{
    delay_congestion_info: time::Duration,

    base_delay_congestion_info: time::Duration,

    next_congestion_info: time::Instant,

    local_congestion_info: Option<FcCongestionInfo>,

    pub(crate) received_congestion_info: Option<FcCongestionInfo>,

    mc_congestion_scheduler: &'static FcCongestionHeuristicOps,

    last_migration: time::Instant,

    pub(crate) probe_state: probe::ProbeState,

    curr_channel_idx: usize,

    lost_count: usize,

    recv_count: usize,
}

impl Default for FcCongestionState{
    fn default() -> Self {
        let delay = Duration::from_millis(100);
        Self {
            delay_congestion_info: delay,
            base_delay_congestion_info: delay,
            next_congestion_info: Instant::now().checked_add(delay).unwrap(),
            local_congestion_info: None,
            received_congestion_info: None,
            mc_congestion_scheduler: &kmeans::KMEANS,
            last_migration: Instant::now(),
            probe_state: ProbeState::default(),
            lost_count: 0,
            recv_count: 0,
            curr_channel_idx: 0,
        }
    }
}

impl FcCongestionState{
    pub(crate) fn new(config: &FcCongestionConf) -> Self{
        Self {
            delay_congestion_info: config.congestion_info_delay,
            base_delay_congestion_info: config.congestion_info_delay,
            next_congestion_info: Instant::now().checked_add(config.congestion_info_delay).unwrap(),
            local_congestion_info: None,
            received_congestion_info: None,
            mc_congestion_scheduler: config.cc_heuristic,
            last_migration: Instant::now(),
            probe_state: ProbeState::new(config),
            lost_count: 0,
            recv_count: 0,
            curr_channel_idx: 0,
        }
    }

    pub(crate) fn congestion_info_timeout(&self) -> Option<Instant> {
        if self.local_congestion_info.is_none(){
            return None;
        }
        Some(self.next_congestion_info)
    }

    pub(crate) fn should_send_congestion_info(&self, now: Instant) -> bool {
        if self.local_congestion_info.is_none(){
            return false;
        }
        now > self.next_congestion_info
    }

    pub(crate) fn get_congestion_info(&self) -> &FcCongestionInfo {
        self.local_congestion_info.as_ref().unwrap()
    }

    pub(crate) fn sending_congestion_info(&mut self) {
        let now = Instant::now();
        // backoff to reduce the number of congestion info if stable
        if now.duration_since(self.last_migration) > 2*self.delay_congestion_info{
            // didn't migrate in the last two congestion info update, increase the delay
            self.delay_congestion_info = (2 * self.delay_congestion_info).min(MAX_DELAY_MULTIPLIER * self.base_delay_congestion_info);
        }else{
            // did migrate, reset delay back to base
            self.delay_congestion_info = self.base_delay_congestion_info;
        }
        self.next_congestion_info = now.checked_add(self.delay_congestion_info).unwrap();

    }

    pub(crate) fn update_recv(&mut self, recv_count: usize){
        println!("Updating recv, curr={}, new={}", self.recv_count, recv_count);
        let new_recv = recv_count - self.recv_count;
        self.recv_count = recv_count;
        self.probe_state.packets_recv(new_recv);
    }

    pub(crate) fn update_loss(&mut self, lost_count: usize){
        println!("Updating loss, curr={}, new={}", self.lost_count, lost_count);
        let new_loss = lost_count - self.lost_count;
        self.lost_count = lost_count;
        self.probe_state.packets_lost(new_loss);
    }

    pub(crate) fn update_ancillaries(&mut self, ancillaries: Vec<Ancillary>){
        let mut marked = false;
        for ancillary in ancillaries{
            match ancillary{
                Ancillary::TTL(ttl) => (), // use it later
                Ancillary::ECN(ecnvalue) => {
                    if ecnvalue == ECNValue::CE{
                        self.probe_state.packet_marked();
                        marked = true;
                    }
                },
            }
        }
        if !marked{
            self.probe_state.packet_not_marked();
        }
    }

    fn reset(&mut self){
        self.recv_count = 0;
        self.lost_count = 0;
    }
}

/// Congestion control for multicast
pub trait FlexicastCongestion {
    /// TODO
    fn mc_set_congestion_info(&mut self, info: FcCongestionInfo);
    /// TODO
    fn should_change_channel(&mut self) -> Option<Vec<u8>>;

    fn should_send_fc_congestion_info(&self) -> bool;
}

impl FlexicastCongestion for FlexicastAttributes{
    fn mc_set_congestion_info(&mut self, info: FcCongestionInfo){
        self.congestion_state.local_congestion_info = Some(info)
    }

    fn should_change_channel(&mut self) -> Option<Vec<u8>> {
        if !matches!(self.mc_role, McRole::Client(_)){
            return None;
        }
        let scheduler = self.congestion_state.mc_congestion_scheduler;
        (scheduler.should_change_channel)(self)
    }

    fn should_send_fc_congestion_info(&self) -> bool{
        self.congestion_state.should_send_congestion_info(Instant::now())
    }
}

/// Internal congestion control for multicast
pub trait FlexicastCongestionConnection {
    /// TODO
    fn mc_update_loss(&mut self) -> Option<()>;
    /// TODO
    fn mc_update_recv(&mut self) -> Option<()>;

    /// TODO: Congestion Info
    fn update_congestion_info(&mut self, fc_channels: Vec<&FlexicastChannelSource>);

    /// Gets the congestion window of the multicast source.
    fn mc_get_cwnd(&self) -> Option<usize>;
}

impl FlexicastCongestionConnection for Connection{

    fn mc_update_loss(&mut self) -> Option<()>{
        let flexicast = self.flexicast.as_mut()?;
        let space_id = flexicast.get_fc_path_id()? as usize;
        let path = self.paths.get_mut(space_id).ok()?;
        let active =
            flexicast
            .get_mc_announce_data_index(
                &flexicast.get_mc_announce_data_active()?.channel_id
            )?;

        let congestion_state = &mut flexicast.congestion_state;
        if congestion_state.curr_channel_idx != active{
            congestion_state.reset();
            congestion_state.curr_channel_idx = active;
        }
        let lost = path.recovery.lost_count();
        flexicast.congestion_state.update_loss(lost);
        None
    }

    fn mc_update_recv(&mut self) -> Option<()>{
        let flexicast = self.flexicast.as_mut()?;
        let space_id = flexicast.get_fc_path_id()? as usize;
        let path = self.paths.get_mut(space_id).ok()?;
        let recv = path.recv_count;
        let active =
        flexicast
            .get_mc_announce_data_index(
                &flexicast.get_mc_announce_data_active()?.channel_id
            )?;

        let congestion_state = &mut flexicast.congestion_state;
        if congestion_state.curr_channel_idx != active{
            congestion_state.reset();
            congestion_state.curr_channel_idx = active;
        }
        flexicast.congestion_state.update_recv(recv);
        None
    }

    fn update_congestion_info(&mut self, fc_channels: Vec<&FlexicastChannelSource>) {
        if self.fc_get_flow_cwnd().is_none(){
            return;
        }

        let cwnd = self.fc_get_flow_cwnd().unwrap();
        let mc_cwnds =
                fc_channels
                    .iter()
                    .map(|fc_channel| {
                        let flexicast = fc_channel.channel.flexicast.as_ref().unwrap();
                        let mc_announce = flexicast.get_mc_announce_data(0).unwrap();
                        let channel_id = mc_announce.channel_id.clone();
                        (channel_id , fc_channel.channel.mc_get_cwnd().unwrap())
                    })
                    .collect();

        let rtt = self.paths.get_active().unwrap().recovery.rtt();

        let info = FcCongestionInfo{
            cwnd,
            mc_cwnds,
            rtt
        };

        if let Some(flexicast) = self.flexicast.as_mut(){
            flexicast.mc_set_congestion_info(info);
        }
    }


    fn mc_get_cwnd(&self) -> Option<usize> {
        let fc_path_id = self.flexicast.as_ref()?.get_fc_path_id()? as usize;
        let cwnd = self.paths.get(fc_path_id).ok()?.recovery.cwnd();
        Some(cwnd)
    }

}

///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcCongestionInfo{
    ///
    pub cwnd: usize,
    ///
    pub mc_cwnds: Vec<(Vec<u8>, usize)>,
    ///
    pub rtt: Duration
}

/// Config of the congestion control for Flexicast
pub struct FcCongestionConf {
    congestion_info_delay: Duration,
    cc_heuristic: &'static FcCongestionHeuristicOps,
}

impl FcCongestionConf{

    pub(crate) fn from_config(config: &Config) -> Self{
        FcCongestionConf{
            congestion_info_delay: config.fc_congestion_info_delay,
            cc_heuristic: config.fc_congestion_heuristic.into()
        }
    }
}

bitflags::bitflags! {
    /// Different quality of services associated with multicast groups
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct FcQos: u8{
        /// No information
        const None = 0x00;
        /// Throughput oriented
        const Throughput = 0x01;
        /// Delay oriented
        const Delay = 0x02;
    }
}


mod kmeans;
mod probe;
mod exp3;
pub mod config;
