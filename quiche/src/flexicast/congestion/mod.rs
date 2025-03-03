//! Handles the congestion control for multicast
use std::time::{self, Duration, Instant};
use config::FcCongestionConf;
use exp3::EXP3State;
use stats::CongestionStats;

use crate::{ancillaries::{Ancillary, ECNValue}, Connection};

use super::{FlexicastAttributes, FlexicastChannelSource, McRole};

const MAX_DELAY_MULTIPLIER: u32 = 16;

/// An heuristic returning whether a multicast client should change
/// the channel it is listening to, in reaction to congestion.
pub struct FcCongestionHeuristicOps {
    should_change_channel: fn(conn: &mut Connection) -> Option<Vec<u8>>
}

pub(crate) struct FcCongestionState{
    delay_congestion_info: time::Duration,
    base_delay_congestion_info: time::Duration,
    next_congestion_info: time::Instant,
    local_congestion_info: Option<FcCongestionInfo>,
    pub(crate) received_congestion_info: Option<FcCongestionInfo>,

    mc_congestion_scheduler: &'static FcCongestionHeuristicOps,

    last_migration: time::Instant,

    pub(crate) exp3_state: exp3::EXP3State,

    statistics: CongestionStats
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
            mc_congestion_scheduler: &exp3::EXP3_HEURISTIC,
            last_migration: Instant::now(),
            exp3_state: EXP3State::default(),
            statistics: CongestionStats::default()
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
            exp3_state: EXP3State::new(config),
            statistics: CongestionStats::default(),
        }
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

    pub(crate) fn update_ancillaries(&mut self, ancillaries: Vec<Ancillary>){
        let mut marked = false;
        for ancillary in ancillaries{
            match ancillary{
                Ancillary::TTL(_ttl) => (), // use it later
                Ancillary::ECN(ecnvalue) => {
                    if ecnvalue == ECNValue::CE{
                        self.statistics.on_marked();
                        marked = true;
                    }
                },
            }
        }
        if !marked{
            self.statistics.on_not_marked();
        }
    }

    pub fn reset(&mut self){
        self.statistics = CongestionStats::default()
    }
}

/// Congestion control for multicast
pub trait FlexicastCongestion {
    /// sets the congestion info of a multicast receiver
    /// This function is typically called when a MC_CONGESTION_INFO
    /// frame is received by a client
    fn mc_set_congestion_info(&mut self, info: FcCongestionInfo);


    /// Whether a server should send a MC_CONGESTION_INFO frame
    /// or not
    fn should_send_fc_congestion_info(&self) -> bool;
}

impl FlexicastCongestion for FlexicastAttributes{
    fn mc_set_congestion_info(&mut self, info: FcCongestionInfo){
        self.congestion_state.local_congestion_info = Some(info)
    }

    fn should_send_fc_congestion_info(&self) -> bool{
        self.congestion_state.should_send_congestion_info(Instant::now())
    }
}

/// Internal congestion control for multicast
pub trait FlexicastCongestionConnection {
    /// Whether a multicast receiver should change of channels
    /// This function returns a the CID of the new channel to
    /// join
    fn fc_should_change_channel(&mut self) -> Option<Vec<u8>>;

    /// Updates the flexicast channel loss rate based on loss rate
    /// measured by the application (typically missing sequence numbers
    /// for RTP)
    fn update_app_data_loss(&mut self, loss_rate: f64);

    /// On the server, updates the congestion info stored that will be
    /// sent to clients in MC_CONGESTION_INFO frames
    fn update_congestion_info(&mut self, fc_channels: Vec<&FlexicastChannelSource>);

    /// Gets the congestion window of the multicast source.
    fn mc_get_cwnd(&self) -> Option<usize>;
}

impl FlexicastCongestionConnection for Connection{

    fn fc_should_change_channel(&mut self) -> Option<Vec<u8>> {
        let flexicast = self.flexicast.as_ref()?;
        if !matches!(flexicast.mc_role, McRole::Client(_)){
            return None;
        }
        let scheduler = flexicast.congestion_state.mc_congestion_scheduler;
        (scheduler.should_change_channel)(self)
    }

    fn update_app_data_loss(&mut self, loss_rate: f64) {
        if let Some(flexicast) = &mut self.flexicast{
            flexicast.congestion_state.statistics.set_loss_rate(loss_rate);
        }
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
mod exp3;
mod stats;
mod window;
pub mod config;
