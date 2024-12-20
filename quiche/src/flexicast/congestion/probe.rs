use std::{borrow::BorrowMut, time::{Duration, Instant}};

use crate::flexicast::{congestion::FcCongestionHeuristicOps, FlexicastAttributes};

use super::FcCongestionConf;

const PROBE_DURATION : u32 = 2;  // probe during 2*RTT
const WAIT_DURATION  : u32  = 8;   // wait  during 8*RTT

enum ProbePhase{
    Probing(Instant),
    Waiting(Instant),
    Joining,
    Fallback,
}

#[derive(Clone)]
struct CongestionStats{
    channel_idx: usize,
    loss_rate: f32,
    rtt: Duration,
    ecn_rate: f32,
}

impl Default for CongestionStats{
    fn default() -> Self {
        Self {
            channel_idx: 0,
            loss_rate: 0.0,
            ecn_rate: 0.0,
            rtt: Duration::from_millis(333),
        }
    }
}

impl CongestionStats{
    const ALPHA: f32 = 0.05;

    fn on_loss(&mut self){
        self.loss_rate = (1.0 - CongestionStats::ALPHA) * self.loss_rate + CongestionStats::ALPHA * 1.0;
    }

    fn on_marked(&mut self){
        self.ecn_rate = (1.0 - CongestionStats::ALPHA) * self.ecn_rate + CongestionStats::ALPHA * 1.0;
    }

    fn on_not_marked(&mut self){
        self.ecn_rate = (1.0 - CongestionStats::ALPHA) * self.ecn_rate + CongestionStats::ALPHA * 0.0;
    }

    fn on_rtt_measured(&mut self, rtt: Duration){
        self.rtt = rtt;
    }

    fn on_received(&mut self){
        // no loss
        self.loss_rate = (1.0 - CongestionStats::ALPHA) * self.loss_rate + CongestionStats::ALPHA * 0.0;
    }

    fn congestion_occured(&self, previous: &CongestionStats) -> bool{
        let increase_rtt = self.rtt.saturating_sub(previous.rtt);
        // more than 15% losses, more than 10% increase for RTT or more than 30% marked packets
        self.loss_rate > 0.15 || increase_rtt > (previous.rtt / 10) || self.ecn_rate > 0.3
    }
}

pub(crate) struct ProbeState{
    phase: ProbePhase,
    wait_duration: u32,
    congestion_stats_previous: CongestionStats,
    congestion_stats_curr: CongestionStats,
    join_channel: Option<usize>,
    started: bool
}

impl Default for ProbeState{
    fn default() -> Self {
        Self {
            phase: ProbePhase::Waiting(Instant::now()),
            wait_duration: WAIT_DURATION,
            congestion_stats_previous: CongestionStats::default(),
            congestion_stats_curr: CongestionStats::default(),
            join_channel: None,
            started: false
        }
    }
}

impl ProbeState{
    pub(crate) fn new(_config: &FcCongestionConf) -> Self{
        ProbeState{
            phase: ProbePhase::Waiting(Instant::now()),
            wait_duration: WAIT_DURATION,
            congestion_stats_previous: CongestionStats::default(),
            congestion_stats_curr: CongestionStats::default(),
            join_channel: None,
            started: false
        }
    }

    pub(crate) fn packets_recv(&mut self, number_recv: usize){
        for _ in 0..number_recv{
            self.congestion_stats_curr.on_received();
        }
    }

    pub(crate) fn packets_lost(&mut self, number_lost: usize){
        for _ in 0..number_lost{
            self.congestion_stats_curr.on_loss();
        }
    }

    pub(crate) fn packet_marked(&mut self){
        self.congestion_stats_curr.on_marked();
    }

    pub(crate) fn packet_not_marked(&mut self){
        self.congestion_stats_curr.on_not_marked();
    }

    fn update_rtt(&mut self, rtt: Duration){
        self.congestion_stats_curr.on_rtt_measured(rtt);
    }

    #[inline]
    fn congestion_occured(&self) -> bool{
        self.congestion_stats_curr.congestion_occured(&self.congestion_stats_previous)
    }

    fn update_phase(&mut self, now: Instant, active_data: Option<usize>){
        let rtt = match self.phase {
            ProbePhase::Probing(_) => self.congestion_stats_previous.rtt, // use the previous rtt as curr rtt might fluctuate
            ProbePhase::Waiting(_) => self.congestion_stats_curr.rtt,
            ProbePhase::Joining => self.congestion_stats_previous.rtt,
            ProbePhase::Fallback => self.congestion_stats_curr.rtt,
        };
        match self.phase{
            ProbePhase::Probing(start) => {
                if self.congestion_occured(){
                    self.phase = ProbePhase::Fallback;
                    self.wait_duration *= 2; // increase the timeout for trying a new probe
                    self.congestion_stats_curr = self.congestion_stats_previous.clone();
                    self.join_channel = Some(self.congestion_stats_previous.channel_idx); // join the previous group
                    return;
                }
                if now.duration_since(start) > PROBE_DURATION*rtt{
                    self.phase = ProbePhase::Waiting(now); // no congestion occured, move to waiting phase
                }
            },
            ProbePhase::Waiting(start) => {
                if now.duration_since(start) > self.wait_duration*rtt{
                    self.phase = ProbePhase::Joining;
                    let new_channel_idx = self.congestion_stats_curr.channel_idx + 1;
                    self.join_channel = Some(new_channel_idx);
                    self.congestion_stats_previous = self.congestion_stats_curr.clone();
                    self.congestion_stats_curr.channel_idx = new_channel_idx;
                }
            },
            ProbePhase::Joining => {
                match active_data {
                    Some(idx) if idx == self.join_channel.unwrap() => {
                        self.join_channel = None;
                        self.phase = ProbePhase::Probing(now)
                    },
                    _ => return,
                }
            },
            ProbePhase::Fallback => {
                match active_data {
                    Some(idx) if idx == self.join_channel.unwrap() => {
                        self.join_channel = None;
                        self.phase = ProbePhase::Waiting(now)
                    },
                    _ => return,
                }
            },
        }
    }
}

fn probe_should_change_channel(flexicast: &mut FlexicastAttributes) -> Option<Vec<u8>>{
    let idx_active =
    flexicast
            .get_mc_announce_data_active()
            .map(|data| flexicast.get_mc_announce_data_index(&data.channel_id))
            .flatten();

    let congestion_state = &mut flexicast.congestion_state;
    let probe_state = congestion_state.probe_state.borrow_mut();
    if !probe_state.started{
        probe_state.started = true;
        probe_state.phase = ProbePhase::Waiting(Instant::now());
        return None;
    }
    let rtt =
        congestion_state
            .local_congestion_info
            .as_ref()
            .map(|info| info.rtt)
            .unwrap_or(Duration::from_millis(333));
    probe_state.update_rtt(rtt);
    probe_state.update_phase(Instant::now(), idx_active);
    let join_channel = probe_state.join_channel?;
    let channel_id = flexicast.get_mc_announce_data(join_channel)?.channel_id.clone();

    if let Some(active) = flexicast.get_mc_announce_data_active(){
        if active.channel_id != channel_id{
            Some(channel_id)
        }else{
            None
        }
    }else{
        Some(channel_id)
    }
}

/// Probing heuristic.
/// After each T period of times, try to join a group with higher bitrate.
/// If congestion signals are detected, fallback to the previous group and
/// increase T.
pub static PROBE: FcCongestionHeuristicOps = FcCongestionHeuristicOps {
    should_change_channel: probe_should_change_channel
};
