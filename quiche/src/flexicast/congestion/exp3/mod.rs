//! EXP3 congestion control algorithm
use std::{collections::HashMap, fmt::Display, time::Instant};

use crate::{flexicast::McAnnounceData, Connection};

use super::{config::EXP3Conf, stats::CongestionStats, FcCongestionHeuristicOps};
use exp3::EXP3;

type CID = Vec<u8>;

/// Join groups depending on an EXP3 heuristic
pub static EXP3_HEURISTIC: FcCongestionHeuristicOps = FcCongestionHeuristicOps {
    should_change_channel: exp3_should_change_channel
};

fn exp3_should_change_channel(conn: &mut Connection) -> Option<Vec<u8>> {
    let flexicast = conn.flexicast.as_mut().unwrap();
    let now = Instant::now();

    let announce_data = &flexicast.mc_announce_data;
    let current_channel = flexicast.get_mc_announce_data_active().unwrap().clone();

    // first update the throughput in the window
    let congestion_stats = &mut flexicast.congestion_state.statistics;
    congestion_stats.update_window(now, current_channel.bitrate.expect("EXP3 expects a fixed bitrate"));
    let congestion_stats = congestion_stats.clone();
    let conf = &flexicast.congestion_state.exp3_state.conf.clone();
    let rewarder = conf.rewarder;
    let exp3_state = &mut flexicast.congestion_state.exp3_state;


    if exp3_state.wait_timeout_elapsed(now){
        exp3_state.last_taken_action = now;

        exp3_state.update_instances(announce_data);
        let current_channel_idx = exp3_state.ordered_channels.iter().position(|c| c == &current_channel.channel_id).unwrap();

        debug!("State:\n{}", exp3_state);

        if let Some(previous_cid) = &exp3_state.previous_channel{
            let reward = (rewarder.reward)(&congestion_stats, conf);
            exp3_state.instances
                .entry(previous_cid.clone())
                .and_modify(|e| {
                    e.reward(reward).expect("Unexpected failure to give a reward");
                });
        }

        let banned = exp3_state.get_banned(&congestion_stats);

        let exp3_instance = exp3_state.instances.get_mut(&current_channel.channel_id)
            .expect("Should have an instance for the current channel");

        debug!("Taking action for channel {}", current_channel_idx);
        let action: Action = exp3_instance.take_action(banned).expect("Failed to take action").into();
        debug!("Action is: {:?}", action);

        exp3_state.previous_channel = Some(current_channel.channel_id);

        let new_channel = match action{
            Action::Increase if current_channel_idx < exp3_state.ordered_channels.len() - 1 => {
                Some(exp3_state.ordered_channels[current_channel_idx + 1].clone())
            },
            Action::Decrease if current_channel_idx > 0 => {
                Some(exp3_state.ordered_channels[current_channel_idx - 1].clone())
            },
            _ => None,
        };
        debug!("New channel is {:?}", new_channel);
        return new_channel;
    }
    None
}

#[derive(Debug)]
enum Action {
    Increase = 0,
    Decrease = 1,
    Stay = 2
}

impl From<usize> for Action{
    fn from(value: usize) -> Self {
        match value {
            0 => Action::Increase,
            1 => Action::Decrease,
            2 => Action::Stay,
            _ => unreachable!("Unknown value")
        }
    }
}

impl From<Action> for usize{
    fn from(value: Action) -> Self {
        match value {
            Action::Increase => 0,
            Action::Decrease => 1,
            Action::Stay => 2,
        }
    }
}

#[derive(Debug)]
pub struct EXP3State{
    instances: HashMap<CID, EXP3>,
    last_taken_action: Instant,
    ordered_channels: Vec<CID>,
    previous_channel: Option<CID>,
    conf: EXP3Conf
}

impl EXP3State {
    pub(crate) fn new(config: &EXP3Conf) -> Self{
        let mut state = Self::default();
        state.conf = config.clone();
        state
    }

    fn update_instances(&mut self, announce_data: &Vec<McAnnounceData>){
        for announce in announce_data{
            if !self.instances.contains_key(&announce.channel_id){
                let instance = EXP3::new(3, self.conf.gamma);
                self.instances.insert(announce.channel_id.clone(), instance);
            }
        }

        let mut ordered_channels: Vec<(CID, u64)> = announce_data
            .iter()
            .map(|announce| (announce.channel_id.clone(), announce.bitrate.unwrap_or(0)))
            .collect();
        ordered_channels.sort_by(|a, b| a.1.cmp(&b.1));
        self.ordered_channels = ordered_channels.into_iter().map(|(cid, _)| cid).collect();
    }

    fn wait_timeout_elapsed(&self, now: Instant) -> bool{
        now > self.last_taken_action + self.conf.migration_timeout
    }

    fn get_banned(&self, stats: &CongestionStats) -> Vec<usize>{
        let banned = if stats.loss_rate < 0.01{
            vec![Action::Decrease]
        }else if stats.loss_rate > self.conf.hard_loss_threshold{
            vec![Action::Stay, Action::Increase]
        }else{
            vec![]
        };
        banned.into_iter().map(|action| action.into()).collect()
    }
}

impl Display for EXP3State{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (cid, instance) in &self.instances{
            for val in cid{
                write!(f, "{:02x}", val)?;
            }
            write!(f, ": ")?;
            write!(f, "{}\n", instance)?;
        }
        Ok(())
    }
}

impl Default for EXP3State{
    fn default() -> Self {
        Self {
            instances: HashMap::new(),
            last_taken_action: Instant::now(),
            ordered_channels: vec![],
            previous_channel: None,
            conf: EXP3Conf::default()
        }
    }
}

pub mod exp3;
pub mod rewarder;
