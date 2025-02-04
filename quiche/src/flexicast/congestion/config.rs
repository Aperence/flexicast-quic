//! Configuration extension for multicast congestion control
use std::time::Duration;

use crate::Config;
use super::{exp3::{self, rewarder::EXP3Rewarder}, kmeans, FcCongestionHeuristicOps};

/// Heuristict used by Flexicast for automatic group migration
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum FcCongestionHeuristic {
    /// Use EXP3 to drive which group to join
    EXP3       = 0,
    /// Join the group with closest rate
    KMEANS     = 1
}

impl From<FcCongestionHeuristic> for &'static FcCongestionHeuristicOps {
    fn from(heuristic: FcCongestionHeuristic) -> Self {
        match heuristic {
            FcCongestionHeuristic::KMEANS => &kmeans::KMEANS,
            FcCongestionHeuristic::EXP3 => &exp3::EXP3_HEURISTIC,
        }
    }
}

/// Heuristict used by Flexicast for automatic group migration
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum FcEXP3Rewarder {
    /// Use a loss reward function
    LOSS       = 0,
}

impl From<FcEXP3Rewarder> for &'static EXP3Rewarder{
    fn from(rewarder: FcEXP3Rewarder) -> Self {
        match rewarder {
            FcEXP3Rewarder::LOSS => &exp3::rewarder::LOSS_REWARDER
        }
    }
}

/// Configuration of the multicast congestion control
pub trait FcCongestionConfig{
    /// Sets the delay between two successive sends of the MC_CONGESTION_INFO
    /// frame
    fn set_fc_congestion_info_delay(&mut self, v: Duration);

    /// Sets the heuristic used for multicast group migration
    fn set_fc_congestion_heuristic(&mut self, v: FcCongestionHeuristic);

    /// Sets the reward function used for the EXP3 heuristic
    fn set_fc_exp3_rewarder(&mut self, rewarder: FcEXP3Rewarder);
}

impl FcCongestionConfig for Config{
    /// Sets the delay between two successive sends of the MC_CONGESTION_INFO
    /// frame
    fn set_fc_congestion_info_delay(&mut self, v: Duration) {
        self.fc_congestion_info_delay = v;
    }
    /// Sets the heuristic used for multicast group migration
    fn set_fc_congestion_heuristic(&mut self, v: FcCongestionHeuristic) {
        self.fc_congestion_heuristic = v;
    }

    fn set_fc_exp3_rewarder(&mut self, rewarder: FcEXP3Rewarder){
        self.fc_exp3_rewarder = rewarder;
    }
}

/// Config of the congestion control for Flexicast
pub struct FcCongestionConf {
    /// Configuration of the congestion info delay, i.e. what is the delay between
    /// the sending of two MC_CONGESTION_INFO frames
    pub congestion_info_delay: Duration,
    /// What is the heuristic used for the channel change
    pub cc_heuristic: &'static FcCongestionHeuristicOps,
    /// Reward function used with the EXP3 heuristic
    pub exp3_rewarder: &'static EXP3Rewarder
}

impl FcCongestionConf{

    pub(crate) fn from_config(config: &Config) -> Self{
        FcCongestionConf{
            congestion_info_delay: config.fc_congestion_info_delay,
            cc_heuristic: config.fc_congestion_heuristic.into(),
            exp3_rewarder: config.fc_exp3_rewarder.into()
        }
    }
}
