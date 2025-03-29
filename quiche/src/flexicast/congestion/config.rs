//! Configuration extension for multicast congestion control
use std::time::Duration;

use crate::Config;
use super::{exp3::{self, rewarder::{EXP3Rewarder, LOSS_REWARDER}}, kmeans, FcCongestionHeuristicOps};

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

/// Configuration of the multicast congestion control
pub trait FcCongestionConfig{
    /// Sets the delay between two successive sends of the MC_CONGESTION_INFO
    /// frame
    fn set_fc_congestion_info_delay(&mut self, v: Duration);

    /// Sets the heuristic used for multicast group migration
    fn set_fc_congestion_heuristic(&mut self, v: FcCongestionHeuristic);

    /// Sets the throughput time window
    fn set_fc_throughput_window(&mut self, v: Duration);

    /// Sets the reward function used for the EXP3 heuristic
    fn set_fc_exp3_rewarder(&mut self, v: &'static EXP3Rewarder);

    /// Sets the loss threshold used for the EXP3 heuristic
    /// Automatically sets the hard loss threshold to 2x this value
    fn set_fc_exp3_loss_threshold(&mut self, v: f64);

    /// Sets the hard loss threshold used for the EXP3 banning
    /// Warning: Always call this function **after** having called
    /// `set_fc_exp3_loss_threshold`, as this function overwrite
    /// the hard loss threshold
    fn set_fc_exp3_hard_loss_threshold(&mut self, v: f64);

    /// Sets the K parameter used for the EXP3 heuristic
    fn set_fc_exp3_k(&mut self, v: f64);

    /// Sets the migration timeout used for the EXP3 heuristic
    fn set_fc_exp3_migration_timeout(&mut self, v: Duration);

    /// Sets the gamma parameter used by the EXP3 instances
    fn set_fc_exp3_gamma(&mut self, v: Option<f64>);
}

impl FcCongestionConfig for Config{

    fn set_fc_congestion_info_delay(&mut self, v: Duration) {
        self.fc_congestion_info_delay = v;
    }

    fn set_fc_congestion_heuristic(&mut self, v: FcCongestionHeuristic) {
        self.fc_congestion_heuristic = v;
    }

    fn set_fc_exp3_rewarder(&mut self, v: &'static EXP3Rewarder){
        self.fc_exp3_conf.rewarder = v;
    }

    fn set_fc_exp3_loss_threshold(&mut self, v: f64) {
        self.fc_exp3_conf.loss_threshold = v;
        self.fc_exp3_conf.hard_loss_threshold = 2.0 * v;
    }

    fn set_fc_exp3_hard_loss_threshold(&mut self, v: f64){
        self.fc_exp3_conf.hard_loss_threshold = v;
    }

    fn set_fc_throughput_window(&mut self, v: Duration) {
        self.fc_throughput_window = v;
    }

    fn set_fc_exp3_migration_timeout(&mut self, v: Duration) {
        self.fc_exp3_conf.migration_timeout = v;
    }

    fn set_fc_exp3_k(&mut self, v: f64){
        self.fc_exp3_conf.k = v;
    }

    fn set_fc_exp3_gamma(&mut self, v: Option<f64>){
        self.fc_exp3_conf.gamma = v;
    }
}

/// Config of the congestion control for Flexicast
pub struct FcCongestionConf {
    /// Configuration of the congestion info delay, i.e. what is the delay between
    /// the sending of two MC_CONGESTION_INFO frames
    pub congestion_info_delay: Duration,
    /// What is the heuristic used for the channel change
    pub cc_heuristic: &'static FcCongestionHeuristicOps,
    /// Window used for the throughput statistics
    pub throughput_window: Duration,
    /// EXP3 configuration
    pub exp3_conf: EXP3Conf
}

impl FcCongestionConf{

    pub(crate) fn from_config(config: &Config) -> Self{
        FcCongestionConf{
            congestion_info_delay: config.fc_congestion_info_delay,
            cc_heuristic: config.fc_congestion_heuristic.into(),
            throughput_window: config.fc_throughput_window,
            exp3_conf: config.fc_exp3_conf.clone()
        }
    }
}

/// Configuration of the EXP3 heuristic
#[derive(Debug, Clone)]
pub struct EXP3Conf{
    /// Migration timeout
    pub migration_timeout: Duration,
    /// Loss threshold used in objective function
    pub loss_threshold: f64,
    /// Loss threshold used in banning of actions
    pub hard_loss_threshold: f64,
    /// K parameter
    pub k: f64,
    /// Gamma parameter used in EXP3
    pub gamma: Option<f64>,
    /// Reward function used with the EXP3 heuristic
    pub rewarder: &'static EXP3Rewarder
}

impl Default for EXP3Conf{
    fn default() -> Self {
        EXP3Conf{
            loss_threshold: 0.05,
            hard_loss_threshold: 2.0 * 0.05,
            k: 25.0,
            gamma: Some(0.15),
            migration_timeout: Duration::from_secs(1),
            rewarder: &LOSS_REWARDER
        }
    }
}
