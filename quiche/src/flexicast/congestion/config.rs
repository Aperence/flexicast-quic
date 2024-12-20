use std::time::Duration;

use crate::Config;
use super::FcCongestionHeuristic;

pub trait FcCongestionConfig{
    /// Sets the delay between two successive sends of the MC_CONGESTION_INFO
    /// frame
    fn set_fc_congestion_info_delay(&mut self, v: Duration);

    /// Sets the heuristic used for multicast group migration
    fn set_fc_congestion_heuristic(&mut self, v: FcCongestionHeuristic);
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
}
