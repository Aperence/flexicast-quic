use crate::flexicast::congestion::{config::EXP3Conf, stats::CongestionStats};

#[derive(Debug, Clone)]
pub struct EXP3Rewarder{
    pub(crate) reward: fn(&CongestionStats, &EXP3Conf) -> f64
}

#[inline]
fn sigmoid(x: f64, k: f64) -> f64{
    let e = std::f64::consts::E;
    1.0 / (1.0 + e.powf(-k * x))
}

pub static LOSS_REWARDER: EXP3Rewarder = EXP3Rewarder{
    reward: |congestion_stats, conf| {
        let tau = conf.loss_threshold;
        let k = conf.k;
        let scaled = congestion_stats.throughput as f64 / congestion_stats.max_throughput() as f64;
        let regret = (1.0 - sigmoid(congestion_stats.loss_rate - tau, k)) / sigmoid(tau, k);
        let reward = scaled * regret;
        // sometimes, due to rounding error we obtain results such as 1.0000000000000002
        // thus, explicitely bound the result
        (reward).min(1.0).max(0.0)
    }
};
