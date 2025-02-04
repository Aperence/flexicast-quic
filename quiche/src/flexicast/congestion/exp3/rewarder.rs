use crate::flexicast::congestion::stats::CongestionStats;

#[derive(Debug)]
pub struct EXP3Rewarder{
    pub(crate) reward: fn(&CongestionStats) -> f64
}

pub static K: f64 = 25.0;
pub static TAU: f64 = 0.15;

#[inline]
fn sigmoid(x: f64, k: f64) -> f64{
    let e = std::f64::consts::E;
    1.0 / (1.0 + e.powf(-k * x))
}

pub static LOSS_REWARDER: EXP3Rewarder = EXP3Rewarder{
    reward: |congestion_state| {
        (1.0 - sigmoid(congestion_state.loss_rate - TAU, K)) / sigmoid(TAU, K)
    }
};
