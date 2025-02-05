use std::time::{Duration, Instant};

use super::window::MaxTimeWindow;

static WINDOW_SPAN: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct CongestionStats{
    pub recv_count: usize,
    pub lost_count: usize,
    pub loss_rate: f64,
    pub rtt: Duration,
    pub ecn_rate: f64,
    pub throughput: u64,
    pub throughput_window: MaxTimeWindow<u64>
}

impl Default for CongestionStats{
    fn default() -> Self {
        Self {
            recv_count: 0,
            lost_count: 0,
            loss_rate: 0.0,
            ecn_rate: 0.0,
            rtt: Duration::from_millis(333),
            throughput: 0,
            throughput_window: MaxTimeWindow::new(WINDOW_SPAN)
        }
    }
}

impl CongestionStats{
    const ALPHA: f64 = 0.05;

    pub fn on_loss(&mut self){
        self.loss_rate = (1.0 - CongestionStats::ALPHA) * self.loss_rate + CongestionStats::ALPHA * 1.0;
        self.lost_count += 1;
    }

    pub fn on_marked(&mut self){
        self.ecn_rate = (1.0 - CongestionStats::ALPHA) * self.ecn_rate + CongestionStats::ALPHA * 1.0;
    }

    pub fn on_not_marked(&mut self){
        self.ecn_rate = (1.0 - CongestionStats::ALPHA) * self.ecn_rate + CongestionStats::ALPHA * 0.0;
    }

    pub fn on_rtt_measured(&mut self, rtt: Duration){
        self.rtt = rtt;
    }

    pub fn on_received(&mut self){
        // no loss
        self.loss_rate = (1.0 - CongestionStats::ALPHA) * self.loss_rate + CongestionStats::ALPHA * 0.0;
        self.recv_count += 1;
    }

    pub fn update_window(&mut self, now: Instant, bitrate: u64){
        let throughput = bitrate as f64 * (1.0 - self.loss_rate);
        self.throughput = throughput as u64;
        self.throughput_window.add_data(now, self.throughput);
    }

    pub fn max_throughput(&self) -> u64{
        *self.throughput_window.max().expect("Should have a value")
    }
}
