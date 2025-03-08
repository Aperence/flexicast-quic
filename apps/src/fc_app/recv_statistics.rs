use std::{collections::HashMap, error::Error, fmt::Display, fs, io};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Migration{
    pub new_channel_idx: usize,
    pub new_channel_bitrate: u64,
    pub last_recv_timestamp: i64
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct TimestampStats{
    recv: usize,
    losses: Vec<f64>,
    instant_losses: Vec<f64>
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MultiChannelRecvStats{
    stats: Vec<(HashMap<i64, TimestampStats>, Migration)>
}

impl MultiChannelRecvStats{
    pub fn new() -> Self{
        Self{
            stats: vec![]
        }
    }

    pub fn record_loss(&mut self, timestamp: u32, loss_rate: f64){
        let last = self.stats.len()-1;
        let (timestamps, _) = &mut self.stats[last];
        let stats = timestamps.entry(timestamp as i64).or_insert(TimestampStats::default());
        stats.losses.push(loss_rate);
    }

    pub fn record_instant_loss(&mut self, timestamp: u32, smoothed_loss_rate: f64){
        let last = self.stats.len()-1;
        let (timestamps, _) = &mut self.stats[last];
        let stats = timestamps.entry(timestamp as i64).or_insert(TimestampStats::default());
        stats.instant_losses.push(smoothed_loss_rate);
    }

    pub fn migrated(&mut self, migration: Migration){
        self.stats.push((HashMap::new(), migration));
    }

    pub fn record_recv(&mut self, timestamp: u32){
        let last = self.stats.len()-1;
        let (timestamps, _) = &mut self.stats[last];
        let stats = timestamps.entry(timestamp as i64).or_insert(TimestampStats::default());
        stats.recv += 1;
    }

    pub fn write(self, path: &str) -> Result<(), StatErr>{
        let json = serde_json::to_string(&self)?;
        fs::write(path, json)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum StatErr{
    JSONError(serde_json::Error),
    IOError(io::Error)
}

impl Display for StatErr{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self{
            StatErr::JSONError(error) => write!(f, "JSONError: {}", error),
            StatErr::IOError(error) => write!(f, "IOError: {}", error),
        }
    }
}

impl Error for StatErr{}

impl From<serde_json::Error> for StatErr{
    fn from(value: serde_json::Error) -> Self {
        Self::JSONError(value)
    }
}

impl From<io::Error> for StatErr{
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}
