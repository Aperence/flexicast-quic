use std::{collections::HashMap, error::Error, fmt::Display, fs, io};
use serde::{Deserialize, Serialize};

type Timestamp = i64;
type LossStats = HashMap<i64, Vec<f64>>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Migration{
    pub new_channel_idx: usize,
    pub new_channel_bitrate: u64,
    pub last_recv_timestamp: Timestamp
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MultiChannelRecvStats{
    stats: Vec<(LossStats, LossStats, Migration)>
}

impl MultiChannelRecvStats{
    pub fn new() -> Self{
        Self{
            stats: vec![]
        }
    }

    pub fn record_loss(&mut self, timestamp: u32, loss_rate: f64){
        let last = self.stats.len()-1;
        let (losses, _, _) = &mut self.stats[last];
        losses.entry(timestamp as i64).or_insert(vec![]).push(loss_rate);
    }

    pub fn record_smoothed_loss(&mut self, timestamp: u32, smoothed_loss_rate: f64){
        let last = self.stats.len()-1;
        let (_, smoothed_losses, _) = &mut self.stats[last];
        smoothed_losses.entry(timestamp as i64).or_insert(vec![]).push(smoothed_loss_rate);
    }

    pub fn migrated(&mut self, migration: Migration){
        self.stats.push((HashMap::new(), HashMap::new(), migration));
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
