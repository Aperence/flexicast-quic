use std::{str::FromStr, time::Instant};

pub struct TokenPacer{
    tokens: usize,
    rate: usize,
    buffer: usize,
    last_action_instant: Instant
}

impl TokenPacer{
    pub fn new(rate: usize, buffer: usize, now: Instant) -> Self{
        let rate = rate / 8; // convert to byterate
        println!("Creating pacer with params rate={} buffer={}", rate, buffer);
        Self {  
            tokens: 0,
            rate,
            buffer,
            last_action_instant: now
        }
    }

    fn update_tokens(&mut self, now: Instant){
        let elapsed = now.duration_since(self.last_action_instant).as_micros() as f64;
        self.last_action_instant = now;
        let new_tokens = (elapsed * self.rate as f64) / 1.0e6;
        self.tokens += new_tokens as usize;
        self.tokens = self.tokens.min(self.buffer);
    }

    pub fn send(&mut self, size: usize, now: Instant) -> bool{
        self.update_tokens(now);
        if self.tokens < size{
            return false;
        }
        self.tokens -= size;
        true
    }
}

#[derive(Debug, Clone)]
pub enum PacerType {
    // Use 1.5x the rate for RTP
    Higher,
    // Use the minimal rate needed for the RTP flow
    Optimal 
}

impl PacerType{
    pub fn get_pacer(&self, rate: usize, buffer: usize, now: Instant) -> TokenPacer{
        match self{
            PacerType::Higher => {
                let mut new_rate = rate as f64;
                new_rate *= 1.5;
                TokenPacer::new(new_rate as usize, buffer, now)
            },
            PacerType::Optimal => {
                let i_ratio = 1.0 / 30.0;
                let i_size_ratio = 5.0;
                let mut new_rate = rate as f64;
                new_rate = new_rate + new_rate * i_ratio * (i_size_ratio - 1.0);
                TokenPacer::new(new_rate as usize, buffer, now)
            },
        }
    }
}

impl FromStr for PacerType{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "optimal" => Ok(PacerType::Optimal),
            "higher" => Ok(PacerType::Higher),
            _ => Err("Invalid type".to_string())
        }
    }
}