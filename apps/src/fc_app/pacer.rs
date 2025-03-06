use std::time::Instant;

pub struct TokenPacer{
    tokens: usize,
    rate: usize,
    buffer: usize,
    last_action_instant: Instant
}

impl TokenPacer{
    pub fn new(rate: usize, buffer: usize, now: Instant) -> Self{
        Self {  
            tokens: 0,
            rate,
            buffer,
            last_action_instant: now
        }
    }

    pub fn new_rtp(bitrate: usize, I_ratio: f64, I_size_ration: f64, buffer: usize, now: Instant) -> Self{
        let bitrate = bitrate as f64;
        let rate = bitrate + bitrate * I_ratio * (I_size_ration - 1.0);
        Self::new(rate as usize, buffer, now)
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

