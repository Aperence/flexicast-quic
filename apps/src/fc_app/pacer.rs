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
        let elapsed = now.duration_since(self.last_action_instant).as_secs_f64();
        self.last_action_instant = now;
        let new_tokens = elapsed * self.rate as f64;
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

#[cfg(test)]
mod tests{
    use std::time::Instant;
    use std::time::Duration;

    use super::TokenPacer;

    #[test]
    fn test_pacer(){
        let start = Instant::now();
        let rate = 800; // 800 bps
        let mut pacer = TokenPacer::new(rate, 50, start);

        assert_eq!(pacer.tokens, 0);

        // 0.1s passes, we should have increase tokens by 80 bits = 10 bytes
        pacer.update_tokens(start + Duration::from_millis(100));
        assert_eq!(pacer.tokens, 10);

        pacer.update_tokens(start + Duration::from_millis(200));
        assert_eq!(pacer.tokens, 20);

        pacer.update_tokens(start + Duration::from_millis(500));
        assert_eq!(pacer.tokens, 50);

        pacer.update_tokens(start + Duration::from_millis(600));
        assert_eq!(pacer.tokens, 50);

        let res = pacer.send(30, start + Duration::from_millis(600));
        assert!(res);
        assert_eq!(pacer.tokens, 20);

        let res = pacer.send(30, start + Duration::from_millis(600));
        assert!(!res);
        assert_eq!(pacer.tokens, 20);

        // 100ms have passed
        let res = pacer.send(30, start + Duration::from_millis(700));
        assert!(res);
        assert_eq!(pacer.tokens, 0);
    }
}
