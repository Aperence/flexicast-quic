//! Implementation of an EXP3 instance
use std::fmt::Display;

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};

#[derive(Debug)]
pub(crate) enum EXP3Error{
    AlreadyTaken,
    NotTaken,
    InvalidReward
}

#[derive(Debug)]
pub(crate) struct EXP3{
    k: usize,
    weights: Vec<f64>,
    probas: Vec<f64>,
    gamma: f64,
    taken_action: Option<usize>,
    rng: SmallRng
}

impl EXP3{
    pub fn new(k: usize, gamma: Option<f64>) -> EXP3{
        let k = k;
        let mut weights = Vec::new();
        weights.resize(k, 1.0);
        let gamma = gamma.unwrap_or(0.1);
        let mut exp3 = EXP3{
            k,
            weights,
            probas: vec![],
            gamma,
            taken_action: None,
            rng: SmallRng::from_entropy()
        };
        exp3.compute_probas();
        exp3
    }

    fn compute_probas(&mut self){
        let sum: f64 = self.weights.iter().sum();
        let probas: Vec<f64> = self.weights.iter().map(|w|
            (1.0 - self.gamma) * (w / sum) + self.gamma * (1.0 / self.k as f64)
        ).collect();
        self.probas = probas;
    }

    pub fn take_action(&mut self, banned: Vec<usize>) -> Result<usize, EXP3Error>{
        if self.taken_action.is_some(){
            return Err(EXP3Error::AlreadyTaken);
        }
        self.compute_probas();

        let updated_probas = self.probas
                .iter()
                .enumerate()
                .map(|(action, proba)|{
                    if banned.contains(&action){
                        0.0
                    }else{
                        *proba
                    }
                }).collect();

        let norm = Self::normalize(&updated_probas);

        let weighted_actions: Vec<(usize, &f64)> = norm.iter().enumerate().collect();
        let action =
            weighted_actions
                .choose_weighted(&mut self.rng, |(_action, prob)| *prob).unwrap().0.clone();

        self.taken_action = Some(action.clone());

        Ok(action)
    }

    fn normalize(array: &Vec<f64>) -> Vec<f64>{
        let sum: f64 = array.iter().sum();
        array.iter().map(|x| x / sum).collect()
    }

    pub fn reward(&mut self, value: f64) -> Result<(), EXP3Error>{
        if value > 1.0 || value < 0.0{
            return Err(EXP3Error::InvalidReward);
        }
        if let Some(taken_action) = self.taken_action{
            let gamma = self.gamma;

            for i in 0..self.k{
                let weight = self.weights[i];
                let proba = self.probas[i];
                let reward = if taken_action == i{
                    value / proba
                }else{
                    0.0
                };
                let e = std::f64::consts::E;
                let new_weight = weight * e.powf(gamma * reward / self.k as f64);
                let new_weight = new_weight.clamp(1e-3, 1e3);
                self.weights[i] = new_weight;
            }

            self.weights = Self::normalize(&self.weights);

            self.taken_action = None;
            Ok(())
        }else{
            Err(EXP3Error::NotTaken)
        }
    }
}

impl Display for EXP3{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let probas: Vec<String> = self.probas.iter().map(|proba| format!("{:.2}", proba)).collect();
        write!(f, "{}", probas.join(","))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn basic() {
        let mut exp3 = EXP3::new(2, None);

        let action = exp3.take_action(vec![]);
        assert!(matches!(action, Ok(0 | 1)));

        let action = exp3.take_action(vec![]);
        assert!(matches!(action, Err(EXP3Error::AlreadyTaken)));
    }

    #[test]
    fn advanced() {
        let mut exp3 = EXP3::new(2, None);

        let mut counts = HashMap::new();
        for _ in 0..100{
            let action = exp3.take_action(vec![]).unwrap();

            *counts.entry(action.clone()).or_insert(0) += 1;

            let reward = action as f64;

            exp3.reward(reward).unwrap();
        }

        assert!(*counts.get(&1).unwrap() > 65);
    }
}
