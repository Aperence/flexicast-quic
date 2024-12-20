use rand::prelude::*;


trait Action: PartialEq + Eq + Sized + Clone{
    fn get_actions() -> Vec<Self>;
}

#[derive(Debug)]
enum EXP3Error{
    AlreadyTaken,
    NotTaken,
    InvalidReward
}

struct EXP3<T: Action>{
    k: usize,
    actions: Vec<T>,
    weights: Vec<f64>,
    probas: Vec<f64>,
    gamma: f64,
    taken_action: Option<T>,
    rng: ThreadRng
}

impl<T: Action> EXP3<T>{
    pub fn new(actions: Vec<T>, gamma: Option<f64>) -> EXP3<T>{
        let k = actions.len();
        let mut weights = Vec::new();
        weights.resize(k, 1.0);
        let gamma = gamma.unwrap_or(0.1);
        let mut exp3 = EXP3{
            k,
            actions,
            weights,
            probas: vec![],
            gamma,
            taken_action: None,
            rng: thread_rng()
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

    pub fn take_action(&mut self, banned: Vec<T>) -> Result<T, EXP3Error>{
        if self.taken_action.is_some(){
            return Err(EXP3Error::AlreadyTaken);
        }
        self.compute_probas();

        let updated_probas =
            self.actions.iter().zip(&self.probas)
                .map(|(action, proba)|{
                    if banned.contains(action){
                        0.0
                    }else{
                        *proba
                    }
                }).collect();

        let norm = Self::normalize(&updated_probas);

        let weighted_actions: Vec<(&T, f64)> = self.actions.iter().zip(norm).collect();
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
        if self.taken_action.is_none(){
            return Err(EXP3Error::NotTaken)
        }

        let gamma = self.gamma;

        self.weights =
            self.actions.iter().zip(&self.weights).zip(&self.probas).map(|((action, weight), proba)|{
                let reward = if self.taken_action.as_ref().unwrap() == action{
                    value / proba
                }else{
                    0.0
                };
                let e = std::f64::consts::E;
                let new_weight = weight * e.powf(gamma * reward / self.k as f64);
                new_weight.clamp(1e-3, 1e3)
            }).collect();

        self.weights = Self::normalize(&self.weights);

        self.taken_action = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(Eq, PartialEq, Clone, Hash, Debug)]
    enum ImplAction{
        A,
        B
    }

    impl Action for ImplAction{
        fn get_actions() -> Vec<Self> {
            vec![Self::A, Self::B]
        }
    }

    #[test]
    fn basic() {
        let mut exp3 = EXP3::new(ImplAction::get_actions(), None);

        let action = exp3.take_action(vec![]);
        assert!(matches!(action, Ok(ImplAction::A | ImplAction::B)));

        let action = exp3.take_action(vec![]);
        assert!(matches!(action, Err(EXP3Error::AlreadyTaken)));
    }

    #[test]
    fn advanced() {
        let mut exp3 = EXP3::new(ImplAction::get_actions(), None);

        let mut counts = HashMap::new();
        for _ in 0..100{
            let action = exp3.take_action(vec![]).unwrap();

            *counts.entry(action.clone()).or_insert(0) += 1;

            let reward = match action {
                ImplAction::A => 1.0,
                ImplAction::B => 0.0,
            };

            exp3.reward(reward).unwrap();
        }

        assert!(*counts.get(&ImplAction::A).unwrap() > 65);
    }
}
