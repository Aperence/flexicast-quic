use std::{collections::BTreeMap, time::{Duration, Instant}};

#[derive(Debug, Clone)]
pub struct MaxTimeWindow<T>
where
    T: Ord + Clone
{
    window: BTreeMap<Instant, T>,
    values: BTreeMap<T, u32>,
    span: Duration
}

impl<T> MaxTimeWindow<T>
where
    T : Ord + Clone
{
    pub fn new(span: Duration) -> MaxTimeWindow<T>{
        MaxTimeWindow{
            window: BTreeMap::new(),
            values: BTreeMap::new(),
            span
        }
    }

    pub fn time_elapsed(&mut self, now: Instant){
        let lowest = now - self.span;
        let to_remove: Vec<(Instant, T)> = self.window.range(..lowest)
            .into_iter()
            .map(|(instant, value)| (instant.clone(), value.clone()))
            .collect();
        for (instant, value) in to_remove{
            self.window.remove(&instant);
            if *self.values.get(&value).expect("Should have value") == 1{
                self.values.remove(&value);
            }else{
                self.values.entry(value).and_modify(|count| *count -= 1);
            }
        }
    }

    pub fn add_data(&mut self, now: Instant, data: T){
        self.time_elapsed(now);
        self.window.insert(now, data.clone());
        self.values.entry(data).and_modify(|count| *count += 1).or_insert(1);
    }

    pub fn max(&self) -> Option<&T>{
        self.values.last_key_value().map(|(val, _)| val)
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &T> + 'a{
        self.window.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum Action{
        Insert(u64, u32),
        TimePass(u64)
    }

    fn assert_iterators(actions: Vec<(Action, Vec<u32>)>){
        let mut window = MaxTimeWindow::new(Duration::from_millis(100));
        let start = Instant::now();
        for (action, expected) in actions{
            match action {
                Action::Insert(delay, value) => window.add_data(start + Duration::from_millis(delay), value),
                Action::TimePass(delay) => window.time_elapsed(start + Duration::from_millis(delay)),
            }
            assert!(window.iter().eq(expected.iter()))
        }
    }

    fn assert_max(actions: Vec<(Action, Option<&u32>)>){
        let mut window = MaxTimeWindow::new(Duration::from_millis(100));
        let start = Instant::now();
        for (action, expected) in actions{
            match action {
                Action::Insert(delay, value) => window.add_data(start + Duration::from_millis(delay), value),
                Action::TimePass(delay) => window.time_elapsed(start + Duration::from_millis(delay)),
            }
            let time = match action{
                Action::Insert(delay, _) => delay,
                Action::TimePass(delay) => delay,
            };
            assert_eq!(window.max(), expected, "Maximums doesn't match at time {time}");
        }
    }

    #[test]
    fn basic() {
        assert_iterators(vec![
            (Action::Insert(0, 1), vec![1]),
            (Action::Insert(50, 2), vec![1, 2]),
        ])
    }

    #[test]
    fn remove() {
        assert_iterators(vec![
            (Action::Insert(0, 1), vec![1]),
            (Action::Insert(50, 2), vec![1, 2]),
            (Action::TimePass(200), vec![]),
        ]);
    }

    #[test]
    fn edge() {
        assert_iterators(vec![
            (Action::Insert(0, 1), vec![1]),
            (Action::Insert(50, 2), vec![1, 2]),
            (Action::TimePass(100), vec![1, 2]),
            (Action::TimePass(150), vec![2]),
            (Action::TimePass(200), vec![])
        ])
    }

    #[test]
    fn max() {
        assert_max(vec![
            (Action::Insert(0, 1), Some(&1)),
            (Action::Insert(50, 2), Some(&2)),
            (Action::TimePass(100), Some(&2)),
            (Action::TimePass(150), Some(&2)),
            (Action::TimePass(200), None)
        ])
    }

    #[test]
    fn multiple_occurences() {
        assert_max(vec![
            (Action::Insert(0, 3), Some(&3)),
            (Action::Insert(50, 2), Some(&3)),
            (Action::Insert(51, 3), Some(&3)),
            (Action::Insert(70, 2), Some(&3)),
            (Action::TimePass(100), Some(&3)),
            (Action::TimePass(150), Some(&3)),
            (Action::TimePass(152), Some(&2)),
            (Action::Insert(153, 3), Some(&3)),
            (Action::TimePass(200), Some(&3)),
            (Action::TimePass(254), None)
        ])
    }
}
