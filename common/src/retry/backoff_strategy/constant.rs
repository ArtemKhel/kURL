use std::time::Duration;

use crate::retry::backoff_strategy::BackoffStrategy;

pub struct ConstantBackoff {
    backoff: Duration,
}

impl ConstantBackoff {
    pub fn new(backoff: Duration) -> ConstantBackoff { Self { backoff } }

    pub fn from_millis(millis: u64) -> ConstantBackoff {
        Self {
            backoff: Duration::from_millis(millis),
        }
    }
}

impl BackoffStrategy for ConstantBackoff {
    fn next_backoff(&mut self, _attempt: usize) -> Duration { self.backoff }
}
