use std::time::Duration;

use crate::retry::backoff_strategy::BackoffStrategy;

pub struct ExponentialBackoff {
    backoff: Duration,
}

impl ExponentialBackoff {
    pub fn new(initial_backoff: Duration) -> Self {
        Self {
            backoff: initial_backoff,
        }
    }

    pub fn from_millis(millis: u64) -> Self {
        Self {
            backoff: Duration::from_millis(millis),
        }
    }
}
impl BackoffStrategy for ExponentialBackoff {
    fn next_backoff(&mut self, _attempt: usize) -> Duration {
        let prev = self.backoff;
        self.backoff = self.backoff.saturating_mul(2);
        prev
    }
}
