use std::time::Duration;

use crate::retry::backoff_strategy::BackoffStrategy;

pub struct LinearBackoff {
    backoff: Duration,
}

impl LinearBackoff {
    pub fn new(initial_backoff: Duration) -> Self {
        Self {
            backoff: initial_backoff,
        }
    }
}
impl BackoffStrategy for LinearBackoff {
    fn next_backoff(&mut self, attempt: usize) -> Duration { self.backoff.saturating_mul(attempt as u32) }
}
