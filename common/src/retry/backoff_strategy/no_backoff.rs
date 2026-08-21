use std::time::Duration;

use crate::retry::backoff_strategy::BackoffStrategy;

pub struct NoBackoff {}

impl BackoffStrategy for NoBackoff {
    fn next_backoff(&mut self, _attempt: usize) -> Duration { Duration::ZERO }
}
