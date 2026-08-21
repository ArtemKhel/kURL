pub mod constant;
pub mod linear;
pub mod no_backoff;

use std::time::Duration;

pub trait BackoffStrategy {
    /// Calculates the backoff before the next attempt
    /// `attempt` - number of attempts that have been made so far (starting from 1)
    fn next_backoff(&mut self, attempt: usize) -> Duration;
}
