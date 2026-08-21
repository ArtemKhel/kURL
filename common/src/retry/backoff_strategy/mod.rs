pub mod constant;
pub mod exponential;
pub mod jitter;
pub mod linear;
pub mod no_backoff;

use std::time::Duration;

use crate::retry::backoff_strategy::jitter::{JitterTy, Jittered};

pub trait BackoffStrategy: Sized {
    /// Calculates the backoff before the next attempt
    /// `attempt` - number of attempts that have been made so far (starting from 1)
    fn next_backoff(&mut self, attempt: usize) -> Duration;

    fn jittered(self, ty: JitterTy) -> Jittered<Self> { Jittered { inner: self, ty } }
}
