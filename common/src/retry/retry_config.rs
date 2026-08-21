use std::time::Duration;

use crate::retry::backoff_strategy::{BackoffStrategy, no_backoff::NoBackoff};

pub struct RetryConfig<BS> {
    pub(crate) backoff_strategy: BS,
    pub(crate) max_delay: Option<Duration>,
    pub(crate) max_retries: usize,
    // pub(crate) jitter: Option<()>,
}

impl<BS> RetryConfig<BS> {
    pub fn with_strategy<NewBS: BackoffStrategy>(self, backoff_strategy: NewBS) -> RetryConfig<NewBS> {
        RetryConfig {
            backoff_strategy,
            max_delay: self.max_delay,
            max_retries: self.max_retries,
        }
    }
}

impl Default for RetryConfig<NoBackoff> {
    fn default() -> Self {
        Self {
            backoff_strategy: NoBackoff {},
            max_delay: None,
            max_retries: 1,
        }
    }
}
