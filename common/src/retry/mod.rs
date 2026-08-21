#![allow(dead_code)]

pub mod backoff_strategy;
mod retry_config;

use std::{cmp::min, time::Duration};

use backoff_strategy::BackoffStrategy;
use retry_config::RetryConfig;

use crate::retry::{
    backoff_strategy::no_backoff::NoBackoff,
    on_retry::{NoopOnRetry, OnRetry},
};

pub struct Retry<F, BS, RF> {
    f: F,
    config: RetryConfig<BS>,
    on_retry: Option<RF>,
}

impl<F, BS, RF> Retry<F, BS, RF> {
    pub fn max_retries(mut self, max_attempts: usize) -> Self {
        self.config.max_retries = max_attempts;
        self
    }

    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.config.max_delay = Some(max_delay);
        self
    }

    pub fn with_strategy<NewBS: BackoffStrategy>(self, strategy: NewBS) -> Retry<F, NewBS, RF> {
        let config = self.config.with_strategy(strategy);
        Retry {
            f: self.f,
            config,
            on_retry: self.on_retry,
        }
    }

    pub fn with_config(self, config: RetryConfig<BS>) -> Retry<F, BS, RF> {
        Retry {
            f: self.f,
            config,
            on_retry: self.on_retry,
        }
    }

    pub fn on_retry<NewRF>(self, on_retry: NewRF) -> Retry<F, BS, NewRF> {
        Retry {
            f: self.f,
            config: self.config,
            on_retry: Some(on_retry),
        }
    }
}

pub fn retry<F, Fut, T, E>(f: F) -> Retry<F, NoBackoff, NoopOnRetry>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>, {
    Retry {
        f,
        config: RetryConfig::default(),
        on_retry: None::<NoopOnRetry>,
    }
}

impl<F, Fut, T, E, BS, OR> IntoFuture for Retry<F, BS, OR>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    BS: BackoffStrategy,
    OR: OnRetry<E>,
{
    type Output = Result<T, E>;

    type IntoFuture = impl Future<Output = Result<T, E>>;

    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            let mut f = self.f;
            for attempt in 0..=self.config.max_retries {
                match f().await {
                    Ok(result) => return Ok(result),
                    Err(err) if attempt < self.config.max_retries => {
                        let strategy_backoff = self.config.backoff_strategy.next_backoff(attempt + 1);

                        let backoff = self
                            .config
                            .max_delay
                            .map_or(strategy_backoff, |max| min(max, strategy_backoff));

                        if let Some(on_retry) = &mut self.on_retry {
                            tokio::spawn(on_retry.on_retry(attempt, backoff, &err));
                        }

                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
            unreachable!()
        }
    }
}

pub mod on_retry;
#[cfg(test)]
#[path = "retry_tests.rs"]
mod tests;
