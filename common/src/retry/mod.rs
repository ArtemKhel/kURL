// pub trait RetryableAction {
//     type Output;
//     type Error;
//     type Future: Future<Output = Result<Self::Output, Self::Error>>;
//     fn retry(&mut self) -> Self::Future;
// }
//
// impl<F, Fut, T, E> RetryableAction for F
// where
//     F: FnMut() -> Fut,
//     Fut: Future<Output = Result<T, E>>,
// {
//     type Output = T;
//     type Error = E;
//     type Future = Fut;
//
//     fn retry(&mut self) -> Self::Future { self() }
// }
//
// struct Retry<A>{
//     action: A
// }

pub mod backoff_strategy;
mod retry_config;

use std::{cmp::min, time::Duration};

use backoff_strategy::BackoffStrategy;
use retry_config::RetryConfig;

use crate::retry::backoff_strategy::no_backoff::NoBackoff;

pub struct Retry<F, BS> {
    f: F,
    config: RetryConfig<BS>,
}

impl<F, BS> Retry<F, BS> {
    pub fn max_retries(mut self, max_attempts: usize) -> Self {
        self.config.max_retries = max_attempts;
        self
    }

    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.config.max_delay = Some(max_delay);
        self
    }

    pub fn with_strategy<NewBS: BackoffStrategy>(self, strategy: NewBS) -> Retry<F, NewBS> {
        let config = self.config.with_strategy(strategy);
        Retry { f: self.f, config }
    }

    pub fn with_config(self, config: RetryConfig<BS>) -> Retry<F, BS> { Retry { f: self.f, config } }
}

fn retry<F, Fut, T, E>(f: F) -> Retry<F, NoBackoff>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>, {
    Retry {
        f,
        config: RetryConfig::default(),
    }
}

impl<F, Fut, T, E, BS> IntoFuture for Retry<F, BS>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    BS: BackoffStrategy,
{
    type Output = Result<T, E>;

    type IntoFuture = impl Future<Output = Result<T, E>>;

    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            let mut f = self.f;
            for attempt in 0..=self.config.max_retries {
                match f().await {
                    Ok(result) => return Ok(result),
                    Err(_err) if attempt < self.config.max_retries => {
                        let backoff = self.config.backoff_strategy.next_backoff(attempt + 1);

                        if let Some(max_delay) = self.config.max_delay {
                            tokio::time::sleep(min(backoff, max_delay)).await;
                        } else {
                            tokio::time::sleep(backoff).await;
                        }
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::retry::retry;

    #[tokio::test]
    async fn success() {
        let counter = AtomicUsize::new(0);

        let res = retry(|| async {
            let attempt = counter.fetch_add(1, Ordering::Relaxed);
            Ok::<usize, Infallible>(attempt)
        })
        .max_retries(10)
        .await
        .unwrap();

        assert_eq!(res, 0);
    }

    #[tokio::test]
    async fn stops_on_success() {
        let counter = AtomicUsize::new(0);
        const MAX_RETRIES: usize = 10;
        const SUCCEED_AFTER: usize = 5;

        let res = retry(|| async {
            match counter.fetch_add(1, Ordering::Relaxed) {
                0..SUCCEED_AFTER => Err("infallible"),
                x => Ok(x),
            }
        })
        .max_retries(MAX_RETRIES)
        .await
        .unwrap();
        assert_eq!(res, SUCCEED_AFTER);
    }

    #[tokio::test]
    async fn max_retriers() {
        let counter = AtomicUsize::new(0);
        const RETRIES: usize = 10;

        let res = retry(|| async {
            let attempt = counter.fetch_add(1, Ordering::Relaxed);
            Err::<Infallible, _>(attempt)
        })
        .max_retries(RETRIES)
        .await
        .unwrap_err();

        assert_eq!(res, RETRIES);
    }

    #[tokio::test]
    async fn zero_retries() {
        let counter = AtomicUsize::new(0);
        const RETRIES: usize = 0;

        let res = retry(|| async {
            let attempt = counter.fetch_add(1, Ordering::Relaxed);
            Err::<Infallible, _>(attempt)
        })
        .max_retries(RETRIES)
        .await
        .unwrap_err();

        assert_eq!(res, RETRIES);
    }
}
