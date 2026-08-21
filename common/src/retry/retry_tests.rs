use std::{
    convert::Infallible,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use tokio::time::Instant;

use super::{
    backoff_strategy::{BackoffStrategy, constant::ConstantBackoff, linear::LinearBackoff, no_backoff::NoBackoff},
    retry,
};
use crate::retry::backoff_strategy::exponential::ExponentialBackoff;

async fn run_failing<BS: BackoffStrategy>(strategy: BS, max_retries: usize, max_delay: Option<Duration>) -> Duration {
    let attempts = AtomicUsize::new(0);
    let started_at = Instant::now();

    let operation = retry(|| async {
        let attempt = attempts.fetch_add(1, Ordering::Relaxed);
        Err::<Infallible, _>(attempt)
    })
    .with_strategy(strategy)
    .max_retries(max_retries);

    let last_attempt = match max_delay {
        Some(max_delay) => operation.max_delay(max_delay).await.unwrap_err(),
        None => operation.await.unwrap_err(),
    };

    assert_eq!(last_attempt, max_retries);
    assert_eq!(attempts.load(Ordering::Relaxed), max_retries + 1);

    started_at.elapsed()
}

#[tokio::test]
async fn success() {
    let counter = AtomicUsize::new(0);

    let result = retry(|| async {
        let attempt = counter.fetch_add(1, Ordering::Relaxed);
        Ok::<usize, Infallible>(attempt)
    })
    .max_retries(10)
    .await
    .unwrap();

    assert_eq!(result, 0);
}

#[tokio::test]
async fn stops_on_success() {
    let counter = AtomicUsize::new(0);
    const MAX_RETRIES: usize = 10;
    const SUCCEED_AFTER: usize = 5;

    let result = retry(|| async {
        match counter.fetch_add(1, Ordering::Relaxed) {
            0..SUCCEED_AFTER => Err("infallible"),
            attempt => Ok(attempt),
        }
    })
    .max_retries(MAX_RETRIES)
    .await
    .unwrap();

    assert_eq!(result, SUCCEED_AFTER);
}

#[tokio::test]
async fn max_retries_bounds_attempts() {
    let counter = AtomicUsize::new(0);
    const RETRIES: usize = 10;

    let result = retry(|| async {
        let attempt = counter.fetch_add(1, Ordering::Relaxed);
        Err::<Infallible, _>(attempt)
    })
    .max_retries(RETRIES)
    .await
    .unwrap_err();

    assert_eq!(result, RETRIES);
    assert_eq!(counter.load(Ordering::Relaxed), RETRIES + 1);
}

#[tokio::test]
async fn zero_retries_runs_once() {
    let counter = AtomicUsize::new(0);

    let result = retry(|| async {
        let attempt = counter.fetch_add(1, Ordering::Relaxed);
        Err::<Infallible, _>(attempt)
    })
    .max_retries(0)
    .await
    .unwrap_err();

    assert_eq!(result, 0);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn no_backoff() {
    let elapsed = run_failing(NoBackoff {}, 3, None).await;

    assert_eq!(elapsed, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn constant_backoff() {
    let elapsed = run_failing(ConstantBackoff::from_millis(100), 3, None).await;

    assert_eq!(elapsed, Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn linear_backoff() {
    let elapsed = run_failing(LinearBackoff::from_millis(100), 3, None).await;

    assert_eq!(elapsed, Duration::from_millis(600));
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff() {
    let elapsed = run_failing(ExponentialBackoff::from_millis(100), 3, None).await;
    assert_eq!(elapsed, Duration::from_millis(700));
}

#[tokio::test(start_paused = true)]
async fn max_delay_caps_each_backoff() {
    let elapsed = run_failing(
        LinearBackoff::new(Duration::from_millis(100)),
        3,
        Some(Duration::from_millis(150)),
    )
    .await;

    assert_eq!(elapsed, Duration::from_millis(400));
}
