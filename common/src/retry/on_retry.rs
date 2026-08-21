use std::{future::ready, time::Duration};

pub trait OnRetry<E> {
    fn on_retry(
        &mut self,
        attempt: usize,
        next_backoff: Duration,
        error: &E,
    ) -> impl Future<Output = ()> + Send + 'static;
}

pub struct NoopOnRetry {}

impl<E> OnRetry<E> for NoopOnRetry {
    fn on_retry(
        &mut self,
        _attempt: usize,
        _next_backoff: Duration,
        _error: &E,
    ) -> impl Future<Output = ()> + Send + 'static {
        ready(())
    }
}

impl<F, Fut, E> OnRetry<E> for F
where
    F: FnMut(usize, Duration, &E) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn on_retry(
        &mut self,
        attempt: usize,
        next_backoff: Duration,
        error: &E,
    ) -> impl Future<Output = ()> + Send + 'static {
        self(attempt, next_backoff, error)
    }
}
