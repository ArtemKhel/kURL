use std::time::Duration;

pub trait OnRetry<E> {
    fn on_retry(&mut self, attempt: usize, next_backoff: Duration, error: &E);
}

pub struct NoopOnRetry {}

impl<E> OnRetry<E> for NoopOnRetry {
    fn on_retry(&mut self, _attempt: usize, _next_backoff: Duration, _error: &E) {}
}

impl<F, E> OnRetry<E> for F
where F: FnMut(usize, Duration, &E)
{
    fn on_retry(&mut self, attempt: usize, next_backoff: Duration, error: &E) { self(attempt, next_backoff, error) }
}
