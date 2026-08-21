use std::time::Duration;

use rand::RngExt;

use crate::retry::backoff_strategy::BackoffStrategy;

pub struct Jittered<BS: BackoffStrategy> {
    pub inner: BS,
    pub ty: JitterTy,
}

pub enum JitterTy {
    Full,
    Equal,
}
impl JitterTy {
    fn jitter(&self, delay: Duration) -> Duration {
        match self {
            JitterTy::Full => delay.mul_f32(rand::rng().random()),
            JitterTy::Equal => delay.mul_f32(rand::rng().random_range(0.5..1.0)),
        }
    }
}

impl<BS: BackoffStrategy> BackoffStrategy for Jittered<BS> {
    fn next_backoff(&mut self, attempt: usize) -> Duration { self.ty.jitter(self.inner.next_backoff(attempt)) }
}
