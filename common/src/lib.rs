pub mod config;
pub mod events;
pub mod logging;
mod shutdown;
pub mod redis_keys;

use std::time::Duration;

use anyhow::anyhow;
pub use shutdown::shutdown;

pub async fn connect_with_retry<F, Fut, T>(
    service_name: &str,
    mut f: F,
    max_retries: u32,
    initial_delay: Duration,
) -> Result<T, anyhow::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn std::error::Error>>>,
{
    let mut delay = initial_delay;
    let mut attempt = 0;

    loop {
        match f().await {
            Ok(result) => {
                tracing::info!("{} connected successfully", service_name);
                return Ok(result);
            }
            Err(e) => {
                attempt += 1;
                if attempt >= max_retries {
                    tracing::error!("{} failed after {} attempts: {}", service_name, attempt, e);
                    return Err(anyhow!("Failed to connect to {}", service_name));
                }
                tracing::warn!(
                    "{} attempt {} failed: {}. Retrying in {:?}...",
                    service_name,
                    attempt,
                    e,
                    delay
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
        }
    }
}
