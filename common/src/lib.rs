pub mod config;

use std::time::Duration;

use tokio::signal;

pub async fn connect_with_retry<F, Fut, T>(
    service_name: &str,
    mut f: F,
    max_retries: u32,
    initial_delay: Duration,
) -> Result<T, Box<dyn std::error::Error>>
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
                    return Err(format!("Failed to connect to {}", service_name).into());
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
pub async fn shutdown() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    println!("Shutting down...");
}
