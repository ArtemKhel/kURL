use std::{fmt::Display, future::Future, time::Duration};

use anyhow::anyhow;
use tracing::{error, info, warn};

use crate::{
    config::{DatabaseConfig, RedisConfig},
    retry::{
        backoff_strategy::{BackoffStrategy, exponential::ExponentialBackoff, jitter::JitterTy},
        retry,
    },
};

const MAX_CONNECTION_ATTEMPTS: usize = 10;
const INITIAL_BACKOFF: Duration = Duration::from_millis(125);

pub async fn connect_postgres(config: &DatabaseConfig) -> anyhow::Result<sqlx::PgPool> {
    let database_url = config.to_string();

    retry_with_exp_backoff(
        "Postgres",
        || {
            let database_url = database_url.clone();
            async move { crate::db_utils::connect(&database_url).await }
        },
        MAX_CONNECTION_ATTEMPTS,
        INITIAL_BACKOFF,
    )
    .await
}

pub async fn connect_redis(config: &RedisConfig) -> anyhow::Result<deadpool_redis::Pool> {
    let redis_url = config.to_string();

    retry_connection("Redis", || {
        let redis_url = redis_url.clone();
        async move {
            let config = deadpool_redis::Config::from_url(redis_url);
            let pool = config.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
            let mut connection = pool.get().await?;
            redis::cmd("PING").query_async::<String>(&mut *connection).await?;
            Ok::<_, anyhow::Error>(pool)
        }
    })
    .await
}

pub async fn retry_connection<F, Fut, T, E>(service_name: &str, operation: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display, {
    retry_with_exp_backoff(service_name, operation, MAX_CONNECTION_ATTEMPTS, INITIAL_BACKOFF).await
}

async fn retry_with_exp_backoff<F, Fut, T, E>(
    service_name: &str,
    operation: F,
    max_attempts: usize,
    initial_backoff: Duration,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    let max_attempts = max_attempts.max(1);
    let result = retry(operation)
        .with_strategy(ExponentialBackoff::new(initial_backoff).jittered(JitterTy::Equal))
        .max_retries(max_attempts - 1)
        .on_retry(|attempt, next_backoff, error: &E| {
            warn!(
                service = service_name,
                attempt = attempt + 1,
                error = %error,
                ?next_backoff,
                "Connection attempt failed; retrying"
            );
        })
        .await;

    match result {
        Ok(value) => {
            info!(service = service_name, "Connected successfully");
            Ok(value)
        }
        Err(error) => {
            error!(service = service_name,attempts = max_attempts,error = %error,"Failed to connect");
            Err(anyhow!("Failed to connect to {service_name}: {error}"))
        }
    }
}
