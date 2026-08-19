use std::time::Duration;

use common::config::AnalyticsConfig;
use tracing::{info, instrument};

#[instrument(skip_all)]
pub async fn init(
    config: &AnalyticsConfig,
) -> Result<(sqlx::PgPool, deadpool_redis::Pool), Box<dyn std::error::Error>> {
    let (db_pool, redis) = tokio::try_join!(
        common::connect_with_retry(
            "Postgres",
            || {
                let database_url = config.database.to_string();
                async move {
                    let pool = common::db_utils::connect(&database_url)
                        .await
                        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?;

                    info!(
                        host = %config.database.host,
                        port = config.database.port,
                        database = %config.database.db_name,
                        "Connected to database"
                    );
                    Ok(pool)
                }
            },
            10,
            Duration::from_millis(250),
        ),
        common::connect_with_retry(
            "Redis",
            || {
                let cache_url = config.redis.to_string();
                async move {
                    let cfg = deadpool_redis::Config::from_url(cache_url);
                    let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
                    let mut conn = pool.get().await?;
                    redis::cmd("PING").query_async::<String>(&mut *conn).await?;
                    Ok(pool)
                }
            },
            10,
            Duration::from_millis(50),
        )
    )?;
    info!("All services initialized successfully");
    Ok((db_pool, redis))
}
