use std::time::Duration;

use tracing::{info, info_span, instrument};

use crate::db;

#[instrument]
pub async fn init(config: &crate::Config) -> Result<(sqlx::PgPool, deadpool_redis::Pool), Box<dyn std::error::Error>> {
    let (db_pool, redis) = tokio::try_join!(
        async {
            let db_pool = db::connect(config.database.to_string().as_str())
                .await
                .expect("Failed to connect to database");
            crate::MIGRATOR.run(&db_pool).await.expect("Failed to apply migrations");
            info!("Connected to database: {}", config.database.to_string());
            Ok(db_pool)
        },
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
