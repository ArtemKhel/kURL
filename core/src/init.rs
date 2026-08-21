use tracing::{info, instrument};

#[instrument]
pub async fn init(config: &crate::Config) -> Result<(sqlx::PgPool, deadpool_redis::Pool), Box<dyn std::error::Error>> {
    let (db_pool, redis) = tokio::try_join!(
        common::connections::connect_postgres(&config.database),
        common::connections::connect_redis(&config.redis),
    )?;
    info!("All services initialized successfully");
    Ok((db_pool, redis))
}
