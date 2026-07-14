use std::time::Duration;

use deadpool_redis::Pool;
use proto::core::link_service_client::LinkServiceClient;
use tonic::transport::Channel;
use tracing::{info, info_span, instrument};

#[instrument]
pub async fn init(config: &crate::Config) -> Result<(Pool, LinkServiceClient<Channel>), Box<dyn std::error::Error>> {
    let (redis_pool, core_client) = tokio::try_join!(
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
            Duration::from_millis(50)
        ),
        common::connect_with_retry(
            "Core gRPC",
            || {
                let core_url = config.core.to_string();
                async move {
                    let grpc_client = LinkServiceClient::connect(core_url).await?;
                    Ok(grpc_client)
                }
            },
            10,
            Duration::from_millis(50)
        )
    )?;
    info!("All services initialized successfully");
    Ok((redis_pool, core_client))
}
