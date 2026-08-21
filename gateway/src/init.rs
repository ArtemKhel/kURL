use deadpool_redis::Pool;
use proto::{analytics::analytics_client::AnalyticsClient, core::link_service_client::LinkServiceClient};
use tonic::transport::Channel;
use tracing::{info, instrument};

#[instrument]
#[allow(clippy::type_complexity)]
pub async fn init(
    config: &crate::Config,
) -> anyhow::Result<(Pool, LinkServiceClient<Channel>, AnalyticsClient<Channel>)> {
    let core_url = config.core.to_string();
    let analytics_url = config.analytics.to_string();

    let (redis_pool, core_client, analytics_client) = tokio::try_join!(
        common::connections::connect_redis(&config.redis),
        common::connections::retry_connection("Core gRPC", || LinkServiceClient::connect(core_url.clone())),
        common::connections::retry_connection("Analytics gRPC", || AnalyticsClient::connect(analytics_url.clone())),
    )?;
    info!("All services initialized successfully");
    Ok((redis_pool, core_client, analytics_client))
}
