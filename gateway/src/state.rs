use std::sync::Arc;

use common::config::GatewayConfig;
use proto::{analytics::analytics_client::AnalyticsClient, core::link_service_client::LinkServiceClient};
use tonic::transport::Channel;

#[derive(Debug)]
pub struct AppState {
    pub config: GatewayConfig,
    pub redis: deadpool_redis::Pool,
    pub grpc_client: LinkServiceClient<Channel>,
    pub analytics_client: AnalyticsClient<Channel>,
}

pub type SharedState = Arc<AppState>;
