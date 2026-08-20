use std::sync::Arc;

use common::config::GatewayConfig;
use proto::analytics::analytics_client::AnalyticsClient;
use tonic::transport::Channel;

use crate::grpc::core_client::CoreClient;

#[derive(Debug)]
pub struct AppState {
    pub config: GatewayConfig,
    pub redis: deadpool_redis::Pool,
    // pub grpc_client: LinkServiceClient<Channel>,
    pub core_client: CoreClient,
    pub analytics_client: AnalyticsClient<Channel>,
}

pub type SharedState = Arc<AppState>;
