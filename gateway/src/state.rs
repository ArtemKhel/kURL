use std::sync::Arc;

use common::config::GatewayConfig;
use proto::url::link_service_client::LinkServiceClient;
use tonic::transport::Channel;

#[derive(Debug)]
pub struct AppState {
    pub config: GatewayConfig,
    pub redis: deadpool_redis::Pool,
    pub grpc_client: LinkServiceClient<Channel>,
}

pub type SharedState = Arc<AppState>;
