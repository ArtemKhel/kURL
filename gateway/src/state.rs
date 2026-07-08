use std::{
    sync::{Arc},
};
use tonic::transport::Channel;
use proto::url::link_service_client::LinkServiceClient;
use common::config::GatewayConfig;

#[derive(Debug)]
pub struct AppState {
    pub config: GatewayConfig,
    pub redis: deadpool_redis::Pool,
    pub grpc_client: LinkServiceClient<Channel>,
}

pub type SharedState = Arc<AppState>;
