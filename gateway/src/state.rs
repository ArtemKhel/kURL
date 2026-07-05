use std::{
    collections::HashMap,
    sync::{Arc},
};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use proto::url::link_service_client::LinkServiceClient;
use crate::config::AppConfig;

#[derive(Debug)]
pub struct AppState {
    pub config: AppConfig,
    pub redis: deadpool_redis::Pool,
    pub core: LinkServiceClient<Channel>,
}

pub type SharedState = Arc<RwLock<AppState>>;
