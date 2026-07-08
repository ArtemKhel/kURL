mod cache;
mod grpc;
mod routes;
mod state;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use common;
use proto::url::link_service_client::LinkServiceClient;
use tokio::net::TcpListener;

use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // todo: tracing-sub
    tracing_subscriber::fmt::init();
    let config: common::config::GatewayConfig = common::config::AppConfig::load()?.into();
    dbg!(&config);

    // Redis
    let redis_config = deadpool_redis::Config::from_url(config.cache.to_string());
    let redis_pool = redis_config.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
    dbg!(&redis_pool.status());

    // GRPC
    let core_client = LinkServiceClient::connect(config.core.to_string()).await?;

    let state = Arc::new(AppState {
        config: config.clone(),
        redis: redis_pool,
        grpc_client: core_client,
    });

    let app = Router::new()
        .without_v07_checks()
        .route("/", get(routes::root::hello))
        .route("/api/create", post(routes::api::create))
        .route("/s/{code}", get(routes::redirect::redirect))
        .with_state(state);
    let listener = TcpListener::bind(config.gateway.to_string()).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown())
        .await?;
    Ok(())
}
