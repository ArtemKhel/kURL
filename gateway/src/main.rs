#![feature(map_try_insert)]

mod cache;
mod config;
mod routes;
mod state;
mod grpc;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use common;
use proto::url::link_service_client::LinkServiceClient;
use tokio::{net::TcpListener, sync::RwLock};

use crate::{
    config::AppConfig,
    state::AppState,
};

#[tokio::main]
async fn main() {
    // todo: tracing-sub
    tracing_subscriber::fmt::init();
    let config = AppConfig::parse();
    dbg!(&config);

    // Redis
    let redis_config = deadpool_redis::Config::from_url(config.redis_url);
    let redis_pool = redis_config.create_pool(Some(deadpool_redis::Runtime::Tokio1)).unwrap();
    dbg!(&redis_pool.status());

    // GRPC
    let core_client = LinkServiceClient::connect(config.core_url).await.unwrap();

    let state = Arc::new(RwLock::new(AppState {
        config: config.clone(),
        redis: redis_pool,
        core: core_client,
    }));

    let app = Router::new()
        .without_v07_checks()
        .route("/", get(routes::root::hello))
        .route("/api/create", post(routes::api::create))
        .route("/s/{code}", get(routes::redirect::redirect))
        .with_state(state);
    let listener = TcpListener::bind(config.listener_address).await.unwrap();
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown())
        .await;
    ()
}
