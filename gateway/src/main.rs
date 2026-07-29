mod api;
mod cache;
mod grpc;
pub mod init;
mod state;
pub mod web;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::{
    routing::{delete, get, post},
    Router,
};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::{init::init, state::AppState};

pub(crate) type Config = common::config::GatewayConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = common::config::AppConfig::load()?.into();
    let otel_guard = common::logging::init_tracing(&config.logging, "gateway");
    info!(?config);

    let (redis, grpc_client) = init(&config)
        .await
        .expect("Failed to connect to database or gRPC server");

    let addr = format!("0.0.0.0:{}", config.gateway.port)
        .parse::<SocketAddr>()
        .context("Failed to parse socket address")?;
    let listener = TcpListener::bind(addr).await?;

    let state = Arc::new(AppState {
        config,
        redis,
        grpc_client,
    });

    let app = Router::new()
        .without_v07_checks()
        .layer(TraceLayer::new_for_http()) //todo: opts, feature flags
        .route("/", get(web::web::hello))
        .route("/api/create", post(api::create::create))
        .route("/api/delete", delete(api::delete::delete))
        .route("/s/{code}", get(api::redirect::redirect))
        .fallback(web::not_found)
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown(async move || eprintln!("shutting down")))
        .await?;

    drop(otel_guard);
    Ok(())
}
