mod cache;
mod grpc;
pub mod init;
mod routes;
mod state;
pub mod web;

use std::{sync::Arc, time::Duration};

use axum::{
    routing::{delete, get, post},
    Router,
};
use common;
use proto::core::link_service_client::LinkServiceClient;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, info_span};

use crate::{init::init, state::AppState};

pub(crate) type Config = common::config::GatewayConfig;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = common::config::AppConfig::load()?.into();
    common::logging::init_tracing(&config.logging.level);
    info!(?config);

    let (redis, grpc_client) = init(&config)
        .await
        .expect("Failed to connect to database or gRPC server");
    let listener = TcpListener::bind(config.gateway.to_string()).await?;

    let state = Arc::new(AppState {
        config,
        redis,
        grpc_client,
    });

    let app = Router::new()
        .without_v07_checks()
        .layer(TraceLayer::new_for_http()) //todo: opts, feature flags
        .route("/", get(web::web::hello))
        .route("/api/create", post(routes::create::create))
        .route("/api/delete", delete(routes::delete::delete))
        .route("/s/{code}", get(routes::redirect::redirect))
        .fallback(routes::not_found)
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown(async {}))
        .await?;
    Ok(())
}
