use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::routing::get;
use proto::core::link_service_server::LinkServiceServer;
use sqlx::migrate::Migrator;
use tokio::net::TcpListener;
use tonic::service::Routes;
use tower::ServiceBuilder;
use tracing::debug;

use crate::{grpc::LinkService, init::init, state::AppState};

pub mod cache;
pub mod db;
mod grpc;
pub mod init;
mod state;
mod utils;

pub(crate) type Config = common::config::CoreConfig;

//noinspection RsCompileErrorMacro
static MIGRATOR: Migrator = sqlx::migrate!("../migrations/");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = common::config::AppConfig::load()
        .expect("Failed to load application config")
        .into();
    let trace_provider = common::logging::init_tracing(&config.logging, "core");
    common::logging::init_metrics(9100);
    debug!(?config);

    let (db_pool, redis) = init(&config).await.expect("Failed to initialize DB or Redis pool");

    let state = Arc::new(AppState {
        db_pool,
        redis,
        config: config.clone(),
    });

    let addr = format!("0.0.0.0:{}", config.core.port)
        .parse::<SocketAddr>()
        .context("Failed to parse socket address")?;

    let grpc = ServiceBuilder::new()
        // .layer()
        .service(LinkServiceServer::new(LinkService { state: state.clone() }));

    let app = Routes::default()
        .add_service(grpc)
        .into_axum_router()
        .with_state(())
        .route("/", get(async || "Hello"))
        .with_state(state);

    let listener = TcpListener::bind(addr).await?;
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown(async move || {}))
        .await;

    let _ = trace_provider.shutdown();
    Ok(())
}
