use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::routing::get;
use proto::core::link_service_server::LinkServiceServer;
use sqlx::migrate::Migrator;
use tokio::{net::TcpListener, sync::mpsc::UnboundedSender};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::service::Routes;
use tower::ServiceBuilder;
use tracing::debug;

use crate::{grpc::LinkService, init::init};

pub mod cache;
pub mod db;
mod grpc;
pub mod init;
mod utils;

pub type Config = common::config::CoreConfig;

#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    pub db_pool: Arc<dyn db::LinkRepository>,
    pub redis_tx: UnboundedSender<cache::CacheOp>,
}

//noinspection RsCompileErrorMacro
static MIGRATOR: Migrator = sqlx::migrate!("../migrations/");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = common::config::AppConfig::load()
        .expect("Failed to load application config")
        .into();
    let otel_guard = common::logging::init_tracing(&config.logging, "core");
    debug!(?config);

    let (db_pool, redis) = init(&config).await.expect("Failed to initialize DB or Redis pool");

    let task_tracker = TaskTracker::new();
    let shutdown = CancellationToken::new();

    let redis_tx = cache::spawn_cache_worker(redis.clone(), &task_tracker, shutdown.clone());
    let state = Arc::new(AppState {
        db_pool: Arc::new(db_pool),
        redis_tx,
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
        .with_state(state.clone());

    let listener = TcpListener::bind(addr).await?;
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown(async move || {
            shutdown.cancel();
            task_tracker.close();
            task_tracker.wait().await;
        }))
        .await;

    drop(otel_guard);
    Ok(())
}
