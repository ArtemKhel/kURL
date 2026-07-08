use std::net::SocketAddr;

use axum::{routing::get};
use proto::url::link_service_server::LinkServiceServer;
use sqlx::migrate::Migrator;
use tokio::net::TcpListener;
use tonic::service::Routes;
use tower::ServiceBuilder;

use crate::{grpc::LinkService, state::AppState};

pub mod cache;
pub mod db;
mod grpc;
mod state;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/");
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: common::config::CoreConfig = common::config::AppConfig::load()
        .expect("Failed to load application config")
        .into();
    let db_pool = db::connect(config.database.to_string().as_str()).await?;
    MIGRATOR.run(&db_pool).await?;

    let redis = deadpool_redis::Config::from_url(config.cache.to_string())
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

    let state = AppState { db_pool, redis };

    let addr = config.core.to_string().parse::<SocketAddr>()?;

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
        .with_graceful_shutdown(common::shutdown())
        .await;

    Ok(())
}
