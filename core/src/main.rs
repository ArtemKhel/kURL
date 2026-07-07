use std::net::SocketAddr;

use axum::{handler::HandlerWithoutStateExt, routing::get};
use proto::url::link_service_server::LinkServiceServer;
use sqlx::migrate::Migrator;
use tokio::net::TcpListener;
use tonic::service::Routes;
use tower::ServiceBuilder;

use crate::grpc::LinkService;
use crate::state::AppState;

pub mod cache;
mod config;
pub mod db;
mod grpc;
mod state;

const DEFAULT_CORE_GRPC: &str = "127.0.0.1:3001";

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/");
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_pool = db::connect("postgresql://postgres:postgres@localhost:5432/kurlyk").await?;
    MIGRATOR.run(&db_pool).await?;

    let redis = deadpool_redis::Config::from_url("redis://127.0.0.1:6379")
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;

    let state = AppState { db_pool, redis };

    let addr = DEFAULT_CORE_GRPC.parse::<SocketAddr>()?;

    let grpc = ServiceBuilder::new()
        // .layer()
        .service(LinkServiceServer::new(LinkService{state:state.clone()}));

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
