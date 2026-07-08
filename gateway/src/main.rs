mod cache;
mod grpc;
mod routes;
mod state;

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    routing::{get, post},
};
use common;
use proto::url::link_service_client::LinkServiceClient;
use tokio::net::TcpListener;
use tracing::info;

use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // todo: tracing-sub
    tracing_subscriber::fmt::init();
    let config: common::config::GatewayConfig = common::config::AppConfig::load()?.into();
    dbg!(&config);

    let (redis_pool, core_client) = tokio::try_join!(
        common::connect_with_retry(
            "Redis",
            || {
                let cache_url = config.cache.to_string();
                async move {
                    let cfg = deadpool_redis::Config::from_url(cache_url);
                    let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
                    let mut conn = pool.get().await?;
                    redis::cmd("PING").query_async::<String>(&mut *conn).await?;
                    Ok(pool)
                }
            },
            8,
            Duration::from_millis(50)
        ),
        common::connect_with_retry(
            "Core gRPC",
            || {
                let core_url = config.core.to_string();
                async move {
                    let grpc_client = LinkServiceClient::connect(core_url).await?;
                    Ok(grpc_client)
                }
            },
            8,
            Duration::from_millis(50)
        )
    )?;

    info!("All services initialized successfully");

    let state = Arc::new(AppState {
        config: config.clone(),
        redis: redis_pool,
        grpc_client: core_client,
    });

    let app = Router::new()
        .without_v07_checks()
        .route("/", get(routes::root::hello))
        .route("/api/create", post(routes::create::create))
        .route("/s/{code}", get(routes::redirect::redirect))
        .with_state(state);
    let listener = TcpListener::bind(config.gateway.to_string()).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown())
        .await?;
    Ok(())
}
