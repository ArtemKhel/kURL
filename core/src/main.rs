use std::{net::SocketAddr, time::Duration};

use axum::routing::get;
use proto::url::link_service_server::LinkServiceServer;
use sqlx::migrate::Migrator;
use tokio::net::TcpListener;
use tonic::service::Routes;
use tower::ServiceBuilder;
use tracing::{debug, info, info_span};

use crate::{grpc::LinkService, state::AppState};

pub mod cache;
pub mod db;
mod grpc;
mod state;

//noinspection RsCompileErrorMacro
static MIGRATOR: Migrator = sqlx::migrate!("../migrations/");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: common::config::CoreConfig = common::config::AppConfig::load()
        .expect("Failed to load application config")
        .into();
    common::logging::init_tracing(&config.logging.level);
    debug!(?config);

    let span = info_span!("init").entered();
    let (db_pool, redis) = tokio::try_join!(
        async {
            let db_pool = db::connect(config.database.to_string().as_str())
                .await
                .expect("Failed to connect to database");
            MIGRATOR.run(&db_pool).await.expect("Failed to apply migrations");
            info!("Connected to database: {}", config.database.to_string());
            Ok(db_pool)
        },
        common::connect_with_retry(
            "Redis",
            || {
                let cache_url = config.redis.to_string();
                async move {
                    let cfg = deadpool_redis::Config::from_url(cache_url);
                    let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
                    let mut conn = pool.get().await?;
                    redis::cmd("PING").query_async::<String>(&mut *conn).await?;
                    Ok(pool)
                }
            },
            8,
            Duration::from_millis(50),
        )
    )?;
    info!("All services initialized successfully");
    drop(span);

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
