pub mod db;
pub mod event_consumer;
pub mod grpc;
pub mod init;
pub mod redis_persistence;
pub mod redis_stats;
pub mod snapshot;

use std::sync::Arc;

use common::config::AnalyticsConfig;
use proto::analytics::analytics_server::AnalyticsServer;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

use crate::{event_consumer::EventConsumer, grpc::AnalyticsService};

type Config = AnalyticsConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = common::config::load::<AnalyticsConfig>();
    let otel_guard = common::logging::init_tracing(&config.logging, "analytics");
    info!(?config.analytics, "analytics configuration loaded");
    check_config(&config)?;

    let task_tracker = TaskTracker::new();
    let shutdown = CancellationToken::new();

    let (db, redis) = init::init(&config).await?;

    let event_consumer = EventConsumer::new(redis.clone(), db.clone(), config.clone());
    let consumer_shutdown = shutdown.child_token();
    let consumer_tt = task_tracker.clone();
    task_tracker.spawn(async move { event_consumer.run(consumer_tt, consumer_shutdown).await });

    let analytics_grpc = AnalyticsServer::new(AnalyticsService {
        db: Arc::new(db.clone()),
    });
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", config.analytics.port)
        .parse()
        .expect("Failed to parse analytics gRPC listen address");

    let grpc_shutdown = shutdown.child_token();
    task_tracker.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(analytics_grpc)
            .serve_with_shutdown(grpc_addr, grpc_shutdown.cancelled_owned())
            .await
            .unwrap();
    });

    common::shutdown(async move || {
        shutdown.cancel();
        task_tracker.close();
        task_tracker.wait().await;
        drop(otel_guard);
    })
    .await;
    Ok(())
}

fn check_config(config: &Config) -> anyhow::Result<()> {
    if config.analytics.read_batch_size == 0 {
        error!("analytics.read_batch_size must be greater than zero");
        return Err(anyhow::anyhow!("Invalid analytics.read_batch_size"));
    }
    if !(1..=500).contains(&config.analytics.read_block.as_millis()) {
        error!("analytics.read_block_millis must be between 1 and 500");
        return Err(anyhow::anyhow!("Invalid analytics.read_block_millis"));
    };
    if config.analytics.flush_interval.is_zero() {
        error!("analytics.flush_interval must be greater than zero");
        return Err(anyhow::anyhow!("Invalid analytics.flush_interval"));
    }
    Ok(())
}
