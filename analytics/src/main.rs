pub mod db;
pub mod event_consumer;
pub mod grpc;
pub mod init;
pub mod redis_persistence;
pub mod redis_stats;

use common::config::AnalyticsConfig;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::info;

use crate::event_consumer::EventConsumer;

type Config = AnalyticsConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = common::config::load::<AnalyticsConfig>();
    let otel_guard = common::logging::init_tracing(&config.logging, "analytics");
    info!(?config);

    let task_tracker = TaskTracker::new();
    let shutdown = CancellationToken::new();

    let (db, redis) = init::init(&config).await?;

    let event_consumer = EventConsumer::new(redis.clone(), db.clone(), config.clone());
    let consumer_shutdown = shutdown.child_token();
    let consumer_tt = task_tracker.clone();
    task_tracker.spawn(async move { event_consumer.run(consumer_tt, consumer_shutdown).await });

    common::shutdown(async move || {
        shutdown.cancel();
        task_tracker.close();
        task_tracker.wait().await;
        drop(otel_guard);
    })
    .await;
    Ok(())
}
