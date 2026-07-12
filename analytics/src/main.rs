pub mod init;
pub mod db;
pub mod event_consumer;
pub mod redis_stats;
pub mod persistence;
pub mod redis_keys;

use tracing::{info, };
use common::config::AnalyticsConfig;
use crate::event_consumer::EventConsumer;
use crate::persistence::Persistence;

type Config = AnalyticsConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config= common::config::load::<AnalyticsConfig>();
    common::logging::init_tracing(&config.logging.level);
    info!(?config);

    let (db, redis) = init::init(&config).await?;

    let event_consumer = EventConsumer::new(redis.clone(), db.clone(), config.clone());

    tokio::spawn(async move {
        event_consumer.run().await
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let pers = Persistence::new(db.clone(), redis.clone());
    pers.snapshot_and_trim().await?;

    Ok(())
}
