pub mod click_counter;
pub mod db;
pub mod event_consumer;
pub mod grpc;
pub mod init;
pub mod redis_persistence;
pub mod redis_stats;

use common::config::AnalyticsConfig;
use tracing::info;

use crate::{event_consumer::EventConsumer, redis_persistence::Persistence};

type Config = AnalyticsConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = common::config::load::<AnalyticsConfig>();
    common::logging::init_tracing(&config.logging.level);
    info!(?config);

    let (db, redis) = init::init(&config).await?;

    let event_consumer = EventConsumer::new(redis.clone(), db.clone(), config.clone());

    event_consumer.run().await?;
    // let pers = Persistence::new(db.clone(), redis.clone());
    // pers.snapshot_and_trim().await?;

    Ok(())
}
