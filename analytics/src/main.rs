pub mod init;
pub mod db;

use tracing::{info, };
use common::config::AnalyticsConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config= common::config::load::<AnalyticsConfig>();
    common::logging::init_tracing(&config.logging.level);
    info!(?config);

    let (db, redis) = init::init(&config).await?;

    Ok(())
}
