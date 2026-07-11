use std::time::Duration;

use redis::AsyncTypedCommands;
use tracing::{debug, error, info, instrument};

const STREAM_NAME: &str = "Events";
const CONSUMER_GROUP: &str = "Analytics";

pub struct EventConsumer {
    redis: deadpool_redis::Pool,
    db: sqlx::PgPool,
    config: crate::Config,
}

impl EventConsumer {
    pub fn new(redis: deadpool_redis::Pool, db: sqlx::PgPool, config: crate::Config) -> Self {
        Self { redis, db, config }
    }

    #[instrument(skip(self))]
    pub async fn run(self) -> anyhow::Result<()> {
        self.ensure_consumer_group().await?;

        self.spawn_persistence_task().await;
        Ok(())
    }

    async fn spawn_persistence_task(&self) {
        let redis = self.redis.clone();
        let db = self.db.clone();
        tokio::spawn(async move {
            // todo: dump data in db, trim redis
            error!("Persistence task not implemented yet");
            let mut ticker = tokio::time::interval(Duration::from_mins(10));
            loop {
                ticker.tick().await;
            }
        });
    }

    async fn ensure_consumer_group(&self) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        let consumer_group = CONSUMER_GROUP;
        let res = conn.xgroup_create_mkstream(STREAM_NAME, consumer_group, "0").await;
        match res {
            Ok(_) => info!(consumer_group, "Created consumer group"),
            Err(e) if e.to_string().contains("BUSYGROUP") => debug!("Consumer group already exists"),
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}
