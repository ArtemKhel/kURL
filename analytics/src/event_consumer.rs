use std::time::Duration;

use common::events::ClickEvent;
use redis::{
    streams::{StreamReadOptions, StreamReadReply}, AsyncTypedCommands,
    RedisResult,
};
use tracing::{debug, error, info, instrument, warn};

const STREAM_NAME: &str = "Events";
const CONSUMER_GROUP: &str = "Analytics";
const CONSUMER_NAME: &str = "worker-0";

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

        loop {
            let mut conn = self.redis.get().await?;
            let opts = StreamReadOptions::default()
                .group(CONSUMER_GROUP, CONSUMER_NAME)
                .count(self.config.analytics.read_batch_size)
                .block(self.config.analytics.read_block_secs.as_millis() as usize); // todo: instead of blocking, it just times out. same on plain redis conn without pool

            let reply = conn
                .xread_options(&[&self.config.redis.streams.events], &[">"], &opts)
                .await;

            let reply = match reply {
                Ok(Some(r)) => r,
                Ok(None) => {
                    debug!("No new events in Redis stream, continuing");
                    continue;
                }
                Err(e) => {
                    warn!(error = %e, "Error reading from Redis stream, retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            for key in reply.keys {
                for entry in key.ids {
                    let id = entry.id;
                    let event = entry
                        .map
                        .get("event")
                        .ok_or_else(|| anyhow::anyhow!("Missing 'event' field in stream entry"))
                        .and_then(|e| redis::from_redis_value_ref::<String>(e).map_err(Into::into))
                        .and_then(|s| serde_json::from_str::<ClickEvent>(&s).map_err(Into::into))?;

                    dbg!(&id, &event);
                }
            }
        }
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
        let res = conn
            .xgroup_create_mkstream(&self.config.redis.streams.events, consumer_group, "0")
            .await;
        match res {
            Ok(_) => info!(consumer_group, "Created consumer group"),
            Err(e) if e.to_string().contains("BUSYGROUP") => debug!("Consumer group already exists"),
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}
