use std::time::Duration;

use common::events::ClickEvent;
use redis::{
    AsyncTypedCommands,
    streams::{StreamDeletionPolicy, StreamId, StreamReadOptions},
};
use tracing::{debug, error, info, instrument, warn};
use crate::click_counter::ClickCounter;
use crate::redis_persistence::Persistence;
use crate::redis_stats::RedisStats;

const CONSUMER_GROUP: &str = "Analytics";
const CONSUMER_NAME: &str = "worker-0";

pub struct EventConsumer {
    redis: deadpool_redis::Pool,
    db: sqlx::PgPool,
    config: crate::Config,
    click_counter: ClickCounter,
    redis_stats: RedisStats,
}

impl EventConsumer {
    pub fn new(redis: deadpool_redis::Pool, db: sqlx::PgPool, config: crate::Config) -> Self {
        let click_counter = ClickCounter::new(db.clone());
        let redis_stats = RedisStats::new(config.clone());
        Self { redis, db, config, click_counter, redis_stats}
    }

    #[instrument(skip(self))]
    pub async fn run(self) -> anyhow::Result<()> {
        // todo: on restart?

        self.ensure_consumer_group().await?;
        self.spawn_persistence_task().await;

        Persistence::spawn(self.db.clone(), self.redis.clone());

        // Current version of `deadpool-redis` doesn't allow to override `DEFAULT_RESPONSE_TIMEOUT`
        // in `redis::AsyncConnectionOptions` for an underlying client
        // making blocking for longer than 0.5s impossible
        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, CONSUMER_NAME)
            .count(self.config.analytics.read_batch_size)
            .block(250);

        loop {
            let mut conn = self.redis.get().await?;

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
                    continue;
                }
            };

            for key in reply.keys {
                for entry in key.ids {
                    self.process_entry(&mut conn, &entry).await;
                }
            }
        }
    }

    async fn process_entry(&self, conn: &mut deadpool_redis::Connection, entry: &StreamId) {
        let event: Option<ClickEvent> = entry.map.get("event").and_then(|v| match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| error!(error = %e, "Error deserializing ClickEvent from BulkString"))
                .ok(),
            redis::Value::SimpleString(s) => serde_json::from_str(s)
                .map_err(|e| error!(error = %e, "Error deserializing ClickEvent from SimpleString"))
                .ok(),
            _ => {
                warn!(value = ?v, "Unexpected value in ClickEvent entry");
                None
            }
        });

        if let Some(event) = event {
            // todo: do something with the event
            if let Err(e) = self.redis_stats.record_click(conn, &event).await {
                error!(error = %e, "Error recording ClickEvent from stats_counter");
            }
            self.click_counter.notify(event)
        }

        // todo: batch ack
        // prob should check status code
        let _ = conn
            .xack_del(
                &self.config.redis.streams.events,
                CONSUMER_GROUP,
                &[entry.id.clone()],
                StreamDeletionPolicy::Acked,
            )
            .await
            .inspect_err(|e| error!(error = %e, "Error acknowledging ClickEvent in Redis stream"));
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
