use common::events::ClickEvent;
use redis::{
    streams::{StreamDeletionPolicy, StreamId, StreamReadOptions},
    AsyncTypedCommands,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, instrument, warn};

use crate::{click_counter::ClickCounter, redis_persistence::Persistence, redis_stats::RedisStats};

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
    pub async fn run(self, task_tracker: TaskTracker, shutdown: CancellationToken) {
        // todo: on restart?
        let click_counter = ClickCounter::spawn(&self.config, self.db.clone(), &task_tracker, shutdown.child_token());
        let redis_stats = RedisStats::new(self.config.clone());

        self.ensure_consumer_group()
            .await
            .unwrap_or_else(|e| error!(error=?e, "Failed to create event consumer group"));
        self.spawn_persistence_task(&task_tracker, shutdown.child_token());

        // Current version of `deadpool-redis` doesn't allow to override `DEFAULT_RESPONSE_TIMEOUT`
        // in `redis::AsyncConnectionOptions` for an underlying client
        // making blocking for longer than 0.5s impossible
        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, CONSUMER_NAME)
            .count(self.config.analytics.read_batch_size)
            .block(250);

        loop {
            let Ok(mut conn) = self.redis.get().await else {
                error!("Failed to get Redis connection");
                continue;
            };
            let stream_keys = [&self.config.redis.streams.events];
            let stream_ids = [">"];

            let reply = tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Shutdown requested, stopping analytics consumer loop");
                    break;
                }
                reply = conn.xread_options(&stream_keys, &stream_ids, &opts) => reply,
            };

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
                    self.process_entry(&mut conn, &entry, &click_counter, &redis_stats)
                        .await;
                }
            }
        }
    }

    async fn process_entry(
        &self,
        conn: &mut deadpool_redis::Connection,
        entry: &StreamId,
        click_counter: &ClickCounter,
        redis_stats: &RedisStats,
    ) {
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
            if let Err(e) = redis_stats.record_click(conn, &event).await {
                error!(error = %e, "Error recording ClickEvent from stats_counter");
            }
            click_counter.notify(event)
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

    fn spawn_persistence_task(&self, task_tracker: &TaskTracker, shutdown: CancellationToken) {
        Persistence::spawn(
            self.db.clone(),
            self.redis.clone(),
            &task_tracker,
            shutdown.child_token(),
        );
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
