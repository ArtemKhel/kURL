use std::time::Duration;

use anyhow::Context;
use common::events::ClickEvent;
use itertools::Either;
use redis::{
    AsyncTypedCommands, RedisResult,
    streams::{StreamAutoClaimOptions, StreamDeletionPolicy, StreamId, StreamReadOptions},
};
use sqlx::{
    Postgres,
    pool::PoolConnection,
    postgres::{PgAdvisoryLock, PgAdvisoryLockGuard, PgAdvisoryLockKey},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    redis_persistence::Persistence,
    redis_stats::{EventOutcome, RedisStats},
};

const CONSUMER_GROUP: &str = "Analytics";
const CONSUMER_NAME: &str = "worker-0";
const ANALYTICS_WRITER_LOCK_ID: i64 = 1_234_567;
const LOCK_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const XAUTOCLAIM_MIN_IDLE_MS: u64 = 0;

pub struct EventConsumer {
    redis: deadpool_redis::Pool,
    db: sqlx::PgPool,
    persistence: Persistence,
    config: crate::Config,
}

impl EventConsumer {
    pub fn new(redis: deadpool_redis::Pool, db: sqlx::PgPool, config: crate::Config) -> Self {
        let persistence = Persistence::new(db.clone(), redis.clone());
        Self {
            redis,
            db,
            persistence,
            config,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            match self.acquire_writer().await {
                Ok(Some(guard)) => {
                    metrics::gauge!("analytics_writer_active").set(1.0);

                    if let Err(error) = self.run_inner(&shutdown).await {
                        error!(%error, "analytics writer returned an error");
                    }

                    metrics::gauge!("analytics_writer_active").set(0.0);
                    if let Err(error) = guard.release_now().await {
                        error!(%error, "failed to release writer lock");
                    }
                }
                Ok(None) => trace!("analytics writer lock is held by another instance"),
                Err(error) => warn!(%error, "failed to acquire writer lock"),
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("Shutdown requested, stopping analytics consumer loop");
                    return
                },
                _ = tokio::time::sleep(LOCK_RETRY_INTERVAL) => {}
            }
        }
    }

    async fn run_inner(&self, shutdown: &CancellationToken) -> anyhow::Result<()> {
        self.ensure_consumer_group()
            .await
            .context("failed to create consumer group, stopping writer")?;

        self.persistence.rehydrate().await.context("failed to rehydrate")?;

        if let Err(error) = self.drain_pending(&shutdown).await {
            error!(%error, "failed to drain pending events");
        }

        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        todo!("run main event consumer loop");
        // // self.spawn_persistence_task(&task_tracker, shutdown.child_token());
        //
        // // Current version of `deadpool-redis` doesn't allow to override `DEFAULT_RESPONSE_TIMEOUT`
        // // in `redis::AsyncConnectionOptions` for an underlying client
        // // making blocking for longer than 0.5s impossible
        // let opts = StreamReadOptions::default()
        //     .group(CONSUMER_GROUP, CONSUMER_NAME)
        //     .count(self.config.analytics.read_batch_size)
        //     .block(250);
        //
        // loop {
        //     let Ok(mut conn) = self.redis.get().await else {
        //         error!("Failed to get Redis connection");
        //         continue;
        //     };
        //     let stream_keys = [&self.config.redis.streams.events];
        //     let stream_ids = [">"];
        //
        //     let reply = tokio::select! {
        //         _ = shutdown.cancelled() => {
        //             info!("Shutdown requested, stopping analytics consumer loop");
        //             break;
        //         }
        //         reply = conn.xread_options(&stream_keys, &stream_ids, &opts) => reply,
        //     };
        //
        //     let reply = match reply {
        //         Ok(Some(r)) => r,
        //         Ok(None) => {
        //             trace!("No new events in Redis stream, continuing");
        //             continue;
        //         }
        //         Err(e) => {
        //             warn!(error = %e, "Error reading from Redis stream, retrying");
        //             continue;
        //         }
        //     };
        //
        //     for key in reply.keys {
        //         for entry in key.ids {
        //             self.process_entry(&mut conn, &entry).await;
        //         }
        //     }
        // }
    }

    fn parse_click_event(entry: &StreamId) -> Option<ClickEvent> {
        let value = entry.map.get("event")?;
        match value {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|error| warn!(%error, entry_id  = entry.id, "malformed ClickEvent (BulkString)"))
                .ok(),
            redis::Value::SimpleString(s) => serde_json::from_str(s)
                .map_err(|error| {
                    warn!(%error, entry_id = %entry.id, "malformed ClickEvent (SimpleString)");
                })
                .ok(),
            _ => {
                warn!(entry_id = %entry.id,"unexpected Redis value type in ClickEvent entry");
                None
            }
        }
    }

    // todo:
    async fn process_entry(&self, conn: &mut deadpool_redis::Connection, entry: &StreamId) {
        let Ok(mut conn) = self.redis.get().await else {
            error!("failed to get redis connection");
            return;
        };

        match Self::parse_click_event(entry) {
            Some(event) => {
                match RedisStats::record_click_event(
                    &mut conn,
                    &self.config.redis.streams.events,
                    CONSUMER_GROUP,
                    &entry.id,
                    &event,
                )
                .await
                {
                    Ok(EventOutcome::Applied) => {
                        metrics::counter!("analytics.events_processed").increment(1);
                    }
                    Ok(EventOutcome::AlreadyHandled) => {}
                    Err(error) => {
                        warn!(%error, "failed to process event due to redis script error");
                    }
                }
            }
            None => {
                match RedisStats::drop_click_event(
                    &mut conn,
                    &self.config.redis.streams.events,
                    CONSUMER_GROUP,
                    &entry.id,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        warn!(%error, "failed to drop event due to redis script error");
                    }
                }
            }
        }
    }

    async fn drain_pending(&self, shutdown: &CancellationToken) -> anyhow::Result<()> {
        let mut total_claimed = 0u64;
        loop {
            if shutdown.is_cancelled() {
                break;
            };
            let claimed = self.drain_pending_batch(shutdown).await?;
            if claimed == 0 {
                break;
            }
            total_claimed += claimed;
        }
        info!(total_claimed = %total_claimed, "drained pending events");
        Ok(())
    }

    async fn drain_pending_batch(&self, shutdown: &CancellationToken) -> anyhow::Result<u64> {
        let mut conn = self.redis.get().await?;
        let opts = StreamAutoClaimOptions::default().count(self.config.analytics.read_batch_size);

        let reply = conn
            .xautoclaim_options(
                &self.config.redis.streams.events,
                CONSUMER_GROUP,
                CONSUMER_NAME,
                XAUTOCLAIM_MIN_IDLE_MS,
                "0-0",
                opts,
            )
            .await?;

        let claimed_count = reply.claimed.len() as u64;
        metrics::counter!("analytics.claimed_events").increment(claimed_count);

        for entry in reply.claimed {
            self.process_entry(&mut conn, &entry).await;
        }

        Ok(claimed_count)
    }

    async fn acquire_writer(&self) -> anyhow::Result<Option<PgAdvisoryLockGuard<PoolConnection<Postgres>>>> {
        let conn = self.db.acquire().await.context("Failed to acquire DB connection")?;
        let lock = PgAdvisoryLock::with_key(PgAdvisoryLockKey::BigInt(ANALYTICS_WRITER_LOCK_ID));
        match lock.try_acquire(conn).await.context("Failed to acquire lock")? {
            Either::Left(guard) => Ok(Some(guard)),
            Either::Right(_) => Ok(None),
        }
    }

    async fn ensure_consumer_group(&self) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        let res = conn
            .xgroup_create_mkstream(&self.config.redis.streams.events, CONSUMER_GROUP, "0")
            .await;
        match res {
            Ok(_) => info!(CONSUMER_GROUP, "Created consumer group"),
            Err(e) if e.to_string().contains("BUSYGROUP") => debug!("Consumer group already exists"),
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}
