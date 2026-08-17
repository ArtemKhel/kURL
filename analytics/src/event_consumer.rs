use std::time::Duration;

use anyhow::Context;
use common::events::ClickEvent;
use itertools::Either;
use redis::{
    AsyncTypedCommands,
    streams::{StreamAutoClaimOptions, StreamId, StreamReadOptions},
};
use sqlx::{
    Postgres,
    pool::PoolConnection,
    postgres::{PgAdvisoryLock, PgAdvisoryLockGuard, PgAdvisoryLockKey},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    redis_persistence::Persistence,
    redis_stats::{EventOutcome, RedisStats},
};

const CONSUMER_GROUP: &str = "Analytics";
const CONSUMER_NAME: &str = "worker-0";
const ANALYTICS_WRITER_LOCK_ID: i64 = 1_234_567;
const LOCK_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const XAUTOCLAIM_MIN_IDLE_MS: u64 = 0;

const REDIS_ERROR_BACKOFF: Duration = Duration::from_millis(500);

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

    #[instrument(skip_all)]
    async fn run_inner(&self, shutdown: &CancellationToken) -> anyhow::Result<()> {
        self.ensure_consumer_group()
            .await
            .context("failed to create consumer group, stopping writer")?;

        self.persistence.rehydrate().await.context("failed to rehydrate")?;

        if let Err(error) = self.drain_pending(shutdown).await {
            error!(%error, "failed to drain pending events");
        }

        self.event_loop(shutdown).await;

        self.persistence
            .flush()
            .await
            .context("snapshot flush before shutdown failed")
    }

    #[instrument(skip_all)]
    async fn event_loop(&self, shutdown: &CancellationToken) {
        let mut flush_ticker = tokio::time::interval(self.config.analytics.flush_interval);
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        flush_ticker.tick().await;

        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, CONSUMER_NAME)
            .count(self.config.analytics.read_batch_size)
            .block(self.config.analytics.read_block.as_millis() as usize);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("shutdown requested, stopping analytics consumer loop");
                    return
                }
                _ = flush_ticker.tick() => {
                    if let Err(error) = self.persistence.flush().await {
                        warn!(%error, "snapshot flush failed");
                    }
                }
                result = self.read_stream(&opts) => {
                    match result {
                        Ok(Some(stream_ids)) => {
                            let Ok(mut conn) = self.redis.get().await else {
                                error!("Failed to get redis connection");
                                continue
                            };
                            for stream_id in stream_ids {
                                self.process_entry(&mut conn, &stream_id).await;
                            }
                        }
                        Ok(None) => {
                            trace!("No new events in stream")
                        }
                        Err(error) => {
                            error!(%error, "Error reading from Redis stream");
                            tokio::time::sleep(REDIS_ERROR_BACKOFF).await;
                        }
                    }
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn read_stream(&self, opts: &StreamReadOptions) -> anyhow::Result<Option<Vec<StreamId>>> {
        let mut conn = self.redis.get().await.context("Failed to get redis connection")?;
        let stream_keys = [&self.config.redis.streams.events];
        let stream_ids = [">"];
        let reply = conn.xread_options(&stream_keys, &stream_ids, opts).await?;

        Ok(reply.map(|r| r.keys.into_iter().flat_map(|k| k.ids.into_iter()).collect()))
    }

    #[instrument(skip_all)]
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

    #[instrument(skip_all)]
    async fn process_entry(&self, conn: &mut deadpool_redis::Connection, entry: &StreamId) {
        match Self::parse_click_event(entry) {
            Some(event) => {
                match RedisStats::record_click_event(
                    conn,
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
                match RedisStats::drop_click_event(conn, &self.config.redis.streams.events, CONSUMER_GROUP, &entry.id)
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

    #[instrument(skip_all)]
    async fn drain_pending(&self, shutdown: &CancellationToken) -> anyhow::Result<()> {
        let mut total_claimed = 0u64;
        loop {
            if shutdown.is_cancelled() {
                break;
            };
            let claimed = self.drain_pending_batch().await?;
            if claimed == 0 {
                break;
            }
            total_claimed += claimed;
        }
        info!(total_claimed = %total_claimed, "drained pending events");
        Ok(())
    }

    #[instrument(skip_all)]
    async fn drain_pending_batch(&self) -> anyhow::Result<u64> {
        let mut conn = self.redis.get().await.context("Failed to get redis connection")?;
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
