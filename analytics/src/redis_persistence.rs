use std::collections::HashMap;

use chrono::{Duration, NaiveDate};
use common::{db_utils::DbError, redis_keys::RedisKeys};
use redis::{AsyncIter, AsyncTypedCommands, ScanOptions};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    db::{self},
    redis_stats::RedisStats,
};

// todo: from config
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const ROLLING_WINDOW_DAYS: i64 = 7;
const STALE_GRACE_DAYS: i64 = 1;
const SCAN_BATCH_SIZE: usize = 100;

pub struct Persistence {
    db: sqlx::PgPool,
    redis: deadpool_redis::Pool,
}

impl Persistence {
    fn new(db: sqlx::PgPool, redis: deadpool_redis::Pool) -> Self { Self { db, redis } }

    pub fn spawn(
        db: sqlx::PgPool,
        redis: deadpool_redis::Pool,
        task_tracker: &TaskTracker,
        shutdown: CancellationToken,
    ) {
        task_tracker.spawn(async move {
            let persistence = Persistence::new(db, redis);
            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("Shutdown requested, snapshotting redis streams to DB");
                        if let Err(e) = persistence.snapshot_and_trim().await {
                            error!(error = %e, "snapshot_and_trim failed");
                        }
                        info!("Shutdown complete");
                        return
                    },
                    _ = ticker.tick() => {
                        if let Err(e) = persistence.snapshot_and_trim().await {
                            error!(error = %e, "snapshot_and_trim failed");
                        }
                    }
                }
            }
        });
    }

    /// Reads every `stats:link:*` hash in Redis, trims the old ones
    /// and return SoA (short_code, day, number of clicks)
    #[instrument(skip(self))]
    async fn get_link_stats(&self) -> anyhow::Result<(Vec<String>, Vec<NaiveDate>, Vec<i64>)> {
        let cutoff = stale_cutoff();
        let key_pattern = RedisKeys::link_stats_key("*");
        let key_prefix = RedisKeys::link_stats_key("");

        let mut short_codes = Vec::new();
        let mut dates = Vec::new();
        let mut clicks = Vec::new();

        let mut scan_conn = self.redis.get().await?;
        let mut cmd_conn = self.redis.get().await?;

        let scan_opts = ScanOptions::default()
            .with_count(SCAN_BATCH_SIZE)
            .with_pattern(&key_pattern);
        let mut iter: AsyncIter<String> = scan_conn.scan_options(scan_opts).await?;

        while let Some(key) = iter.next_item().await {
            let key = match key {
                Ok(key) => key,
                Err(e) => {
                    error!(error = %e, "SCAN yielded an error entry, skipping");
                    continue;
                }
            };
            let Some(short_code) = key.strip_prefix(&key_prefix) else {
                error!(%key, "link stats key missing expected prefix, skipping");
                continue;
            };

            let fields = cmd_conn.hgetall(&key).await?;
            let (fresh, stale) = partition_daily_counts(fields, cutoff);

            for (date, count) in fresh {
                short_codes.push(short_code.to_string());
                dates.push(date);
                clicks.push(count);
            }

            if !stale.is_empty() {
                if let Err(e) = cmd_conn.hdel(&key, &stale).await {
                    warn!(error = %e, %key, "failed to trim stale day-buckets");
                }
            }
        }

        Ok((short_codes, dates, clicks))
    }

    /// Reads every `stats:global` hash in Redis, trims the old ones
    /// and return SoA (day, number of clicks)
    #[instrument(skip(self))]
    async fn get_global_stats(&self) -> anyhow::Result<(Vec<NaiveDate>, Vec<i64>)> {
        let cutoff = stale_cutoff();
        let key = RedisKeys::global_stats_key();

        let mut conn = self.redis.get().await?;
        let fields = conn.hgetall(&key).await?;
        let (fresh, stale) = partition_daily_counts(fields, cutoff);

        if !stale.is_empty() {
            if let Err(e) = conn.hdel(&key, &stale).await {
                warn!(error = %e, "failed to trim stale global day-buckets");
            }
        }

        Ok(fresh.into_iter().unzip())
    }

    /// Calculates the difference between global click counter in Redis and Postgres
    #[instrument(skip_all)]
    async fn compute_global_delta(&self, global_dates: &[NaiveDate], global_clicks: &[i64]) -> Option<i64> {
        let old_global_values = match db::get_global_daily_values(&self.db, global_dates).await {
            Ok(values) => values,
            Err(e) => {
                log_db_error(&e, "get_global_daily_values");
                return None;
            }
        };

        Some(
            global_dates
                .iter()
                .zip(global_clicks)
                .map(|(day, new_val)| new_val - old_global_values.get(day).copied().unwrap_or(0))
                .sum(),
        )
    }

    async fn apply_global_delta(&self, delta: i64) {
        if delta == 0 {
            return;
        }
        match db::update_global_click_count(&self.db, delta).await {
            Ok(()) => debug!(delta, "applied global click_count delta"),
            Err(e) => log_db_error(&e, "update_global_click_count"),
        }
    }

    /// Calculates the difference between per-link click counter in Redis and Postgres
    #[instrument(skip(self))]
    async fn compute_link_deltas(
        &self,
        link_short_codes: &[String],
        link_dates: &[NaiveDate],
        link_clicks: &[i64],
    ) -> Option<HashMap<String, i64>> {
        if link_short_codes.is_empty() {
            return Some(HashMap::new());
        }

        let old_link_values = match db::get_link_daily_values(&self.db, link_short_codes, link_dates).await {
            Ok(values) => values,
            Err(e) => {
                log_db_error(&e, "get_link_daily_values");
                return None;
            }
        };

        let mut link_deltas: HashMap<String, i64> = HashMap::new();
        for ((code, day), new_val) in link_short_codes.iter().zip(link_dates).zip(link_clicks) {
            let key = (code.clone(), *day);
            let old = old_link_values.get(&key).copied().unwrap_or(0);
            *link_deltas.entry(key.0).or_insert(0) += new_val - old;
        }
        link_deltas.retain(|_, delta| *delta != 0);

        Some(link_deltas)
    }

    async fn apply_link_deltas(&self, link_deltas: HashMap<String, i64>) {
        if link_deltas.is_empty() {
            return;
        }

        let link_codes: Vec<String> = link_deltas.keys().cloned().collect();
        let last_clicked = match self.redis.get().await {
            Ok(mut conn) => RedisStats::last_clicked_at_batch(&mut conn, &link_codes)
                .await
                .unwrap_or_else(|e| {
                    warn!(error = %e, "failed to fetch last_clicked_at batch, defaulting to no update");
                    Default::default()
                }),
            Err(e) => {
                warn!(error = %e, "failed to get redis connection for last_clicked_at batch");
                Default::default()
            }
        };

        let mut codes = Vec::with_capacity(link_deltas.len());
        let mut deltas = Vec::with_capacity(link_deltas.len());
        let mut last_clicked_ts = Vec::with_capacity(link_deltas.len());
        for (code, delta) in link_deltas {
            let ts = last_clicked.get(&code).cloned().unwrap_or_default();
            codes.push(code);
            deltas.push(delta);
            last_clicked_ts.push(ts);
        }

        match db::update_link_total_and_last_click(&self.db, &codes, &deltas, &last_clicked_ts).await {
            Ok(()) => debug!(links = codes.len(), "applied link click_count/last_clicked_at deltas"),
            Err(e) => log_db_error(&e, "update_link_total_and_last_click"),
        }
    }

    /// Snapshots Redis's 7-day click buckets into Postgres, and trims stale entries
    #[instrument(skip(self))]
    pub async fn snapshot_and_trim(&self) -> anyhow::Result<()> {
        let (link_short_codes, link_dates, link_clicks) = self.get_link_stats().await?;
        let (global_dates, global_clicks) = self.get_global_stats().await?;

        let global_delta = self.compute_global_delta(&global_dates, &global_clicks).await;
        let link_deltas = self
            .compute_link_deltas(&link_short_codes, &link_dates, &link_clicks)
            .await;

        if let Err(e) = db::update_link_daily_clicks(&self.db, &link_short_codes, &link_dates, &link_clicks).await {
            log_db_error(&e, "update_link_daily_clicks");
        }
        if let Err(e) = db::update_global_daily_clicks(&self.db, &global_dates, &global_clicks).await {
            log_db_error(&e, "update_global_daily_clicks");
        }

        if let Some(delta) = global_delta {
            self.apply_global_delta(delta).await;
        }
        if let Some(deltas) = link_deltas {
            self.apply_link_deltas(deltas).await;
        }

        Ok(())
    }
}

fn stale_cutoff() -> NaiveDate {
    (chrono::Utc::now() - Duration::days(ROLLING_WINDOW_DAYS + STALE_GRACE_DAYS)).date_naive()
}

/// Parses and splits Redis hash of `date_string -> click_count_string` fields into fresh entries and stale entries
fn partition_daily_counts(fields: HashMap<String, String>, cutoff: NaiveDate) -> (Vec<(NaiveDate, i64)>, Vec<String>) {
    let mut fresh = Vec::with_capacity(fields.len());
    let mut stale = Vec::new();

    for (date_str, clicks_str) in fields {
        let (date, clicks) = match (date_str.parse::<NaiveDate>(), clicks_str.parse::<i64>()) {
            (Ok(date), Ok(clicks)) => (date, clicks),
            _ => {
                warn!(date = %date_str, clicks = %clicks_str, "failed to parse Redis hash, skipping");
                continue;
            }
        };

        if date < cutoff {
            stale.push(date_str);
        } else {
            fresh.push((date, clicks));
        }
    }

    (fresh, stale)
}

fn log_db_error(err: &DbError, context: &str) {
    if err.is_transient() {
        warn!(error = %err, context, "transient database error, will retry next snapshot");
    } else {
        error!(error = %err, context, "database error");
    }
}
