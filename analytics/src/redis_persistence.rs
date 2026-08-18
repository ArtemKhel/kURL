use std::collections::{HashMap, HashSet};

use anyhow::Context;
use chrono::{Duration, NaiveDate, Utc};
use common::{db_utils::DbError, redis_keys::RedisKeys};
use redis::{AsyncIter, AsyncTypedCommands, ScanOptions};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    db::SnapshotRepository,
    redis_stats::RedisStats,
    snapshot::{MergeOutcome, RedisSnapshot},
};

// todo: from config
const ROLLING_WINDOW_DAYS: i64 = 7;
const STALE_GRACE_DAYS: i64 = 1;
const SCAN_BATCH_SIZE: usize = 100;

// TODO: metrics
pub struct Persistence<SR: SnapshotRepository> {
    db: SR,
    redis: deadpool_redis::Pool,
}

impl<SR: SnapshotRepository> Persistence<SR> {
    pub fn new(db: SR, redis: deadpool_redis::Pool) -> Self { Self { db, redis } }

    #[instrument(skip_all)]
    pub async fn rehydrate(&self) -> anyhow::Result<()> {
        let window_start = rolling_window_start();

        let data = self
            .db
            .get_daily_clicks_since(window_start)
            .await
            .context("failed to get data for rehydration")?;

        let mut conn = self.redis.get().await?;

        let global_stats_key = RedisKeys::global_stats_key();
        for stat in &data.global_daily {
            let date = stat.day.to_string();
            if let Err(error) = RedisStats::hmax(&mut conn, &global_stats_key, &date, stat.clicks).await {
                warn!(%error, date, "failed to rehydrate global day-bucket");
            }
        }
        info!(
            buckets = data.global_daily.len(),
            "rehydrated global day-buckets from db"
        );

        for stat in &data.link_daily {
            let key = RedisKeys::link_stats_key(&stat.short_code);
            let date = stat.day.to_string();
            if let Err(error) = RedisStats::hmax(&mut conn, &key, &date, stat.clicks).await {
                warn!(%error, short_code = %stat.short_code, date, "failed to rehydrate link day-bucket");
            }
        }
        info!(buckets = data.link_daily.len(), "rehydrated link day-buckets from db");

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn flush(&self) -> anyhow::Result<()> {
        let snapshot = self.capture_snapshot().await?;

        if snapshot.global_daily.is_empty() && snapshot.link_daily.is_empty() {
            metrics::counter!("analytics.snapshot_success_total").increment(1);
            return Ok(());
        }

        let outcome = match self.db.merge_snapshot(&snapshot).await {
            Ok(outcome) => outcome,
            Err(error) => {
                metrics::counter!("analytics.snapshot_failures_total").increment(1);
                anyhow::bail!(error);
            }
        };

        if let Err(error) = self.reconcile_and_cleanup(&snapshot, &outcome).await {
            warn!(%error, "post-commit Redis reconciliation/cleanup failed, will retry next snapshot");
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn capture_snapshot(&self) -> anyhow::Result<RedisSnapshot> {
        let cutoff = stale_cutoff();
        let key_pattern = RedisKeys::link_stats_key("*");
        let key_prefix = RedisKeys::link_stats_key("");

        let mut snapshot = RedisSnapshot::default();

        // Per-link daily stats
        let mut scan_conn = self.redis.get().await.context("failed to get redis connection")?;
        let mut cmd_conn = self.redis.get().await.context("failed to get redis connection")?;

        let scan_opts = ScanOptions::default()
            .with_count(SCAN_BATCH_SIZE)
            .with_pattern(&key_pattern);
        let mut iter: AsyncIter<String> = scan_conn.scan_options(scan_opts).await?;

        while let Some(key) = iter.next_item().await {
            let key = match key {
                Ok(key) => key,
                Err(e) => {
                    error!(error = %e, "SCAN yielded an error entry, aborting");
                    return Err(e.into());
                }
            };
            let Some(short_code) = key.strip_prefix(&key_prefix) else {
                error!(%key, "link stats key missing expected prefix, skipping");
                continue;
            };

            let fields = cmd_conn.hgetall(&key).await?;
            let (parsed, malformed) = parse_daily_counts(fields);

            for (date, count) in parsed {
                snapshot.link_daily.insert((short_code.to_string(), date), count);
            }

            if !malformed.is_empty() {
                metrics::counter!("analytics.malformed_redis_fields").increment(malformed.len() as u64);
                warn!(?malformed, "malformed fields in redis");
            }
        }

        // Global daily stats
        let global_key = RedisKeys::global_stats_key();
        let global_fields: HashMap<String, String> = cmd_conn.hgetall(&global_key).await?;
        let (global_parsed, global_malformed) = parse_daily_counts(global_fields);

        snapshot.global_daily = global_parsed;

        if !global_malformed.is_empty() {
            metrics::counter!("analytics.malformed_redis_fields").increment(global_malformed.len() as u64);
            warn!(?global_malformed, "malformed fields in redis");
        }

        // Last clicked ats
        let link_codes: Vec<String> = snapshot
            .link_daily
            .keys()
            .map(|(code, _)| code.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        snapshot.last_clicked_at = RedisStats::last_clicked_at_batch(&mut cmd_conn, &link_codes)
            .await
            .context("failed to fetch 'last_clicked_at' timestamps")?;

        // Stales
        snapshot.stale_global = stale_global_fields(&snapshot.global_daily, cutoff);
        snapshot.stale_links = stale_link_fields(&snapshot.link_daily, cutoff);

        Ok(snapshot)
    }

    #[instrument(skip_all)]
    async fn reconcile_and_cleanup(&self, snapshot: &RedisSnapshot, outcome: &MergeOutcome) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await.context("failed to get redis connection")?;

        let global_key = RedisKeys::global_stats_key();
        let mut global_reconciled = HashSet::new();

        for (date, count) in &outcome.committed_global {
            let date_str = date.to_string();
            match RedisStats::hmax(&mut conn, &global_key, &date_str, *count).await {
                Ok(_) => {
                    global_reconciled.insert(date);
                }
                Err(error) => {
                    warn!(%error, %date, "failed to update global day-bucket after merge");
                }
            }
        }

        for date in &snapshot.stale_global {
            if !global_reconciled.contains(&date) {
                debug!(%date, "skipping stale global cleanup: reconciliation failed");
                continue;
            }
            let committed_count = outcome.committed_global.get(date).copied().unwrap_or(0);
            let date_str = date.to_string();
            let expected = committed_count.to_string();
            match RedisStats::compare_and_delete(&mut conn, &global_key, &date_str, &expected).await {
                Ok(true) => debug!(%date, "deleted stale global day-bucket"),
                Ok(false) => debug!(%date, "stale global day-bucket changed, retained for next snapshot"),
                Err(error) => {
                    warn!(%error, %date, "failed to cleanup stale global day-bucket after merge");
                }
            }
        }

        let mut links_reconciled = HashSet::new();

        for ((code, date), count) in &outcome.committed_links {
            let link_key = RedisKeys::link_stats_key(code);
            let date_str = date.to_string();
            match RedisStats::hmax(&mut conn, &link_key, &date_str, *count).await {
                Ok(_) => {
                    links_reconciled.insert((code, *date));
                }
                Err(error) => {
                    warn!(%error, code, %date, "failed to update link stats key after merge");
                }
            }
        }

        for (code, date) in &snapshot.stale_links {
            if !links_reconciled.contains(&(code, *date)) {
                debug!(%code, %date, "skipping stale link cleanup: reconciliation failed");
                continue;
            }
            let link_key = RedisKeys::link_stats_key(code);
            let date_str = date.to_string();
            let expected_committed_count = outcome
                .committed_links
                .get(&(code.clone(), *date))
                .copied()
                .unwrap_or(0)
                .to_string();
            match RedisStats::compare_and_delete(&mut conn, &link_key, &date_str, &expected_committed_count).await {
                Ok(true) => debug!(%code, %date, "deleted stale link day-bucket"),
                Ok(false) => debug!(%code, %date, "stale link day-bucket changed, retained for next snapshot"),
                Err(error) => {
                    warn!(%error, code, %date, "failed to cleanup stale link day-bucket after merge");
                }
            }
        }

        Ok(())
    }
}

// todo: move?
#[derive(Debug)]
pub struct MalformedField {
    pub key: String,
    pub value: String,
    pub reason: &'static str,
}

fn rolling_window_start() -> NaiveDate { (Utc::now() - Duration::days(ROLLING_WINDOW_DAYS)).date_naive() }

fn stale_cutoff() -> NaiveDate { (Utc::now() - Duration::days(ROLLING_WINDOW_DAYS + STALE_GRACE_DAYS)).date_naive() }

pub fn stale_global_fields(daily: &HashMap<NaiveDate, i64>, cutoff: NaiveDate) -> HashSet<NaiveDate> {
    daily.keys().filter(|&d| *d < cutoff).copied().collect()
}

pub fn stale_link_fields(daily: &HashMap<(String, NaiveDate), i64>, cutoff: NaiveDate) -> HashSet<(String, NaiveDate)> {
    daily.keys().filter(|(_, d)| *d < cutoff).cloned().collect()
}

fn parse_daily_counts(fields: HashMap<String, String>) -> (HashMap<NaiveDate, i64>, Vec<MalformedField>) {
    let mut valid = HashMap::with_capacity(fields.len());
    let mut malformed = Vec::new();

    for (date_str, count_str) in fields {
        let Ok(date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") else {
            malformed.push(MalformedField {
                key: date_str,
                value: count_str,
                reason: "invalid date format",
            });
            continue;
        };
        let clicks = match count_str.parse::<i64>() {
            Ok(c) if c >= 0 => c,
            Ok(_) => {
                malformed.push(MalformedField {
                    key: date_str,
                    value: count_str,
                    reason: "negative count",
                });
                continue;
            }
            Err(_) => {
                malformed.push(MalformedField {
                    key: date_str,
                    value: count_str,
                    reason: "invalid count",
                });
                continue;
            }
        };
        valid.insert(date, clicks);
    }
    (valid, malformed)
}

fn log_db_error(err: &DbError, context: &str) {
    if err.is_transient() {
        warn!(error = %err, context, "transient database error, will retry next snapshot");
    } else {
        error!(error = %err, context, "database error");
    }
}
