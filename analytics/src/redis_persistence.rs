use std::collections::{HashMap, HashSet};

use anyhow::Context;
use chrono::{Duration, NaiveDate, Utc};
use common::{db_utils::DbError, redis_keys::RedisKeys};
use redis::{AsyncIter, AsyncTypedCommands, ScanOptions};
use tracing::{error, info, instrument, warn};

use crate::{
    db::{AnalyticsRepository, SnapshotRepository},
    redis_stats::RedisStats,
    snapshot::RedisSnapshot,
};

// todo: from config
const ROLLING_WINDOW_DAYS: i64 = 7;
const STALE_GRACE_DAYS: i64 = 1;
const SCAN_BATCH_SIZE: usize = 100;

// TODO: metrics
pub struct Persistence {
    db: sqlx::PgPool,
    redis: deadpool_redis::Pool,
}

impl Persistence {
    pub fn new(db: sqlx::PgPool, redis: deadpool_redis::Pool) -> Self { Self { db, redis } }

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
    pub async fn flush(&self) -> anyhow::Result<()> { todo!() }

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

            metrics::counter!("analytics.malformed_redis_fields").increment(malformed.len() as u64)
        }

        // Global daily stats
        let global_key = RedisKeys::global_stats_key();
        let global_fields: HashMap<String, String> = cmd_conn.hgetall(&global_key).await?;
        let (global_parsed, global_malformed) = parse_daily_counts(global_fields);

        snapshot.global_daily = global_parsed;
        metrics::counter!("analytics.malformed_redis_fields").increment(global_malformed.len() as u64);

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
            .context("failed to fetch last clicked at timestamps")?;

        // Stales
        snapshot.stale_global = stale_global_fields(&snapshot.global_daily, cutoff);
        snapshot.stale_links = stale_link_fields(&snapshot.link_daily, cutoff);

        Ok(snapshot)
    }

    // async fn rehydrate_global(&self, window_start: NaiveDate) {
    //     let snapshot = match db::get_global_daily_clicks_since(&self.db, window_start).await {
    //         Ok(rows) => rows,
    //         Err(e) => return log_db_error(&e, "get_global_daily_clicks_since (rehydrate)"),
    //     };
    //     if snapshot.is_empty() {
    //         return;
    //     }
    //
    //     let key = RedisKeys::global_stats_key();
    //     let mut conn = match self.redis.get().await {
    //         Ok(conn) => conn,
    //         Err(e) => return warn!(error = %e, "failed to get redis connection for rehydrate"),
    //     };
    //
    //     let existing: HashMap<String, String> = conn.hgetall(&key).await.unwrap_or_default();
    //     let missing: Vec<(String, String)> = snapshot
    //         .into_iter()
    //         .filter(|(date, _)| !existing.contains_key(&date.to_string()))
    //         .map(|(date, clicks)| (date.to_string(), clicks.to_string()))
    //         .collect();
    //
    //     if missing.is_empty() {
    //         return debug!("global day-buckets already present in redis, nothing to rehydrate");
    //     }
    //
    //     match conn.hset_multiple(&key, &missing).await {
    //         Ok(()) => info!(fields = missing.len(), "rehydrated global day-buckets from postgres"),
    //         Err(e) => warn!(error = %e, "failed to write rehydrated global day-buckets"),
    //     }
    // }
    //
    // async fn rehydrate_links(&self, window_start: NaiveDate) {
    //     let snapshot = match db::get_link_daily_clicks_since(&self.db, window_start).await {
    //         Ok(rows) => rows, // Vec<(short_code, NaiveDate, i64)>
    //         Err(e) => return log_db_error(&e, "get_link_daily_clicks_since (rehydrate)"),
    //     };
    //     if snapshot.is_empty() {
    //         return;
    //     }
    //
    //     let mut by_link: HashMap<String, Vec<(NaiveDate, i64)>> = HashMap::new();
    //     for (code, date, clicks) in snapshot {
    //         by_link.entry(code).or_default().push((date, clicks));
    //     }
    //
    //     let mut conn = match self.redis.get().await {
    //         Ok(conn) => conn,
    //         Err(e) => return warn!(error = %e, "failed to get redis connection for rehydrate"),
    //     };
    //
    //     let mut rehydrated = 0usize;
    //     // todo: pipeline maybe?
    //     for (code, rows) in by_link {
    //         let key = RedisKeys::link_stats_key(&code);
    //         let existing: HashMap<String, String> = conn.hgetall(&key).await.unwrap_or_default();
    //
    //         let missing: Vec<(String, String)> = rows
    //             .into_iter()
    //             .filter(|(date, _)| !existing.contains_key(&date.to_string()))
    //             .map(|(date, clicks)| (date.to_string(), clicks.to_string()))
    //             .collect();
    //
    //         if missing.is_empty() {
    //             continue;
    //         }
    //         match conn.hset_multiple(&key, &missing).await {
    //             Ok(()) => rehydrated += 1,
    //             Err(e) => warn!(error = %e, %code, "failed to write rehydrated link day-buckets"),
    //         }
    //     }
    //
    //     if rehydrated > 0 {
    //         info!(
    //             rehydrated_links = rehydrated,
    //             "rehydrated link day-buckets from postgres"
    //         );
    //     }
    // }
    //
    // /// Reads every `stats:link:*` hash in Redis, trims the old ones
    // /// and return SoA (short_code, day, number of clicks)
    // #[instrument(skip(self))]
    // async fn get_link_stats(&self) -> anyhow::Result<(Vec<String>, Vec<NaiveDate>, Vec<i64>)> {
    //     let cutoff = stale_cutoff();
    //     let key_pattern = RedisKeys::link_stats_key("*");
    //     let key_prefix = RedisKeys::link_stats_key("");
    //
    //     let mut short_codes = Vec::new();
    //     let mut dates = Vec::new();
    //     let mut clicks = Vec::new();
    //
    //     let mut scan_conn = self.redis.get().await?;
    //     let mut cmd_conn = self.redis.get().await?;
    //
    //     let scan_opts = ScanOptions::default()
    //         .with_count(SCAN_BATCH_SIZE)
    //         .with_pattern(&key_pattern);
    //     let mut iter: AsyncIter<String> = scan_conn.scan_options(scan_opts).await?;
    //
    //     while let Some(key) = iter.next_item().await {
    //         let key = match key {
    //             Ok(key) => key,
    //             Err(e) => {
    //                 error!(error = %e, "SCAN yielded an error entry, skipping");
    //                 continue;
    //             }
    //         };
    //         let Some(short_code) = key.strip_prefix(&key_prefix) else {
    //             error!(%key, "link stats key missing expected prefix, skipping");
    //             continue;
    //         };
    //
    //         let fields = cmd_conn.hgetall(&key).await?;
    //         let (fresh, stale) = partition_daily_counts(fields, cutoff);
    //
    //         for (date, count) in fresh {
    //             short_codes.push(short_code.to_string());
    //             dates.push(date);
    //             clicks.push(count);
    //         }
    //
    //         if !stale.is_empty()
    //             && let Err(e) = cmd_conn.hdel(&key, &stale).await
    //         {
    //             warn!(error = %e, %key, "failed to trim stale day-buckets");
    //         };
    //     }
    //
    //     Ok((short_codes, dates, clicks))
    // }
    //
    // /// Reads every `stats:global` hash in Redis, trims the old ones
    // /// and return SoA (day, number of clicks)
    // #[instrument(skip(self))]
    // async fn get_global_stats(&self) -> anyhow::Result<(Vec<NaiveDate>, Vec<i64>)> {
    //     let cutoff = stale_cutoff();
    //     let key = RedisKeys::global_stats_key();
    //
    //     let mut conn = self.redis.get().await?;
    //     let fields = conn.hgetall(&key).await?;
    //     let (fresh, stale) = partition_daily_counts(fields, cutoff);
    //
    //     if !stale.is_empty()
    //         && let Err(e) = conn.hdel(&key, &stale).await
    //     {
    //         warn!(error = %e, "failed to trim stale global day-buckets");
    //     }
    //
    //     Ok(fresh.into_iter().unzip())
    // }
    //
    // /// Calculates the difference between global click counter in Redis and Postgres
    // #[instrument(skip_all)]
    // async fn compute_global_delta(&self, global_dates: &[NaiveDate], global_clicks: &[i64]) -> Option<i64> {
    //     let old_global_values = match db::get_global_daily_values(&self.db, global_dates).await {
    //         Ok(values) => values,
    //         Err(e) => {
    //             log_db_error(&e, "get_global_daily_values");
    //             return None;
    //         }
    //     };
    //
    //     Some(
    //         global_dates
    //             .iter()
    //             .zip(global_clicks)
    //             .map(|(day, new_val)| new_val - old_global_values.get(day).copied().unwrap_or(0))
    //             .sum(),
    //     )
    // }
    //
    // async fn apply_global_delta(&self, delta: i64) {
    //     if delta == 0 {
    //         return;
    //     }
    //     match db::update_global_click_count(&self.db, delta).await {
    //         Ok(()) => debug!(delta, "applied global click_count delta"),
    //         Err(e) => log_db_error(&e, "update_global_click_count"),
    //     }
    // }
    //
    // /// Calculates the difference between per-link click counter in Redis and Postgres
    // #[instrument(skip(self))]
    // async fn compute_link_deltas(
    //     &self,
    //     link_short_codes: &[String],
    //     link_dates: &[NaiveDate],
    //     link_clicks: &[i64],
    // ) -> Option<HashMap<String, i64>> {
    //     if link_short_codes.is_empty() {
    //         return Some(HashMap::new());
    //     }
    //
    //     let old_link_values = match db::get_link_daily_values(&self.db, link_short_codes, link_dates).await {
    //         Ok(values) => values,
    //         Err(e) => {
    //             log_db_error(&e, "get_link_daily_values");
    //             return None;
    //         }
    //     };
    //
    //     let mut link_deltas: HashMap<String, i64> = HashMap::new();
    //     for ((code, day), new_val) in link_short_codes.iter().zip(link_dates).zip(link_clicks) {
    //         let key = (code.clone(), *day);
    //         let old = old_link_values.get(&key).copied().unwrap_or(0);
    //         *link_deltas.entry(key.0).or_insert(0) += new_val - old;
    //     }
    //     link_deltas.retain(|_, delta| *delta != 0);
    //
    //     Some(link_deltas)
    // }
    //
    // async fn apply_link_deltas(&self, link_deltas: HashMap<String, i64>) {
    //     if link_deltas.is_empty() {
    //         return;
    //     }
    //
    //     let link_codes: Vec<String> = link_deltas.keys().cloned().collect();
    //     let last_clicked = match self.redis.get().await {
    //         Ok(mut conn) => RedisStats::last_clicked_at_batch(&mut conn, &link_codes)
    //             .await
    //             .unwrap_or_else(|e| {
    //                 warn!(error = %e, "failed to fetch last_clicked_at batch, defaulting to no update");
    //                 Default::default()
    //             }),
    //         Err(e) => {
    //             warn!(error = %e, "failed to get redis connection for last_clicked_at batch");
    //             Default::default()
    //         }
    //     };
    //
    //     let mut codes = Vec::with_capacity(link_deltas.len());
    //     let mut deltas = Vec::with_capacity(link_deltas.len());
    //     let mut last_clicked_ts = Vec::with_capacity(link_deltas.len());
    //     for (code, delta) in link_deltas {
    //         let ts = last_clicked.get(&code).cloned().unwrap_or_default();
    //         codes.push(code);
    //         deltas.push(delta);
    //         last_clicked_ts.push(ts);
    //     }
    //
    //     match db::update_link_total_and_last_click(&self.db, &codes, &deltas, &last_clicked_ts).await {
    //         Ok(()) => debug!(links = codes.len(), "applied link click_count/last_clicked_at deltas"),
    //         Err(e) => log_db_error(&e, "update_link_total_and_last_click"),
    //     }
    // }
    //
    // /// Snapshots Redis's 7-day click buckets into Postgres, and trims stale entries
    // #[instrument(skip(self))]
    // pub async fn snapshot_and_trim(&self) -> anyhow::Result<()> {
    //     let (link_short_codes, link_dates, link_clicks) = self.get_link_stats().await?;
    //     let (global_dates, global_clicks) = self.get_global_stats().await?;
    //
    //     let global_delta = self.compute_global_delta(&global_dates, &global_clicks).await;
    //     let link_deltas = self
    //         .compute_link_deltas(&link_short_codes, &link_dates, &link_clicks)
    //         .await;
    //
    //     if let Err(e) = db::update_link_daily_clicks(&self.db, &link_short_codes, &link_dates, &link_clicks).await {
    //         log_db_error(&e, "update_link_daily_clicks");
    //     }
    //     if let Err(e) = db::update_global_daily_clicks(&self.db, &global_dates, &global_clicks).await {
    //         log_db_error(&e, "update_global_daily_clicks");
    //     }
    //
    //     if let Some(delta) = global_delta {
    //         self.apply_global_delta(delta).await;
    //     }
    //     if let Some(deltas) = link_deltas {
    //         self.apply_link_deltas(deltas).await;
    //     }
    //
    //     Ok(())
    // }
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
