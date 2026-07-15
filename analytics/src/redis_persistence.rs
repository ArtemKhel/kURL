use std::{borrow::Borrow, collections::HashMap, ops::Deref};

use chrono::{DateTime, Duration, NaiveDate};
use common::redis_keys::RedisKeys;
use redis::{AsyncIter, AsyncTypedCommands, ScanOptions};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, instrument};

use crate::{db, redis_stats::RedisStats};

// todo: from config
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

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

    #[instrument(skip(self))]
    async fn get_link_stats(&self) -> anyhow::Result<(Vec<String>, Vec<NaiveDate>, Vec<i64>)> {
        let now = chrono::Utc::now();
        let stale_cutoff = (now - Duration::weeks(1) - Duration::days(1)).date_naive();

        let all_link_stats_keys = RedisKeys::link_stats_key("*");
        let link_prefix = RedisKeys::link_stats_key("");

        let mut stale = vec![];
        let mut link_short_codes = vec![];
        let mut link_dates = vec![];
        let mut link_clicks = vec![];

        let mut conn = self.redis.get().await?;
        let mut conn_ = self.redis.get().await?;

        let scan_opts = ScanOptions::default()
            .with_count(100)
            .with_pattern(&all_link_stats_keys);
        let mut iter: AsyncIter<String> = conn.scan_options(scan_opts).await?;

        while let Some(key) = iter.next_item().await {
            let Ok(key) = key else {
                error!(?key);
                continue;
            };
            let Some(short_code) = key.strip_prefix(&link_prefix) else {
                error!(?key, "Failed to strip prefix");
                continue;
            };

            let map = conn_.hgetall(&key).await?;
            for (date_str, clicks) in map {
                let date = date_str.parse::<NaiveDate>()?;
                let clicks = clicks.parse::<i64>()?;

                if date < stale_cutoff {
                    stale.push(date_str);
                } else {
                    link_short_codes.push(short_code.to_string());
                    link_clicks.push(clicks);
                    link_dates.push(date);
                }
            }
            let _ = conn_.hdel(key, &stale).await;
        }
        Ok((link_short_codes, link_dates, link_clicks))
    }

    #[instrument(skip(self))]
    async fn get_global_stats(&self) -> anyhow::Result<(Vec<NaiveDate>, Vec<i64>)> {
        let now = chrono::Utc::now();
        let stale_cutoff = (now - Duration::weeks(1) - Duration::days(1)).date_naive();

        let global_key = RedisKeys::global_stats_key();

        let mut stale = vec![];
        let mut global_dates = vec![];
        let mut global_clicks = vec![];

        let mut conn = self.redis.get().await?;

        let map = conn.hgetall(&global_key).await?;
        for (date_str, clicks) in map {
            let date = date_str.parse::<NaiveDate>()?;
            let clicks = clicks.parse::<i64>()?;

            if date < stale_cutoff {
                stale.push(date_str);
            } else {
                global_clicks.push(clicks);
                global_dates.push(date);
            }
        }
        let _ = conn.hdel(&global_key, &stale).await;

        Ok((global_dates, global_clicks))
    }

    #[instrument(skip_all)]
    async fn update_global_click_count(&self, global_dates: &[NaiveDate], global_clicks: &[i64]) -> anyhow::Result<()> {
        let old_global_values: HashMap<NaiveDate, i64> =
            db::get_global_daily_values(&self.db, &global_dates).await.unwrap();

        let global_delta: i64 = global_dates
            .iter()
            .zip(global_clicks)
            .map(|(day, new_val)| new_val - old_global_values.get(day).copied().unwrap_or(0))
            .sum();

        if let Err(e) = db::update_global_click_count(&self.db, global_delta).await {
            error!(error = %e, "failed to apply global_delta");
        }

        Ok(())
    }

    async fn update_link_click_count(
        &self,
        link_short_codes: &[String],
        link_dates: &[NaiveDate],
        link_clicks: &[i64],
    ) -> anyhow::Result<()> {
        // 2. Fetch the previously-durable values for exactly the rows we're
        // about to touch, so we can compute what changed since last snapshot.
        let old_link_values: HashMap<(String, NaiveDate), i64> = if !link_short_codes.is_empty() {
            db::get_link_daily_values(&self.db, &link_short_codes, &link_dates)
                .await
                .unwrap()
        } else {
            HashMap::new()
        };

        // 3. Deltas = new - old (defaulting old to 0 for a day never seen before)
        let mut link_deltas: HashMap<String, i64> = HashMap::new();
        for ((code, day), new_val) in link_short_codes
            .iter()
            .cloned()
            .zip(link_dates)
            .zip(link_clicks)
            .map(|((c, d), v)| ((c, d), v))
        {
            // todo: add type for hm key that allows borrowing
            let old = old_link_values.get(&(code.clone(), day.clone())).copied().unwrap_or(0);
            *link_deltas.entry(code).or_insert(0) += new_val - old;
        }

        // 4. Fetch and flush last_clicked_at as an absolute overwrite — same
        // idempotent pattern as the day buckets, no delta needed.
        let codes: Vec<String> = link_deltas.keys().cloned().collect();
        let mut conn = self.redis.get().await?;
        let last_clicked = RedisStats::last_clicked_at_batch(&mut conn, &codes)
            .await
            .unwrap_or_default(); // todo:

        // 5. Apply everything to Postgres in one batch.
        let (upd_codes, upd_deltas, upd_ts): (Vec<_>, Vec<_>, Vec<_>) = link_deltas
            .into_iter()
            .filter(|(_, d)| *d != 0)
            .map(|(code, delta)| {
                let ts = last_clicked.get(&code).cloned().unwrap_or_default();
                (code, delta, ts)
            })
            .fold((Vec::new(), Vec::new(), Vec::new()), |mut acc, (c, d, t)| {
                acc.0.push(c);
                acc.1.push(d);
                acc.2.push(t);
                acc
            });

        if !upd_codes.is_empty() {
            let result = db::update_link_total_and_last_click(&self.db, &upd_codes, &upd_deltas, &upd_ts).await;
            match result {
                Ok(r) => debug!("applied click_count/last_clicked_at deltas"),
                Err(e) => error!(error = %e, "failed to apply click_count deltas"),
            }
        }

        Ok(())
    }

    // TODO: ret type, consts, unwraps, error handling, retry, unwraps, clones
    #[instrument(skip(self))]
    pub async fn snapshot_and_trim(&self) -> anyhow::Result<()> {
        let (link_short_codes, link_dates, link_clicks) = self.get_link_stats().await?;
        let (global_dates, global_clicks) = self.get_global_stats().await?;

        db::update_link_daily_clicks(&self.db, &link_short_codes, &link_dates, &link_clicks).await;
        db::update_global_daily_clicks(&self.db, &global_dates, &global_clicks).await;

        self.update_global_click_count(&global_dates, &global_clicks).await;
        self.update_link_click_count(&link_short_codes, &link_dates, &link_clicks)
            .await;

        Ok(())
    }
}
