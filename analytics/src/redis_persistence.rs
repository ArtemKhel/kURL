use chrono::NaiveDate;
use common::redis_keys::RedisKeys;
use redis::{AsyncIter, AsyncTypedCommands, ScanOptions};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

use crate::db;

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
    ) -> () {
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

    // TODO: ret type, consts, unwraps, err handling
    pub async fn snapshot_and_trim(&self) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let stale_cutoff = (now - chrono::Duration::weeks(1) - chrono::Duration::days(1)).date_naive();

        let all_stats_keys = RedisKeys::stats_key("*");
        let global_key = RedisKeys::global_stats_key();
        let link_prefix = RedisKeys::link_stats_key("");

        let mut stale = vec![];
        let mut link_short_codes = vec![];
        let mut link_dates = vec![];
        let mut link_clicks = vec![];
        let mut global_dates = vec![];
        let mut global_clicks = vec![];

        let mut conn = self.redis.get().await?;
        let mut conn_ = self.redis.get().await?;

        let scan_opts = ScanOptions::default().with_count(100).with_pattern(&all_stats_keys);
        let mut iter: AsyncIter<String> = conn.scan_options(scan_opts).await?;
        while let Some(key) = iter.next_item().await {
            let Ok(key) = key else {
                error!(?key);
                continue;
            };
            let short_code = (key != global_key).then(|| key.strip_prefix(&link_prefix).unwrap());

            let map = conn_.hgetall(&key).await?;
            for (date_str, clicks) in map {
                let date = date_str.parse::<NaiveDate>()?;
                let clicks = clicks.parse::<i64>()?;

                if date < stale_cutoff {
                    stale.push(date_str);
                } else {
                    if let Some(short_code) = short_code {
                        link_short_codes.push(short_code.to_string());
                        link_clicks.push(clicks);
                        link_dates.push(date);
                    } else {
                        global_clicks.push(clicks);
                        global_dates.push(date);
                    }
                }
            }

            let _ = conn_.hdel(key, &stale).await;
        }

        // dbg!(&link_short_codes,&link_dates,&link_clicks,&global_dates,&global_clicks,&stale,);
        let _ = db::update_link_daily_clicks(&self.db, &link_short_codes, &link_dates, &link_clicks).await;
        let _ = db::update_global_daily_clicks(&self.db, &global_dates, &global_clicks).await;

        Ok(())
    }
}
