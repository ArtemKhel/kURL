use chrono::NaiveDate;
use redis::{AsyncIter, AsyncTypedCommands, ScanOptions};

use crate::redis_keys::RedisKeys;

pub struct Persistence {
    db: sqlx::PgPool,
    redis: deadpool_redis::Pool,
}

impl Persistence {
    pub fn new(db: sqlx::PgPool, redis: deadpool_redis::Pool) -> Self { Self { db, redis } }

    // TODO: ret type, consts, unwraps
    pub async fn snapshot_and_trim(&self) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let stale_cutoff = (now - chrono::Duration::weeks(1) - chrono::Duration::days(1)).date_naive();

        let global_key = RedisKeys::global_key();
        let link_keys = RedisKeys::link_key("*");
        let link_prefix = link_keys.strip_suffix("*").unwrap();

        let mut stale = vec![];
        let mut link_short_codes = vec![];
        let mut link_dates = vec![];
        let mut link_clicks = vec![];

        let mut conn = self.redis.get().await?;
        let mut conn_ = self.redis.get().await?;

        let scan_opts = ScanOptions::default().with_count(100).with_pattern(&link_keys);
        let mut iter: AsyncIter<String> = conn.scan_options(scan_opts).await?;
        while let Some(key) = iter.next_item().await {
            let key = key?;
            let short_code = key.strip_prefix(&link_prefix).unwrap();

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
        }

        let res = sqlx::query!(
            r#"
            insert into link_daily_clicks (short_code, day, clicks)
            select * from unnest($1::text[], $2::date[], $3::bigint[])
            on conflict (short_code, day) do update set clicks = excluded.clicks
            "#,
            &link_short_codes,
            &link_dates,
            &link_clicks
        )
        .execute(&self.db).await?;

        Ok(())
    }
}
