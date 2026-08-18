use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use common::db_utils::DbError;
use tracing::debug;

use crate::{
    db::{SnapshotRepository, get_global_daily_clicks_since, get_link_daily_clicks_since},
    snapshot,
    snapshot::{MergeOutcome, RedisSnapshot, RehydrationData},
};

#[derive(Debug)]
pub struct SnapshotRepoPg {
    pool: sqlx::PgPool,
}
impl SnapshotRepoPg {
    pub fn new(pool: sqlx::PgPool) -> Self { Self { pool } }
}

#[async_trait::async_trait]
impl SnapshotRepository for SnapshotRepoPg {
    async fn get_daily_clicks_since(&self, since: NaiveDate) -> Result<RehydrationData, DbError> {
        let global_daily = get_global_daily_clicks_since(&self.pool, since).await?;
        let link_daily = get_link_daily_clicks_since(&self.pool, since).await?;
        Ok(RehydrationData {
            global_daily,
            link_daily,
        })
    }

    async fn merge_snapshot(&self, snapshot: &RedisSnapshot) -> Result<MergeOutcome, DbError> {
        let mut outcome = MergeOutcome::default();

        if snapshot.global_daily.is_empty() && snapshot.link_daily.is_empty() {
            return Ok(outcome);
        }

        let mut tx = self.pool.begin().await.map_err(DbError::from)?;

        if !snapshot.global_daily.is_empty() {
            let dates: Vec<NaiveDate> = snapshot.global_daily.keys().copied().collect();

            let existing_global: HashMap<NaiveDate, i64> = sqlx::query_as::<_, (NaiveDate, i64)>(
                r#"
                    select day, clicks
                    from global_daily_clicks
                    where day = any ($1) for update
                "#,
            )
            .bind(&dates)
            .fetch_all(&mut *tx)
            .await
            .map(|rows| rows.into_iter().collect())
            .map_err(DbError::from)?;

            let mut upsert_dates = Vec::with_capacity(snapshot.global_daily.len());
            let mut upsert_clicks = Vec::with_capacity(snapshot.global_daily.len());
            let mut total_global_delta: i64 = 0;

            for (&date, &incoming) in &snapshot.global_daily {
                let existing = existing_global.get(&date).copied().unwrap_or(0);
                let (committed, delta) = snapshot::calculate_merge_delta(existing, incoming);
                upsert_dates.push(date);
                upsert_clicks.push(committed);
                total_global_delta += delta;
                outcome.committed_global.insert(date, committed);
            }

            if !upsert_dates.is_empty() {
                sqlx::query(
                    r#"
                        insert into global_daily_clicks as global (day, clicks)
                        select *
                        from unnest($1::date[], $2::bigint[])
                        on conflict (day) do update set clicks = greatest(excluded.clicks, global.clicks)
                    "#,
                )
                .bind(&upsert_dates)
                .bind(&upsert_clicks)
                .execute(&mut *tx)
                .await
                .map_err(DbError::from)?;
            }

            if total_global_delta > 0 {
                sqlx::query(
                    r#"
                        update analytics_global
                        set total_clicks = total_clicks + $1
                    "#,
                )
                .bind(total_global_delta)
                .execute(&mut *tx)
                .await
                .map_err(DbError::from)?;
            }

            outcome.global_delta = total_global_delta;
        }

        if !snapshot.link_daily.is_empty() {
            let (lock_codes, lock_dates): (Vec<String>, Vec<NaiveDate>) = snapshot
                .link_daily
                .keys()
                .map(|(code, date)| (code.clone(), *date))
                .unzip();

            let existing_links: HashMap<(String, NaiveDate), i64> = sqlx::query_as::<_, (String, NaiveDate, i64)>(
                r#"
                    select short_code, day, clicks
                    from link_daily_clicks
                    where (short_code, day) in (select * from unnest($1::text[], $2::date[])) for update
                "#,
            )
            .bind(&lock_codes)
            .bind(&lock_dates)
            .fetch_all(&mut *tx)
            .await
            .map(|rows| rows.into_iter().map(|(s, d, c)| ((s, d), c)).collect())
            .map_err(DbError::from)?;

            let mut upsert_codes = Vec::with_capacity(snapshot.link_daily.len());
            let mut upsert_dates = Vec::with_capacity(snapshot.link_daily.len());
            let mut upsert_clicks = Vec::with_capacity(snapshot.link_daily.len());

            for ((code, date), &incoming) in &snapshot.link_daily {
                let key = (code.clone(), *date);
                let existing = existing_links.get(&key).copied().unwrap_or(0);
                let (committed, delta) = snapshot::calculate_merge_delta(existing, incoming);
                upsert_codes.push(code.clone());
                upsert_dates.push(*date);
                upsert_clicks.push(committed);
                *outcome.link_deltas.entry(code.clone()).or_insert(0) += delta;
                outcome.committed_links.insert(key, committed);
            }

            if !upsert_codes.is_empty() {
                sqlx::query(
                    r#"
                        insert into link_daily_clicks as daily (short_code, day, clicks)
                        select short_code, day, clicks
                        from unnest($1::text[], $2::date[], $3::bigint[]) as incoming(short_code, day, clicks)
                        where exists (select 1 from links where short_code = incoming.short_code)
                        on conflict (short_code, day) do update set clicks = greatest(excluded.clicks, daily.clicks)
                   "#,
                )
                .bind(&upsert_codes)
                .bind(&upsert_dates)
                .bind(&upsert_clicks)
                .execute(&mut *tx)
                .await
                .map_err(DbError::from)?;
            }

            let mut delta_codes = Vec::new();
            let mut delta_counts = Vec::new();
            let mut delta_timestamps: Vec<DateTime<Utc>> = Vec::new();

            for (code, &delta) in &outcome.link_deltas {
                if delta > 0 || snapshot.last_clicked_at.contains_key(code) {
                    delta_codes.push(code.clone());
                    delta_counts.push(delta.max(0));
                    delta_timestamps.push(
                        snapshot
                            .last_clicked_at
                            .get(code)
                            .copied()
                            .unwrap_or(DateTime::UNIX_EPOCH),
                    );
                }
            }

            for (code, &ts) in &snapshot.last_clicked_at {
                if !outcome.link_deltas.contains_key(code) {
                    delta_codes.push(code.clone());
                    delta_counts.push(0);
                    delta_timestamps.push(ts);
                }
            }

            if !delta_codes.is_empty() {
                sqlx::query(
                    r#"
                        update links
                        set click_count     = links.click_count + incoming.delta,
                            last_clicked_at = greatest(links.last_clicked_at, incoming.ts)
                        from unnest($1::text[], $2::bigint[], $3::timestamptz[]) as incoming(code, delta, ts)
                        where links.short_code = incoming.code
                   "#,
                )
                .bind(&delta_codes)
                .bind(&delta_counts)
                .bind(&delta_timestamps)
                .execute(&mut *tx)
                .await
                .map_err(DbError::from)?;
            }
        }

        tx.commit().await.map_err(DbError::from)?;

        debug!(
            global_delta = outcome.global_delta,
            link_count = outcome.link_deltas.len(),
            "snapshot merge committed"
        );

        Ok(outcome)
    }
}
