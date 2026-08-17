use std::collections::HashMap;

pub use common::db_utils::DbError;
use tracing::instrument;

use crate::db::AnalyticsRepository;
// todo: check if update_* doesn't lose/overrride data

#[instrument(skip_all)]
pub async fn update_link_daily_clicks(
    exec: impl sqlx::PgExecutor<'_>,
    link_short_codes: &[String],
    link_dates: &[chrono::NaiveDate],
    link_clicks: &[i64],
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
            insert into link_daily_clicks as l (short_code, day, clicks)
            select * from unnest($1::text[], $2::date[], $3::bigint[])
            on conflict (short_code, day) do update set clicks = greatest(excluded.clicks, l.clicks)
        "#,
        &link_short_codes,
        &link_dates,
        &link_clicks
    )
    .execute(exec)
    .await
    .map(|_| ())
    .map_err(DbError::from)
}

#[instrument(skip_all)]
pub async fn update_global_daily_clicks(
    exec: impl sqlx::PgExecutor<'_>,
    dates: &[chrono::NaiveDate],
    clicks: &[i64],
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
            insert into global_daily_clicks as g (day, clicks)
            select * from unnest($1::date[], $2::bigint[])
            on conflict (day) do update set clicks = greatest(excluded.clicks, g.clicks)
        "#,
        &dates,
        &clicks
    )
    .execute(exec)
    .await
    .map(|_| ())
    .map_err(DbError::from)
}

#[instrument(skip_all)]
pub async fn update_global_click_count(exec: impl sqlx::PgExecutor<'_>, count: i64) -> Result<(), DbError> {
    sqlx::query!(
        r#"
            update analytics_global as g
            set total_clicks = g.total_clicks + $1
        "#,
        count
    )
    .execute(exec)
    .await
    .map(|_| ())
    .map_err(DbError::from)
}

#[derive(Debug)]
pub struct LinkDailyStats {
    pub short_code: String,
    pub day: chrono::NaiveDate,
    pub clicks: i64,
}
#[instrument(skip_all)]
pub async fn get_link_daily_values(
    exec: impl sqlx::PgExecutor<'_>,
    short_codes: &[String],
    dates: &[chrono::NaiveDate],
) -> Result<HashMap<(String, chrono::NaiveDate), i64>, DbError> {
    sqlx::query_as!(
        LinkDailyStats,
        r#"
            select short_code, day, clicks from link_daily_clicks
            where (short_code, day) in (select * from unnest($1::text[], $2::date[]))
        "#,
        short_codes,
        dates
    )
    .fetch_all(exec)
    .await
    .map(|rows| rows.into_iter().map(|s| ((s.short_code, s.day), s.clicks)).collect())
    .map_err(DbError::from)
}

#[derive(Debug)]
pub struct GlobalDailyStats {
    pub day: chrono::NaiveDate,
    pub clicks: i64,
}

#[instrument(skip_all)]
pub async fn get_global_daily_values(
    exec: impl sqlx::PgExecutor<'_>,
    dates: &[chrono::NaiveDate],
) -> Result<HashMap<chrono::NaiveDate, i64>, DbError> {
    sqlx::query_as!(
        GlobalDailyStats,
        r#"
            select day, clicks from global_daily_clicks
            where day in (select * from unnest($1::date[]))
        "#,
        dates
    )
    .fetch_all(exec)
    .await
    .map(|rows| rows.into_iter().map(|s| (s.day, s.clicks)).collect())
    .map_err(DbError::from)
}

#[instrument(skip_all)]
pub async fn update_link_total_and_last_click(
    exec: impl sqlx::PgExecutor<'_>,
    short_codes: &[String],
    click_count_deltas: &[i64],
    click_ats: &[chrono::DateTime<chrono::Utc>],
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
            update links as l
            set click_count = l.click_count + d.click_count,
                last_clicked_at = greatest(l.last_clicked_at, d.last_clicked_at)
            from unnest($1::text[], $2::bigint[], $3::timestamptz[]) as d(short_code, click_count, last_clicked_at)
            where l.short_code = d.short_code
        "#,
        short_codes,
        click_count_deltas,
        click_ats
    )
    .execute(exec)
    .await
    .map(|_| ())
    .map_err(DbError::from)
}

pub async fn get_global_daily_clicks_since(
    exec: impl sqlx::PgExecutor<'_>,
    date: chrono::NaiveDate,
) -> Result<Vec<GlobalDailyStats>, DbError> {
    sqlx::query_as!(
        GlobalDailyStats,
        r#"
            select day, clicks from global_daily_clicks
            where day >= $1
            order by day
        "#,
        date
    )
    .fetch_all(exec)
    .await
    .map_err(DbError::from)
}

pub async fn get_link_daily_clicks_since(
    exec: impl sqlx::PgExecutor<'_>,
    date: chrono::NaiveDate,
) -> Result<Vec<LinkDailyStats>, DbError> {
    sqlx::query_as!(
        LinkDailyStats,
        r#"
            select short_code, day, clicks from link_daily_clicks
            where day >= $1
        "#,
        date
    )
    .fetch_all(exec)
    .await
    .map_err(DbError::from)
}

#[async_trait::async_trait]
impl AnalyticsRepository for sqlx::PgPool {
    async fn get_link_totals(&self, short_code: &str) -> Result<(i64, Option<chrono::DateTime<chrono::Utc>>), DbError> {
        get_link_totals(self, short_code).await
    }

    async fn get_link_stats(&self, short_code: &str, days: i32) -> Result<Vec<(chrono::NaiveDate, i64)>, DbError> {
        get_link_stats(self, short_code, days).await
    }

    async fn get_global_total_clicks(&self) -> Result<i64, DbError> { get_global_total_clicks(self).await }

    async fn get_global_daily_stats(&self, days: i32) -> Result<Vec<(chrono::NaiveDate, i64)>, DbError> {
        let since = (chrono::Utc::now() - chrono::Duration::days(days.into())).date_naive();
        todo!()
        // get_global_daily_clicks_since(self, since).await
    }
}

#[instrument(skip(exec))]
pub async fn get_link_stats(
    exec: impl sqlx::PgExecutor<'_>,
    short_code: &str,
    days: i32,
) -> Result<Vec<(chrono::NaiveDate, i64)>, DbError> {
    let since = (chrono::Utc::now() - chrono::Duration::days(days.into())).date_naive();
    sqlx::query_as!(
        LinkDailyStats,
        r#"
            select short_code, day, clicks from link_daily_clicks
            where short_code = $1 and day >= $2
            order by day
        "#,
        short_code,
        since,
    )
    .fetch_all(exec)
    .await
    .map(|rows| rows.into_iter().map(|r| (r.day, r.clicks)).collect())
    .map_err(DbError::from)
}

#[instrument(skip(exec))]
pub async fn get_global_total_clicks(exec: impl sqlx::PgExecutor<'_>) -> Result<i64, DbError> {
    sqlx::query_scalar!(
        r#"
            select total_clicks as "total_clicks!"
            from analytics_global
            where id = 1
        "#
    )
    .fetch_one(exec)
    .await
    .map_err(DbError::from)
}

#[instrument(skip(exec))]
pub async fn get_link_totals(
    exec: impl sqlx::PgExecutor<'_>,
    short_code: &str,
) -> Result<(i64, Option<chrono::DateTime<chrono::Utc>>), DbError> {
    let row = sqlx::query!(
        r#"
            select click_count, last_clicked_at from links
            where short_code = $1
        "#,
        short_code,
    )
    .fetch_one(exec)
    .await
    .map_err(DbError::from)?;
    Ok((row.click_count, row.last_clicked_at)) // todo: type?
}
