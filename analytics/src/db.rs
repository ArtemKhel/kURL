use std::collections::HashMap;

use chrono::NaiveDate;
pub use common::db_utils::DbError;
use tracing::instrument;

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
            insert into link_daily_clicks (short_code, day, clicks)
            select * from unnest($1::text[], $2::date[], $3::bigint[])
            on conflict (short_code, day) do update set clicks = excluded.clicks
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
            insert into global_daily_clicks (day, clicks)
            select * from unnest($1::date[], $2::bigint[])
            on conflict (day) do update set clicks = excluded.clicks
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
struct LinkDailyStats {
    short_code: String,
    day: chrono::NaiveDate,
    clicks: i64,
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
struct GlobalDailyStats {
    day: chrono::NaiveDate,
    clicks: i64,
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

pub(crate) async fn get_global_daily_clicks_since(
    exec: impl sqlx::PgExecutor<'_>,
    date: NaiveDate,
) -> Result<Vec<(NaiveDate, i64)>, DbError> {
    sqlx::query_as!(
        GlobalDailyStats,
        r#"
            select day, clicks from global_daily_clicks
            where day >= $1
        "#,
        date
    )
    .fetch_all(exec)
    .await
    .map(|rows| rows.into_iter().map(|s| (s.day, s.clicks)).collect())
    .map_err(DbError::from)
}

pub(crate) async fn get_link_daily_clicks_since(
    exec: impl sqlx::PgExecutor<'_>,
    date: NaiveDate,
) -> Result<Vec<(String, NaiveDate, i64)>, DbError> {
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
    .map(|rows| rows.into_iter().map(|s| (s.short_code, s.day, s.clicks)).collect())
    .map_err(DbError::from)
}
