use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tracing::error;

pub async fn connect(url: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        // . todo: copypaste from core
        .acquire_timeout(Duration::from_secs(10))
        .max_connections(5)
        .connect(url)
        .await
}

// todo: check if update_* doesn't lose/overrride data

pub async fn update_link_daily_clicks(
    db: &sqlx::PgPool,
    link_short_codes: &[String],
    link_dates: &[chrono::NaiveDate],
    link_clicks: &[i64],
) -> Result<(), sqlx::Error> {
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
    .execute(db)
    .await
    .inspect_err(|e| error!(error=?e, "failed to execute query"))
    .map(|_| ())
}

pub async fn update_global_daily_clicks(
    db: &sqlx::PgPool,
    dates: &[chrono::NaiveDate],
    clicks: &[i64],
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
            insert into global_daily_clicks (day, clicks)
            select * from unnest($1::date[], $2::bigint[])
            on conflict (day) do update set clicks = excluded.clicks
            "#,
        &dates,
        &clicks
    )
    .execute(db)
    .await
    .inspect_err(|e| error!(error=?e, "failed to execute query"))
    .map(|_| ())
}

pub async fn update_link_total_clicks(
    db: &sqlx::PgPool,
    short_codes: &[String],
    click_counts: &[i64],
    click_ats: &[chrono::DateTime<chrono::Utc>],
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        update links as l
        set click_count = l.click_count + d.click_count,
            last_clicked_at = greatest(l.last_clicked_at, d.last_clicked_at)
        from unnest($1::text[], $2::bigint[], $3::timestamptz[]) as d(short_code, click_count, last_clicked_at)
        where l.short_code = d.short_code
        "#,
        short_codes,
        click_counts,
        click_ats
    )
    .execute(db)
    .await
    .inspect_err(|e| error!(error=?e, "failed to execute query"))
    .map(|_| ())
}

pub async fn update_total_clicks(db: &sqlx::PgPool, count: i64) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        update analytics_global as g
        set total_clicks = g.total_clicks + $1
        "#,
        count
    )
    .execute(db)
    .await
    .inspect_err(|e| error!(error=?e, "failed to execute query"))
    .map(|_| ())
}
