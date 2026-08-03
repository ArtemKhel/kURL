use chrono::{DateTime, Utc};
use common::db_utils::DbError;
use tracing::instrument;

#[derive(Debug, sqlx::FromRow)]
pub struct Link {
    pub target: String,
    pub expiration: Option<DateTime<Utc>>,
}

#[instrument(skip(exec))]
pub async fn get_link(exec: impl sqlx::PgExecutor<'_>, short_code: &str) -> Result<Link, DbError> {
    sqlx::query_as(
        r#"
            select target, expiration
            from links
            where short_code = $1
        "#,
    )
    .bind(short_code)
    .fetch_optional(exec)
    .await?
    .ok_or_else(|| DbError::NotFound)
}

#[instrument(skip(exec))]
pub async fn create_link(
    exec: impl sqlx::PgExecutor<'_>,
    short_code: &str,
    target: &str,
    expiration: Option<DateTime<Utc>>,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
            insert into links (short_code, target, expiration)
            values ($1, $2, $3)
        "#,
    )
    .bind(short_code)
    .bind(target)
    .bind(expiration)
    .execute(exec)
    .await
    .map_err(DbError::from)?;

    Ok(())
}

#[instrument(skip(exec))]
pub async fn delete_link(exec: impl sqlx::PgExecutor<'_>, short_code: &str) -> Result<(), DbError> {
    sqlx::query!(
        r#"
            delete from links 
            where short_code = $1
        "#,
        short_code
    )
    .execute(exec)
    .await
    .map_err(DbError::from)
    .and_then(|res| {
        if res.rows_affected() == 0 {
            Err(DbError::NotFound)
        } else {
            Ok(())
        }
    })
}
