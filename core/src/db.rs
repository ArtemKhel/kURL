use common::db_utils::DbError;
use tracing::instrument;

#[instrument(skip(exec))]
pub async fn get_link(exec: impl sqlx::PgExecutor<'_>, short_code: &str) -> Result<String, DbError> {
    sqlx::query_scalar!("select target from links where short_code = $1", short_code)
        .fetch_optional(exec)
        .await?
        .ok_or_else(|| DbError::NotFound)
}

#[instrument(skip(exec))]
pub async fn create_link(exec: impl sqlx::PgExecutor<'_>, short_code: &str, target: &str) -> Result<(), DbError> {
    sqlx::query!(
        "insert into links (short_code, target) values ($1, $2)",
        short_code,
        target
    )
    .execute(exec)
    .await
    .map_err(DbError::from)?;

    Ok(())
}

#[instrument(skip(exec))]
pub async fn delete_link(exec: impl sqlx::PgExecutor<'_>, short_code: &str) -> Result<(), DbError> {
    sqlx::query!(" delete from links where short_code = $1", short_code)
        .execute(exec)
        .await
        .map_err(DbError::Other)
        .and_then(|target| {
            if target.rows_affected() == 0 {
                Err(DbError::NotFound)
            } else {
                Ok(())
            }
        })
}
