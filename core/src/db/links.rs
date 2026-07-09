use sqlx::PgPool;
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Error)]
pub enum GetLinkError {
    #[error("Link not found")]
    NotFound,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[instrument(skip(pool))]
pub async fn get_link(pool: &PgPool, short_code: &str) -> Result<String, GetLinkError> {
    sqlx::query_scalar!("select target from links where short_code = $1", short_code)
        .fetch_optional(pool)
        .await?
        .inspect(|target| info!(%short_code, %target, "Link found"))
        .ok_or_else(|| {
            info!(%short_code, "Link not found");
            GetLinkError::NotFound
        })
}

#[derive(Debug, Error)]
pub enum CreateLinkError {
    #[error("Short code already exists")]
    Duplicate,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[instrument(skip(pool))]
pub async fn create_link(pool: &PgPool, short_code: &str, target: &str) -> Result<(), CreateLinkError> {
    sqlx::query!(
        "insert into links (short_code, target) values ($1, $2)",
        short_code,
        target
    )
    .execute(pool)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            info!(%short_code, "Duplicate short code");
            CreateLinkError::Duplicate
        } else {
            warn!(error = %e, %short_code, %target, "DB error while creating link");
            CreateLinkError::Database(e)
        }
    })?;

    Ok(())
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Database(db_err)
            if db_err.kind() == sqlx::error::ErrorKind::UniqueViolation
    )
}

#[derive(Debug, Error)]
pub enum DeleteLinkError {
    #[error("Link not found")]
    NotFound,
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}
#[instrument(skip(pool))]
pub async fn delete_link(pool: &PgPool, short_code: &str) -> Result<(), DeleteLinkError> {
    sqlx::query!(" delete from links where short_code = $1", short_code)
        .execute(pool)
        .await
        .map_err(|e| {
            warn!(error = %e, %short_code, "DB error while deleting the link");
            DeleteLinkError::Database(e)
        })
        .and_then(|target| {
            if target.rows_affected() == 0 {
                debug!(%short_code, "No rows affected while deleting the link");
                Err(DeleteLinkError::NotFound)
            } else {
                debug!(rows_affected = target.rows_affected(), "link deleted successfully");
                Ok(())
            }
        })
}
