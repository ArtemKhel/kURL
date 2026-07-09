use sqlx::PgPool;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum GetLinkError {
    #[error("Link not found")]
    NotFound,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}
pub async fn get_link(pool: &PgPool, short_code: &str) -> Result<String, GetLinkError> {
    sqlx::query_scalar!("select target from links where short_code = $1", short_code)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| GetLinkError::NotFound)
}

#[derive(Debug, Error)]
pub enum CreateLinkError {
    #[error("Short code already exists")]
    Duplicate,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

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
            CreateLinkError::Duplicate
        } else {
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
pub async fn delete_link(pool: &PgPool, short_code: &str) -> Result<(), DeleteLinkError> {
    sqlx::query!(" delete from links where short_code = $1", short_code)
        .execute(pool)
        .await
        .map_err(DeleteLinkError::Database)
        .and_then(|x| {
            if x.rows_affected() == 0 {
                Err(DeleteLinkError::NotFound)
            } else {
                Ok(())
            }
        })
}
