use std::time::Duration;
use sqlx::postgres::PgPoolOptions;

/// Postgres only
/// 
pub async fn connect(url: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(10))
        .max_connections(10)
        .test_before_acquire(true)
        .connect(url)
        .await
}


#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("transient: {0}")]
    Transient(sqlx::Error),
    #[error("unique violation: {0}")]
    Conflict(sqlx::Error),
    #[error("not found")]
    NotFound,
    #[error(transparent)]
    Other(sqlx::Error),
}

impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::RowNotFound => DbError::NotFound,
            sqlx::Error::PoolTimedOut => DbError::Transient(e),
            sqlx::Error::Io(_) => DbError::Transient(e),
            sqlx::Error::Database(_) if is_unique_violation(&e) => DbError::Conflict(e),
            sqlx::Error::Database(db_err) => {
                match db_err.code().as_deref() {
                    // 08* = connection exception
                    // 40001 = serialization_failure
                    // 40P01 = deadlock_detected
                    // 57P03 = cannot_connect_now
                    Some(code) if code.starts_with("08") || code == "40001" || code == "40P01" || code == "57P03" => {
                        DbError::Transient(e)
                    }
                    _ => DbError::Other(e),
                }
            }
            _ => DbError::Other(e),
        }
    }
}

pub fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Database(db_err)
            if db_err.kind() == sqlx::error::ErrorKind::UniqueViolation
    )
}
