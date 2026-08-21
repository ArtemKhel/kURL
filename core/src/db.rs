use chrono::{DateTime, Utc};
use common::db_utils::DbError;
use tracing::instrument;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Link {
    pub target: String,
    pub expiration: Option<DateTime<Utc>>,
}

#[async_trait::async_trait]
pub trait LinkRepository: std::fmt::Debug + Send + Sync {
    async fn get_link(&self, short_code: &str) -> Result<Link, DbError>;
    async fn link_exists(&self, short_code: &str) -> Result<bool, DbError>;
    async fn create_link(
        &self,
        short_code: &str,
        target: &str,
        expiration: Option<DateTime<Utc>>,
    ) -> Result<(), DbError>;
    async fn delete_link(&self, short_code: &str) -> Result<(), DbError>;
}

#[async_trait::async_trait]
impl LinkRepository for sqlx::PgPool {
    async fn get_link(&self, short_code: &str) -> Result<Link, DbError> { get_link(self, short_code).await }

    async fn link_exists(&self, short_code: &str) -> Result<bool, DbError> { link_exists(self, short_code).await }

    async fn create_link(
        &self,
        short_code: &str,
        target: &str,
        expiration: Option<DateTime<Utc>>,
    ) -> Result<(), DbError> {
        create_link(self, short_code, target, expiration).await
    }

    async fn delete_link(&self, short_code: &str) -> Result<(), DbError> { delete_link(self, short_code).await }
}

#[instrument(skip(exec))]
pub async fn get_link(exec: impl sqlx::PgExecutor<'_>, short_code: &str) -> Result<Link, DbError> {
    sqlx::query_as!(
        Link,
        r#"
            select target, expiration
            from links
            where short_code = $1
        "#,
        short_code
    )
    .fetch_optional(exec)
    .await?
    .ok_or_else(|| DbError::NotFound)
}

#[instrument(skip(exec))]
pub async fn link_exists(exec: impl sqlx::PgExecutor<'_>, short_code: &str) -> Result<bool, DbError> {
    sqlx::query_scalar!(
        r#"
            select exists(select 1 from links where short_code = $1)::bool as "link_exists!"
        "#,
        short_code
    )
    .fetch_one(exec)
    .await
    .map_err(DbError::from)
}

#[instrument(skip(exec))]
pub async fn create_link(
    exec: impl sqlx::PgExecutor<'_>,
    short_code: &str,
    target: &str,
    expiration: Option<DateTime<Utc>>,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
            insert into links (short_code, target, expiration)
            values ($1, $2, $3)
        "#,
        short_code,
        target,
        expiration
    )
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
