use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

pub mod links;

pub async fn connect(url: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        // . todo: what else is there?
        .acquire_timeout(Duration::from_secs(10))
        .max_connections(5)
        .connect(url)
        .await
}
