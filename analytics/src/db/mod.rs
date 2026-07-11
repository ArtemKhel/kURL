use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

pub async fn connect(url: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        // . todo: copypaste from core
        .acquire_timeout(Duration::from_secs(10))
        .max_connections(5)
        .connect(url)
        .await
}
