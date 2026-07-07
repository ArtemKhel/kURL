use sqlx::postgres::PgPoolOptions;

pub mod links;

pub async fn connect(url: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        // . todo: what else is there?
        .connect(url)
        .await
}