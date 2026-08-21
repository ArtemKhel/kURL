use anyhow::Context;
use sqlx::migrate::Migrator;
use tracing::info;
use tracing_subscriber::EnvFilter;

static MIGRATOR: Migrator = sqlx::migrate!("../migrations");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let database = common::config::load_database().context("Failed to load database configuration")?;
    let pool = common::connections::connect_postgres(&database).await?;

    info!(
        host = %database.host,
        port = database.port,
        database = %database.db_name,
        "Applying database migrations"
    );
    MIGRATOR
        .run(&pool)
        .await
        .context("Failed to apply database migrations")?;
    pool.close().await;
    info!("Database migrations applied successfully");

    Ok(())
}
