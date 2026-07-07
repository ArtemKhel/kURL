use redis::AsyncTypedCommands;
use tracing::warn;

pub async fn cache_link(redis: deadpool_redis::Pool, short_code: String, target: String) {
    tokio::spawn(async move {
        if let Err(err) = insert_into_cache(&redis, short_code, target).await {
            warn!("Failed to cache link in Redis: {err}");
        }
    });
}

async fn insert_into_cache(
    redis: &deadpool_redis::Pool,
    short_code: String,
    target: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis.get().await?;
    conn.set(&short_code, &target).await?;
    Ok(())
}
