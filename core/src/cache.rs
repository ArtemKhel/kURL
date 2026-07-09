use redis::AsyncTypedCommands;
use tracing::warn;

pub async fn insert_link(redis: deadpool_redis::Pool, short_code: String, target: String) {
    tokio::spawn(async move {
        if let Err(err) = inner_insert_link(&redis, short_code, target).await {
            warn!("Failed to cache link in Redis: {err}");
        }
    });
}

async fn inner_insert_link(
    redis: &deadpool_redis::Pool,
    short_code: String,
    target: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis.get().await?;
    conn.set(&short_code, &target).await?;
    Ok(())
}

pub async fn delete_link(redis: deadpool_redis::Pool, short_code: String) {
    tokio::spawn(async move {
        if let Err(e) = inner_delete_link(&redis, short_code).await {
            warn!("Failed to remove link from Redis: {e}");
        }
    });
}

async fn inner_delete_link(redis: &deadpool_redis::Pool, short_code: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis.get().await?;
    conn.del(&short_code).await?;
    Ok(())
}
