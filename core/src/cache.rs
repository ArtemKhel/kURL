use common::redis_keys::RedisKeys;
use redis::AsyncTypedCommands;
use tracing::{error, warn};

pub async fn insert_link(redis: deadpool_redis::Pool, short_code: String, target: String) {
    tokio::spawn(async move {
        if let Err(e) = inner_insert_link(&redis, short_code, target).await {
            warn!(error = %e, "Failed to cache link in Redis");
        }
    });
}

async fn inner_insert_link(
    redis: &deadpool_redis::Pool,
    short_code: String,
    target: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis.get().await?;
    conn.set(RedisKeys::link_cache_key(&short_code), &target).await?;
    Ok(())
}

pub async fn delete_link(redis: deadpool_redis::Pool, short_code: String) {
    tokio::spawn(async move {
        if let Err(e) = inner_delete_link(redis, short_code).await {
            error!(error = %e, "Failed to remove link from Redis");
        }
    });
}

async fn inner_delete_link(redis: deadpool_redis::Pool, short_code: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis.get().await?;
    conn.del(RedisKeys::link_cache_key(&short_code)).await?;
    Ok(())
}
