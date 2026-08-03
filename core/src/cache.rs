use std::time::Duration;

use common::redis_keys::RedisKeys;
use redis::AsyncTypedCommands;
use tokio::sync::mpsc;
use tracing::{error, instrument, warn};

// Alternative:
// Add `version` column to links` table, bump it on each update with trigger.
// In Redis store version and tombstone, do CAS on update.
// Something like:
// ```lua
// -- KEYS[1] = cache key
// -- ARGV[1] = value
// -- ARGV[2] = version
// -- ARGV[3] = ttl_seconds
// -- ARGV[4] = is_tombstone ("1"/"0")
// local current = redis.call('HGET', KEYS[1], 'version')
// if current and tonumber(current) >= tonumber(ARGV[2]) then
//   return 0
// end
// redis.call('HSET', KEYS[1], 'value', ARGV[1], 'version', ARGV[2], 'tomb', ARGV[4])
// redis.call('EXPIRE', KEYS[1], ARGV[3])
// return 1
// ```

#[derive(Debug)]
pub enum CacheOp {
    Set { key: String, value: String, ttl: Duration },
    Del { key: String },
}

#[instrument(skip_all)]
pub fn spawn_cache_worker(redis: deadpool_redis::Pool) -> mpsc::UnboundedSender<CacheOp> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(op) = rx.recv().await {
            let mut conn = match redis.get().await {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "pool error, dropping op");
                    continue;
                }
            };
            let result = match &op {
                CacheOp::Set { key, value, ttl } => {
                    if ttl.as_secs() > 0 {
                        conn.set_ex(RedisKeys::link_cache_key(key), value, ttl.as_secs()).await
                    } else {
                        Ok(())
                    }
                }
                CacheOp::Del { key } => conn.del(RedisKeys::link_cache_key(key)).await.map(|_| ()),
            };
            if let Err(e) = result {
                warn!(error = %e, operation=?op, "cache op failed");
            }
        }
    });
    tx
}
