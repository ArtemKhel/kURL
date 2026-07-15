use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::{events::ClickEvent, redis_keys::RedisKeys};
use redis::{AsyncTypedCommands, RedisResult};
use tracing::{error, instrument};

pub struct RedisStats {}

impl RedisStats {
    #[instrument(skip(conn))]
    pub async fn record_click(conn: &mut deadpool_redis::Connection, event: &ClickEvent) -> RedisResult<()> {
        let global_key = RedisKeys::global_stats_key();
        let link_key = RedisKeys::link_stats_key(&event.short_code);
        let last_clicked_key = RedisKeys::link_last_clicked_at_key(&event.short_code);
        let date = event.at.format("%Y-%m-%d").to_string();
        redis::pipe()
            .atomic()
            .hincr(&global_key, &date, 1)
            .ignore()
            .hincr(&link_key, &date, 1)
            .ignore()
            .set(&last_clicked_key, &event.at.to_string())
            .ignore()
            .query_async(conn)
            .await
    }

    #[instrument(skip(conn))]
    pub async fn last_clicked_at_batch(
        conn: &mut deadpool_redis::Connection,
        short_codes: &[String],
    ) -> RedisResult<HashMap<String, DateTime<Utc>>> {
        if short_codes.is_empty() {
            return Ok(HashMap::new());
        }

        let keys: Vec<String> = short_codes
            .iter()
            .map(|s| RedisKeys::link_last_clicked_at_key(s))
            .collect();
        let values = conn.mget(keys).await?;
        Ok(short_codes
            .iter()
            .cloned()
            .zip(values)
            .filter_map(|(c, v)| {
                v.and_then(|v| {
                    v.parse::<DateTime<Utc>>()
                        .inspect_err(|e| error!(error=?e, "Failed to parse date from Redis"))
                        .ok()
                        .map(|v| (c, v))
                })
            })
            .collect())
    }
}
