use common::events::ClickEvent;
use redis::RedisResult;
use tracing::instrument;
use crate::redis_keys::RedisKeys;

pub struct RedisStats {
    config: crate::Config,
}

impl RedisStats {

    pub fn new(config: crate::Config) -> Self { Self { config } }

    #[instrument(skip(self, conn))]
    pub async fn record_click(&self, conn: &mut deadpool_redis::Connection, event: &ClickEvent) -> RedisResult<()> {
        let global_key = RedisKeys::global_key();
        let link_key = RedisKeys::link_key(&event.short_code);
        let date = event.at.format("%Y-%m-%d").to_string();
        redis::pipe()
            .atomic()
            .hincr(&global_key, &date, 1)
            .ignore()
            .hincr(&link_key, &date, 1)
            .ignore()
            .query_async(conn)
            .await
    }
}
