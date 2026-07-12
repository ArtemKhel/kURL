use common::events::ClickEvent;
use redis::RedisResult;
use tracing::instrument;

pub struct RedisStats {
    config: crate::Config,
}

impl RedisStats {
    const STATS: &str = "stats";

    pub fn new(config: crate::Config) -> Self { Self { config } }

    fn global_key() -> String { format!("{}_global", Self::STATS) }

    fn link_key(short_code: &str) -> String { format!("{}:{}", Self::STATS, short_code) }

    #[instrument(skip(self, conn))]
    pub async fn record_click(&self, conn: &mut deadpool_redis::Connection, event: &ClickEvent) -> RedisResult<()> {
        let global_key = Self::global_key();
        let link_key = Self::link_key(&event.short_code);
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
