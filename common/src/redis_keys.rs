pub struct RedisKeys {}

impl RedisKeys {
    pub const CACHE: &str = "cache";
    pub const STATS: &str = "stats";

    pub fn stats_key(stat: &str) -> String { format!("{}:{}", Self::STATS, stat) }

    pub fn global_stats_key() -> String { format!("{}:global", Self::STATS) }

    pub fn link_stats_key(short_code: &str) -> String { format!("{}:link:{}", Self::STATS, short_code) }

    pub fn link_cache_key(short_code: &str) -> String { format!("{}:{}", Self::CACHE, short_code) }

    pub fn link_last_clicked_at_key(short_code: &str) -> String {
        format!("{}:last_clicked_at:{}", Self::STATS, short_code)
    }
}
