pub struct RedisKeys {}

impl RedisKeys {
    pub const STATS: &str = "stats";

    pub fn global_key() -> String { format!("{}_global", Self::STATS) }

    pub fn link_key(short_code: &str) -> String { format!("{}:{}", Self::STATS, short_code) }

    // pub fn link_prefix() -> String { format!("{}:", Self::STATS) }
}
