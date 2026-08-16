use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDate, Utc};

#[derive(Debug, Default)]
pub struct RedisSnapshot {
    pub global_daily: HashMap<NaiveDate, i64>,
    pub link_daily: HashMap<(String, NaiveDate), i64>,
    pub last_clicked_at: HashMap<String, DateTime<Utc>>,
    pub stale_global: HashSet<NaiveDate>,
    pub stale_links: HashSet<(String, NaiveDate)>,
}

#[derive(Debug, Default)]
pub struct MergeOutcome {
    pub committed_global: HashMap<NaiveDate, i64>,
    pub committed_links: HashMap<(String, NaiveDate), i64>,
    pub global_delta: i64,
    pub link_deltas: HashMap<String, i64>,
}
