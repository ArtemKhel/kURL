use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDate, Utc};

use crate::db::{GlobalDailyStats, LinkDailyStats};
// todo: move here from db

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

#[derive(Debug, Default)]
pub struct RehydrationData {
    pub global_daily: Vec<GlobalDailyStats>,
    pub link_daily: Vec<LinkDailyStats>,
}

pub fn calculate_merge_delta(existing: i64, incoming: i64) -> (i64, i64) {
    let committed = existing.max(incoming);
    let applied_delta = committed - existing;
    (committed, applied_delta)
}

pub fn stale_global_fields(daily: &HashMap<NaiveDate, i64>, cutoff: NaiveDate) -> HashSet<NaiveDate> {
    daily.keys().filter(|&d| *d < cutoff).copied().collect()
}

pub fn stale_link_fields(daily: &HashMap<(String, NaiveDate), i64>, cutoff: NaiveDate) -> HashSet<(String, NaiveDate)> {
    daily.keys().filter(|(_, d)| *d < cutoff).cloned().collect()
}

pub fn parse_daily_counts(fields: HashMap<String, String>) -> (HashMap<NaiveDate, i64>, Vec<MalformedField>) {
    let mut valid = HashMap::with_capacity(fields.len());
    let mut malformed = Vec::new();

    for (date_str, count_str) in fields {
        let Ok(date) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") else {
            malformed.push(MalformedField {
                key: date_str,
                value: count_str,
                reason: "invalid date format",
            });
            continue;
        };
        let clicks = match count_str.parse::<i64>() {
            Ok(c) if c >= 0 => c,
            Ok(_) => {
                malformed.push(MalformedField {
                    key: date_str,
                    value: count_str,
                    reason: "negative count",
                });
                continue;
            }
            Err(_) => {
                malformed.push(MalformedField {
                    key: date_str,
                    value: count_str,
                    reason: "invalid count",
                });
                continue;
            }
        };
        valid.insert(date, clicks);
    }
    (valid, malformed)
}

// todo: move?
#[derive(Debug)]
pub struct MalformedField {
    pub key: String,
    pub value: String,
    pub reason: &'static str,
}
