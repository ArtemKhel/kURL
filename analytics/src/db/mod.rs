pub mod db;
pub mod pg_snapshot_repo;

use std::fmt::Debug;

use chrono::{DateTime, NaiveDate, Utc};
pub use db::*;

use crate::snapshot::{MergeOutcome, RedisSnapshot, RehydrationData};

#[async_trait::async_trait]
pub trait AnalyticsRepository: Debug + Send + Sync {
    async fn get_link_totals(&self, short_code: &str) -> Result<(i64, Option<DateTime<Utc>>), DbError>;

    async fn get_link_stats(&self, short_code: &str, days: i32) -> Result<Vec<(NaiveDate, i64)>, DbError>;

    async fn get_global_total_clicks(&self) -> Result<i64, DbError>;

    async fn get_global_daily_stats(&self, days: i32) -> Result<Vec<(NaiveDate, i64)>, DbError>;
}

#[async_trait::async_trait]
pub trait SnapshotRepository: Debug + Send + Sync {
    async fn get_daily_clicks_since(&self, since: NaiveDate) -> Result<RehydrationData, DbError>;

    async fn merge_snapshot(&self, snapshot: &RedisSnapshot) -> Result<MergeOutcome, DbError>;
}
