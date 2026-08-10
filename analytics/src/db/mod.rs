pub mod db;

use std::fmt::Debug;

pub use db::*;

#[tonic::async_trait]
pub trait AnalyticsRepository: Debug + Send + Sync {
    async fn get_link_totals(&self, short_code: &str) -> Result<(i64, Option<chrono::DateTime<chrono::Utc>>), DbError>;

    async fn get_link_stats(&self, short_code: &str, days: i32) -> Result<Vec<(chrono::NaiveDate, i64)>, DbError>;

    async fn get_global_total_clicks(&self) -> Result<i64, DbError>;

    async fn get_global_daily_stats(&self, days: i32) -> Result<Vec<(chrono::NaiveDate, i64)>, DbError>;
}
