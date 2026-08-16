use chrono::NaiveDate;
use common::db_utils::DbError;

use crate::{
    db::{SnapshotRepository, get_global_daily_clicks_since, get_link_daily_clicks_since},
    snapshot::RehydrationData,
};

#[tonic::async_trait]
impl SnapshotRepository for sqlx::PgPool {
    async fn get_daily_clicks_since(&self, since: NaiveDate) -> Result<RehydrationData, DbError> {
        let global_daily = get_global_daily_clicks_since(self, since).await?;
        let link_daily = get_link_daily_clicks_since(self, since).await?;
        Ok(RehydrationData {
            global_daily,
            link_daily,
        })
    }
}
