use std::sync::Arc;

use proto::analytics::{
    DailyClicks, GetGlobalStatsRequest, GetGlobalStatsResponse, GetLinkStatsRequest, GetLinkStatsResponse,
    analytics_server::Analytics,
};
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::db::{self, AnalyticsRepository};

const DEFAULT_DAYS: u32 = 7;
const MAX_DAYS: u32 = 90;

#[derive(Debug)]
pub struct AnalyticsService {
    pub db: Arc<dyn AnalyticsRepository>,
}

fn clamp_days(days: Option<u32>) -> i32 { days.unwrap_or(DEFAULT_DAYS).clamp(1, MAX_DAYS) as i32 }

fn to_daily_clicks(rows: Vec<(chrono::NaiveDate, i64)>) -> Vec<DailyClicks> {
    rows.into_iter()
        .map(|(day, clicks)| DailyClicks {
            date: day.to_string(),
            clicks: clicks as u64,
        })
        .collect()
}

#[tonic::async_trait]
impl Analytics for AnalyticsService {
    #[instrument(skip(self))]
    async fn get_link_stats(
        &self,
        request: Request<GetLinkStatsRequest>,
    ) -> Result<Response<GetLinkStatsResponse>, Status> {
        let req = request.into_inner();
        let days = clamp_days(req.days);

        let (total_clicks, last_clicked_at) = self.db.get_link_totals(&req.short_code).await.map_err(|e| match e {
            db::DbError::NotFound => Status::not_found("Short code not found"),
            _ => Status::internal("Database error"),
        })?;

        let daily = self
            .db
            .get_link_stats(&req.short_code, days)
            .await
            .map_err(|_| Status::internal("Database error"))?;

        let last_clicked_at = last_clicked_at.map(Into::into);

        Ok(Response::new(GetLinkStatsResponse {
            short_code: req.short_code,
            total_clicks: total_clicks as u64,
            daily_clicks: to_daily_clicks(daily),
            last_clicked_at,
        }))
    }

    #[instrument(skip(self))]
    async fn get_global_stats(
        &self,
        request: Request<GetGlobalStatsRequest>,
    ) -> Result<Response<GetGlobalStatsResponse>, Status> {
        let req = request.into_inner();
        let days = clamp_days(req.days);

        let total_clicks = self
            .db
            .get_global_total_clicks()
            .await
            .map_err(|_| Status::internal("Database error"))?;

        let daily = self
            .db
            .get_global_daily_stats(days)
            .await
            .map_err(|_| Status::internal("Database error"))?;

        Ok(Response::new(GetGlobalStatsResponse {
            total_clicks: total_clicks as u64,
            daily_clicks: to_daily_clicks(daily),
        }))
    }
}

#[cfg(test)]
#[path = "grpc_tests.rs"]
mod grpc_tests;
