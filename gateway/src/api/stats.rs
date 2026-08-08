use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tonic::Code;
use tracing::{instrument, warn};

use crate::{grpc, state::SharedState};

#[derive(Debug, Deserialize)]
pub struct StatsQuery {
    pub days: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct LinkStatsResp {
    pub short_code: String,
    pub total_clicks: u64,
    pub daily_clicks: Vec<DailyClicksResp>,
    pub last_clicked_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DailyClicksResp {
    pub date: String,
    pub clicks: u64,
}

#[instrument(skip_all, fields(short_code = short_code))]
pub async fn link_stats(
    State(state): State<SharedState>,
    Path(short_code): Path<String>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<LinkStatsResp>, StatusCode> {
    match grpc::analytics_get_link_stats(&state, short_code, query.days).await {
        Ok(resp) => Ok(Json(LinkStatsResp {
            short_code: resp.short_code,
            total_clicks: resp.total_clicks,
            daily_clicks: resp
                .daily_clicks
                .into_iter()
                .map(|d| DailyClicksResp {
                    date: d.date,
                    clicks: d.clicks,
                })
                .collect(),
            last_clicked_at: resp.last_clicked_at.map(|ts| {
                chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
            }),
        })),
        Err(e) => match e.code() {
            Code::NotFound => Err(StatusCode::NOT_FOUND),
            _ => {
                warn!(error = %e, "Failed to get link stats");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

#[derive(Debug, Serialize)]
pub struct GlobalStatsResp {
    pub total_clicks: u64,
    pub daily_clicks: Vec<DailyClicksResp>,
}

#[instrument(skip_all)]
pub async fn global_stats(
    State(state): State<SharedState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<GlobalStatsResp>, StatusCode> {
    match grpc::analytics_get_global_stats(&state, query.days).await {
        Ok(resp) => Ok(Json(GlobalStatsResp {
            total_clicks: resp.total_clicks,
            daily_clicks: resp
                .daily_clicks
                .into_iter()
                .map(|d| DailyClicksResp {
                    date: d.date,
                    clicks: d.clicks,
                })
                .collect(),
        })),
        Err(e) => {
            warn!(error = %e, "Failed to get global stats");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
