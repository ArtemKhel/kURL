pub mod core_client;

use tonic::{Response, Status};

use crate::state::SharedState;

// TODO: timeouts

pub async fn analytics_get_link_stats(
    state: &SharedState,
    short_code: String,
    days: Option<u32>,
) -> Result<proto::analytics::GetLinkStatsResponse, Status> {
    let mut client = state.analytics_client.clone();
    let request = tonic::Request::new(proto::analytics::GetLinkStatsRequest { short_code, days });
    let response = client.get_link_stats(request).await;
    response.map(Response::into_inner)
}

pub async fn analytics_get_global_stats(
    state: &SharedState,
    days: Option<u32>,
) -> Result<proto::analytics::GetGlobalStatsResponse, Status> {
    let mut client = state.analytics_client.clone();
    let request = tonic::Request::new(proto::analytics::GetGlobalStatsRequest { days });
    let response = client.get_global_stats(request).await;
    response.map(Response::into_inner)
}
