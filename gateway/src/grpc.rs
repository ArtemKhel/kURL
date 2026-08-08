use proto::core::{CreateLinkRequest, CreateLinkResponse, DeleteLinkRequest, GetLinkRequest, GetLinkResponse};
use tonic::{Response, Status};

use crate::state::SharedState;

// TODO: timeouts
pub async fn core_get_link(state: &SharedState, short_code: String) -> Result<GetLinkResponse, Status> {
    let mut client = state.grpc_client.clone();
    let request = tonic::Request::new(GetLinkRequest { short_code });
    let response = client.get_link(request).await;
    response.map(Response::into_inner)
}

pub async fn core_create_link(state: &SharedState, request: CreateLinkRequest) -> Result<CreateLinkResponse, Status> {
    let mut client = state.grpc_client.clone();
    let response = client.create_link(request).await;
    response.map(Response::into_inner)
}

pub async fn core_delete_link(state: &SharedState, request: DeleteLinkRequest) -> Result<(), Status> {
    let mut client = state.grpc_client.clone();
    let response = client.delete_link(request).await;
    response.map(Response::into_inner)
}

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
