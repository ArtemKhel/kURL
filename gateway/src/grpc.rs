use proto::url::{CreateLinkRequest, CreateLinkResponse, GetLinkRequest, GetLinkResponse};
use tonic::{Response, Status};

use crate::state::SharedState;

pub async fn core_get_link(state: &SharedState, short_code: String) -> Result<Response<GetLinkResponse>, Status> {
    let request = tonic::Request::new(GetLinkRequest { short_code });
    let response = state.core.get_link(request).await;
    response
}

pub async fn core_create_link(
    state: &SharedState,
    short_code: String,
    target: String,
) -> Result<Response<CreateLinkResponse>, Status> {
    let request = tonic::Request::new(CreateLinkRequest { short_code, target });
    let response = state.core.create_link(request).await;
    response
}
