use proto::url::{CreateLinkRequest, GetLinkRequest};
use tonic::Status;

use crate::state::SharedState;

pub async fn core_get_link(state: &SharedState, short_code: String) -> Result<String, Status> {
    let mut client = state.grpc_client.clone();
    let request = tonic::Request::new(GetLinkRequest { short_code });
    let response = client.get_link(request).await;
    response.map(|r| r.into_inner().target)
}

pub async fn core_create_link(state: &SharedState, short_code: String, target: String) -> Result<String, Status> {
    let mut client = state.grpc_client.clone();
    let request = tonic::Request::new(CreateLinkRequest { short_code, target });
    let response = client.create_link(request).await;
    response.map(|r| r.into_inner().short_code)
}
