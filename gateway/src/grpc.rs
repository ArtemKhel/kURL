use proto::url::{GetLinkRequest, GetLinkResponse};
use tonic::{Response, Status};

use crate::state::SharedState;

pub async fn core_request(state: &SharedState, short_code: String) -> Result<Response<GetLinkResponse>, Status> {
    let request = tonic::Request::new(GetLinkRequest { short_code });
    let response = state.read().await.core.get_link(request).await;
    response
}
