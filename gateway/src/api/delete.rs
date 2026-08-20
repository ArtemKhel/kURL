use axum::{Json, extract::State, http::StatusCode};
use proto::core::DeleteLinkRequest;
use serde::Deserialize;
use tonic::Code;
use tracing::{info, instrument, warn};
use utoipa::ToSchema;

use crate::state::SharedState;

#[utoipa::path(
    delete,
    path = "/api/delete",
    request_body = DeleteLinkReq,
    responses(
        (status = 200, description = "Link deleted successfully"),
        (status = 404, description = "Link not found"),
        (status = 500, description = "Internal server error")
    ),
    tag = "links"
)]
#[instrument(skip_all, fields(short_code = delete_req.short_code))]
pub async fn delete(State(state): State<SharedState>, Json(delete_req): Json<DeleteLinkReq>) -> Result<(), StatusCode> {
    match state.core_client.delete_link(delete_req.into()).await {
        Ok(()) => Ok(()),
        Err(error) => match error.code() {
            Code::NotFound => {
                info!("Short code not found");
                Err(StatusCode::NOT_FOUND)
            }
            _ => {
                warn!(%error, "Failed to delete link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DeleteLinkReq {
    short_code: String,
}

impl From<DeleteLinkReq> for DeleteLinkRequest {
    fn from(value: DeleteLinkReq) -> Self {
        DeleteLinkRequest {
            short_code: value.short_code,
        }
    }
}
