use axum::{extract::State, http::StatusCode, Json};
use proto::core::DeleteLinkRequest;
use serde::Deserialize;
use tonic::Code;
use tracing::{info, instrument, warn};

use crate::{grpc, state::SharedState};

#[instrument(skip_all, fields(short_code = delete_req.short_code))]
pub async fn delete(State(state): State<SharedState>, Json(delete_req): Json<DeleteLinkReq>) -> Result<(), StatusCode> {
    match grpc::core_delete_link(&state, delete_req.into()).await {
        Ok(()) => Ok(()),
        Err(e) => match e.code() {
            Code::NotFound => {
                info!("Short code not found");
                Err(StatusCode::NOT_FOUND)
            }
            _ => {
                warn!(error = %e, "Failed to delete link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

#[derive(Debug, Deserialize)]
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
