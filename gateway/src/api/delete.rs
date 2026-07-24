use axum::{Json, extract::State, http::StatusCode};
use proto::core::DeleteLinkRequest;
use tonic::Code;
use tracing::{debug, info, instrument, warn};

use crate::{grpc, state::SharedState};

#[instrument(skip_all, fields(short_code = delete_req.short_code))]
pub async fn delete(
    State(state): State<SharedState>,
    Json(delete_req): Json<DeleteLinkRequest>,
) -> Result<(), StatusCode> {
    debug!("deleting a link");
    match grpc::core_delete_link(&state, delete_req.short_code.clone()).await {
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
