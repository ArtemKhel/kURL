use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use tonic::Code;
use tracing::{debug, info, instrument, warn};

use crate::{grpc, state::SharedState};

#[derive(Deserialize, Debug)]
pub struct DeleteReq {
    short_code: String,
}

#[instrument(skip_all, fields(short_code = delete_req.short_code))]
pub async fn delete(State(state): State<SharedState>, Json(delete_req): Json<DeleteReq>) -> Result<(), StatusCode> {
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
