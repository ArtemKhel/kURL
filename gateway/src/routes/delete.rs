use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Redirect},
};
use serde::Deserialize;
use tonic::Code;
use tracing::{info, instrument, warn};

use crate::{grpc, state::SharedState};

#[derive(Deserialize)]
pub struct DeleteReq {
    short_code: String,
}

#[instrument(skip_all, fields(short_code = delete_req.short_code))]
pub async fn delete(State(state): State<SharedState>, Json(delete_req): Json<DeleteReq>) -> Result<(), StatusCode> {
    match grpc::core_delete_link(&state, delete_req.short_code.clone()).await {
        Ok(()) => Ok(()),
        Err(e) => match e.code() {
            Code::NotFound => {
                info!("Short code not found");
                Err(StatusCode::NOT_FOUND)
            }
            _ => {
                warn!(error = %e, "Failed to get link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
