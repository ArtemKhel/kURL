use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Redirect},
    Json,
};
use serde::Deserialize;
use tonic::Code;
use tracing::{info, warn};

use crate::{grpc, state::SharedState};

#[derive(Deserialize)]
pub struct DeleteReq {
    short_code: String,
}

pub async fn delete(State(state): State<SharedState>, Json(delete_req): Json<DeleteReq>) -> Result<(), StatusCode> {
    match grpc::core_delete_link(&state, delete_req.short_code.clone()).await {
        Ok(()) => Ok(()),
        Err(e) => match e.code() {
            Code::NotFound => {
                info!(short_code = %delete_req.short_code, "Short code not found");
                Err(StatusCode::NOT_FOUND)
            }
            _ => {
                warn!(error = %e, short_code = %delete_req.short_code, "Failed to get link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
