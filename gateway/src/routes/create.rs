use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use tonic::Code;
use tracing::{instrument, warn};

use crate::{grpc, state::SharedState};

#[derive(Deserialize)]
pub struct CreateReq {
    short_code: String,
    target: String,
}

#[derive(Serialize)]
pub struct CreateResp {
    // todo: url
    url: String,
}

#[instrument(skip_all, fields(short_code = create_req.short_code, target = create_req.target))]
pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateReq>,
) -> Result<Json<CreateResp>, StatusCode> {
    match grpc::core_create_link(&state, create_req.short_code, create_req.target).await {
        Ok(short_code) => Ok(Json(CreateResp { url: short_code })),
        Err(e) => match e.code() {
            Code::AlreadyExists => Err(StatusCode::CONFLICT),
            _ => {
                warn!(error = %e, "Failed to create link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
