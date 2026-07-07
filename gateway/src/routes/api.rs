use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use crate::{grpc, state::SharedState};

#[derive(Deserialize)]
pub struct CreateReq {
    short_code: String,
    target: String,
}

// TODO:
#[derive(Serialize)]
pub struct CreateResp {
    url: String,
}

pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateReq>,
) -> Result<Json<CreateResp>, StatusCode> {
    match grpc::core_create_link(&state, create_req.short_code, create_req.target) {
        Ok(resp) => Ok(Json(CreateResp { url: resp.clone() })),
        Err(_) => Err(StatusCode::CONFLICT),
    }
}
