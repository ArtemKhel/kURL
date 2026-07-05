use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use crate::state::SharedState;

#[derive(Deserialize)]
pub struct CreateReq {
    short: String,
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
    match state
        .write()
        .await
        .db
        .try_insert(create_req.short.clone(), create_req.target.clone())
    {
        Ok(resp) => Ok(Json(CreateResp { url: resp.clone() })),
        Err(_) => Err(StatusCode::CONFLICT),
    }
}
