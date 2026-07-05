use std::collections::hash_map::OccupiedError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use crate::state::SharedState;

#[derive(Deserialize)]
pub struct CreateReq{
    code: String,
}

#[derive(Serialize)]
pub struct CreateResp{
    code: String,
}

pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateReq>
) -> Result<Json<CreateResp>, StatusCode> {
    let mut guard = state.write().unwrap();
    match guard.db.try_insert(create_req.code.clone(), format!("hello_{}", create_req.code.clone())){
        Ok(resp) => {
            Ok(Json(CreateResp{code: resp.clone()}))
        }
        Err(_) => {
            Err(StatusCode::CONFLICT)
        }
    }
}
