use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};

use crate::state::SharedState;

pub async fn redirect(State(state): State<SharedState>, Path(path): Path<String>) -> Response {
    dbg!(&path);
    let url = state.read().unwrap().db.get(&path).cloned();
    match url {
        Some(url) => Redirect::permanent(&url).into_response(),
        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}
