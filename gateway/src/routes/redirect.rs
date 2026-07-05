use axum::extract::{Path, State};

use crate::state::SharedState;

pub async fn redirect(State(state): State<SharedState>, Path(path): Path<String>) -> String {
    state
        .read()
        .unwrap()
        .db
        .get(&path)
        .cloned()
        .unwrap_or_else(|| "Not found".to_string())
}
