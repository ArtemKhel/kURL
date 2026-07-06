use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use proto::url::GetLinkResponse;
use redis::AsyncTypedCommands;
use tonic::Status;
use tracing::info;

use crate::{
    cache::{redis_query, redis_set},
    grpc,
    state::SharedState,
};

pub async fn redirect(State(state): State<SharedState>, Path(short_code): Path<String>) -> Response {
    // todo: unwraps
    if let Some(url) = redis_query(&state, short_code.clone()).await {
        info!(short_code = %short_code, "Cache hit");
        return Redirect::permanent(&url).into_response();
    }

    info!(short_code = %short_code, "Cache miss");
    match grpc::core_get_link(&state, short_code.clone()).await {
        Ok(GetLinkResponse { target }) => Redirect::permanent(&target).into_response(),
        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}
