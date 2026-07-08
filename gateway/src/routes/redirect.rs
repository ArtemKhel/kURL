use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
};
use tonic::Code;
use tracing::{info, warn};

use crate::{cache::redis_query, grpc, state::SharedState};

pub async fn redirect(State(state): State<SharedState>, Path(short_code): Path<String>) -> Response {
    // todo: unwraps
    if let Some(url) = redis_query(&state, short_code.clone()).await {
        info!(short_code = %short_code, "Cache hit");
        return Redirect::permanent(&url).into_response();
    }

    info!(short_code = %short_code, "Cache miss");
    match grpc::core_get_link(&state, short_code.clone()).await {
        // todo: permanent with expire?
        Ok(target) => Redirect::permanent(&target).into_response(),
        Err(e) => match e.code() {
            Code::NotFound => {
                info!(short_code = %short_code, "Short code not found");
                (StatusCode::NOT_FOUND, "Not Found").into_response()
            }
            _ => {
                warn!(error = %e, short_code = %short_code, "Failed to get link");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
            }
        },
    }
}
