use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Redirect,
};
use tonic::Code;
use tracing::{debug, info, instrument, warn};

use crate::{
    cache::{redis_query, send_click_event},
    grpc,
    state::SharedState,
};

#[instrument(skip_all, fields(short_code = short_code))]
pub async fn redirect(
    State(state): State<SharedState>,
    Path(short_code): Path<String>,
) -> Result<Redirect, StatusCode> {
    debug!("redirecting");
    if let Some(url) = redis_query(&state, short_code.clone()).await {
        info!("Cache hit");
        send_click_event(&state, &short_code).await;
        return Ok(Redirect::permanent(&url));
    }

    info!("Cache miss");
    match grpc::core_get_link(&state, short_code.clone()).await {
        // todo: permanent with expire?
        Ok(target) => {
            send_click_event(&state, &short_code).await;
            Ok(Redirect::permanent(&target))
        }
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
