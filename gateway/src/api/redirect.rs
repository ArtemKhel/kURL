use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Redirect,
};
use metrics::counter;
use tonic::Code;
use tracing::{info, instrument, warn};

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
    if let Some(url) = redis_query(&state, short_code.clone()).await {
        info!("Cache hit");
        counter!("gateway_redirects").increment(1);
        counter!("gateway_cache_hits").increment(1);
        send_click_event(&state, &short_code).await;
        return Ok(Redirect::permanent(&url));
    }

    match grpc::core_get_link(&state, short_code.clone()).await {
        // todo: permanent with expire?
        Ok(target) => {
            counter!("gateway_redirects").increment(1);
            send_click_event(&state, &short_code).await;
            Ok(Redirect::permanent(&target.target))
        }
        Err(e) => match e.code() {
            Code::NotFound => {
                info!("Short code not found");
                Err(StatusCode::NOT_FOUND)
            }
            Code::FailedPrecondition => {
                info!("Link has expired");
                Err(StatusCode::GONE)
            }
            _ => {
                warn!(error = %e, "Failed to get link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
