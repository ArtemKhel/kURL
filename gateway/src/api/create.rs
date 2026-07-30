use axum::{extract::State, http::StatusCode, Json};
use metrics::counter;
use proto::core::{CreateLinkRequest, CreateLinkResponse};
use tonic::Code;
use tracing::{info, instrument, warn};

use crate::{grpc, state::SharedState};

#[instrument(skip_all, fields(short_code = ?create_req.short_code, target = create_req.target))]
pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateLinkRequest>,
) -> Result<Json<CreateLinkResponse>, StatusCode> {
    match grpc::core_create_link(&state, create_req).await {
        // todo: actual url
        Ok(response) => {
            info!(short_code = response.short_code, "Link created successfully");
            counter!("gateway_links_created").increment(1);
            Ok(Json(response))
        }
        Err(e) => match e.code() {
            Code::AlreadyExists => Err(StatusCode::CONFLICT),
            _ => {
                warn!(error = %e, "Failed to create link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
