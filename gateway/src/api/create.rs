use axum::{extract::State, http::StatusCode, Json};
use proto::core::CreateLinkRequest;
use serde::{Deserialize, Serialize};
use tonic::Code;
use tracing::{debug, instrument, warn};

use crate::{grpc, state::SharedState};

// #[derive(Deserialize, Debug)]
// pub struct CreateReq {
//     short_code: Option<String>,
//     target: String,
//     expiration: Option<String>,
//     private: bool,
// }

#[derive(Serialize)]
pub struct CreateResp {
    // todo: url
    url: String,
}

#[instrument(skip_all, fields(short_code = ?create_req.short_code, target = create_req.target))]
pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateLinkRequest>,
) -> Result<Json<CreateResp>, StatusCode> {
    debug!("creating a new link");
    // let short_code = create_req.short_code.expect("auto generation isn't supported");
    match grpc::core_create_link(&state, create_req).await {
        Ok(short_code) => Ok(Json(CreateResp { url: short_code })),
        Err(e) => match e.code() {
            Code::AlreadyExists => Err(StatusCode::CONFLICT),
            _ => {
                warn!(error = %e, "Failed to create link");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}
