use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use metrics::counter;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tonic::Code;
use tracing::{info, instrument, warn};
use url::Url;

use crate::{grpc, state::SharedState};

#[instrument(skip_all, fields(short_code = ?create_req.short_code, target = create_req.target))]
pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateLinkReq>,
) -> Result<Json<CreateLinkResp>, RequestError> {
    let request = match proto::core::CreateLinkRequest::try_from(create_req) {
        Ok(req) => req,
        Err(e) => return Err(e), // todo: match errors
    };

    match grpc::core_create_link(&state, request).await {
        // todo: actual url
        Ok(response) => {
            info!(short_code = response.short_code, "Link created successfully");
            counter!("gateway_links_created").increment(1);
            Ok(Json(response.into()))
        }
        Err(e) => match e.code() {
            Code::AlreadyExists => Err(RequestError::AlreadyExists),
            Code::InvalidArgument => todo!("also check other codes and handlers"),
            _ => {
                warn!(error = %e, "Failed to create link");
                Err(RequestError::Internal)
            }
        },
    }
}
#[derive(Deserialize, Debug)]
pub struct CreateLinkReq {
    short_code: Option<String>,
    target: String,
    expiration: Option<Expiration>,
}

#[derive(Deserialize, Debug)]
pub enum Expiration {
    DateTime(chrono::DateTime<chrono::Utc>),
    Duration(chrono::Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("Invalid short code")]
    InvalidShortCode,
    #[error("Invalid target URL")]
    InvalidTarget,
    #[error("Invalid expiration date")]
    InvalidExpiration,
    #[error("Link already exists")]
    AlreadyExists,
    #[error("Internal server error")]
    Internal,
}

impl TryFrom<CreateLinkReq> for proto::core::CreateLinkRequest {
    type Error = RequestError;

    fn try_from(value: CreateLinkReq) -> Result<Self, Self::Error> {
        let expiration: Option<chrono::DateTime<chrono::Utc>> = match value.expiration {
            Some(Expiration::DateTime(dt)) => Some(dt),
            Some(Expiration::Duration(dur)) => Some(chrono::Utc::now() + dur),
            None => None,
        };

        let expiration: Option<proto::prost_wkt_types::Timestamp> = if let Some(exp) = expiration
            && exp < chrono::Utc::now()
            && let Ok(ts) = exp.try_into()
        {
            Some(ts)
        } else {
            return Err(RequestError::InvalidExpiration);
        };

        let target = Url::parse(&value.target).map_err(|_| RequestError::InvalidTarget)?;

        let short_code = value.short_code; // todo: validate?

        Ok(proto::core::CreateLinkRequest {
            short_code,
            target: target.to_string(),
            expiration,
        })
    }
}

impl IntoResponse for RequestError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            RequestError::InvalidShortCode => (StatusCode::BAD_REQUEST, self.to_string()),
            RequestError::InvalidTarget => (StatusCode::BAD_REQUEST, self.to_string()),
            RequestError::InvalidExpiration => (StatusCode::BAD_REQUEST, self.to_string()),
            RequestError::AlreadyExists => (StatusCode::CONFLICT, self.to_string()),
            RequestError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = Json(json!({"error": message}));

        (status, body).into_response()
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct CreateLinkResp {
    short_code: String,
}

impl Into<CreateLinkResp> for proto::core::CreateLinkResponse {
    fn into(self) -> CreateLinkResp {
        CreateLinkResp {
            short_code: self.short_code,
        }
    }
}
