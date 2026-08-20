use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::NaiveDate;
use metrics::counter;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use tonic::Code;
use tracing::{info, instrument, warn};
use url::Url;
use utoipa::ToSchema;

use crate::state::SharedState;

#[utoipa::path(
    post,
    path = "/api/create",
    request_body = CreateLinkReq,
    responses(
        (status = 200, description = "Link created successfully", body = CreateLinkResp),
        (status = 400, description = "Invalid request"),
        (status = 409, description = "Link already exists"),
        (status = 500, description = "Internal server error")
    ),
    tag = "links"
)]
#[instrument(skip_all, fields(short_code = ?create_req.short_code, target = create_req.target))]
pub async fn create(
    State(state): State<SharedState>,
    Json(create_req): Json<CreateLinkReq>,
) -> Result<Json<CreateLinkResp>, RequestError> {
    let request = match proto::core::CreateLinkRequest::try_from(create_req) {
        Ok(req) => req,
        Err(e) => return Err(e), // todo: match errors
    };

    match state.core_client.create_link(request).await {
        // todo: actual url
        Ok(response) => {
            info!(short_code = response.short_code, "Link created successfully");
            counter!("gateway_links_created").increment(1);
            info!(monotonic_counter.gateway_links_created_tracing = 1);
            Ok(Json(response.into()))
        }
        Err(error) => match error.code() {
            Code::AlreadyExists => Err(RequestError::AlreadyExists),
            Code::InvalidArgument => todo!("also check other codes and handlers"),
            _ => {
                warn!(%error, "Failed to create link");
                Err(RequestError::Internal)
            }
        },
    }
}
#[derive(Deserialize, Debug, ToSchema)]
pub struct CreateLinkReq {
    short_code: Option<String>,
    target: String,
    expiration: Option<Expiration>,
}

#[derive(Deserialize, Debug, ToSchema)]
pub enum Expiration {
    /// RFC 3339 timestamp (`"2027-01-01T00:00:00Z"`) or partial date strings (`"2027-01-01"`) (assumes midnight UTC)
    #[serde(deserialize_with = "deserialize_datetime")]
    DateTime(chrono::DateTime<chrono::Utc>),

    /// Relative duration from now, anything parsable with [`humantime`]
    #[serde(deserialize_with = "deserialize_duration")]
    Duration(chrono::Duration),
}
fn deserialize_datetime<'de, D>(deserializer: D) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
        return Ok(dt.with_timezone(&chrono::Utc));
    }

    NaiveDate::parse_from_str(&s, "%Y-%m-%d")
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
        .map_err(|_| serde::de::Error::custom(format!("invalid date/datetime `{s}`; expected RFC 3339 or YYYY-MM-DD")))
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<chrono::Duration, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;

    let std_dur = s
        .parse::<humantime::Duration>()
        .map_err(|e| serde::de::Error::custom(format!("invalid duration `{s}`: {e}")))?;

    chrono::Duration::from_std(*std_dur).map_err(|e| serde::de::Error::custom(format!("duration out of range: {e}")))
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

        let expiration: Option<proto::prost_wkt_types::Timestamp> = match expiration {
            None => None,
            Some(exp) if exp > chrono::Utc::now() => {
                #[allow(clippy::unnecessary_fallible_conversions)] // `.into()` fails to infer type
                Some(exp.try_into().map_err(|_| RequestError::InvalidExpiration)?)
            }
            Some(_) => return Err(RequestError::InvalidExpiration),
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
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct CreateLinkResp {
    short_code: String,
}

impl From<proto::core::CreateLinkResponse> for CreateLinkResp {
    fn from(val: proto::core::CreateLinkResponse) -> Self {
        CreateLinkResp {
            short_code: val.short_code,
        }
    }
}

#[cfg(test)]
#[path = "create_tests.rs"]
mod tests;
