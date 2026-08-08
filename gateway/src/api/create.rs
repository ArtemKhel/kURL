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
            info!(monotonic_counter.gateway_links_created_tracing = 1);
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
            Some(exp) if exp > chrono::Utc::now() => Some(exp.try_into().map_err(|_| RequestError::InvalidExpiration)?),
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

#[cfg(test)]
mod tests {
    use super::*;

    // -- DateTime: full RFC 3339 --

    #[test]
    fn test_expiration_datetime_rfc3339() {
        let json_data = json!({
            "short_code": "example_link",
            "target": "https://example.com",
            "expiration": {"DateTime": "2037-01-01T12:34:56Z"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_some());
        assert_eq!(
            proto_req.expiration.unwrap().seconds,
            chrono::DateTime::parse_from_rfc3339("2037-01-01T12:34:56Z")
                .unwrap()
                .timestamp()
        );
    }

    // -- DateTime: partial date (YYYY-MM-DD) --

    #[test]
    fn test_expiration_datetime_partial_date() {
        let json_data = json!({
            "short_code": "example_link",
            "target": "https://example.com",
            "expiration": {"DateTime": "2037-01-01"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_some());
        assert_eq!(
            proto_req.expiration.unwrap().seconds,
            chrono::DateTime::parse_from_rfc3339("2037-01-01T00:00:00Z")
                .unwrap()
                .timestamp()
        );
    }

    // -- DateTime: invalid string --

    #[test]
    fn test_expiration_datetime_invalid_string() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"DateTime": "not-a-date"}
        });
        let result: Result<CreateLinkReq, _> = serde_json::from_value(json_data);
        assert!(result.is_err(), "expected parse error for invalid date");
    }

    // -- DateTime: past date rejected at conversion --

    #[test]
    fn test_expiration_datetime_past_date_rejected() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"DateTime": "2020-01-01T00:00:00Z"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let res = proto::core::CreateLinkRequest::try_from(req);
        assert!(matches!(res, Err(RequestError::InvalidExpiration)));
    }

    // -- DateTime: past partial date rejected at conversion --

    #[test]
    fn test_expiration_datetime_past_partial_date_rejected() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"DateTime": "2020-06-15"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let res = proto::core::CreateLinkRequest::try_from(req);
        assert!(matches!(res, Err(RequestError::InvalidExpiration)));
    }

    // -- No expiration --

    #[test]
    fn test_no_expiration() {
        let json_data = json!({
            "short_code": "example_link",
            "target": "https://example.com"
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_none());
    }

    // -- Duration: humantime strings --

    #[test]
    fn test_expiration_duration_days() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"Duration": "30days"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_some());

        let exp_secs = proto_req.expiration.unwrap().seconds;
        let now = chrono::Utc::now().timestamp();
        let expected = now + 30 * 24 * 3600;
        // Allow ±5 s of clock drift.
        assert!((exp_secs - expected).abs() < 5, "expected ~{expected} got {exp_secs}");
    }

    #[test]
    fn test_expiration_duration_hours() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"Duration": "2h"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_some());

        let exp_secs = proto_req.expiration.unwrap().seconds;
        let now = chrono::Utc::now().timestamp();
        let expected = now + 2 * 3600;
        assert!((exp_secs - expected).abs() < 5, "expected ~{expected} got {exp_secs}");
    }

    #[test]
    fn test_expiration_duration_seconds() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"Duration": "90s"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_some());
    }

    #[test]
    fn test_expiration_duration_compound() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"Duration": "1day 12h 30min"}
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let proto_req = proto::core::CreateLinkRequest::try_from(req).unwrap();
        assert!(proto_req.expiration.is_some());
    }

    // -- Duration: invalid string --

    #[test]
    fn test_expiration_duration_invalid_string() {
        let json_data = json!({
            "target": "https://example.com",
            "expiration": {"Duration": "not-a-duration"}
        });
        let result: Result<CreateLinkReq, _> = serde_json::from_value(json_data);
        assert!(result.is_err(), "expected parse error for invalid duration");
    }

    // -- Invalid target URL --

    #[test]
    fn test_invalid_target_url() {
        let json_data = json!({
            "target": "not-a-url"
        });
        let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
        let res = proto::core::CreateLinkRequest::try_from(req);
        assert!(matches!(res, Err(RequestError::InvalidTarget)));
    }
}
