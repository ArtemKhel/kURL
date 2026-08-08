use serde_json::json;

use crate::api::create::{CreateLinkReq, RequestError};

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

#[test]
fn test_expiration_datetime_invalid_string() {
    let json_data = json!({
        "target": "https://example.com",
        "expiration": {"DateTime": "not-a-date"}
    });
    let result: Result<CreateLinkReq, _> = serde_json::from_value(json_data);
    assert!(result.is_err(), "expected parse error for invalid date");
}

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

#[test]
fn test_expiration_duration_invalid_string() {
    let json_data = json!({
        "target": "https://example.com",
        "expiration": {"Duration": "not-a-duration"}
    });
    let result: Result<CreateLinkReq, _> = serde_json::from_value(json_data);
    assert!(result.is_err(), "expected parse error for invalid duration");
}

#[test]
fn test_invalid_target_url() {
    let json_data = json!({
        "target": "not-a-url"
    });
    let req: CreateLinkReq = serde_json::from_value(json_data).unwrap();
    let res = proto::core::CreateLinkRequest::try_from(req);
    assert!(matches!(res, Err(RequestError::InvalidTarget)));
}
