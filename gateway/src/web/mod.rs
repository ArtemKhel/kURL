use axum::{http::StatusCode, response::IntoResponse};

pub mod web;
pub async fn not_found() -> impl IntoResponse { (StatusCode::NOT_FOUND, "_") }
