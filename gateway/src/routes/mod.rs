use axum::{http::StatusCode, response::IntoResponse};

pub mod create;
pub mod delete;
pub mod redirect;
pub mod root;

pub async fn not_found() -> impl IntoResponse { (StatusCode::NOT_FOUND, "_") }
