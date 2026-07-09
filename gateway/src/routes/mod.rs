use axum::{
    http::StatusCode,
    response::IntoResponse,
};

pub mod create;
mod delete;
pub mod redirect;
pub mod root;
pub(crate) mod web;

pub async fn not_found() -> impl IntoResponse { (StatusCode::NOT_FOUND, "_") }
