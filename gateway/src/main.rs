#![feature(map_try_insert)]

mod config;
mod routes;
mod state;

use axum::{
    Router,
    response::Html,
    routing::{get, post},
};
use clap::Parser;
use common;
use tokio::net::TcpListener;

use crate::{config::Config, state::SharedState};

#[tokio::main]
async fn main() {
    // todo: tracing-sub
    tracing_subscriber::fmt::init();
    let config = Config::parse();
    dbg!(config);

    let state = SharedState::default();

    let app = Router::new()
        .without_v07_checks()
        .route("/", get(hello))
        .route("/:{code}", get(routes::redirect::redirect))
        .route("/api/create", post(routes::api::create))
        .with_state(state);
    let listener = TcpListener::bind("localhost:3000").await.unwrap();
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown())
        .await;
    ()
}

async fn hello() -> Html<&'static str> { Html("<h1>Hello, World!</h1>") }
