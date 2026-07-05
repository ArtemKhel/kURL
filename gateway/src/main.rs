#![feature(map_try_insert)]

mod config;
mod routes;
mod state;

use axum::{
    routing::{get, post}
    ,
    Router,
};
use clap::Parser;
use common;
use tokio::net::TcpListener;
use routes::root;
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
        .route("/", get(routes::root::hello))
        .route("/api/create", post(routes::api::create))
        .route("/s/{code}", get(routes::redirect::redirect))
        .with_state(state);
    let listener = TcpListener::bind(config.listener_address).await.unwrap();
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown())
        .await;
    ()
}

