mod api;
mod cache;
mod grpc;
mod init;
mod state;
mod web;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use axum::{
    Router,
    routing::{delete, get, post},
};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::info;
use utoipa::OpenApi;
use utoipa_redoc::{Redoc, Servable};

use crate::{init::init, state::AppState};

pub(crate) type Config = common::config::GatewayConfig;

#[derive(OpenApi)]
#[openapi(
    paths(
        api::create::create,
        api::delete::delete,
        api::stats::link_stats,
        api::stats::global_stats,
        api::redirect::redirect
    ),
    components(
        schemas(
            api::create::CreateLinkReq,
            api::create::Expiration,
            api::create::CreateLinkResp,
            api::delete::DeleteLinkReq,
            api::stats::LinkStatsResp,
            api::stats::DailyClicksResp,
            api::stats::GlobalStatsResp,
        )
    ),
    tags(
        (name = "links", description = "Link management API")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = common::config::AppConfig::load()?.into();
    let otel_guard = common::logging::init_tracing(&config.logging, "gateway");
    info!(?config);

    let (redis, grpc_client, analytics_client) =
        init(&config).await.expect("Failed to connect to Redis or gRPC server");

    let addr = format!("0.0.0.0:{}", config.gateway.port)
        .parse::<SocketAddr>()
        .context("Failed to parse socket address")?;
    let listener = TcpListener::bind(addr).await?;

    let state = Arc::new(AppState {
        config,
        redis,
        grpc_client,
        analytics_client,
    });

    let app = Router::new()
        .without_v07_checks()
        .layer(TraceLayer::new_for_http()) //todo: opts, feature flags
        .route("/", get(web::web::hello))
        .route("/api/create", post(api::create::create))
        .route("/api/delete", delete(api::delete::delete))
        .route("/api/stats/{code}", get(api::stats::link_stats))
        .route("/api/stats", get(api::stats::global_stats))
        .route("/s/{code}", get(api::redirect::redirect))
        .merge(Redoc::with_url("/api-docs", ApiDoc::openapi()))
        .fallback(web::not_found)
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(common::shutdown(async move || eprintln!("shutting down")))
        .await?;

    drop(otel_guard);
    Ok(())
}
