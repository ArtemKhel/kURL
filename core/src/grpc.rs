use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use common::db_utils::DbError;
use proto::core::{
    CreateLinkRequest, CreateLinkResponse, DeleteLinkRequest, GetLinkRequest, GetLinkResponse, link_service_server,
};
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::{AppState, cache::CacheOp, db, utils};

#[derive(Debug)]
pub struct LinkService {
    pub state: Arc<AppState>,
}

#[tonic::async_trait]
impl link_service_server::LinkService for LinkService {
    #[instrument(skip(self))]
    async fn create_link(&self, request: Request<CreateLinkRequest>) -> Result<Response<CreateLinkResponse>, Status> {
        let CreateLinkRequest {
            short_code,
            target,
            expiration,
        } = request.into_inner();

        // let short_code = short_code.unwrap_or_else(|| generate_short_code(self.state.db_pool));
        let short_code = if let Some(code) = short_code {
            code
        } else {
            generate_short_code(self.state.db_pool.clone())
                .await
                .ok_or_else(|| Status::internal("Failed to generate a unique short code"))?
        };

        let expiration = expiration.map(timestamp_to_datetime).transpose()?;
        if expiration.is_some_and(|expiration| expiration <= Utc::now()) {
            return Err(Status::invalid_argument("Expiration must be in the future"));
        }

        self.state
            .db_pool
            .create_link(&short_code, &target, expiration)
            .await
            .map_err(|error| match error {
                DbError::Conflict(_) => Status::already_exists("Short code already exists"),
                _ => {
                    error!(%error, "Database error while creating link");
                    Status::internal("Failed to create link")
                }
            })?;

        if let Some(ttl) = calculate_cache_ttl(self.state.config.redis.cache_ttl, expiration) {
            let _ = self
                .state
                .redis_tx
                .send(CacheOp::Set {
                    key: short_code.clone(),
                    value: target.clone(),
                    ttl,
                })
                .map_err(|error| error!(%error, "Cache worker channel is closed"));
        }

        info!("Link created successfully");
        Ok(Response::new(CreateLinkResponse { short_code }))
    }

    #[instrument(skip(self))]
    async fn get_link(&self, request: Request<GetLinkRequest>) -> Result<Response<GetLinkResponse>, Status> {
        let GetLinkRequest { short_code } = request.into_inner();

        let link = self
            .state
            .db_pool
            .get_link(&short_code)
            .await
            .map_err(|error| match error {
                DbError::NotFound => Status::not_found("Short code not found"),
                _ => {
                    error!(%error, "Database error while getting the link");
                    Status::internal("Failed to get link")
                }
            })?;

        if link.expiration.is_some_and(|expiration| expiration <= Utc::now()) {
            return Err(Status::failed_precondition("Link has expired"));
        }

        if let Some(ttl) = calculate_cache_ttl(self.state.config.redis.cache_ttl, link.expiration) {
            let _ = self
                .state
                .redis_tx
                .send(CacheOp::Set {
                    key: short_code,
                    value: link.target.clone(),
                    ttl,
                })
                .map_err(|error| error!(%error, "Cache worker channel is closed"));
        }

        info!(target = %link.target, "Link found");
        Ok(Response::new(GetLinkResponse { target: link.target }))
    }

    #[instrument(skip(self))]
    async fn delete_link(&self, request: Request<DeleteLinkRequest>) -> Result<Response<()>, Status> {
        let DeleteLinkRequest { short_code } = request.into_inner();

        self.state
            .db_pool
            .delete_link(&short_code)
            .await
            .map_err(|error| match error {
                DbError::NotFound => Status::not_found("Short code not found"),
                _ => {
                    error!(%error, "Database error while deleting the link");
                    Status::internal("Failed to delete link")
                }
            })?;
        info!(short_code, "Link deleted successfully");

        let _ = self
            .state
            .redis_tx
            .send(CacheOp::Del { key: short_code })
            .map_err(|error| {
                error!(%error, "Cache worker channel is closed");
            });

        Ok(Response::new(()))
    }
}

async fn generate_short_code(db_pool: Arc<dyn db::LinkRepository>) -> Option<String> {
    const RETRIES: usize = 10;
    const LEN: usize = 6;
    for _ in 0..RETRIES {
        let code = utils::random_string(LEN);
        if let Ok(false) = db_pool.link_exists(&code).await {
            return Some(code);
        }
    }
    None
}

fn timestamp_to_datetime(timestamp: proto::prost_wkt_types::Timestamp) -> Result<DateTime<Utc>, Status> {
    let timestamp: SystemTime = timestamp
        .try_into()
        .map_err(|_| Status::invalid_argument("Invalid expiration timestamp"))?;
    Ok(timestamp.into())
}

fn calculate_cache_ttl(cache_ttl: Duration, expiration: Option<DateTime<Utc>>) -> Option<Duration> {
    let ttl = match expiration {
        Some(expiration) => (expiration - Utc::now())
            .to_std()
            .ok()
            .map(|remaining| remaining.min(cache_ttl)),
        None => Some(cache_ttl),
    };

    ttl.filter(|ttl| ttl.as_secs() > 0)
}

#[cfg(test)]
#[path = "grpc_tests.rs"]
mod grpc_tests;
