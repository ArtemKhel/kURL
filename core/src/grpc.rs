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

use crate::{AppState, cache::CacheOp, utils};

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

        let short_code = short_code.unwrap_or_else(|| utils::random_string(6));

        let expiration = expiration.map(timestamp_to_datetime).transpose()?;
        if expiration.is_some_and(|expiration| expiration <= Utc::now()) {
            return Err(Status::invalid_argument("Expiration must be in the future"));
        }

        crate::db::create_link(&self.state.db_pool, &short_code, &target, expiration)
            .await
            .map_err(|e| match e {
                DbError::Conflict(_) => Status::already_exists("Short code already exists"),
                _ => {
                    error!(error=%e, "Database error while creating link");
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
                .map_err(|e| error!(error=%e, "Cache worker channel is closed"));
        }

        info!("Link created successfully");
        Ok(Response::new(CreateLinkResponse { short_code }))
    }

    #[instrument(skip(self))]
    async fn get_link(&self, request: Request<GetLinkRequest>) -> Result<Response<GetLinkResponse>, Status> {
        let GetLinkRequest { short_code } = request.into_inner();

        let link = crate::db::get_link(&self.state.db_pool, &short_code)
            .await
            .map_err(|e| match e {
                DbError::NotFound => Status::not_found("Short code not found"),
                _ => {
                    error!(error=%e, "Database error while getting the link");
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
                .map_err(|e| error!(error=%e, "Cache worker channel is closed"));
        }

        info!(target = %link.target, "Link found");
        Ok(Response::new(GetLinkResponse { target: link.target }))
    }

    #[instrument(skip(self))]
    async fn delete_link(&self, request: Request<DeleteLinkRequest>) -> Result<Response<()>, Status> {
        let DeleteLinkRequest { short_code } = request.into_inner();

        crate::db::delete_link(&self.state.db_pool, &short_code)
            .await
            .map_err(|e| match e {
                DbError::NotFound => Status::not_found("Short code not found"),
                _ => {
                    error!(error=%e, "Database error while deleting the link");
                    Status::internal("Failed to delete link")
                }
            })?;
        info!(short_code, "Link deleted successfully");

        let _ = self.state.redis_tx.send(CacheOp::Del { key: short_code }).map_err(|e| {
            error!(error=%e, "Cache worker channel is closed");
        });

        Ok(Response::new(()))
    }
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
mod tests {
    use chrono::Duration as ChronoDuration;

    use super::*;

    #[test]
    fn cache_ttl_uses_configured_ttl_without_expiration() {
        let configured_ttl = Duration::from_secs(60);

        assert_eq!(calculate_cache_ttl(configured_ttl, None), Some(configured_ttl));
    }

    #[test]
    fn cache_ttl_is_capped_by_expiration() {
        let configured_ttl = Duration::from_secs(60);
        let expiration = Utc::now() + ChronoDuration::seconds(5);

        let ttl = calculate_cache_ttl(configured_ttl, Some(expiration)).expect("expiration should be cacheable");

        assert!(ttl <= Duration::from_secs(5));
    }

    #[test]
    fn cache_ttl_does_not_cache_expired_links() {
        let expiration = Utc::now() - ChronoDuration::seconds(1);

        assert_eq!(calculate_cache_ttl(Duration::from_secs(60), Some(expiration)), None);
    }
}
