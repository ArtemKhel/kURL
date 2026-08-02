use std::sync::Arc;

use common::db_utils::DbError;
use proto::core::{
    CreateLinkRequest, CreateLinkResponse, DeleteLinkRequest, GetLinkRequest, GetLinkResponse, link_service_server,
};
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};

use crate::{AppState, utils};

#[derive(Debug)]
pub struct LinkService {
    pub state: Arc<AppState>,
}

#[tonic::async_trait]
impl link_service_server::LinkService for LinkService {
    #[instrument(skip(self))]
    async fn create_link(&self, request: Request<CreateLinkRequest>) -> Result<Response<CreateLinkResponse>, Status> {
        let CreateLinkRequest { short_code, target, .. } = request.into_inner();

        let short_code = short_code.unwrap_or_else(|| utils::random_string(6));

        crate::db::create_link(&self.state.db_pool, &short_code, &target)
            .await
            .map_err(|e| match e {
                DbError::Conflict(_) => Status::already_exists("Short code already exists"),
                _ => {
                    error!(error=%e, "Database error while creating link");
                    Status::internal("Failed to create link")
                }
            })?;
        info!("Link created successfully");

        crate::cache::insert_link(
            self.state.redis.clone(),
            short_code.clone(),
            target,
            self.state.config.redis.cache_ttl,
        );

        Ok(Response::new(CreateLinkResponse { short_code }))
    }

    #[instrument(skip(self))]
    async fn get_link(&self, request: Request<GetLinkRequest>) -> Result<Response<GetLinkResponse>, Status> {
        let GetLinkRequest { short_code } = request.into_inner();

        let target = crate::db::get_link(&self.state.db_pool, &short_code)
            .await
            .map_err(|e| match e {
                DbError::NotFound => Status::not_found("Short code not found"),
                _ => {
                    error!(error=%e, "Database error while getting the link");
                    Status::internal("Failed to get link")
                }
            })?;
        info!(target, "Link found");

        crate::cache::insert_link(
            self.state.redis.clone(),
            short_code,
            target.clone(),
            self.state.config.redis.cache_ttl,
        );

        Ok(Response::new(GetLinkResponse { target }))
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

        crate::cache::delete_link(self.state.redis.clone(), short_code);
        Ok(Response::new(()))
    }
}
