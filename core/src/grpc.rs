use proto::url::{
    link_service_server, CreateLinkRequest, CreateLinkResponse, DeleteLinkRequest, GetLinkRequest, GetLinkResponse,
};
use tonic::{Request, Response, Status};
use tracing::error;

use crate::{
    db,
    db::links::{CreateLinkError, DeleteLinkError, GetLinkError},
    state::AppState,
};

#[derive(Debug)]
pub struct LinkService {
    pub state: AppState,
}

#[tonic::async_trait]
impl link_service_server::LinkService for LinkService {
    async fn create_link(&self, request: Request<CreateLinkRequest>) -> Result<Response<CreateLinkResponse>, Status> {
        let CreateLinkRequest { short_code, target } = request.into_inner();

        db::links::create_link(&self.state.db_pool, &short_code, &target)
            .await
            .map_err(|err| match err {
                CreateLinkError::Duplicate => Status::already_exists("Short code already exists"),
                CreateLinkError::Database(e) => {
                    error!("Database error while creating link: {e}");
                    Status::internal("Failed to create link")
                }
            })?;

        crate::cache::insert_link(self.state.redis.clone(), short_code.clone(), target).await;

        Ok(Response::new(CreateLinkResponse { short_code }))
        // todo: stats?
    }

    async fn get_link(&self, request: Request<GetLinkRequest>) -> Result<Response<GetLinkResponse>, Status> {
        let GetLinkRequest { short_code } = request.into_inner();

        let target = db::links::get_link(&self.state.db_pool, &short_code)
            .await
            .map_err(|err| match err {
                GetLinkError::NotFound => Status::not_found("Short code not found"),
                GetLinkError::Database(e) => {
                    error!("Database error while getting link: {e}");
                    Status::internal("Failed to get link")
                }
            })?;

        crate::cache::insert_link(self.state.redis.clone(), short_code, target.clone()).await;

        Ok(Response::new(GetLinkResponse { target }))
        // todo: stats?
    }

    async fn delete_link(&self, request: Request<DeleteLinkRequest>) -> Result<Response<()>, Status> {
        let DeleteLinkRequest { short_code } = request.into_inner();

        db::links::delete_link(&self.state.db_pool, &short_code)
            .await
            .map_err(|err| match err {
                DeleteLinkError::NotFound => Status::not_found("Short code not found"),
                DeleteLinkError::Database(e) => {
                    error!("Database error while getting link: {e}");
                    Status::internal("Failed to get link")
                }
            })?;

        crate::cache::delete_link(self.state.redis.clone(), short_code).await;
        Ok(Response::new(()))
    }
}
