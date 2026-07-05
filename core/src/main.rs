use tonic::{Request, Response, Status};
use proto::url::{CreateLinkRequest, CreateLinkResponse, GetLinkRequest, GetLinkResponse};
use proto::url::link_service_server::LinkService;
struct Test;

impl LinkService for Test {
    async fn create_link(&self, request: Request<CreateLinkRequest>) -> Result<Response<CreateLinkResponse>, Status> {
        todo!()
    }

    async fn get_link(&self, request: Request<GetLinkRequest>) -> Result<Response<GetLinkResponse>, Status> {
        todo!()
    }
}
fn main() {
    println!("Hello, world!");
}
