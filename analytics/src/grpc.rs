use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct AnalyticsService {
    pub state: (),
}

#[tonic::async_trait]
impl proto::analytics::analytics_server::Analytics for AnalyticsService {
    async fn whatever(&self, request: Request<()>) -> Result<Response<()>, Status> { Ok(Response::new(())) }
}
