use tonic::{Request, Response, Status};
use proto::analytics::{GetLinkStatsRequest, LinkStats};

#[derive(Debug)]
pub struct AnalyticsService {
    pub state: (),
}

#[tonic::async_trait]
impl proto::analytics::analytics_server::Analytics for AnalyticsService {
    async fn get_link_stats(&self, request: Request<GetLinkStatsRequest>) -> Result<Response<LinkStats>, Status> {
        let GetLinkStatsRequest{short_code} = request.into_inner();

        todo!()
    }
}
