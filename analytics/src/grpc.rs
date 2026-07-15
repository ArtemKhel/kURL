use proto::analytics::{GetLinkStatsRequest, LinkStats};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct AnalyticsService {
    pub state: (),
}

#[tonic::async_trait]
impl proto::analytics::analytics_server::Analytics for AnalyticsService {
    async fn get_link_stats(&self, request: Request<GetLinkStatsRequest>) -> Result<Response<LinkStats>, Status> {
        let GetLinkStatsRequest { short_code: _ } = request.into_inner();

        todo!()
    }
}
