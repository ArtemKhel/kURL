use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use proto::core::{
    CreateLinkRequest, CreateLinkResponse, DeleteLinkRequest, GetLinkRequest, GetLinkResponse,
    link_service_client::LinkServiceClient,
    link_service_server::{LinkService, LinkServiceServer},
};
use tokio::{
    net::TcpListener,
    sync::Barrier,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::task::TaskTracker;
use tonic::{Code, Request, Response, Status, transport::Server};

use super::CoreClient;

#[derive(Debug)]
struct ServiceState {
    calls: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    delay: Duration,
}

impl ServiceState {
    fn new(delay: Duration) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            delay,
        }
    }
}

#[derive(Clone, Debug)]
struct TestLinkService {
    state: Arc<ServiceState>,
}

#[async_trait::async_trait]
impl LinkService for TestLinkService {
    async fn create_link(&self, _request: Request<CreateLinkRequest>) -> Result<Response<CreateLinkResponse>, Status> {
        Err(Status::unimplemented("not used by these tests"))
    }

    async fn get_link(&self, request: Request<GetLinkRequest>) -> Result<Response<GetLinkResponse>, Status> {
        self.state.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.state.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.state.max_active.fetch_max(active, Ordering::SeqCst);

        sleep(self.state.delay).await;
        self.state.active.fetch_sub(1, Ordering::SeqCst);

        let short_code = request.into_inner().short_code;
        if short_code == "missing" {
            return Err(Status::not_found("short code not found"));
        }

        Ok(Response::new(GetLinkResponse {
            target: format!("https://example.com/{short_code}"),
        }))
    }

    async fn delete_link(&self, _request: Request<DeleteLinkRequest>) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("not used by these tests"))
    }
}

async fn setup(
    max_in_flight: usize,
    delay: Duration,
) -> (Arc<CoreClient>, Arc<ServiceState>, TaskTracker, JoinHandle<()>) {
    let state = Arc::new(ServiceState::new(delay));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let service = TestLinkService { state: state.clone() };

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(LinkServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let grpc_client = LinkServiceClient::connect(format!("http://{address}")).await.unwrap();
    let task_tracker = TaskTracker::new();
    let client = Arc::new(CoreClient::with_max_in_flight(
        grpc_client,
        task_tracker.clone(),
        max_in_flight,
    ));

    (client, state, task_tracker, server)
}

async fn wait_until(condition: impl Fn() -> bool) {
    timeout(Duration::from_secs(1), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition was not met in time");
}

async fn finish(task_tracker: TaskTracker, server: JoinHandle<()>) {
    task_tracker.close();
    task_tracker.wait().await;
    server.abort();
}

#[tokio::test]
async fn concurrent_requests_for_the_same_code_share_one_rpc() {
    const REQUESTS: usize = 20;

    let (client, state, task_tracker, server) = setup(10, Duration::from_millis(50)).await;
    let barrier = Arc::new(Barrier::new(REQUESTS + 1));
    let mut requests = Vec::with_capacity(REQUESTS);

    for _ in 0..REQUESTS {
        let client = client.clone();
        let barrier = barrier.clone();
        requests.push(tokio::spawn(async move {
            barrier.wait().await;
            client.get_link("shared".to_string()).await
        }));
    }

    barrier.wait().await;
    for request in requests {
        let response = request.await.unwrap().unwrap();
        assert_eq!(response.target, "https://example.com/shared");
    }

    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    finish(task_tracker, server).await;
}

#[tokio::test]
async fn shared_errors_are_broadcast_and_the_flight_is_removed() {
    const REQUESTS: usize = 10;

    let (client, state, task_tracker, server) = setup(10, Duration::from_millis(50)).await;
    let barrier = Arc::new(Barrier::new(REQUESTS + 1));
    let mut requests = Vec::with_capacity(REQUESTS);

    for _ in 0..REQUESTS {
        let client = client.clone();
        let barrier = barrier.clone();
        requests.push(tokio::spawn(async move {
            barrier.wait().await;
            client.get_link("missing".to_string()).await
        }));
    }

    barrier.wait().await;
    for request in requests {
        let error = request.await.unwrap().unwrap_err();
        assert_eq!(error.code(), Code::NotFound);
    }

    assert_eq!(state.calls.load(Ordering::SeqCst), 1);
    wait_until(|| client.in_flight.is_empty()).await;

    let error = client.get_link("missing".to_string()).await.unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    assert_eq!(state.calls.load(Ordering::SeqCst), 2);

    finish(task_tracker, server).await;
}

#[tokio::test]
async fn semaphore_limits_concurrent_rpcs_for_distinct_codes() {
    const REQUESTS: usize = 8;
    const MAX_IN_FLIGHT: usize = 2;

    let (client, state, task_tracker, server) = setup(MAX_IN_FLIGHT, Duration::from_millis(50)).await;
    let barrier = Arc::new(Barrier::new(REQUESTS + 1));
    let mut requests = Vec::with_capacity(REQUESTS);

    for index in 0..REQUESTS {
        let client = client.clone();
        let barrier = barrier.clone();
        requests.push(tokio::spawn(async move {
            barrier.wait().await;
            client.get_link(format!("code-{index}")).await
        }));
    }

    barrier.wait().await;
    for request in requests {
        request.await.unwrap().unwrap();
    }

    assert_eq!(state.calls.load(Ordering::SeqCst), REQUESTS);
    assert_eq!(state.max_active.load(Ordering::SeqCst), MAX_IN_FLIGHT);
    finish(task_tracker, server).await;
}

#[tokio::test]
async fn cancelling_the_first_waiter_does_not_cancel_a_queued_flight() {
    let (client, state, task_tracker, server) = setup(1, Duration::from_millis(100)).await;

    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.get_link("first".to_string()).await });
    wait_until(|| state.active.load(Ordering::SeqCst) == 1).await;

    let second_client = client.clone();
    let second = tokio::spawn(async move { second_client.get_link("second".to_string()).await });
    wait_until(|| client.in_flight.contains_key("second")).await;
    second.abort();
    assert!(second.await.unwrap_err().is_cancelled());

    first.await.unwrap().unwrap();
    wait_until(|| state.calls.load(Ordering::SeqCst) == 2).await;

    let response = client.get_link("second".to_string()).await.unwrap();
    assert_eq!(response.target, "https://example.com/second");
    assert_eq!(state.calls.load(Ordering::SeqCst), 2);

    finish(task_tracker, server).await;
}
