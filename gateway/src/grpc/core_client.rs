use std::sync::Arc;

use dashmap::{DashMap, Entry};
use proto::core::{
    CreateLinkRequest, CreateLinkResponse, DeleteLinkRequest, GetLinkRequest, GetLinkResponse,
    link_service_client::LinkServiceClient,
};
use tokio::{sync::Semaphore, time::timeout};
use tokio_util::task::TaskTracker;
use tonic::{Response, Status, transport::Channel};

const GRPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const MAX_IN_FLIGHT: usize = 100;

#[derive(Debug)]
pub struct CoreClient {
    client: LinkServiceClient<Channel>,
    in_flight: Arc<DashMap<String, tokio::sync::watch::Receiver<Option<tonic::Result<GetLinkResponse>>>>>,
    task_tracker: TaskTracker,
    semaphore: Arc<Semaphore>,
}

impl CoreClient {
    pub fn new(client: LinkServiceClient<Channel>, task_tracker: TaskTracker) -> Self {
        Self::with_max_in_flight(client, task_tracker, MAX_IN_FLIGHT)
    }

    fn with_max_in_flight(client: LinkServiceClient<Channel>, task_tracker: TaskTracker, max_in_flight: usize) -> Self {
        Self {
            client,
            in_flight: Arc::new(DashMap::new()),
            task_tracker,
            semaphore: Arc::new(Semaphore::new(max_in_flight)),
        }
    }

    pub async fn get_link(&self, code: String) -> tonic::Result<GetLinkResponse> {
        let mut rx = match self.in_flight.entry(code.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let (tx, rx) = tokio::sync::watch::channel(None);

                let key = entry.key().clone();
                let client = self.client.clone();

                entry.insert(rx.clone());

                // todo: other options: shared future, OnceCell, or leader + reelection?
                let in_flight = self.in_flight.clone();
                let rx_clone = rx.clone();
                let semaphore = self.semaphore.clone();
                self.task_tracker.spawn(async move {
                    let permit = semaphore.acquire_owned().await.expect("Semaphore closed unexpectedly");
                    let result = Self::core_get_link(client, &key).await;
                    drop(permit);
                    let _ = tx.send_replace(Some(result));
                    in_flight.remove_if(&key, |_, map_rx| rx_clone.same_channel(map_rx));
                });
                rx
            }
        };

        let rx_clone = rx.clone();
        match rx.wait_for(Option::is_some).await {
            Ok(v) => v.clone().expect("waiting for Some"),
            Err(_) => {
                self.in_flight
                    .remove_if(&code, |_, map_rx| map_rx.same_channel(&rx_clone));
                Err(Status::unavailable("in-flight gRPC task stopped"))
            }
        }
    }

    async fn call_with_timeout<F, T>(f: F) -> tonic::Result<T>
    where F: IntoFuture<Output = tonic::Result<Response<T>>> {
        timeout(GRPC_TIMEOUT, f)
            .await
            .unwrap_or(Err(Status::deadline_exceeded("gRPC request timed out")))
            .map(Response::into_inner)
    }

    async fn core_get_link(mut client: LinkServiceClient<Channel>, short_code: &str) -> tonic::Result<GetLinkResponse> {
        let request = tonic::Request::new(GetLinkRequest {
            short_code: short_code.to_string(),
        });
        Self::call_with_timeout(client.get_link(request)).await
    }

    pub async fn create_link(&self, request: CreateLinkRequest) -> tonic::Result<CreateLinkResponse> {
        let mut client = self.client.clone();
        Self::call_with_timeout(client.create_link(request)).await
    }

    pub async fn delete_link(&self, request: DeleteLinkRequest) -> tonic::Result<()> {
        let mut client = self.client.clone();
        Self::call_with_timeout(client.delete_link(request)).await
    }
}

#[cfg(test)]
#[path = "core_client_tests.rs"]
mod tests;
