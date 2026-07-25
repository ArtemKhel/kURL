use tokio::signal;
use tracing::info;

pub async fn shutdown<F, Fut>(on_shutdown: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>, {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutting down...");
    on_shutdown().await;
    info!("Shutdown complete");
}
