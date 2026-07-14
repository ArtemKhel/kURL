use common::{events::ClickEvent, redis_keys::RedisKeys};
use redis::AsyncTypedCommands;
use tracing::{error, info, instrument, warn};

use crate::state::SharedState;

// trait Cache {
//     // todo:
//     async fn query(state: &SharedState, short_code: String) -> Option<String> { None }
// }

#[instrument(skip(state))]
pub async fn redis_query(state: &SharedState, short_code: String) -> Option<String> {
    // todo: unwraps
    let mut redis_conn = state.redis.get().await.unwrap();
    redis_conn.get(RedisKeys::link_cache_key(&short_code)).await.unwrap()
}

#[instrument(skip(state))]
pub async fn send_click_event(state: &SharedState, short_code: &str) {
    let short_code = short_code.to_string();
    let state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = inner_send_click_event(state, short_code).await {
            error!(error = %e, "Failed to send click event");
        }
    });
}
#[instrument(skip(state))]
async fn inner_send_click_event(state: SharedState, short_code: String) -> Result<(), Box<dyn std::error::Error>> {
    let click_event = ClickEvent {
        short_code,
        at: chrono::Utc::now(),
    };
    info!(?click_event, "Click event");
    let mut redis_conn = state.redis.get().await?;
    redis_conn
        .xadd("Clicks", "*", &[(
            "event".to_string(),
            serde_json::to_string(&click_event).unwrap(),
        )])
        .await?;
    Ok(())
}
