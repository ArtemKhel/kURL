use redis::AsyncTypedCommands;

use crate::state::SharedState;

pub async fn redis_query(state: &SharedState, short_code: String) -> Option<String> {
    // todo: unwraps
    let mut redis_conn = state.read().await.redis.get().await.unwrap();
    redis_conn.get(short_code).await.unwrap()
}

pub async fn redis_set(state: &SharedState, short_code: String, target_url: String) {
    let mut redis_conn = state.write().await.redis.get().await.unwrap();
    redis_conn
        .set_ex(short_code, target_url, state.read().await.config.redis_ttl)
        .await
        .unwrap()
}
