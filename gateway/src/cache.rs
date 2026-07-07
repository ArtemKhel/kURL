use redis::AsyncTypedCommands;

use crate::state::SharedState;

trait Cache {
    // todo:
    async fn query(state: &SharedState, short_code: String) -> Option<String> { None }
}

pub async fn redis_query(state: &SharedState, short_code: String) -> Option<String> {
    // todo: unwraps
    let mut redis_conn = state.redis.get().await.unwrap();
    redis_conn.get(short_code).await.unwrap()
}
