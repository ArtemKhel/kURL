#[derive(Debug, Clone)]
pub struct AppState {
    pub db_pool: sqlx::PgPool,
    pub redis: deadpool_redis::Pool,
}