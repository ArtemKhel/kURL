#[derive(Debug)]
pub struct AppState {
    pub config: crate::Config,
    pub db_pool: sqlx::PgPool,
    pub redis: deadpool_redis::Pool,
}
