use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Debug, Default)]
pub struct State {
    // redis: deadpool_redis::Pool,
    pub(crate) db: HashMap<String, String>,
}

pub type SharedState = Arc<RwLock<State>>;
