#[derive(Debug)]
pub struct ClickEvent {
    pub short_code: String,
    pub time: chrono::DateTime<chrono::Utc>,
}

// TODO: proc macro
impl ClickEvent {
    pub fn as_redis_args(&self) -> Vec<(String, String)> {
        vec![
            ("short_code".to_string(), self.short_code.clone()),
            ("time".to_string(), self.time.to_string()),
        ]
    }
}
