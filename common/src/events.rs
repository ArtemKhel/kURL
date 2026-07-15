use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ClickEvent {
    pub short_code: String,
    pub at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LinkStats {
    pub total_clicks: i64,
    pub weekly_clicks: [i64; 7],
    pub last_clicked_at: chrono::DateTime<chrono::Utc>,
}
