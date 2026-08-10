use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, NaiveDate, Utc};
use tonic::Code;

use super::*;

#[derive(Debug, Default)]
struct FakeAnalyticsRepo {
    link_totals: HashMap<String, (i64, Option<DateTime<Utc>>)>,
    link_daily: HashMap<String, Vec<(NaiveDate, i64)>>,
    global_total: i64,
    global_daily: Vec<(NaiveDate, i64)>,
    last_requested_link_days: Mutex<Option<i32>>,
    last_requested_global_days: Mutex<Option<i32>>,
    fail_link_totals: bool,
    fail_link_daily: bool,
    fail_global_total: bool,
    fail_global_daily: bool,
}

#[tonic::async_trait]
impl AnalyticsRepository for FakeAnalyticsRepo {
    async fn get_link_totals(&self, short_code: &str) -> Result<(i64, Option<DateTime<Utc>>), db::DbError> {
        if self.fail_link_totals {
            return Err(db::DbError::Other(sqlx::Error::RowNotFound));
        }
        self.link_totals.get(short_code).cloned().ok_or(db::DbError::NotFound)
    }

    async fn get_link_stats(&self, short_code: &str, days: i32) -> Result<Vec<(NaiveDate, i64)>, db::DbError> {
        if self.fail_link_daily {
            return Err(db::DbError::Other(sqlx::Error::RowNotFound));
        }
        if let Ok(mut guard) = self.last_requested_link_days.lock() {
            *guard = Some(days);
        }
        Ok(self.link_daily.get(short_code).cloned().unwrap_or_default())
    }

    async fn get_global_total_clicks(&self) -> Result<i64, db::DbError> {
        if self.fail_global_total {
            return Err(db::DbError::Other(sqlx::Error::RowNotFound));
        }
        Ok(self.global_total)
    }

    async fn get_global_daily_stats(&self, days: i32) -> Result<Vec<(NaiveDate, i64)>, db::DbError> {
        if self.fail_global_daily {
            return Err(db::DbError::Other(sqlx::Error::RowNotFound));
        }
        if let Ok(mut guard) = self.last_requested_global_days.lock() {
            *guard = Some(days);
        }
        Ok(self.global_daily.clone())
    }
}

#[test]
fn test_clamp_days() {
    assert_eq!(clamp_days(None), 7);
    assert_eq!(clamp_days(Some(14)), 14);
    assert_eq!(clamp_days(Some(0)), 1);
    assert_eq!(clamp_days(Some(200)), 90);
    assert_eq!(clamp_days(Some(1)), 1);
    assert_eq!(clamp_days(Some(90)), 90);
}

#[test]
fn test_to_daily_clicks() {
    let date1 = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
    let date2 = NaiveDate::from_ymd_opt(2026, 8, 2).unwrap();
    let rows = vec![(date1, 42), (date2, 100)];
    let daily = to_daily_clicks(rows);
    assert_eq!(daily.len(), 2);
    assert_eq!(daily[0].date, "2026-08-01");
    assert_eq!(daily[0].clicks, 42);
    assert_eq!(daily[1].date, "2026-08-02");
    assert_eq!(daily[1].clicks, 100);

    let empty: Vec<DailyClicks> = to_daily_clicks(vec![]);
    assert!(empty.is_empty());
}

#[tokio::test]
async fn test_get_link_stats_success() {
    let date = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
    let dt = DateTime::from_timestamp(1700000000, 0).unwrap();
    let repo = FakeAnalyticsRepo {
        link_totals: HashMap::from([("test123".into(), (42, Some(dt)))]),
        link_daily: HashMap::from([("test123".into(), vec![(date, 42)])]),
        ..Default::default()
    };
    let service = AnalyticsService { db: Arc::new(repo) };

    let req = Request::new(GetLinkStatsRequest {
        short_code: "test123".into(),
        days: Some(7),
    });
    let resp = service.get_link_stats(req).await.unwrap().into_inner();
    assert_eq!(resp.short_code, "test123");
    assert_eq!(resp.total_clicks, 42);
    assert_eq!(resp.daily_clicks.len(), 1);
    assert_eq!(resp.daily_clicks[0].date, "2026-08-01");
    assert_eq!(resp.daily_clicks[0].clicks, 42);
    assert!(resp.last_clicked_at.is_some());
}

#[tokio::test]
async fn test_get_link_stats_not_found() {
    let repo = FakeAnalyticsRepo::default();
    let service = AnalyticsService { db: Arc::new(repo) };

    let req = Request::new(GetLinkStatsRequest {
        short_code: "unknown".into(),
        days: None,
    });
    let err = service.get_link_stats(req).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn test_get_link_stats_no_clicks() {
    let repo = FakeAnalyticsRepo {
        link_totals: HashMap::from([("noclicks".into(), (0, None))]),
        ..Default::default()
    };
    let service = AnalyticsService { db: Arc::new(repo) };

    let req = Request::new(GetLinkStatsRequest {
        short_code: "noclicks".into(),
        days: None,
    });
    let resp = service.get_link_stats(req).await.unwrap().into_inner();
    assert_eq!(resp.total_clicks, 0);
    assert!(resp.daily_clicks.is_empty());
    assert!(resp.last_clicked_at.is_none());
}

#[tokio::test]
async fn test_get_link_stats_clamping() {
    let repo = FakeAnalyticsRepo {
        link_totals: HashMap::from([("test".into(), (1, None))]),
        ..Default::default()
    };
    let repo = Arc::new(repo);
    let service = AnalyticsService { db: repo.clone() };

    let req = Request::new(GetLinkStatsRequest {
        short_code: "test".into(),
        days: Some(200),
    });
    let _ = service.get_link_stats(req).await.unwrap();
    assert_eq!(*repo.last_requested_link_days.lock().unwrap(), Some(90));
}

#[tokio::test]
async fn test_get_link_stats_db_errors() {
    let repo1 = FakeAnalyticsRepo {
        fail_link_totals: true,
        ..Default::default()
    };
    let service1 = AnalyticsService { db: Arc::new(repo1) };
    let err1 = service1
        .get_link_stats(Request::new(GetLinkStatsRequest {
            short_code: "abc".into(),
            days: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err1.code(), Code::Internal);

    let repo2 = FakeAnalyticsRepo {
        link_totals: HashMap::from([("abc".into(), (10, None))]),
        fail_link_daily: true,
        ..Default::default()
    };
    let service2 = AnalyticsService { db: Arc::new(repo2) };
    let err2 = service2
        .get_link_stats(Request::new(GetLinkStatsRequest {
            short_code: "abc".into(),
            days: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err2.code(), Code::Internal);
}

#[tokio::test]
async fn test_get_global_stats_success() {
    let date = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
    let repo = FakeAnalyticsRepo {
        global_total: 1000,
        global_daily: vec![(date, 100)],
        ..Default::default()
    };
    let service = AnalyticsService { db: Arc::new(repo) };

    let req = Request::new(GetGlobalStatsRequest { days: Some(7) });
    let resp = service.get_global_stats(req).await.unwrap().into_inner();
    assert_eq!(resp.total_clicks, 1000);
    assert_eq!(resp.daily_clicks.len(), 1);
    assert_eq!(resp.daily_clicks[0].date, "2026-08-01");
    assert_eq!(resp.daily_clicks[0].clicks, 100);
}

#[tokio::test]
async fn test_get_global_stats_db_errors() {
    let repo1 = FakeAnalyticsRepo {
        fail_global_total: true,
        ..Default::default()
    };
    let service1 = AnalyticsService { db: Arc::new(repo1) };
    let err1 = service1
        .get_global_stats(Request::new(GetGlobalStatsRequest { days: None }))
        .await
        .unwrap_err();
    assert_eq!(err1.code(), Code::Internal);

    let repo2 = FakeAnalyticsRepo {
        global_total: 500,
        fail_global_daily: true,
        ..Default::default()
    };
    let service2 = AnalyticsService { db: Arc::new(repo2) };
    let err2 = service2
        .get_global_stats(Request::new(GetGlobalStatsRequest { days: None }))
        .await
        .unwrap_err();
    assert_eq!(err2.code(), Code::Internal);
}

#[tokio::test]
async fn test_get_global_stats_empty() {
    let repo = FakeAnalyticsRepo {
        global_total: 0,
        global_daily: vec![],
        ..Default::default()
    };
    let service = AnalyticsService { db: Arc::new(repo) };

    let req = Request::new(GetGlobalStatsRequest { days: None });
    let resp = service.get_global_stats(req).await.unwrap().into_inner();
    assert_eq!(resp.total_clicks, 0);
    assert!(resp.daily_clicks.is_empty());
}
