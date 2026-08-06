use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use common::db_utils::DbError;
use proto::core::{
    CreateLinkRequest, DeleteLinkRequest, GetLinkRequest, link_service_server::LinkService as LinkServiceTrait,
};
use tokio::sync::mpsc;
use tonic::{Code, Request};

use crate::{
    AppState, Config,
    cache::CacheOp,
    db::{Link, LinkRepository},
    grpc::{LinkService, calculate_cache_ttl, generate_short_code},
};

#[derive(Debug, Default)]
pub struct FakeLinkRepository {
    pub links: Mutex<HashMap<String, Link>>,
    pub fail_exists_count: Mutex<usize>,
}

#[tonic::async_trait]
impl LinkRepository for FakeLinkRepository {
    async fn get_link(&self, short_code: &str) -> Result<Link, DbError> {
        let links = self.links.lock().unwrap();
        links.get(short_code).cloned().ok_or(DbError::NotFound)
    }

    async fn link_exists(&self, short_code: &str) -> Result<bool, DbError> {
        let mut fail_count = self.fail_exists_count.lock().unwrap();
        if *fail_count > 0 {
            *fail_count -= 1;
            return Ok(true);
        }
        let links = self.links.lock().unwrap();
        Ok(links.contains_key(short_code))
    }

    async fn create_link(
        &self,
        short_code: &str,
        target: &str,
        expiration: Option<DateTime<Utc>>,
    ) -> Result<(), DbError> {
        let mut links = self.links.lock().unwrap();
        if links.contains_key(short_code) {
            return Err(DbError::Conflict(sqlx::Error::RowNotFound));
        }
        links.insert(short_code.to_string(), Link {
            target: target.to_string(),
            expiration,
        });
        Ok(())
    }

    async fn delete_link(&self, short_code: &str) -> Result<(), DbError> {
        let mut links = self.links.lock().unwrap();
        if links.remove(short_code).is_some() {
            Ok(())
        } else {
            Err(DbError::NotFound)
        }
    }
}

fn setup_test_service() -> (LinkService, Arc<FakeLinkRepository>, mpsc::UnboundedReceiver<CacheOp>) {
    let repo = Arc::new(FakeLinkRepository::default());
    let (tx, rx) = mpsc::unbounded_channel();
    let config = Config {
        core: common::config::CoreServiceConfig {
            host: "localhost".into(),
            port: 3001,
        },
        redis: common::config::RedisConfig {
            host: "localhost".into(),
            port: 6379,
            cache_ttl: Duration::from_secs(3600),
            streams: common::config::Streams {
                events: "events".into(),
            },
        },
        database: common::config::DatabaseConfig {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "password".into(),
            db_name: "test".into(),
        },
        logging: common::config::LoggingConfig {
            level: "info".into(),
            otlp_endpoint: None,
        },
    };

    let state = Arc::new(AppState {
        config,
        db_pool: repo.clone(),
        redis_tx: tx,
    });

    (LinkService { state }, repo, rx)
}

#[tokio::test]
async fn test_create_link_generates_code_and_emits_cache() {
    let (service, _repo, mut rx) = setup_test_service();

    let req = Request::new(CreateLinkRequest {
        short_code: None,
        target: "https://example.com".to_string(),
        expiration: None,
    });

    let res = service.create_link(req).await.unwrap().into_inner();
    assert_eq!(res.short_code.len(), 6);

    let cache_op = rx.try_recv().unwrap();
    match cache_op {
        CacheOp::Set { key, value, .. } => {
            assert_eq!(key, res.short_code);
            assert_eq!(value, "https://example.com");
        }
        _ => panic!("Expected CacheOp::Set"),
    }
}

#[tokio::test]
async fn test_create_link_specific_code() {
    let (service, _repo, mut rx) = setup_test_service();

    let req = Request::new(CreateLinkRequest {
        short_code: Some("custom123".to_string()),
        target: "https://example.com/custom".to_string(),
        expiration: None,
    });

    let res = service.create_link(req).await.unwrap().into_inner();
    assert_eq!(res.short_code, "custom123");

    let cache_op = rx.try_recv().unwrap();
    match cache_op {
        CacheOp::Set { key, value, .. } => {
            assert_eq!(key, "custom123");
            assert_eq!(value, "https://example.com/custom");
        }
        _ => panic!("Expected CacheOp::Set"),
    }
}

#[tokio::test]
async fn test_create_link_expired() {
    let (service, _repo, mut rx) = setup_test_service();

    let past_time: std::time::SystemTime = (Utc::now() - chrono::Duration::seconds(60)).into();
    let expired_ts = past_time.into();

    let req = Request::new(CreateLinkRequest {
        short_code: Some("expired1".to_string()),
        target: "https://example.com".to_string(),
        expiration: Some(expired_ts),
    });

    let err = service.create_link(req).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_create_link_duplicate() {
    let (service, repo, mut rx) = setup_test_service();

    repo.create_link("dup123", "https://example.com", None).await.unwrap();

    let req = Request::new(CreateLinkRequest {
        short_code: Some("dup123".to_string()),
        target: "https://example.com/new".to_string(),
        expiration: None,
    });

    let err = service.create_link(req).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_get_link_valid() {
    let (service, repo, mut rx) = setup_test_service();

    repo.create_link("valid123", "https://example.com/valid", None)
        .await
        .unwrap();

    let req = Request::new(GetLinkRequest {
        short_code: "valid123".to_string(),
    });

    let res = service.get_link(req).await.unwrap().into_inner();
    assert_eq!(res.target, "https://example.com/valid");

    let cache_op = rx.try_recv().unwrap();
    match cache_op {
        CacheOp::Set { key, value, .. } => {
            assert_eq!(key, "valid123");
            assert_eq!(value, "https://example.com/valid");
        }
        _ => panic!("Expected CacheOp::Set"),
    }
}

#[tokio::test]
async fn test_get_link_expired() {
    let (service, repo, mut rx) = setup_test_service();

    let expired_at = Utc::now() - chrono::Duration::seconds(10);
    repo.create_link("exp123", "https://example.com/exp", Some(expired_at))
        .await
        .unwrap();

    let req = Request::new(GetLinkRequest {
        short_code: "exp123".to_string(),
    });

    let err = service.get_link(req).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_get_link_not_found() {
    let (service, _repo, mut rx) = setup_test_service();

    let req = Request::new(GetLinkRequest {
        short_code: "missing".to_string(),
    });

    let err = service.get_link(req).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_delete_link_valid() {
    let (service, repo, mut rx) = setup_test_service();

    repo.create_link("del123", "https://example.com/del", None)
        .await
        .unwrap();

    let req = Request::new(DeleteLinkRequest {
        short_code: "del123".to_string(),
    });

    service.delete_link(req).await.unwrap();

    let cache_op = rx.try_recv().unwrap();
    match cache_op {
        CacheOp::Del { key } => assert_eq!(key, "del123"),
        _ => panic!("Expected CacheOp::Del"),
    }
}

#[tokio::test]
async fn test_delete_link_not_found() {
    let (service, _repo, mut rx) = setup_test_service();

    let req = Request::new(DeleteLinkRequest {
        short_code: "absent".to_string(),
    });

    let err = service.delete_link(req).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_generate_short_code_retry() {
    let repo = Arc::new(FakeLinkRepository::default());
    *repo.fail_exists_count.lock().unwrap() = 2;

    let code = generate_short_code(repo).await.unwrap();
    assert_eq!(code.len(), 6);
}

#[test]
fn cache_ttl_uses_configured_ttl_without_expiration() {
    let configured_ttl = Duration::from_secs(60);

    assert_eq!(calculate_cache_ttl(configured_ttl, None), Some(configured_ttl));
}

#[test]
fn cache_ttl_is_capped_by_expiration() {
    let configured_ttl = Duration::from_secs(60);
    let expiration = Utc::now() + chrono::Duration::seconds(5);

    let ttl = calculate_cache_ttl(configured_ttl, Some(expiration)).expect("expiration should be cacheable");

    assert!(ttl <= Duration::from_secs(5));
}

#[test]
fn cache_ttl_does_not_cache_expired_links() {
    let expiration = Utc::now() - chrono::Duration::seconds(1);

    assert_eq!(calculate_cache_ttl(Duration::from_secs(60), Some(expiration)), None);
}
