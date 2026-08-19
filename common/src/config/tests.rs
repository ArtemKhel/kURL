use std::{collections::HashMap, path::Path, time::Duration};

use config::Environment;

use super::{AppConfig, Validate, loader};

fn environment(values: &[(&str, &str)]) -> Environment {
    Environment::with_prefix("APP")
        .prefix_separator("_")
        .separator("__")
        .try_parsing(true)
        .source(Some(
            values
                .iter()
                .map(|(key, value)| ((*key).into(), (*value).into()))
                .collect::<HashMap<_, _>>(),
        ))
}

fn load(contents: &str) -> Result<AppConfig, config::ConfigError> { loader::load_toml(contents, environment(&[])) }

fn assert_invalid(update: impl FnOnce(&mut AppConfig), field: &str) {
    let mut config = AppConfig::default();
    update(&mut config);

    let error = config.validate().unwrap_err().to_string();
    assert!(error.contains(field), "expected `{error}` to contain `{field}`");
}

#[test]
fn loads_defaults_without_a_file() {
    let config = load("").unwrap();

    assert_eq!(config.redis.host, "localhost");
    assert_eq!(config.redis.port, 6379);
    assert_eq!(config.redis.cache_ttl, Duration::from_secs(300));
    assert_eq!(config.redis.streams.events, "Events");
    assert_eq!(config.database.host, "localhost");
    assert_eq!(config.database.port, 5432);
    assert_eq!(config.database.user, "postgres");
    assert_eq!(config.database.password, "postgres");
    assert_eq!(config.database.db_name, "kurlyk");
    assert_eq!(config.logging.level, "info");
    assert!(!config.logging.enabled);
    assert_eq!(config.logging.otlp_endpoint.as_deref(), Some("http://alloy:4317"));
    assert_eq!(config.gateway.host, "localhost");
    assert_eq!(config.gateway.port, 3000);
    assert_eq!(config.core.host, "localhost");
    assert_eq!(config.core.port, 3001);
    assert_eq!(config.analytics.host, "localhost");
    assert_eq!(config.analytics.port, 3002);
    assert_eq!(config.analytics.read_batch_size, 100);
    assert_eq!(config.analytics.read_block, Duration::from_millis(250));
    assert_eq!(config.analytics.flush_interval, Duration::from_secs(60));
}

#[test]
fn environment_overrides_file_and_handles_underscored_fields() {
    let config = loader::load_toml(
        r#"
            [redis]
            host = "file-redis"
            cache_ttl_secs = 120

            [analytics]
            read_batch_size = 25
        "#,
        environment(&[
            ("APP_REDIS__HOST", "env-redis"),
            ("APP_ANALYTICS__READ_BATCH_SIZE", "50"),
        ]),
    )
    .unwrap();

    assert_eq!(config.redis.host, "env-redis");
    assert_eq!(config.redis.cache_ttl, Duration::from_secs(120));
    assert_eq!(config.analytics.read_batch_size, 50);
}

#[test]
fn database_loader_ignores_unrelated_configuration() {
    let database = loader::load_database_toml(
        r#"
            [database]
            host = "file-db"

            [analytics]
            read_batch_size = 0
        "#,
        environment(&[("APP_DATABASE__HOST", "env-db")]),
    )
    .unwrap();

    assert_eq!(database.host, "env-db");
}

#[test]
fn explicit_file_is_required_but_fallback_is_optional() {
    let missing = std::env::temp_dir().join(format!("kurlyk-config-missing-{}-{}.toml", std::process::id(), line!()));

    let error = loader::load_path(Some(&missing), Path::new("unused.toml"), environment(&[])).unwrap_err();
    assert!(error.to_string().contains("not found"));

    let config = loader::load_path(None, &missing, environment(&[])).unwrap();
    assert_eq!(config.redis.streams.events, "Events");
}

#[test]
fn empty_explicit_path_is_rejected() {
    let error = loader::load_path(Some(Path::new("")), Path::new("unused.toml"), environment(&[])).unwrap_err();

    assert!(error.to_string().contains("must not be empty"));
}

#[test]
fn malformed_toml_is_rejected() {
    assert!(load("[redis").is_err());
}

#[test]
fn unknown_fields_are_rejected_at_every_level() {
    let top_level = load("unexpected = true").unwrap_err().to_string();
    assert!(top_level.contains("unexpected"));

    let nested = load(
        r#"
            [analytics]
            unexpected = true
        "#,
    )
    .unwrap_err()
    .to_string();
    assert!(nested.contains("unexpected"));
}

#[test]
fn validates_required_strings() {
    assert_invalid(|config| config.redis.host = " ".into(), "redis.host");
    assert_invalid(|config| config.redis.streams.events.clear(), "redis.streams.events");
    assert_invalid(|config| config.database.host.clear(), "database.host");
    assert_invalid(|config| config.database.user.clear(), "database.user");
    assert_invalid(|config| config.database.db_name.clear(), "database.db_name");
    assert_invalid(|config| config.logging.level.clear(), "logging.level");
    assert_invalid(|config| config.gateway.host.clear(), "gateway.host");
    assert_invalid(|config| config.core.host.clear(), "core.host");
    assert_invalid(|config| config.analytics.host.clear(), "analytics.host");
}

#[test]
fn validates_ports() {
    assert_invalid(|config| config.redis.port = 0, "redis.port");
    assert_invalid(|config| config.database.port = 0, "database.port");
    assert_invalid(|config| config.gateway.port = 0, "gateway.port");
    assert_invalid(|config| config.core.port = 0, "core.port");
    assert_invalid(|config| config.analytics.port = 0, "analytics.port");
}

#[test]
fn validates_operational_limits() {
    assert_invalid(|config| config.redis.cache_ttl = Duration::ZERO, "redis.cache_ttl_secs");
    assert_invalid(
        |config| config.analytics.read_batch_size = 0,
        "analytics.read_batch_size",
    );
    assert_invalid(
        |config| config.analytics.read_block = Duration::ZERO,
        "analytics.read_block_millis",
    );
    assert_invalid(
        |config| config.analytics.flush_interval = Duration::ZERO,
        "analytics.flush_interval_secs",
    );
}

#[test]
fn allows_empty_database_password_and_missing_otlp_endpoint() {
    let mut config = AppConfig::default();
    config.database.password.clear();
    config.logging.otlp_endpoint = None;

    config.validate().unwrap();
}
