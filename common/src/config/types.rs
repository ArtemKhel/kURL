use std::{
    fmt::{Display, Formatter},
    time::Duration,
};

use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    #[serde(rename = "cache_ttl_secs", deserialize_with = "duration_from_secs")]
    pub cache_ttl: Duration,
    pub streams: Streams,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 6379,
            cache_ttl: Duration::from_secs(300),
            streams: Streams::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Streams {
    pub events: String,
}

impl Default for Streams {
    fn default() -> Self {
        Self {
            events: "Events".into(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            db_name: "kurlyk".into(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: String,
    pub enabled: bool,
    pub otlp_endpoint: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            enabled: false,
            otlp_endpoint: Some("http://alloy:4317".into()),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServiceAddress {
    pub scheme: Option<String>,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct RedisAddress {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct GatewayServiceConfig {
    pub host: String,
    pub port: u16,
}

impl Default for GatewayServiceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 3000,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct CoreServiceConfig {
    pub host: String,
    pub port: u16,
}

impl Default for CoreServiceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 3001,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct AnalyticsServiceConfig {
    pub host: String,
    pub port: u16,
    pub read_batch_size: usize,
    #[serde(rename = "read_block_millis", deserialize_with = "duration_from_millis")]
    pub read_block: Duration,
    #[serde(rename = "flush_interval_secs", deserialize_with = "duration_from_secs")]
    pub flush_interval: Duration,
}

impl Default for AnalyticsServiceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 3002,
            read_batch_size: 100,
            read_block: Duration::from_millis(250),
            flush_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub redis: RedisConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    pub gateway: GatewayServiceConfig,
    pub core: CoreServiceConfig,
    pub analytics: AnalyticsServiceConfig,
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub gateway: GatewayServiceConfig,
    pub redis: RedisConfig,
    pub core: ServiceAddress,
    pub analytics: ServiceAddress,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub core: CoreServiceConfig,
    pub redis: RedisConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone)]
pub struct AnalyticsConfig {
    pub analytics: AnalyticsServiceConfig,
    pub redis: RedisConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

fn duration_from_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where D: Deserializer<'de> {
    let secs = u64::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}

fn duration_from_millis<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where D: Deserializer<'de> {
    let millis = u64::deserialize(deserializer)?;
    Ok(Duration::from_millis(millis))
}

macro_rules! impl_display {
    ($t:ty, $fmt:expr, $( $field:ident ),*) => {
        impl ::core::fmt::Display for $t {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}", format!($fmt, $( self.$field ),*))
            }
        }
    };
}

impl Display for ServiceAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(scheme) = &self.scheme {
            write!(f, "{}://{}:{}", scheme, self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl_display!(RedisConfig, "redis://{}:{}", host, port);
impl_display!(CoreServiceConfig, "grpc://{}:{}", host, port);
impl_display!(AnalyticsServiceConfig, "grpc://{}:{}", host, port);
impl_display!(
    DatabaseConfig,
    "postgresql://{}:{}@{}:{}/{}",
    user,
    password,
    host,
    port,
    db_name
);

impl From<AppConfig> for GatewayConfig {
    fn from(value: AppConfig) -> Self {
        Self {
            gateway: value.gateway,
            redis: value.redis,
            core: ServiceAddress {
                scheme: Some("grpc".into()),
                host: value.core.host,
                port: value.core.port,
            },
            analytics: ServiceAddress {
                scheme: Some("grpc".into()),
                host: value.analytics.host,
                port: value.analytics.port,
            },
            logging: value.logging,
        }
    }
}

impl From<AppConfig> for CoreConfig {
    fn from(value: AppConfig) -> Self {
        Self {
            core: value.core,
            redis: value.redis,
            database: value.database,
            logging: value.logging,
        }
    }
}

impl From<AppConfig> for AnalyticsConfig {
    fn from(value: AppConfig) -> Self {
        Self {
            analytics: value.analytics,
            redis: value.redis,
            logging: value.logging,
            database: value.database,
        }
    }
}
