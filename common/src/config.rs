use std::{
    fmt::{Display, Formatter},
    time::Duration,
};

use config::{Config, ConfigError, File};
use serde::{Deserialize, Deserializer};
use tracing::info;

//  SHARED CONFIGS

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    #[serde(rename = "cache_ttl_secs", deserialize_with = "duration_from_secs")]
    pub cache_ttl: Duration,
    pub streams: Streams,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Streams {
    pub events: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: String,
    #[serde(default)]
    pub enabled: bool,
    pub otlp_endpoint: Option<String>,
}

//  SERVICE ADDRESS

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

//  SERVICE-SPECIFIC CONFIGS

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct GatewayServiceConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CoreServiceConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsServiceConfig {
    pub host: String,
    pub port: u16,
    pub read_batch_size: usize,
    #[serde(deserialize_with = "duration_from_secs")]
    pub read_block_secs: Duration,
    #[serde(rename = "flush_interval_secs", deserialize_with = "duration_from_secs")]
    pub flush_interval: Duration,
}

//  MASTER CONFIG

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub redis: RedisConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    pub gateway: GatewayServiceConfig,
    pub core: CoreServiceConfig,
    pub analytics: AnalyticsServiceConfig,
}

//  SERVICE-SPECIFIC CONFIGS (what each service gets)

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

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            // Priority 1 (lowest): Defaults
            .set_default("redis.host", "localhost")?
            .set_default("redis.port", 6379)?
            .set_default("redis.cache_ttl_secs", 300)?
            .set_default("database.host", "localhost")?
            .set_default("database.port", 5432)?
            .set_default("database.user", "postgres")?
            .set_default("database.password", "postgres")?
            .set_default("database.db_name", "kurlyk")?
            .set_default("logging.level", "info")?
            .set_default("logging.enabled", false)?
            .set_default("logging.otlp_endpoint", "http://alloy:4317")?
            .set_default("gateway.host", "localhost")?
            .set_default("gateway.port", 3000)?
            .set_default("core.host", "localhost")?
            .set_default("core.port", 3001)?
            .set_default("analytics.host", "localhost")?
            .set_default("analytics.port", 3002)?
            .set_default("analytics.read_batch_size", 100)?
            .set_default("analytics.read_block_secs", 5)?
            .set_default("analytics.flush_interval_secs", 60)?
            // Priority 2: TOML file (if it exists)
            .add_source(File::with_name("./config/config.toml").required(false))
            // Priority 3 (highest): Environment variables
            .add_source(config::Environment::with_prefix("APP").try_parsing(true).separator("_"))
            .build()?;

        config
            .try_deserialize()
            .inspect(|config| info!(?config, "config loaded"))
    }
}

pub fn load<T>() -> T
where T: From<AppConfig> {
    AppConfig::load().map(T::from).expect("Failed to load config")
}

fn duration_from_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where D: Deserializer<'de> {
    let secs = u64::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
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
// impl_display!(GatewayServiceConfig, "{}:{}", host, port);
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
        GatewayConfig {
            gateway: value.gateway,
            redis: value.redis,
            core: ServiceAddress {
                scheme: Some("grpc".into()),
                host: value.core.host,
                port: value.core.port,
            },
            analytics: ServiceAddress {
                scheme: Some("grpc".into()),
                host: value.analytics.host.clone(),
                port: value.analytics.port,
            },
            logging: value.logging,
        }
    }
}
impl From<AppConfig> for CoreConfig {
    fn from(value: AppConfig) -> Self {
        CoreConfig {
            core: value.core,
            redis: value.redis,
            database: value.database,
            logging: value.logging,
        }
    }
}

impl From<AppConfig> for AnalyticsConfig {
    fn from(value: AppConfig) -> Self {
        AnalyticsConfig {
            analytics: value.analytics,
            redis: value.redis,
            logging: value.logging,
            database: value.database,
        }
    }
}
