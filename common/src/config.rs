use std::fmt::{Display, Formatter};

use config::{Config, ConfigError, File};
use serde::Deserialize;

//  SHARED CONFIGS

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    pub host: String,
    pub port: u16,
    pub ttl: u64,
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
pub struct CacheAddress {
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

//  MASTER CONFIG

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub cache: CacheConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    pub gateway: GatewayServiceConfig,
    pub core: CoreServiceConfig,
}

//  SERVICE-SPECIFIC CONFIGS (what each service gets)

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub gateway: GatewayServiceConfig,
    pub cache: ServiceAddress,
    pub core: ServiceAddress,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub core: CoreServiceConfig,
    pub cache: CacheConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            // Priority 1 (lowest): Defaults
            .set_default("cache.host", "localhost")?
            .set_default("cache.port", 6379)?
            .set_default("cache.ttl", 3600)?
            .set_default("database.host", "localhost")?
            .set_default("database.port", 5432)?
            .set_default("database.user", "postgres")?
            .set_default("database.password", "postgres")?
            .set_default("database.db_name", "kurlyk")?
            .set_default("logging.level", "info")?
            .set_default("gateway.host", "localhost")?
            .set_default("gateway.port", 3000)?
            .set_default("core.host", "localhost")?
            .set_default("core.port", 3001)?
            // Priority 2: TOML file (if it exists)
            .add_source(File::with_name("config/config.toml").required(false))
            // Priority 3 (highest): Environment variables
            .add_source(config::Environment::with_prefix("APP").try_parsing(true).separator("_"))
            .build()?;

        config.try_deserialize()
    }
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

impl_display!(CacheConfig, "redis://{}:{}", host, port);
impl_display!(GatewayServiceConfig, "{}:{}", host, port);
impl_display!(CoreServiceConfig, "{}:{}", host, port);
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
    fn from(app_config: AppConfig) -> Self {
        GatewayConfig {
            gateway: app_config.gateway,
            cache: ServiceAddress {
                scheme: Some("redis".into()),
                host: app_config.cache.host,
                port: app_config.cache.port,
            },
            core: ServiceAddress {
                scheme: Some("grpc".into()),
                host: app_config.core.host,
                port: app_config.core.port,
            },
            logging: app_config.logging,
        }
    }
}
impl From<AppConfig> for CoreConfig {
    fn from(app_config: AppConfig) -> Self {
        CoreConfig {
            core: app_config.core.clone(),
            cache: app_config.cache.clone(),
            database: app_config.database.clone(),
            logging: app_config.logging.clone(),
        }
    }
}
