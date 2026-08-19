use config::ConfigError;

use super::{
    AnalyticsServiceConfig, AppConfig, CoreServiceConfig, DatabaseConfig, GatewayServiceConfig, LoggingConfig,
    RedisConfig, Streams,
};

pub trait Validate {
    fn validate(&self) -> Result<(), ConfigError>;
}

fn invalid(field: &str, message: &str) -> ConfigError {
    ConfigError::Message(format!("invalid configuration `{field}`: {message}"))
}

fn require_non_blank(value: &str, field: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(invalid(field, "must not be blank"));
    }
    Ok(())
}

fn require_port(port: u16, field: &str) -> Result<(), ConfigError> {
    if port == 0 {
        return Err(invalid(field, "must be greater than zero"));
    }
    Ok(())
}

impl Validate for AppConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        self.redis.validate()?;
        self.database.validate()?;
        self.logging.validate()?;
        self.gateway.validate()?;
        self.core.validate()?;
        self.analytics.validate()
    }
}

impl Validate for RedisConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        require_non_blank(&self.host, "redis.host")?;
        require_port(self.port, "redis.port")?;
        if self.cache_ttl.is_zero() {
            return Err(invalid("redis.cache_ttl_secs", "must be greater than zero"));
        }
        self.streams.validate()
    }
}

impl Validate for Streams {
    fn validate(&self) -> Result<(), ConfigError> { require_non_blank(&self.events, "redis.streams.events") }
}

impl Validate for DatabaseConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        require_non_blank(&self.host, "database.host")?;
        require_port(self.port, "database.port")?;
        require_non_blank(&self.user, "database.user")?;
        require_non_blank(&self.db_name, "database.db_name")
    }
}

impl Validate for LoggingConfig {
    fn validate(&self) -> Result<(), ConfigError> { require_non_blank(&self.level, "logging.level") }
}

impl Validate for GatewayServiceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        require_non_blank(&self.host, "gateway.host")?;
        require_port(self.port, "gateway.port")
    }
}

impl Validate for CoreServiceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        require_non_blank(&self.host, "core.host")?;
        require_port(self.port, "core.port")
    }
}

impl Validate for AnalyticsServiceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        require_non_blank(&self.host, "analytics.host")?;
        require_port(self.port, "analytics.port")?;
        if self.read_batch_size == 0 {
            return Err(invalid("analytics.read_batch_size", "must be greater than zero"));
        }
        if !(1..=500).contains(&self.read_block.as_millis()) {
            return Err(invalid("analytics.read_block_millis", "must be between 1 and 500"));
        }
        if self.flush_interval.is_zero() {
            return Err(invalid("analytics.flush_interval_secs", "must be greater than zero"));
        }
        Ok(())
    }
}
