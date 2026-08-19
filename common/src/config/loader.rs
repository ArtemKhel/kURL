use std::{env, path::Path};

use config::{Config, ConfigError, Environment, File, FileFormat, Source};
use tracing::info;

use super::{AppConfig, DatabaseConfig, Validate};

const CONFIG_FILE_ENV: &str = "KURLYK_CONFIG_FILE";
const DEFAULT_CONFIG_FILE: &str = "./config/config.toml";

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let explicit_path = env::var_os(CONFIG_FILE_ENV);
        let file = config_file(explicit_path.as_deref().map(Path::new), Path::new(DEFAULT_CONFIG_FILE))?;

        load_sources(file, app_environment()).inspect(|_| {
            info!(
                config_file = %explicit_path
                    .as_deref()
                    .map(Path::new)
                    .unwrap_or_else(|| Path::new(DEFAULT_CONFIG_FILE))
                    .display(),
                "config loaded"
            );
        })
    }
}

pub fn load_database() -> Result<DatabaseConfig, ConfigError> {
    let explicit_path = env::var_os(CONFIG_FILE_ENV);
    let file = config_file(explicit_path.as_deref().map(Path::new), Path::new(DEFAULT_CONFIG_FILE))?;
    load_database_sources(file, app_environment())
}

fn app_environment() -> Environment {
    Environment::with_prefix("APP")
        .prefix_separator("_")
        .separator("__")
        .try_parsing(true)
}

fn config_file<'a>(
    explicit_path: Option<&'a Path>,
    fallback_path: &'a Path,
) -> Result<File<config::FileSourceFile, FileFormat>, ConfigError> {
    match explicit_path {
        Some(path) if path.as_os_str().is_empty() => {
            Err(ConfigError::Message(format!("{CONFIG_FILE_ENV} must not be empty")))
        }
        Some(path) => Ok(File::from(path).required(true)),
        None => Ok(File::from(fallback_path).required(false)),
    }
}

fn load_sources<S>(file: S, env: Environment) -> Result<AppConfig, ConfigError>
where S: Source + Send + Sync + 'static {
    let config = Config::builder().add_source(file).add_source(env).build()?;
    let config = config.try_deserialize::<AppConfig>()?;

    config.validate()?;
    Ok(config)
}

fn load_database_sources<S>(file: S, env: Environment) -> Result<DatabaseConfig, ConfigError>
where S: Source + Send + Sync + 'static {
    #[derive(serde::Deserialize)]
    struct DatabaseOnly {
        #[serde(default)]
        database: DatabaseConfig,
    }

    let database = Config::builder()
        .add_source(file)
        .add_source(env)
        .build()?
        .try_deserialize::<DatabaseOnly>()?
        .database;

    database.validate()?;
    Ok(database)
}

#[cfg(test)]
pub(super) fn load_toml(contents: &str, env: Environment) -> Result<AppConfig, ConfigError> {
    load_sources(File::from_str(contents, FileFormat::Toml), env)
}

#[cfg(test)]
pub(super) fn load_database_toml(contents: &str, env: Environment) -> Result<DatabaseConfig, ConfigError> {
    load_database_sources(File::from_str(contents, FileFormat::Toml), env)
}

#[cfg(test)]
pub(super) fn load_path(
    explicit_path: Option<&Path>,
    fallback_path: &Path,
    env: Environment,
) -> Result<AppConfig, ConfigError> {
    load_sources(config_file(explicit_path, fallback_path)?, env)
}
