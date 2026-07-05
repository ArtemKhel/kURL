use std::net::SocketAddr;

const DEFAULT_ADDRESS: &str = "127.0.0.1:3000";
const DEFAULT_REDIS: &str = "redis://127.0.0.1:6379";
const DEFAULT_REDIS_TTL: &str = "3600";

#[derive(Debug, Clone, clap::Parser)]
pub struct Config {
    #[clap(env = "LISTENER_ADDRESS", default_value = DEFAULT_ADDRESS)]
    listener_address: SocketAddr,
    #[clap(env = "REDIS_URL", default_value = DEFAULT_REDIS)]
    redis_url: String,
    #[clap(env = "REDIS_TTL", default_value = DEFAULT_REDIS_TTL)]
    redis_ttl: usize,
}

// pub enum ConfigError {
//     VarError(std::env::VarError),
//     AddrParseError(std::net::AddrParseError),
// }
// impl Config {
//     pub fn from_env() -> Result<Self, ConfigError> {
//         let listener_address = match env::var("LISTENER_ADDRESS") {
//             Ok(addr) => addr.parse::<SocketAddr>()?,
//             Err(env::VarError::NotPresent) => DEFAULT_ADDRESS.parse()?,
//             Err(e) => return Err(e.into()),
//         };
//         Ok(Self { listener_address })
//     }
// }

// fn env_or_default() {}
//
// impl From<std::env::VarError> for ConfigError {
//     fn from(err: std::env::VarError) -> Self { ConfigError::VarError(err) }
// }
//
// impl From<std::net::AddrParseError> for ConfigError {
//     fn from(err: std::net::AddrParseError) -> Self { ConfigError::AddrParseError(err) }
// }
