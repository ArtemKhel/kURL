mod loader;
mod types;
mod validation;

pub use loader::load_database;
pub use types::*;
pub use validation::Validate;

pub fn load<T>() -> T
where T: From<AppConfig> {
    AppConfig::load().map(T::from).expect("Failed to load config")
}

#[cfg(test)]
mod tests;
