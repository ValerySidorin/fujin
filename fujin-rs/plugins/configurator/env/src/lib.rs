//! Optional bootstrap configurator reading JSON or YAML from one environment variable.

use async_trait::async_trait;
use fujin_runtime::{
    RuntimeConfig, RuntimeError,
    configurator::{Configurator, ConfiguratorPlugin},
};

pub const CONFIG_ENV: &str = "FUJIN_CONFIGURATOR_ENV_CONFIG";

#[derive(Debug, Default)]
pub struct EnvConfigurator;

#[async_trait]
impl Configurator for EnvConfigurator {
    async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
        let value = std::env::var(CONFIG_ENV).map_err(|_| {
            RuntimeError::InvalidConfig(format!(
                "env configurator: {CONFIG_ENV} is not set or empty"
            ))
        })?;
        if value.is_empty() {
            return Err(RuntimeError::InvalidConfig(format!(
                "env configurator: {CONFIG_ENV} is not set or empty"
            )));
        }
        tracing::info!(
            variable = CONFIG_ENV,
            "loading configuration with env configurator"
        );
        decode(value.as_bytes(), CONFIG_ENV)
    }
}

fn decode(bytes: &[u8], source: &str) -> Result<RuntimeConfig, RuntimeError> {
    if let Ok(config) = serde_json::from_slice(bytes) {
        return Ok(config);
    }
    yaml_serde::from_slice(bytes).map_err(|error| RuntimeError::Parse {
        path: source.to_owned(),
        message: error.to_string(),
    })
}

#[must_use]
pub fn plugin() -> ConfiguratorPlugin {
    ConfiguratorPlugin::new("env", || Ok(EnvConfigurator))
}
