//! Optional bootstrap configurator loading the first existing JSON or YAML file.

use std::path::PathBuf;

use async_trait::async_trait;
use fujin_configurator::{Configurator, ConfiguratorPlugin, RuntimeConfig, RuntimeError};

pub const PATHS_ENV: &str = "FUJIN_CONFIGURATOR_YAML_PATHS";
const DEFAULT_PATHS: [&str; 3] = ["./config.yaml", "conf/config.yaml", "config/config.yaml"];

#[derive(Debug)]
pub struct YamlConfigurator {
    paths: Vec<PathBuf>,
}

impl YamlConfigurator {
    #[must_use]
    pub fn from_environment() -> Self {
        let paths = std::env::var(PATHS_ENV).map_or_else(
            |_| DEFAULT_PATHS.iter().map(PathBuf::from).collect(),
            |value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .map(PathBuf::from)
                    .collect()
            },
        );
        Self { paths }
    }

    #[must_use]
    pub fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}

#[async_trait]
impl Configurator for YamlConfigurator {
    async fn load(&self) -> Result<RuntimeConfig, RuntimeError> {
        if self.paths.is_empty() {
            return Err(RuntimeError::InvalidConfig(format!(
                "yaml configurator: {PATHS_ENV} contains no paths"
            )));
        }
        for path in &self.paths {
            match tokio::fs::read(path).await {
                Ok(bytes) => {
                    tracing::info!(path = %path.display(), "loading configuration with yaml configurator");
                    if let Ok(config) = serde_json::from_slice(&bytes) {
                        return Ok(config);
                    }
                    return yaml_serde::from_slice(&bytes).map_err(|source| RuntimeError::Parse {
                        path: path.display().to_string(),
                        message: source.to_string(),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(RuntimeError::Read {
                        path: path.display().to_string(),
                        source,
                    });
                }
            }
        }
        Err(RuntimeError::InvalidConfig(format!(
            "yaml configurator: failed to find configuration in paths {:?}",
            self.paths
        )))
    }
}

#[must_use]
pub fn plugin() -> ConfiguratorPlugin {
    ConfiguratorPlugin::new("yaml", || Ok(YamlConfigurator::from_environment()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uses_first_existing_path_and_decodes_json() {
        let directory = std::env::temp_dir().join(format!(
            "fujin-configurator-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("yaml")
        ));
        tokio::fs::create_dir_all(&directory)
            .await
            .expect("create fixture directory");
        let existing = directory.join("config.json");
        tokio::fs::write(&existing, br#"{"grpc":{"enabled":false}}"#)
            .await
            .expect("write fixture");
        let configurator = YamlConfigurator::new(vec![directory.join("missing"), existing]);
        let config = configurator.load().await.expect("load first existing path");
        assert!(!config.grpc.enabled);
        tokio::fs::remove_dir_all(directory)
            .await
            .expect("remove fixture directory");
    }
}
