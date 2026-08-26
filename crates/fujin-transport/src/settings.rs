use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const fn default_ping_interval() -> Duration {
    Duration::from_secs(2)
}

const fn default_ping_timeout() -> Duration {
    Duration::from_secs(5)
}

const fn default_ping_max_retries() -> u32 {
    3
}

const fn default_write_buffer_size() -> usize {
    4 * 1024 * 1024
}

const fn default_write_deadline() -> Duration {
    Duration::from_secs(10)
}

const fn default_force_terminate_timeout() -> Duration {
    Duration::from_secs(15)
}

/// Fujin native-protocol liveness, output, and shutdown controls.
#[derive(Clone, Debug)]
pub struct NativeProtocolConfig {
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
    pub ping_max_retries: u32,
    pub write_buffer_size: usize,
    pub write_deadline: Duration,
    pub force_terminate_timeout: Duration,
}

impl Default for NativeProtocolConfig {
    fn default() -> Self {
        Self {
            ping_interval: default_ping_interval(),
            ping_timeout: default_ping_timeout(),
            ping_max_retries: default_ping_max_retries(),
            write_buffer_size: default_write_buffer_size(),
            write_deadline: default_write_deadline(),
            force_terminate_timeout: default_force_terminate_timeout(),
        }
    }
}

impl NativeProtocolConfig {
    #[must_use]
    pub fn session_config(&self, ping_stream: bool) -> crate::NativeSessionConfig {
        crate::NativeSessionConfig {
            ping_interval: self.ping_interval,
            ping_timeout: self.ping_timeout,
            ping_max_retries: self.ping_max_retries,
            maximum_pending_output_bytes: self.write_buffer_size,
            write_deadline: self.write_deadline,
            force_terminate_timeout: self.force_terminate_timeout,
            ping_stream,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProtocolSettings {
    #[serde(default)]
    ping_interval: Option<String>,
    #[serde(default)]
    ping_timeout: Option<String>,
    #[serde(default)]
    ping_max_retries: Option<u32>,
    #[serde(default)]
    write_buffer_size: Option<usize>,
    #[serde(default)]
    write_deadline: Option<String>,
    #[serde(default)]
    force_terminate_timeout: Option<String>,
}

impl NativeProtocolSettings {
    /// Parses user-facing duration strings into the native-session runtime contract.
    ///
    /// # Errors
    /// Returns an error for zero sizes/retries or invalid duration values.
    pub fn compile(&self) -> Result<NativeProtocolConfig> {
        let defaults = NativeProtocolConfig::default();
        let config = NativeProtocolConfig {
            ping_interval: parse_duration(
                "fujin.ping_interval",
                self.ping_interval.as_deref(),
                defaults.ping_interval,
            )?,
            ping_timeout: parse_duration(
                "fujin.ping_timeout",
                self.ping_timeout.as_deref(),
                defaults.ping_timeout,
            )?,
            ping_max_retries: self.ping_max_retries.unwrap_or(defaults.ping_max_retries),
            write_buffer_size: self.write_buffer_size.unwrap_or(defaults.write_buffer_size),
            write_deadline: parse_duration(
                "fujin.write_deadline",
                self.write_deadline.as_deref(),
                defaults.write_deadline,
            )?,
            force_terminate_timeout: parse_duration(
                "fujin.force_terminate_timeout",
                self.force_terminate_timeout.as_deref(),
                defaults.force_terminate_timeout,
            )?,
        };
        if config.ping_max_retries == 0 {
            bail!("fujin.ping_max_retries must be positive");
        }
        if config.write_buffer_size == 0 {
            bail!("fujin.write_buffer_size must be positive");
        }
        Ok(config)
    }
}

fn parse_duration(name: &str, value: Option<&str>, default: Duration) -> Result<Duration> {
    value.map_or(Ok(default), |value| {
        humantime::parse_duration(value).with_context(|| format!("parse {name}"))
    })
}

/// User-facing listener TLS settings shared by native transports and gRPC.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub client_certs_dir: String,
    #[serde(default)]
    pub server_cert_pem_path: String,
    #[serde(default)]
    pub server_key_pem_path: String,
    #[serde(default)]
    pub require_and_verify_client_cert: bool,
}

impl TlsSettings {
    /// Compiles validated listener TLS settings without reading certificate files.
    ///
    /// # Errors
    ///
    /// Returns an error for incomplete or contradictory TLS settings.
    pub fn listener_config(&self, listener: &str) -> Result<Option<crate::tls::TlsConfig>> {
        if !self.enabled {
            if self.require_and_verify_client_cert {
                bail!("{listener} cannot require client certificates while TLS is disabled");
            }
            return Ok(None);
        }
        if self.server_cert_pem_path.is_empty() {
            bail!("{listener} certificate is empty");
        }
        if self.server_key_pem_path.is_empty() {
            bail!("{listener} private key is empty");
        }
        if self.require_and_verify_client_cert && self.client_certs_dir.is_empty() {
            bail!("{listener} client certificates directory is empty");
        }
        Ok(Some(crate::tls::TlsConfig {
            certificate: self.server_cert_pem_path.clone(),
            private_key: self.server_key_pem_path.clone(),
            client_certificates: (!self.client_certs_dir.is_empty())
                .then(|| self.client_certs_dir.clone()),
            require_client_certificate: self.require_and_verify_client_cert,
        }))
    }
}
