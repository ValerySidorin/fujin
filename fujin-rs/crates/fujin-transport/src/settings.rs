use anyhow::{Result, bail};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeProtocolSettings {
    #[serde(default)]
    ping_interval: Option<String>,
    #[serde(default)]
    ping_timeout: Option<String>,
    #[serde(default)]
    ping_max_retries: u32,
    #[serde(default)]
    write_buffer_size: Option<usize>,
    #[serde(default)]
    write_deadline: Option<String>,
    #[serde(default)]
    force_terminate_timeout: Option<String>,
    #[serde(default)]
    ping_stream: bool,
}

impl NativeProtocolSettings {
    /// Rejects native protocol tuning unsupported by the Rust adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when any unsupported tuning field is configured.
    pub fn validate_supported(&self) -> Result<()> {
        if self.ping_interval.is_some()
            || self.ping_timeout.is_some()
            || self.ping_max_retries != 0
            || self.write_buffer_size.is_some()
            || self.write_deadline.is_some()
            || self.force_terminate_timeout.is_some()
            || self.ping_stream
        {
            bail!(
                "native Fujin ping and write tuning controls are unavailable in the Rust adapter"
            );
        }
        Ok(())
    }
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
