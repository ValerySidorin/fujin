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
