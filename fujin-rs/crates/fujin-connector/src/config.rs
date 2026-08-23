use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Complete connector snapshot keyed by client-visible connector name.
pub type ConnectorsConfig = BTreeMap<String, ConnectorConfig>;

/// Configuration for one compiled connector instance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectorConfig {
    #[serde(rename = "type")]
    pub connector_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overridable: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bind_middlewares: Vec<MiddlewareConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connector_middlewares: Vec<MiddlewareConfig>,
    #[serde(default)]
    pub settings: Value,
}

/// Inline plugin configuration. Plugin-specific fields remain beside name and enabled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MiddlewareConfig {
    pub name: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(flatten)]
    pub settings: BTreeMap<String, Value>,
}

const fn default_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn middleware_fields_are_inline() {
        let parsed: MiddlewareConfig =
            serde_json::from_str(r#"{"name":"auth_api_key","api_key":"secret"}"#)
                .expect("valid middleware config");

        assert!(parsed.enabled);
        assert_eq!(parsed.settings["api_key"], "secret");
    }
}
