use std::collections::BTreeMap;

use serde_json::{Map, Number, Value};

use crate::{ConnectorConfig, ConnectorDescriptor, CoreError, Result};

/// Applies a validated set of textual BIND overrides to an immutable connector config clone.
///
/// # Errors
///
/// Returns an error when a path is not whitelisted, conversion fails, or an intermediate
/// settings value is not an object.
pub fn apply_overrides(
    base: &ConnectorConfig,
    descriptor: &dyn ConnectorDescriptor,
    overrides: &BTreeMap<String, String>,
) -> Result<ConnectorConfig> {
    let mut config = base.clone();
    for (path, value) in overrides {
        validate_override_path(path, &config.overridable)?;
        let converted = match descriptor.convert_override(path, value) {
            Ok(value) => value,
            Err(CoreError::OperationUnsupported) => convert_generic(value),
            Err(error) => {
                return Err(CoreError::InvalidConfig(format!(
                    "apply override {path:?}: {error}"
                )));
            }
        };
        set_nested_value(&mut config.settings, path, converted)?;
    }
    Ok(config)
}

/// Validates one path against exact and single-segment wildcard patterns.
///
/// # Errors
///
/// Returns [`CoreError::InvalidConfig`] when the whitelist is empty or no pattern matches.
pub fn validate_override_path(path: &str, whitelist: &[String]) -> Result<()> {
    if whitelist.is_empty() {
        return Err(CoreError::InvalidConfig(format!(
            "override path {path:?} is not allowed: no overridable paths configured"
        )));
    }
    if whitelist
        .iter()
        .any(|pattern| pattern == "*" || matches_path(path, pattern))
    {
        return Ok(());
    }
    Err(CoreError::InvalidConfig(format!(
        "override path {path:?} is not allowed"
    )))
}

fn matches_path(path: &str, pattern: &str) -> bool {
    let path: Vec<_> = path.split('.').collect();
    let pattern: Vec<_> = pattern.split('.').collect();
    path.len() == pattern.len()
        && path
            .iter()
            .zip(pattern)
            .all(|(actual, expected)| expected == "*" || actual == &expected)
}

fn convert_generic(value: &str) -> Value {
    match value {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    if let Ok(value) = value.parse::<i64>() {
        return Value::Number(value.into());
    }
    if let Ok(value) = value.parse::<f64>()
        && let Some(value) = Number::from_f64(value)
    {
        return Value::Number(value);
    }
    if value.contains(',') {
        let values: Vec<_> = value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| Value::String(value.to_owned()))
            .collect();
        if !values.is_empty() {
            return Value::Array(values);
        }
    }
    Value::String(value.to_owned())
}

fn set_nested_value(settings: &mut Value, path: &str, value: Value) -> Result<()> {
    if settings.is_null() {
        *settings = Value::Object(Map::new());
    }
    let parts: Vec<_> = path.split('.').collect();
    if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        return Err(CoreError::InvalidConfig("override path is empty".into()));
    }
    let mut current = settings
        .as_object_mut()
        .ok_or_else(|| CoreError::InvalidConfig("connector settings must be an object".into()))?;
    for (index, part) in parts[..parts.len() - 1].iter().enumerate() {
        let next = current
            .entry((*part).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        current = next.as_object_mut().ok_or_else(|| {
            CoreError::InvalidConfig(format!(
                "override path {path:?} contains non-object value at {:?}",
                parts[..=index].join(".")
            ))
        })?;
    }
    current.insert(parts[parts.len() - 1].to_owned(), value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{CompiledConnector, ConnectorDescriptor};

    struct GenericDescriptor;

    impl ConnectorDescriptor for GenericDescriptor {
        fn compile(&self, _settings: &Value) -> Result<Arc<dyn CompiledConnector>> {
            Err(CoreError::Internal("not used".into()))
        }
    }

    #[test]
    fn wildcard_matches_exactly_one_segment() {
        let whitelist = vec!["routes.*.topic".to_owned()];
        assert!(validate_override_path("routes.pub.topic", &whitelist).is_ok());
        assert!(validate_override_path("routes.pub", &whitelist).is_err());
        assert!(validate_override_path("routes.pub.topic.extra", &whitelist).is_err());
    }

    #[test]
    fn applies_nested_generic_values_without_mutating_base() {
        let base = ConnectorConfig {
            connector_type: "test".into(),
            overridable: vec!["routes.*.*".into()],
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: serde_json::json!({"routes":{"pub":{"topic":"before"}}}),
        };
        let overrides = BTreeMap::from([
            ("routes.pub.topic".into(), "after".into()),
            ("routes.pub.enabled".into(), "true".into()),
        ]);

        let modified = apply_overrides(&base, &GenericDescriptor, &overrides)
            .expect("apply allowed overrides");
        assert_eq!(base.settings["routes"]["pub"]["topic"], "before");
        assert_eq!(modified.settings["routes"]["pub"]["topic"], "after");
        assert_eq!(modified.settings["routes"]["pub"]["enabled"], true);
    }
}
