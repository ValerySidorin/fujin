use std::sync::Arc;

use fujin::connector::{CompiledConnector, ConnectorDescriptor, ConnectorPlugin, CoreError, Result};

#[derive(Debug)]
struct TestDescriptor;

impl ConnectorDescriptor for TestDescriptor {
    fn compile(&self, _settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        Err(CoreError::Internal(
            "test connector must not be configured".into(),
        ))
    }
}

pub fn plugin() -> ConnectorPlugin {
    ConnectorPlugin::new("test", TestDescriptor)
}
