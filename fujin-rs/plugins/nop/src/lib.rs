use std::{collections::BTreeMap, sync::Arc};

use fujin_plugin_api::connector::{
    AcceptanceGuarantee, BoxFuture, Capabilities, CompiledConnector, Completion, CompletionSink,
    ConnectorDescriptor, ConnectorPlugin, ConnectorRuntime, CoreError, Message, OperationToken,
    Reader, ReaderEventSink, Result, RouteProfile, Writer,
};

#[derive(Debug)]
pub struct NopDescriptor;

impl ConnectorDescriptor for NopDescriptor {
    fn compile(&self, settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        if !settings.is_null() && !settings.as_object().is_some_and(serde_json::Map::is_empty) {
            return Err(CoreError::InvalidConfig(
                "nop connector settings must be empty".into(),
            ));
        }
        Ok(Arc::new(NopCompiled {
            routes: BTreeMap::from([(
                "default".into(),
                RouteProfile {
                    capabilities: Capabilities::PRODUCE,
                    produce_guarantee: AcceptanceGuarantee::Local,
                    ..RouteProfile::default()
                },
            )]),
        }))
    }
}

#[derive(Debug)]
struct NopCompiled {
    routes: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for NopCompiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.routes
    }

    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
        Ok(Arc::new(NopRuntime))
    }
}

#[derive(Debug)]
struct NopRuntime;

impl ConnectorRuntime for NopRuntime {
    fn open_reader(
        &self,
        _route: &str,
        _auto_settle: bool,
        _events: Arc<dyn ReaderEventSink>,
    ) -> Result<Arc<dyn Reader>> {
        Err(CoreError::OperationUnsupported)
    }

    fn open_writer(
        &self,
        route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> Result<Arc<dyn Writer>> {
        if route != "default" {
            return Err(CoreError::InvalidConfig(format!(
                "nop connector route {route:?} is unknown"
            )));
        }
        Ok(Arc::new(NopWriter { completions }))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct NopWriter {
    completions: Arc<dyn CompletionSink>,
}

impl std::fmt::Debug for NopWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NopWriter").finish_non_exhaustive()
    }
}

impl Writer for NopWriter {
    fn produce(&self, token: OperationToken, _message: Message) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn begin_transaction(&self, _token: OperationToken) -> Result<()> {
        Err(CoreError::OperationUnsupported)
    }

    fn commit_transaction(&self, _token: OperationToken) -> Result<()> {
        Err(CoreError::OperationUnsupported)
    }

    fn rollback_transaction(&self, _token: OperationToken) -> Result<()> {
        Err(CoreError::OperationUnsupported)
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer_contract_compliant(&self) -> bool {
        true
    }
}

impl NopWriter {
    fn complete(&self, token: OperationToken) {
        self.completions.complete(Completion {
            token,
            result: Ok(()),
        });
    }
}

#[must_use]
pub fn plugin() -> ConnectorPlugin {
    ConnectorPlugin::new("nop", NopDescriptor)
}
