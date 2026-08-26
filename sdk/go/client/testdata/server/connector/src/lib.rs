use std::{collections::BTreeMap, sync::Arc};

use fujin::connector::{
    AcceptanceGuarantee, BoxFuture, Capabilities, CompiledConnector, Completion, CompletionSink,
    ConnectorDescriptor, ConnectorPlugin, ConnectorRuntime, CoreError, Message, OperationToken,
    Reader, ReaderEventSink, Result, RouteProfile, Writer,
};

#[derive(Debug)]
struct Descriptor;

impl ConnectorDescriptor for Descriptor {
    fn compile(&self, _settings: &serde_json::Value) -> Result<Arc<dyn CompiledConnector>> {
        Ok(Arc::new(Compiled {
            routes: BTreeMap::from([(
                "default".into(),
                RouteProfile {
                    capabilities: Capabilities::PRODUCE
                        .union(Capabilities::HEADERS)
                        .union(Capabilities::TRANSACTIONS),
                    produce_guarantee: AcceptanceGuarantee::Local,
                    ..RouteProfile::default()
                },
            )]),
        }))
    }
}

struct Compiled {
    routes: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for Compiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.routes
    }

    fn open_runtime(&self) -> Result<Arc<dyn ConnectorRuntime>> {
        Ok(Arc::new(Runtime))
    }
}

struct Runtime;

impl ConnectorRuntime for Runtime {
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
            return Err(CoreError::RouteNotFound(route.into()));
        }
        Ok(Arc::new(ImmediateWriter { completions }))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct ImmediateWriter {
    completions: Arc<dyn CompletionSink>,
}

impl ImmediateWriter {
    fn complete(&self, token: OperationToken) {
        self.completions.complete(Completion {
            token,
            result: Ok(()),
        });
    }
}

impl Writer for ImmediateWriter {
    fn produce(&self, token: OperationToken, _message: Message) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn flush(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn begin_transaction(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn commit_transaction(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn rollback_transaction(&self, token: OperationToken) -> Result<()> {
        self.complete(token);
        Ok(())
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer_contract_compliant(&self) -> bool {
        true
    }
}

#[must_use]
pub fn plugin() -> ConnectorPlugin {
    ConnectorPlugin::new("compat", Descriptor)
}
