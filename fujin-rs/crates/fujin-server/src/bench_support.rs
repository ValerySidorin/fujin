//! Support shared by the feature-gated no-broker performance benchmark binaries.

use std::{collections::BTreeMap, sync::Arc};

use fujin_core::{
    AcceptanceGuarantee, BoxFuture, Capabilities, Catalog, CompiledConnector, Completion,
    CompletionSink, ConnectorConfig, ConnectorDescriptor, ConnectorRuntime, DescriptorRegistry,
    GenerationCompiler, Message, OperationToken, Reader, ReaderEventSink, RouteProfile,
    SettlementProfile, Writer,
};

/// Builds the one-route, locally-acknowledged connector used by protocol benchmarks.
///
/// # Errors
///
/// Returns an error if the in-process Nop connector cannot be registered or compiled.
pub async fn nop_catalog() -> fujin_core::Result<Arc<Catalog>> {
    let registry = Arc::new(DescriptorRegistry::default());
    registry.register("nop", Arc::new(NopDescriptor))?;
    let compiler = Arc::new(GenerationCompiler::without_middlewares(registry));
    let configs = BTreeMap::from([(
        "connector".into(),
        ConnectorConfig {
            connector_type: "nop".into(),
            overridable: Vec::new(),
            bind_middlewares: Vec::new(),
            connector_middlewares: Vec::new(),
            settings: serde_json::Value::Null,
        },
    )]);
    Ok(Arc::new(Catalog::compile(&configs, compiler).await?))
}

struct NopDescriptor;

impl ConnectorDescriptor for NopDescriptor {
    fn compile(
        &self,
        _settings: &serde_json::Value,
    ) -> fujin_core::Result<Arc<dyn CompiledConnector>> {
        Ok(Arc::new(NopCompiled {
            routes: BTreeMap::from([(
                "pub".into(),
                RouteProfile {
                    capabilities: Capabilities::PRODUCE
                        .union(Capabilities::HEADERS)
                        .union(Capabilities::TRANSACTIONS),
                    produce_guarantee: AcceptanceGuarantee::Local,
                    settlement: SettlementProfile::default(),
                },
            )]),
        }))
    }
}

struct NopCompiled {
    routes: BTreeMap<String, RouteProfile>,
}

impl CompiledConnector for NopCompiled {
    fn routes(&self) -> &BTreeMap<String, RouteProfile> {
        &self.routes
    }

    fn open_runtime(&self) -> fujin_core::Result<Arc<dyn ConnectorRuntime>> {
        Ok(Arc::new(NopRuntime))
    }
}

struct NopRuntime;

impl ConnectorRuntime for NopRuntime {
    fn open_reader(
        &self,
        _route: &str,
        _auto_settle: bool,
        _events: Arc<dyn ReaderEventSink>,
    ) -> fujin_core::Result<Arc<dyn Reader>> {
        Err(fujin_core::CoreError::OperationUnsupported)
    }

    fn open_writer(
        &self,
        _route: &str,
        completions: Arc<dyn CompletionSink>,
    ) -> fujin_core::Result<Arc<dyn Writer>> {
        Ok(Arc::new(NopWriter { completions }))
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, fujin_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct NopWriter {
    completions: Arc<dyn CompletionSink>,
}

impl NopWriter {
    fn complete(&self, token: OperationToken) {
        self.completions.complete(Completion {
            token,
            result: Ok(()),
        });
    }
}

impl Writer for NopWriter {
    fn produce(&self, token: OperationToken, _message: Message) -> fujin_core::Result<()> {
        self.complete(token);
        Ok(())
    }

    fn flush(&self, token: OperationToken) -> fujin_core::Result<()> {
        self.complete(token);
        Ok(())
    }

    fn begin_transaction(&self, token: OperationToken) -> fujin_core::Result<()> {
        self.complete(token);
        Ok(())
    }

    fn commit_transaction(&self, token: OperationToken) -> fujin_core::Result<()> {
        self.complete(token);
        Ok(())
    }

    fn rollback_transaction(&self, token: OperationToken) -> fujin_core::Result<()> {
        self.complete(token);
        Ok(())
    }

    fn close(self: Arc<Self>) -> BoxFuture<'static, fujin_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer_contract_compliant(&self) -> bool {
        true
    }
}
