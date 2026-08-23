//! Fujin embedding facade and public plugin development surface.

mod application;
pub mod plugins;

pub use application::{Application, ApplicationBuilder, ApplicationHandle, RunningApplication};
pub use fujin_plugin_api::{configurator, connector, middleware, transport};
pub use fujin_runtime::{
    Endpoint, FujinConfig, GrpcConfig, HealthConfig, RuntimeConfig, RuntimeError, TransportConfig,
};
