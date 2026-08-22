//! Fujin embedding facade and public plugin development surface.

mod application;

pub use application::{Application, ApplicationBuilder, ApplicationHandle, RunningApplication};
pub use fujin_plugin_api::{configurator, connector, middleware, transport};
pub use fujin_runtime::{RuntimeConfig, RuntimeError};
pub use fujin_server::Endpoint;
