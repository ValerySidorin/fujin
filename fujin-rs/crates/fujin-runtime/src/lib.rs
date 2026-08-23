//! Fujin runtime host, configuration, connector reload, and listener orchestration.

pub mod configurator;
#[cfg(feature = "grpc")]
mod grpc;
mod server;

pub use fujin_transport::Endpoint;
#[cfg(feature = "grpc")]
pub use grpc::{GrpcOutput, GrpcService, GrpcSession};
pub use server::{
    ServerConfig, configured_listener_count, serve, serve_with_readiness,
    serve_with_readiness_and_upgrade,
};
