//! Native transport and gRPC server orchestration.

#[cfg(feature = "bench")]
pub mod bench_support;

#[cfg(feature = "grpc")]
mod grpc;
mod server;
pub mod transport;

#[cfg(feature = "grpc")]
pub use grpc::{GrpcOutput, GrpcService, GrpcSession};

pub use server::{
    ServerConfig, configured_listener_count, serve, serve_with_readiness,
    serve_with_readiness_and_upgrade,
};
pub use transport::{
    CompiledTransport, ConfiguredTransport, Endpoint, TransportContext, TransportPlugin,
    TransportRegistration, TransportRegistry,
};

#[cfg(feature = "websocket")]
#[doc(hidden)]
pub use server::NativeWebSocketStream;
