//! Native transport and gRPC server orchestration.

#[cfg(feature = "bench")]
pub mod bench_support;

#[cfg(feature = "grpc")]
mod grpc;
mod server;

#[cfg(feature = "grpc")]
pub use grpc::{GrpcOutput, GrpcService, GrpcSession};

pub use server::{
    configured_listener_count, serve, serve_with_readiness, serve_with_readiness_and_upgrade,
};

#[cfg(feature = "websocket")]
#[doc(hidden)]
pub use server::NativeWebSocketStream;
