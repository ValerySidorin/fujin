//! Native transport and gRPC server orchestration.

#[cfg(feature = "bench")]
pub mod bench_support;

#[cfg(feature = "grpc")]
mod grpc;
mod server;

#[cfg(feature = "grpc")]
pub use grpc::{GrpcOutput, GrpcService, GrpcSession};

pub use server::serve;
