//! Fujin native protocol v1 codec and Session Core adapter.

mod codec;
mod encode;
mod session;
mod wire;

use std::io;

pub use codec::Decoder;
pub use session::{NativeSession, SessionAction, SessionOutput, run, run_with_shutdown};
pub use wire::{
    DEFAULT_MAX_FRAME_SIZE, HELLO_FORMAT, HelloRequest, Request, RequestCode, ResponseCode,
    WIRE_VERSION,
};

#[derive(Debug, thiserror::Error)]
pub enum NativeError {
    #[error("malformed native frame: {0}")]
    Malformed(&'static str),
    #[error("native frame exceeds configured maximum")]
    FrameTooLarge,
    #[error("native stream I/O: {0}")]
    Io(#[from] io::Error),
    #[error("native session: {0}")]
    Session(#[from] fujin_error::CoreError),
    #[error("native session output closed")]
    OutputClosed,
}
