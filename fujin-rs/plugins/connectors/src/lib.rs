//! Optional message-broker connector adapters.

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "nop")]
pub mod nop;
