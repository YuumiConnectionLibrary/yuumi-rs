//! Rust engine SDK for Yuumi Wire Protocol version 1.
//!
//! [`Engine`] dials the platform-native endpoint owned by the Go shell.
//! Application callbacks are synchronous `Arc<dyn Fn(..) + Send + Sync>`
//! values and are serialized on a bounded dispatcher for each session.

mod engine;
mod protocol;
mod transport;
mod types;

pub use engine::{Engine, Responder};
pub use types::*;
