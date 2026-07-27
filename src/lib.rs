//! Rust engine SDK for Yuumi Wire Protocol version 1.
//!
//! [`Engine`] opens the platform-native local endpoint and accepts Go shell
//! sessions. Callbacks are synchronous `Arc<dyn Fn(..) + Send + Sync>` values.
//! Events are serialized per session; separate sessions may execute callbacks
//! concurrently. Sequential awaited sends to one session preserve order.

mod engine;
mod protocol;
mod transport;
mod types;

pub use engine::Engine;
pub use transport::resolve_transport_address;
pub use types::*;
