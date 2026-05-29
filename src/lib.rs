//! # yuumi-rs
//!
//! Rust SDK for the [Yuumi IPC protocol v2](https://github.com/YuumiConnectionLibrary/yuumi-spec).
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use yuumi::{connect, Channel};
//!
//! #[tokio::main]
//! async fn main() -> yuumi::types::Result<()> {
//!     let client = connect("my-service", None).await?;
//!     client.on_message(|data, ch| println!("{ch:?}: {data}"));
//!     client.listen().await;
//!     client.send(serde_json::json!({"status":"ready"}), Channel::Command).await?;
//!     let (data, ch) = client.receive().await?;
//!     println!("{ch:?}: {data}");
//!     client.close().await;
//!     Ok(())
//! }
//! ```

pub mod types;
pub mod protocol;
pub mod transport;
pub mod client;
pub mod diagnostic;

pub use types::{
    Channel, Encoding, ReconnectPolicy, Result, StatusCode, YuumiError,
    MAGIC, PROTOCOL_VERSION, MAX_MESSAGE_SIZE, MAX_PIPE_NAME_BYTES,
};
pub use client::{Client, MessageHandler, HeartbeatHandler, ErrorHandler, connect};
pub use diagnostic::Diagnostic;
pub use protocol::{
    build_handshake_packet, encode_payload, decode_payload, build_frame,
    read_frame, perform_handshake,
};
pub use transport::resolve_transport_address;
