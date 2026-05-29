//! # yuumi-rs
//!
//! Rust SDK for the [Yuumi IPC protocol v2](https://github.com/YuumiConnectionLibrary/yuumi-spec).
//!
//! ## Planned public API
//!
//! ```rust,ignore
//! use yuumi::{Client, Channel, ReconnectPolicy};
//!
//! #[tokio::main]
//! async fn main() -> yuumi::Result<()> {
//!     let client = Client::connect("my-service").await?;
//!
//!     client.on_message(|data, channel| {
//!         println!("received on {channel:?}: {data:?}");
//!     });
//!
//!     client.send(serde_json::json!({"status": "ready"}), Channel::Command).await?;
//!
//!     let (data, channel) = client.receive().await?;
//!     println!("{data:?} on {channel:?}");
//!
//!     client.close().await
//! }
//! ```
//!
//! See [yuumi-spec](https://github.com/YuumiConnectionLibrary/yuumi-spec) for the wire format.

// Implementation pending — see yuumi-spec for protocol details.
