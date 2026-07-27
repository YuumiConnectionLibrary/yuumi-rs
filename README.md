# yuumi-rs

Rust Engine SDK for Yuumi Wire Protocol version 1.

`yuumi-rs` opens a local endpoint and accepts sessions from the Go shell. It is
not a client SDK and does not expose an outbound dialer, reconnect policy,
process launcher, restart policy, or application semantics.

## Platform transport

| Platform | Transport | Endpoint security |
|---|---|---|
| Linux | Unix domain stream socket | Owner-only mode `0600` |
| macOS | Unix domain stream socket | Owner-only mode `0600` |
| Windows | Named Pipe byte stream | Current-user ACL and remote-client rejection |

The address is derived from `endpoint_name` and the 32-character lowercase
hexadecimal `token`. Full transport paths cannot be supplied through the public
Engine API.

## Engine example

```rust,no_run
use std::sync::Arc;

use serde_json::json;
use yuumi::{Channel, Engine, EngineConfig, MessageEvent, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = EngineConfig::new(
        "example",
        "0123456789abcdef0123456789abcdef",
    );
    let engine = Engine::new(config);

    engine.on_session_connected(Arc::new(|session| {
        println!("session connected: {}", session.handle.session_id);
    }));

    let responder = engine.clone();
    engine.on_message(Arc::new(move |event: MessageEvent| {
        let responder = responder.clone();
        tokio::spawn(async move {
            let result = match event.correlation_id {
                Some(id) => responder
                    .send_correlated(&event.session, Channel::Data, id, &json!({"ok": true}))
                    .await,
                None => responder
                    .send(&event.session, Channel::Data, &json!({"ok": true}))
                    .await,
            };
            if let Err(error) = result {
                eprintln!("send failed: {error}");
            }
        });
    }));

    engine.on_error(Arc::new(|error| {
        eprintln!("engine error {}: {}", error.status as u16, error.cause);
    }));

    engine.open().await?;
    application_shutdown().await;
    engine.close().await
}

async fn application_shutdown() {
}
```

Calling `close` is the application policy decision; the SDK only guarantees
safe teardown once it is called.

## Configuration defaults

| Option | Default |
|---|---|
| `max_sessions` | `1` |
| `supported_encodings` | MessagePack, then JSON |
| `supported_capabilities` | `CAP_CORRELATION` |
| `expected_pid` | absent |
| heartbeat | 30 seconds, 3 missed intervals |
| fragmentation | 15-second timeout, 16 active sequences per session |

Set `HeartbeatSettings::disabled` to `true` when the application deliberately
does not want SDK heartbeat emission or timeout enforcement. Other enabled
heartbeat values and all fragmentation limits must be positive.

## Callback and send execution

Callbacks are synchronous `Arc<dyn Fn(..) + Send + Sync>` values. Events for
one session are serialized, while different sessions may execute callbacks
concurrently on Tokio runtime tasks. A callback should return promptly; it can
spawn async application work as shown above.

The per-session writer lock preserves the order of sequential awaited sends.
Concurrent sends follow lock-acquisition order. A stale handle is rejected by
both opaque `session_id` and local `epoch`, so it cannot address a replacement
session.

Applications may send only `Log` and `Data`. Control traffic is SDK-owned, and
`Command` is client-to-engine only. Correlated sends require negotiated
`CAP_CORRELATION` and preserve the supplied `uint32` identifier.

## Conformance

The native test runner implements EC-001 through EC-064 from
`yuumi-spec/ENGINE_CONFORMANCE.md` and consumes the canonical binary vectors
from the sibling `yuumi-spec/test-vectors` directory.

```text
cargo test --all-targets
```

The crate has no dependencies beyond Tokio, Serde, `serde_json`, and
`rmp-serde`.
