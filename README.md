# yuumi-rs

Rust Engine SDK for Yuumi Wire Protocol version 1.

The Go application is the only client and owns the local listener. A Rust
engine derives the canonical address and performs one explicit outbound
connection. The crate exposes no listener, client, Runner, process launcher,
automatic reconnection policy, or application payload schema.

## Requirements

- Rust 1.97 or newer with edition 2021 support;
- Linux, macOS, or Windows;
- Tokio, Serde, `serde_json`, and `rmp-serde`.

Windows uses a byte-stream Named Pipe. Linux and macOS use a Unix domain stream
socket. There is no TCP or transport fallback.

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
        println!("connected: {} epoch={}", session.session_id, session.epoch);
    }));

    engine.on_message(Arc::new(|event: MessageEvent| {
        if let Some(responder) = event.responder {
            tokio::spawn(async move {
                if let Err(error) = responder.respond(&json!({"ok": true})).await {
                    eprintln!("response failed: {error}");
                }
            });
        }
    }));

    engine.on_error(Arc::new(|error| {
        eprintln!("{:?}: {}", error.kind, error.cause);
    }));

    engine.connect().await?;
    run_application_loop().await;
    engine.close().await
}

async fn run_application_loop() {}
```

A complete compilable form is available in
[`examples/engine.rs`](examples/engine.rs).

## Connection lifecycle

`Engine::new` stores configuration without touching the transport.
`connect().await` performs exactly one bounded dial and handshake attempt. It
receives and validates the 16-byte Go handshake, writes the four-byte ACK,
writes session assignment, and returns only when the session is usable. It
never retries.

After a terminal disconnect, the engine returns to `Idle`. Reconnection
requires another explicit `connect` call and creates a higher local epoch.
Work captured by a previous epoch cannot write to or close the replacement
session.

`close().await` is valid in every state and is idempotent. It cancels a
connecting attempt, rejects new work, wakes the reader and dispatcher, stops
heartbeat and fragmentation work, and joins owned tasks within a bounded
deadline. `Drop` requests the same teardown and aborts remaining owned tasks
when asynchronous joining is no longer possible.

## Configuration

`EngineConfig::new` requires an endpoint name and a 32-character lowercase
hexadecimal token. Defaults are:

- MessagePack preferred over JSON;
- `CAP_CORRELATION` enabled;
- 10-second complete dial and handshake timeout;
- bounded application queue capacity of 64;
- heartbeat every 30 seconds with a three-interval miss limit;
- fragment expiry after 15 seconds and at most 16 active sequences.

`heartbeat.disabled` is the explicit opt-out. `expected_go_pid` is an
optional additional check of the PID carried by the handshake; zero is
distinct from absence.

The canonical address is:

```text
Windows: \\.\pipe\yuumi-<endpoint_name>-<token>
Unix:   <system-temp-dir>/yuumi-<first-32-hex-of-SHA-256>.sock
```

The Unix digest input is the exact UTF-8 byte sequence
`yuumi NUL endpoint_name NUL token`. macOS rejects a pathname longer than 103
encoded bytes before dialing. The helper remains internal because arbitrary or
public transport addresses are outside the Engine API.

## Callbacks and backpressure

Callbacks use stable synchronous `Arc<dyn Fn + Send + Sync>` values. One
dispatcher invokes events for a session in wire order without overlap. The
reader, heartbeat worker, writes, and frame parser never invoke application
logic directly.

A callback should return promptly. It may use `tokio::spawn` for asynchronous
application work, including `Responder::respond`. A CPU-bound callback
occupies one Tokio worker thread; applications needing stronger isolation
should move that work to a dedicated executor.

The application queue is bounded. When full, the engine stops accepting
application events, closes the session, drains accepted events in order, and
then delivers the reserved backpressure error and disconnected event. No event
is silently dropped while the session continues.

Applications may send only `Channel::Log` and `Channel::Data`.
`Responder` is present only for correlated inbound traffic, is single-use,
is bound to the captured epoch, and always responds on `Data`.

## Verification

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo run --example yuumi_go_integration_adapter
```

The native suite implements the 25 canonical Engine cases and consumes frozen
vectors from the sibling `yuumi-spec/test-vectors` directory.
The first three commands are standalone verification. The final explicit
command builds the private fixture and runs the real Go-to-Rust cell through
the shared tagged Go driver; missing prerequisites fail rather than skip. The
crate manifest excludes the adapter and fixture sources from package content.

The authoritative contracts are `yuumi-spec/ENGINE_API.md`,
`yuumi-spec/ENGINE_CONFORMANCE.md`, and `yuumi-spec/PROTOCOL.md`.

The required CI matrix, immutable spec pin, artifacts, timeout, cleanup, and
local equivalents are documented in [`CI.md`](CI.md).
