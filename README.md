# yuumi-rs

> **Work in progress** — Rust SDK for the [Yuumi IPC protocol](https://github.com/YuumiConnectionLibrary/yuumi-spec).

## Planned API

```rust
use yuumi::{Client, Channel, ReconnectPolicy};

#[tokio::main]
async fn main() -> yuumi::Result<()> {
    let client = Client::connect("my-service").await?;

    client.on_message(|data, channel| {
        println!("received on {channel:?}: {data:?}");
    });

    client.on_heartbeat(|ts| println!("heartbeat ts={ts}"));
    client.on_error(|err| eprintln!("error: {err}"));

    client.send(serde_json::json!({"status": "ready"}), Channel::Command).await?;
    let (data, channel) = client.receive().await?;

    client.close().await
}
```

## Planned reconnect policy

```rust
let client = Client::connect_with_policy("my-service", ReconnectPolicy {
    max_attempts: 5,
    initial_delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(2),
    jitter: 0.10,
}).await?;
```

## Planned API surface

| Symbol | Description |
|---|---|
| `Client::connect(pipe_name)` | Dial, handshake, return `Client` |
| `Client::connect_with_policy(pipe_name, policy)` | Dial with exponential-backoff retry |
| `client.send(value, channel)` | Encode and send a data frame |
| `client.receive()` | Read one frame → `(Value, Channel)` |
| `client.listen()` | Spawn async read task |
| `client.on_message(fn)` | `fn(Value, Channel)` |
| `client.on_heartbeat(fn)` | `fn(ts: u64)` |
| `client.on_error(fn)` | `fn(YuumiError)` |
| `client.close()` | Graceful shutdown |
| `ReconnectPolicy` | Backoff config: `max_attempts`, `initial_delay`, `max_delay`, `jitter` |
| `Diagnostic` | Structured stdout logging |

## Channels

| Constant | Value | Purpose |
|---|---|---|
| `Channel::Control` | `0` | Heartbeat, lifecycle |
| `Channel::Command` | `1` | Commands |
| `Channel::Log` | `2` | Log output |
| `Channel::Data` | `3` | Application payload |

## Planned dependencies

- [`tokio`](https://tokio.rs) — async runtime + Unix socket I/O
- [`serde_json`](https://github.com/serde-rs/json) — JSON encoding
- [`rmp-serde`](https://github.com/3Hren/msgpack-rust) — MsgPack encoding

## Wire protocol

See [yuumi-spec](https://github.com/YuumiConnectionLibrary/yuumi-spec) for the canonical wire format and conformance test vectors.

## Issues

Questions or design suggestions? [Open an issue](https://github.com/YuumiConnectionLibrary/yuumi-rs/issues).
