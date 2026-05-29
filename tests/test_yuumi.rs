use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use yuumi::types::{Channel, Encoding, ReconnectPolicy, StatusCode};
use yuumi::{
    build_handshake_packet, decode_payload, resolve_transport_address, Client,
};

fn vectors_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("yuumi-spec")
        .join("test-vectors")
}

fn read_vector(name: &str) -> Vec<u8> {
    std::fs::read(vectors_dir().join(name))
        .unwrap_or_else(|e| panic!("missing vector {name}: {e}"))
}

#[cfg(unix)]
fn unique_pipe(tag: &str) -> String {
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    format!("yuumi-rs-{tag}-{ns}")
}

// ─── 1. Handshake vector ───────────────────────────────────────────────────

#[test]
fn test_handshake_packet_matches_vector() {
    let got = build_handshake_packet(1234);
    let expected = read_vector("handshake_valid.bin");
    assert_eq!(got.as_ref(), expected.as_slice());
}

// ─── 2. Frame decode round-trip ───────────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_frame_decode_roundtrip_from_vector() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    server_stream.write_all(&read_vector("frame_channel_command.bin")).await.unwrap();
    server_stream.shutdown().await.unwrap();

    let (data, channel) = client.receive().await.unwrap();
    assert_eq!(channel, Channel::Command);
    assert_eq!(data["action"], "test");
}

// ─── 3. Oversized frame guard ─────────────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_frame_max_size_guard_from_vector() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    server_stream.write_all(&read_vector("frame_oversized.bin")).await.unwrap();
    server_stream.shutdown().await.unwrap();

    let err = client.receive().await.unwrap_err();
    assert_eq!(err.code, StatusCode::ErrProtocolViolation);
}

// ─── 4. Channel dispatch ──────────────────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_channel_dispatch() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    let received: Arc<Mutex<Vec<Channel>>> = Arc::new(Mutex::new(vec![]));
    let notify = Arc::new(tokio::sync::Notify::new());

    let received_c = Arc::clone(&received);
    let notify_c = Arc::clone(&notify);
    client.on_message(move |_data, ch| {
        received_c.lock().unwrap().push(ch);
        notify_c.notify_one();
    });
    client.listen().await;

    let payload = b"{\"msg\":\"hello\"}";
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.push(Channel::Log as u8);
    frame.push(0u8);
    frame.extend_from_slice(payload);
    server_stream.write_all(&frame).await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), notify.notified()).await.unwrap();
    assert_eq!(*received.lock().unwrap(), vec![Channel::Log]);
    client.close().await;
}

// ─── 5. Pipe name truncation ──────────────────────────────────────────────

#[test]
fn test_resolve_transport_address_pipe_name_limit() {
    let long = "a".repeat(96);
    let expected = resolve_transport_address(&"a".repeat(64));
    assert_eq!(resolve_transport_address(&long), expected);
}

// ─── 6. Connect / send / receive / close ─────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_connect_send_receive_close() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    let pipe_name = unique_pipe("csrc");
    let socket_path = resolve_transport_address(&pipe_name);
    let listener = UnixListener::bind(&socket_path).unwrap();

    let expected_handshake = build_handshake_packet(std::process::id());

    let server = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        let mut hs = [0u8; 16];
        conn.read_exact(&mut hs).await.unwrap();
        assert_eq!(hs, expected_handshake);
        conn.write_all(&[0x01, 0x00, 0x00, 0x00]).await.unwrap();

        let mut hdr = [0u8; 6];
        conn.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let mut body = vec![0u8; len];
        conn.read_exact(&mut body).await.unwrap();
        assert_eq!(hdr[4], Channel::Command as u8);
        assert_eq!(body, b"{\"ping\":\"pong\"}");

        let reply = b"{\"ok\":true}";
        let mut frame = Vec::new();
        frame.extend_from_slice(&(reply.len() as u32).to_be_bytes());
        frame.push(Channel::Data as u8);
        frame.push(0u8);
        frame.extend_from_slice(reply);
        conn.write_all(&frame).await.unwrap();
    });

    let client = Client::connect(&pipe_name).await.unwrap();
    client.send(json!({"ping": "pong"}), Channel::Command).await.unwrap();
    let (data, channel) = client.receive().await.unwrap();
    client.close().await;

    server.await.unwrap();
    assert_eq!(channel, Channel::Data);
    assert_eq!(data["ok"], true);

    let _ = std::fs::remove_file(&socket_path);
}

// ─── 7. Heartbeat callback ────────────────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_heartbeat_callback() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
    let tx = Mutex::new(Some(tx));
    client.on_heartbeat(move |ts| {
        if let Some(s) = tx.lock().unwrap().take() { let _ = s.send(ts); }
    });
    client.listen().await;

    let payload = b"{\"type\":\"heartbeat\",\"ts\":1717000000}";
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.push(Channel::Control as u8);
    frame.push(0u8);
    frame.extend_from_slice(payload);
    server_stream.write_all(&frame).await.unwrap();

    let ts = tokio::time::timeout(Duration::from_secs(1), rx).await.unwrap().unwrap();
    assert_eq!(ts, 1_717_000_000);
    client.close().await;
}

// ─── 8. Heartbeat not dispatched to on_message ────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_heartbeat_not_dispatched_to_on_message() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    let msg_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let msg_called_c = Arc::clone(&msg_called);
    client.on_message(move |_, _| { msg_called_c.store(true, std::sync::atomic::Ordering::SeqCst); });
    client.on_heartbeat(|_| {});
    client.listen().await;

    let payload = b"{\"type\":\"heartbeat\",\"ts\":1717000000}";
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.push(Channel::Control as u8);
    frame.push(0u8);
    frame.extend_from_slice(payload);
    server_stream.write_all(&frame).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!msg_called.load(std::sync::atomic::Ordering::SeqCst), "heartbeat must not reach on_message");
    client.close().await;
}

// ─── 9. on_error called on connection lost ────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_on_error_called_on_connection_lost() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    let (tx, rx) = tokio::sync::oneshot::channel::<StatusCode>();
    let tx = Mutex::new(Some(tx));
    client.on_error(move |e| {
        if let Some(s) = tx.lock().unwrap().take() { let _ = s.send(e.code); }
    });
    client.listen().await;

    server_stream.shutdown().await.unwrap();
    drop(server_stream);

    let code = tokio::time::timeout(Duration::from_secs(1), rx).await.unwrap().unwrap();
    assert_eq!(code, StatusCode::ErrConnectionLost);
}

// ─── 10. Frame flags violation ────────────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_frame_flags_violation() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::net::UnixStream::pair().unwrap();
    let client = Client::raw(client_stream, Encoding::Json);

    let payload = b"{\"x\":1}";
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.push(Channel::Command as u8);
    frame.push(0xFF); // invalid flags
    frame.extend_from_slice(payload);
    server_stream.write_all(&frame).await.unwrap();

    let err = client.receive().await.unwrap_err();
    assert_eq!(err.code, StatusCode::ErrProtocolViolation);
}

// ─── 11. ACK reserved bytes rejected ─────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_ack_reserved_bytes_rejected() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let pipe_name = unique_pipe("ack-bad");
    let socket_path = resolve_transport_address(&pipe_name);
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 16];
        conn.read_exact(&mut buf).await.ok();
        conn.write_all(&[0x01, 0x00, 0x00, 0xFF]).await.ok(); // bad reserved
    });

    let err = Client::connect(&pipe_name).await.unwrap_err();
    assert_eq!(err.code, StatusCode::ErrProtocolViolation);
    let _ = std::fs::remove_file(&socket_path);
}

// ─── 12. Drop closes (RAII) ───────────────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_drop_closes() {
    let (client_stream, _server_stream) = tokio::net::UnixStream::pair().unwrap();
    {
        let client = Client::raw(client_stream, Encoding::Json);
        // Drop at end of block — must not panic
        drop(client);
    }
    // If we get here without panic, RAII works
}

// ─── 13. Reconnect policy — no retry ─────────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_reconnect_policy_no_retry() {
    let pipe_name = unique_pipe("no-retry");
    let policy = ReconnectPolicy { max_attempts: 0, initial_delay: 0.05, ..Default::default() };
    let start = std::time::Instant::now();
    let err = Client::connect_with_policy(&pipe_name, &policy).await.unwrap_err();
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(err.code, StatusCode::ErrPipeFailed);
}

// ─── 14. Reconnect policy — max attempts ─────────────────────────────────

#[cfg(unix)]
#[tokio::test]
async fn test_reconnect_policy_max_attempts() {
    let pipe_name = unique_pipe("max-retry");
    let policy = ReconnectPolicy {
        max_attempts: 3,
        initial_delay: 0.02,
        max_delay: 0.02,
        jitter: 0.0,
    };
    let start = std::time::Instant::now();
    let err = Client::connect_with_policy(&pipe_name, &policy).await.unwrap_err();
    // Must have waited at least 2 backoff intervals (attempt 1→2, 2→3)
    assert!(start.elapsed() > Duration::from_millis(20));
    assert_eq!(err.code, StatusCode::ErrPipeFailed);
}
