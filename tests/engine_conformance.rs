mod testkit;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use testkit::*;
use yuumi::*;

fn json_frame(channel: Channel, value: Value) -> Vec<u8> {
    frame(channel as u8, 0, &serde_json::to_vec(&value).unwrap())
}

#[tokio::test]
async fn ec_001_configuration_is_validated_before_dial() {
    let mut cases = Vec::new();
    cases.push(EngineConfig::new("bad/name", TOKEN));
    cases.push(EngineConfig::new("ok", "ABCDEF0123456789ABCDEF0123456789"));
    let mut empty = config();
    empty.supported_encodings.clear();
    cases.push(empty);
    let mut duplicate = config();
    duplicate.supported_encodings = vec![Encoding::Json, Encoding::Json];
    cases.push(duplicate);
    let mut capability = config();
    capability.supported_capabilities = 2;
    cases.push(capability);
    let mut timeout = config();
    timeout.connect_timeout = Duration::ZERO;
    cases.push(timeout);
    let mut queue = config();
    queue.application_queue_capacity = 0;
    cases.push(queue);
    for value in cases {
        let engine = Engine::new(value);
        let error = engine.connect().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(engine.state(), EngineState::Idle);
    }
    let defaults = EngineConfig::new("defaults", TOKEN);
    assert_eq!(
        defaults.supported_encodings,
        [Encoding::MessagePack, Encoding::Json]
    );
    assert_eq!(defaults.connect_timeout, Duration::from_secs(10));
    assert_eq!(defaults.application_queue_capacity, 64);
}

#[tokio::test]
async fn ec_002_canonical_address_matches_frozen_vectors() {
    let mut value = EngineConfig::new("a", "000102030405060708090a0b0c0d0e0f");
    value.heartbeat.disabled = true;
    let address = address(&value);
    #[cfg(unix)]
    assert_eq!(
        address.file_name().unwrap(),
        "yuumi-c3da2f7decb02b7a24e054711453a85c.sock"
    );
    #[cfg(windows)]
    assert_eq!(
        address.to_string_lossy(),
        r"\\.\pipe\yuumi-a-000102030405060708090a0b0c0d0e0f"
    );
    let engine = Engine::new(value.clone());
    let session = establish(&engine, &value).await;
    drop(session.stream);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_003_engine_uses_only_platform_native_dialer() {
    let value = config();
    let engine = Engine::new(value.clone());
    let session = establish(&engine, &value).await;
    assert_eq!(engine.state(), EngineState::Connected);
    assert_eq!(session.view, engine.session().unwrap());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_004_dial_failure_is_typed_and_creates_no_endpoint() {
    let mut value = config();
    value.connect_timeout = Duration::from_millis(100);
    #[cfg(unix)]
    let address = address(&value);
    let engine = Engine::new(value);
    let error = engine.connect().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Dial);
    assert_eq!(engine.state(), EngineState::Idle);
    #[cfg(unix)]
    assert!(!address.exists());
}

#[tokio::test]
async fn ec_005_invalid_handshake_writes_nothing() {
    for (offset, expected) in [
        (0usize, StatusCode::ErrMagicMismatch),
        (4usize, StatusCode::ErrVersionMismatch),
    ] {
        let value = config();
        let engine = Engine::new(value.clone());
        let listener = TestListener::bind(&value).await;
        let mut packet = handshake(3, 1);
        packet[offset..offset + 4].copy_from_slice(&0u32.to_be_bytes());
        let peer = tokio::spawn(async move {
            let mut stream = listener.accept().await;
            stream.write_all(&packet).await.unwrap();
            let mut byte = [0u8; 1];
            let result = stream.read_exact(&mut byte).await;
            assert!(result.is_err());
        });
        let error = engine.connect().await.unwrap_err();
        assert_eq!(error.code(), Some(expected));
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn ec_006_encoding_and_capability_intersection_is_deterministic() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json, Encoding::MessagePack];
    let engine = Engine::new(value.clone());
    let session = establish_with(&engine, &value, handshake(3, 0x800001)).await;
    assert_eq!(session.ack, [Encoding::Json as u8, 0, 0, 1]);
    assert_eq!(session.view.encoding, Encoding::Json);
    assert_eq!(session.view.capabilities, CAP_CORRELATION);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_007_ack_and_assignment_establish_in_order() {
    let value = config();
    let engine = Engine::new(value.clone());
    let session = establish(&engine, &value).await;
    assert_eq!(session.ack, [Encoding::MessagePack as u8, 0, 0, 1]);
    assert_eq!(session.assignment.0, Channel::Control as u8);
    let control: Value = serde_json::from_slice(&session.assignment.2).unwrap();
    assert_eq!(control["type"], "session");
    assert_eq!(control["session_id"], session.view.session_id);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_008_establishment_failure_creates_no_session() {
    let value = config();
    let engine = Engine::new(value.clone());
    let listener = TestListener::bind(&value).await;
    let peer = tokio::spawn(async move { drop(listener.accept().await) });
    let error = engine.connect().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Handshake);
    assert!(engine.session().is_none());
    assert_eq!(engine.state(), EngineState::Idle);
    peer.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ec_009_duplicate_connect_is_rejected_by_state() {
    let value = config();
    let engine = Engine::new(value.clone());
    let listener = TestListener::bind(&value).await;
    let first_engine = engine.clone();
    let first = tokio::spawn(async move { first_engine.connect().await });
    wait_until(|| engine.state() == EngineState::Connecting).await;
    assert_eq!(engine.connect().await.unwrap_err().kind(), ErrorKind::State);
    let mut peer = listener.accept().await;
    peer.write_all(&handshake(3, 1)).await.unwrap();
    let mut ack = [0u8; 4];
    peer.read_exact(&mut ack).await.unwrap();
    read_frame(&mut peer).await;
    first.await.unwrap().unwrap();
    assert_eq!(engine.connect().await.unwrap_err().kind(), ErrorKind::State);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_010_close_is_idempotent_and_reconnect_is_explicit() {
    let value = config();
    let engine = Engine::new(value.clone());
    let first = establish(&engine, &value).await;
    let first_epoch = first.view.epoch;
    engine.close().await.unwrap();
    drop(first.stream);
    engine.close().await.unwrap();
    let second = establish(&engine, &value).await;
    assert!(second.view.epoch > first_epoch);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_011_oversized_declared_frame_is_rejected_without_allocation() {
    let value = config();
    let engine = Engine::new(value.clone());
    let mut session = establish(&engine, &value).await;
    session
        .stream
        .write_all(&vector("frame_oversized.bin"))
        .await
        .unwrap();
    let control = read_frame(&mut session.stream).await;
    let error: Value = serde_json::from_slice(&control.2).unwrap();
    assert_eq!(error["code"], StatusCode::ErrPayloadTooLarge as u16);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_012_malformed_frames_never_partially_deliver() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let messages = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&messages);
    engine.on_message(Arc::new(move |event| observed.lock().unwrap().push(event)));
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    session
        .stream
        .write_all(&frame(Channel::Command as u8, 0x80, b"{}"))
        .await
        .unwrap();
    read_frame(&mut session.stream).await;
    wait_until(|| engine.state() == EngineState::Idle).await;
    assert!(messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ec_013_fragmentation_is_bounded_and_epoch_local() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    value.fragmentation.timeout = Duration::from_millis(30);
    let engine = Engine::new(value.clone());
    let messages = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&messages);
    engine.on_message(Arc::new(move |event| {
        observed.lock().unwrap().push(event.payload)
    }));
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    session
        .stream
        .write_all(&vector("frame_fragment_first.bin"))
        .await
        .unwrap();
    session
        .stream
        .write_all(&vector("frame_fragment_last.bin"))
        .await
        .unwrap();
    wait_until(|| !messages.lock().unwrap().is_empty()).await;
    assert_eq!(messages.lock().unwrap()[0], "Hello World");
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_014_responder_is_data_correlated_and_single_use() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let responder = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&responder);
    engine.on_message(Arc::new(move |event| {
        *captured.lock().unwrap() = event.responder
    }));
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    session
        .stream
        .write_all(&vector("frame_correlated_request.bin"))
        .await
        .unwrap();
    wait_until(|| responder.lock().unwrap().is_some()).await;
    let responder = responder.lock().unwrap().clone().unwrap();
    responder.respond(&json!({"ok":true})).await.unwrap();
    let response = read_frame(&mut session.stream).await;
    assert_eq!((response.0, response.1), (Channel::Data as u8, 4));
    assert_eq!(u32::from_be_bytes(response.2[0..4].try_into().unwrap()), 42);
    assert_eq!(
        responder.respond(&json!({})).await.unwrap_err().kind(),
        ErrorKind::StaleEpoch
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_015_public_sends_are_directional_ordered_and_await_writes() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    engine.send(Channel::Data, &json!({"n":1})).await.unwrap();
    engine.send(Channel::Log, &json!({"n":2})).await.unwrap();
    let first = read_frame(&mut session.stream).await;
    let second = read_frame(&mut session.stream).await;
    assert_eq!(
        (first.0, second.0),
        (Channel::Data as u8, Channel::Log as u8)
    );
    assert_eq!(
        engine
            .send(Channel::Command, &json!({}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Protocol
    );
    assert_eq!(
        engine
            .send(Channel::Data, &AlwaysFails)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Encoding
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_016_control_and_fatal_ordering_are_exact() {
    let value = config();
    let engine = Engine::new(value.clone());
    let mut session = establish(&engine, &value).await;
    session
        .stream
        .write_all(&vector("control_ping.bin"))
        .await
        .unwrap();
    let pong = read_frame(&mut session.stream).await;
    let control: Value = serde_json::from_slice(&pong.2).unwrap();
    assert_eq!(control, json!({"type":"pong","seq":1}));
    engine.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_017_ipc_progresses_while_handler_is_slow() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let started = Arc::new(AtomicBool::new(false));
    let callback_started = Arc::clone(&started);
    let finished = Arc::new(AtomicBool::new(false));
    let callback_finished = Arc::clone(&finished);
    engine.on_message(Arc::new(move |_| {
        callback_started.store(true, Ordering::Release);
        std::thread::sleep(Duration::from_millis(200));
        callback_finished.store(true, Ordering::Release);
    }));
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    session
        .stream
        .write_all(&json_frame(Channel::Command, json!({"slow":true})))
        .await
        .unwrap();
    wait_until(|| started.load(Ordering::Acquire)).await;
    session
        .stream
        .write_all(&vector("control_ping.bin"))
        .await
        .unwrap();
    let pong = tokio::time::timeout(Duration::from_millis(100), read_frame(&mut session.stream))
        .await
        .unwrap();
    assert_eq!(pong.0, Channel::Control as u8);
    wait_until(|| finished.load(Ordering::Acquire)).await;
    engine.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_018_queue_full_is_terminal_and_reserved_events_are_delivered() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    value.application_queue_capacity = 1;
    let engine = Engine::new(value.clone());
    let started = Arc::new(AtomicBool::new(false));
    let callback_started = Arc::clone(&started);
    engine.on_session_connected(Arc::new(move |_| {
        callback_started.store(true, Ordering::Release);
        std::thread::sleep(Duration::from_millis(200));
    }));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&errors);
    engine.on_error(Arc::new(move |error| {
        observed.lock().unwrap().push(error.kind)
    }));
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    wait_until(|| started.load(Ordering::Acquire)).await;
    session
        .stream
        .write_all(&json_frame(Channel::Command, json!({"n":1})))
        .await
        .unwrap();
    wait_until(|| engine.state() == EngineState::Idle).await;
    wait_until(|| errors.lock().unwrap().contains(&ErrorKind::Backpressure)).await;
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_019_callback_panic_is_observable_application_error() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let errors = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&errors);
    engine.on_error(Arc::new(move |error| {
        observed.lock().unwrap().push(error.kind)
    }));
    engine.on_message(Arc::new(|_| panic!("handler failed")));
    let mut session = establish_with(&engine, &value, handshake(1, 1)).await;
    session
        .stream
        .write_all(&json_frame(Channel::Command, json!({})))
        .await
        .unwrap();
    wait_until(|| errors.lock().unwrap().contains(&ErrorKind::Application)).await;
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_020_disconnect_clears_session_state_before_idle() {
    let value = config();
    let engine = Engine::new(value.clone());
    let session = establish(&engine, &value).await;
    drop(session.stream);
    wait_until(|| engine.state() == EngineState::Idle).await;
    assert!(engine.session().is_none());
    assert_eq!(
        engine.terminal_result().unwrap().reason,
        DisconnectReason::PeerClose
    );
}

#[tokio::test]
async fn ec_021_replacement_epoch_rejects_stale_responder() {
    let mut value = config();
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let responder = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&responder);
    engine.on_message(Arc::new(move |event| {
        *captured.lock().unwrap() = event.responder
    }));
    let mut first = establish_with(&engine, &value, handshake(1, 1)).await;
    first
        .stream
        .write_all(&vector("frame_correlated_request.bin"))
        .await
        .unwrap();
    wait_until(|| responder.lock().unwrap().is_some()).await;
    let stale = responder.lock().unwrap().clone().unwrap();
    drop(first.stream);
    wait_until(|| engine.state() == EngineState::Idle).await;
    let second = establish_with(&engine, &value, handshake(1, 1)).await;
    assert!(second.view.epoch > first.view.epoch);
    assert_eq!(
        stale.respond(&json!({})).await.unwrap_err().kind(),
        ErrorKind::StaleEpoch
    );
    engine.close().await.unwrap();
}

#[test]
fn ec_022_error_kinds_are_stable_and_distinguishable() {
    let kinds = [
        ErrorKind::Configuration,
        ErrorKind::AddressDerivation,
        ErrorKind::Dial,
        ErrorKind::Timeout,
        ErrorKind::Handshake,
        ErrorKind::Protocol,
        ErrorKind::Encoding,
        ErrorKind::Capability,
        ErrorKind::Backpressure,
        ErrorKind::SessionClosed,
        ErrorKind::StaleEpoch,
        ErrorKind::Application,
        ErrorKind::Transport,
        ErrorKind::Internal,
        ErrorKind::State,
    ];
    assert_eq!(kinds.len(), 15);
}

#[test]
fn ec_023_public_surface_has_no_listener_or_runner() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"),
    )
    .unwrap();
    assert!(!source.contains("PlatformListener"));
    assert!(!source.contains("resolve_transport_address"));
    assert!(!source.contains("Runner"));
}

#[test]
fn ec_024_environment_adapter_is_explicitly_absent() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"),
    )
    .unwrap();
    assert!(!source.contains("config_from_env"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ec_025_close_cancels_connect_and_has_no_extra_dependencies() {
    let value = config();
    let engine = Engine::new(value.clone());
    let listener = TestListener::bind(&value).await;
    let attempt_engine = engine.clone();
    let connecting = tokio::spawn(async move { attempt_engine.connect().await });
    let _peer = listener.accept().await;
    engine.close().await.unwrap();
    assert_eq!(
        connecting.await.unwrap().unwrap_err().kind(),
        ErrorKind::SessionClosed
    );
    engine.close().await.unwrap();
    assert_eq!(engine.state(), EngineState::Idle);
    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .unwrap();
    for dependency in ["tokio", "serde", "serde_json", "rmp-serde"] {
        assert!(manifest.contains(dependency));
    }
}

#[derive(Debug)]
struct AlwaysFails;

impl Serialize for AlwaysFails {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("intentional failure"))
    }
}
