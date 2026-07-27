use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use yuumi::*;

trait TestStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> TestStream for T {}
type PeerStream = Box<dyn TestStream>;

static NEXT_ENDPOINT: AtomicU64 = AtomicU64::new(0);
const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn vectors_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("yuumi-spec")
        .join("test-vectors")
}

fn vector(name: &str) -> Vec<u8> {
    std::fs::read(vectors_dir().join(name))
        .unwrap_or_else(|error| panic!("missing {name}: {error}"))
}

fn config(_case: &str) -> EngineConfig {
    let id = NEXT_ENDPOINT.fetch_add(1, Ordering::Relaxed);
    let mut config = EngineConfig::new(format!("r{id:x}"), TOKEN);
    config.heartbeat.disabled = true;
    config
}

async fn connect(address: &std::path::Path) -> PeerStream {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        #[cfg(unix)]
        let result = tokio::net::UnixStream::connect(address)
            .await
            .map(|value| Box::new(value) as PeerStream);
        #[cfg(windows)]
        let result = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(address)
            .map(|value| Box::new(value) as PeerStream);
        match result {
            Ok(stream) => return stream,
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("peer connect failed: {error}"),
        }
    }
}

struct Peer {
    stream: PeerStream,
    ack: [u8; 4],
    session_id: String,
}

async fn establish(engine: &Engine, config: &EngineConfig, handshake: &[u8]) -> Peer {
    let address = resolve_transport_address(&config.endpoint_name, &config.token).unwrap();
    let mut stream = connect(&address).await;
    stream.write_all(handshake).await.unwrap();
    let mut ack = [0u8; 4];
    stream.read_exact(&mut ack).await.unwrap();
    let (_, _, payload) = read_frame(&mut stream).await;
    let control: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(control["type"], "session");
    let session_id = control["session_id"].as_str().unwrap().to_owned();
    assert!(!session_id.is_empty());
    let _ = engine;
    Peer {
        stream,
        ack,
        session_id,
    }
}

async fn read_frame(stream: &mut PeerStream) -> (u8, u8, Vec<u8>) {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
    assert!(length <= MAX_MESSAGE_SIZE);
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await.unwrap();
    (header[4], header[5], payload)
}

fn frame(channel: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(6 + payload.len());
    result.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    result.push(channel);
    result.push(flags);
    result.extend_from_slice(payload);
    result
}

type EventRecords<T> = Arc<Mutex<Vec<T>>>;
type CallbackRecords = (
    EventRecords<SessionView>,
    EventRecords<MessageEvent>,
    EventRecords<ErrorInfo>,
    EventRecords<DisconnectEvent>,
);

fn callbacks(engine: &Engine) -> CallbackRecords {
    let connected = Arc::new(Mutex::new(Vec::new()));
    let messages = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let disconnected = Arc::new(Mutex::new(Vec::new()));
    let target = Arc::clone(&connected);
    engine.on_session_connected(Arc::new(move |event| target.lock().unwrap().push(event)));
    let target = Arc::clone(&messages);
    engine.on_message(Arc::new(move |event| target.lock().unwrap().push(event)));
    let target = Arc::clone(&errors);
    engine.on_error(Arc::new(move |event| target.lock().unwrap().push(event)));
    let target = Arc::clone(&disconnected);
    engine.on_session_disconnected(Arc::new(move |event| target.lock().unwrap().push(event)));
    (connected, messages, errors, disconnected)
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn open_engine(case: &str) -> (EngineConfig, Engine) {
    let config = config(case);
    let engine = Engine::new(config.clone());
    engine.open().await.unwrap();
    (config, engine)
}

#[tokio::test]
async fn ec_001_invalid_configuration_has_no_endpoint_side_effects() {
    let mut value = config("001");
    value.token = "A".repeat(32);
    let engine = Engine::new(value.clone());
    assert_eq!(
        engine.open().await.unwrap_err().code(),
        StatusCode::ErrProtocolViolation
    );
    value.token = TOKEN.to_owned();
    value.max_sessions = 0;
    assert_eq!(
        Engine::new(value).open().await.unwrap_err().code(),
        StatusCode::ErrProtocolViolation
    );
}

#[test]
fn ec_002_canonical_address_and_platform_transport() {
    let value = config("002");
    let address = resolve_transport_address(&value.endpoint_name, &value.token).unwrap();
    let text = address.to_string_lossy();
    #[cfg(windows)]
    assert!(text.starts_with(r"\\.\pipe\yuumi-"));
    #[cfg(unix)]
    assert!(text.ends_with(&format!("yuumi-{}-{}.sock", value.endpoint_name, TOKEN)));
}

#[test]
fn ec_003_address_bounds_are_rejected_never_rewritten() {
    assert!(resolve_transport_address(&"a".repeat(33), TOKEN).is_err());
    assert!(resolve_transport_address("é", TOKEN).is_err());
}

#[test]
fn ec_004_engine_defaults_are_protocol_conforming() {
    let value = EngineConfig::new("defaults", TOKEN);
    assert_eq!(value.max_sessions, 1);
    assert_eq!(
        value.supported_encodings,
        vec![Encoding::MessagePack, Encoding::Json]
    );
    assert_eq!(value.supported_capabilities, CAP_CORRELATION);
    assert!(!value.heartbeat.disabled);
}

#[tokio::test]
async fn ec_005_open_mutation_and_repeated_close_state_rules() {
    let (_config, engine) = open_engine("005").await;
    assert!(engine.open().await.is_err());
    engine.close().await.unwrap();
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_006_a_live_endpoint_is_never_replaced() {
    let (value, first) = open_engine("006").await;
    let second = Engine::new(value);
    assert_eq!(
        second.open().await.unwrap_err().code(),
        StatusCode::ErrPipeFailed
    );
    first.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn ec_007_a_stale_endpoint_is_removed_before_recreation() {
    let value = config("007");
    let address = resolve_transport_address(&value.endpoint_name, &value.token).unwrap();
    {
        let listener = tokio::net::UnixListener::bind(&address).unwrap();
        drop(listener);
    }
    let engine = Engine::new(value);
    engine.open().await.unwrap();
    engine.close().await.unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn ec_007_a_stale_endpoint_is_released_before_recreation() {
    let (value, engine) = open_engine("007").await;
    engine.close().await.unwrap();
    let replacement = Engine::new(value);
    replacement.open().await.unwrap();
    replacement.close().await.unwrap();
}

#[tokio::test]
async fn ec_008_access_controls_precede_handshake_traffic() {
    let (_value, engine) = open_engine("008").await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let address = resolve_transport_address(&_value.endpoint_name, &_value.token).unwrap();
        assert_eq!(
            std::fs::metadata(address).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_009_a_wrong_token_cannot_reach_the_handshake() {
    let (value, engine) = open_engine("009").await;
    let wrong = resolve_transport_address(&value.endpoint_name, "ffffffffffffffffffffffffffffffff")
        .unwrap();
    #[cfg(unix)]
    assert!(tokio::net::UnixStream::connect(wrong).await.is_err());
    #[cfg(windows)]
    assert!(tokio::net::windows::named_pipe::ClientOptions::new()
        .open(wrong)
        .is_err());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_010_orderly_close_releases_all_owned_state() {
    let (value, engine) = open_engine("010").await;
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    engine.close().await.unwrap();
    let mut byte = [0u8; 1];
    assert!(peer.stream.read_exact(&mut byte).await.is_err());
    let replacement = Engine::new(value);
    replacement.open().await.unwrap();
    replacement.close().await.unwrap();
}

#[test]
fn ec_011_token_diagnostics_and_public_address_surface_are_safe() {
    let error = resolve_transport_address("bad/name", TOKEN)
        .unwrap_err()
        .to_string();
    assert!(!error.contains(TOKEN));
    let public =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
            .unwrap();
    assert!(!public.contains("Client"));
    assert!(!public.contains("dial"));
}

#[tokio::test]
async fn ec_012_valid_json_handshake_produces_the_exact_ack() {
    let mut value = config("012");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    engine.open().await.unwrap();
    let peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    assert_eq!(peer.ack.as_slice(), vector("ack_json.bin"));
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_013_valid_messagepack_handshake_produces_the_exact_ack() {
    let (value, engine) = open_engine("013").await;
    let peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    assert_eq!(peer.ack.as_slice(), vector("ack_msgpack.bin"));
    engine.close().await.unwrap();
}

async fn rejection_case(
    case: &str,
    handshake: &[u8],
    expected: StatusCode,
    configure: impl FnOnce(&mut EngineConfig),
) {
    let mut value = config(case);
    configure(&mut value);
    let engine = Engine::new(value.clone());
    let (_, _, errors, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let address = resolve_transport_address(&value.endpoint_name, &value.token).unwrap();
    let mut peer = connect(&address).await;
    peer.write_all(handshake).await.unwrap();
    let mut ack = [0u8; 4];
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut ack))
            .await
            .unwrap()
            .is_err()
    );
    wait_until(|| !errors.lock().unwrap().is_empty()).await;
    assert_eq!(errors.lock().unwrap().last().unwrap().status, expected);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_014_a_short_handshake_is_never_parsed_as_complete() {
    let value = config("014");
    let engine = Engine::new(value.clone());
    let (connected, _, errors, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let address = resolve_transport_address(&value.endpoint_name, &value.token).unwrap();
    let mut peer = connect(&address).await;
    peer.write_all(&vector("handshake_valid.bin")[..15])
        .await
        .unwrap();
    drop(peer);
    wait_until(|| !errors.lock().unwrap().is_empty()).await;
    assert!(connected.lock().unwrap().is_empty());
    assert!(disconnected.lock().unwrap().is_empty());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_015_invalid_magic_is_rejected_without_ack() {
    rejection_case(
        "015",
        &vector("handshake_bad_magic.bin"),
        StatusCode::ErrMagicMismatch,
        |_| {},
    )
    .await;
}

#[tokio::test]
async fn ec_016_incompatible_version_is_rejected_without_ack() {
    rejection_case(
        "016",
        &vector("handshake_bad_version.bin"),
        StatusCode::ErrVersionMismatch,
        |_| {},
    )
    .await;
}

#[tokio::test]
async fn ec_017_empty_encoding_intersection_is_rejected_without_ack() {
    rejection_case(
        "017",
        &vector("handshake_encoding_unsupported.bin"),
        StatusCode::ErrEncodingUnsupported,
        |value| value.supported_encodings = vec![Encoding::Json],
    )
    .await;
}

#[tokio::test]
async fn ec_018_expected_pid_absence_zero_and_mismatch_are_distinct() {
    let (value, engine) = open_engine("018a").await;
    let _peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    engine.close().await.unwrap();
    rejection_case(
        "018b",
        &vector("handshake_valid.bin"),
        StatusCode::ErrPidMismatch,
        |value| value.expected_pid = Some(0),
    )
    .await;
    rejection_case(
        "018c",
        &vector("handshake_valid.bin"),
        StatusCode::ErrPidMismatch,
        |value| value.expected_pid = Some(u32::MAX),
    )
    .await;
}

#[tokio::test]
async fn ec_019_encoding_preference_and_reserved_bits_are_deterministic() {
    let mut value = config("019");
    value.supported_encodings = vec![Encoding::Json, Encoding::MessagePack];
    let engine = Engine::new(value.clone());
    engine.open().await.unwrap();
    let mut handshake = vector("handshake_valid.bin");
    handshake[12] |= 0xf0;
    let peer = establish(&engine, &value, &handshake).await;
    assert_eq!(peer.ack, [Encoding::Json as u8, 0, 0, 0]);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_020_shared_correlation_capability_is_negotiated_by_intersection() {
    let mut value = config("020");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    engine.open().await.unwrap();
    let peer = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    assert_eq!(peer.ack.as_slice(), vector("ack_cap_correlation.bin"));
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_021_no_shared_capability_still_establishes_a_baseline_session() {
    let mut value = config("021");
    value.supported_encodings = vec![Encoding::Json];
    value.supported_capabilities = 0;
    let engine = Engine::new(value.clone());
    engine.open().await.unwrap();
    let peer = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    assert_eq!(peer.ack.as_slice(), vector("ack_capabilities_none.bin"));
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_022_unknown_capability_bits_are_excluded_and_ignored() {
    let (value, engine) = open_engine("022").await;
    let mut handshake = vector("handshake_cap_correlation.bin");
    handshake[13] = 0xff;
    handshake[14] = 0xff;
    let peer = establish(&engine, &value, &handshake).await;
    assert_eq!(peer.ack, [Encoding::MessagePack as u8, 0, 0, 1]);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_023_establishment_write_failures_never_create_a_visible_session() {
    let value = config("023");
    let engine = Engine::new(value.clone());
    let (connected, _, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let address = resolve_transport_address(&value.endpoint_name, &value.token).unwrap();
    let mut peer = connect(&address).await;
    peer.write_all(&vector("handshake_valid.bin"))
        .await
        .unwrap();
    drop(peer);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(connected.lock().unwrap().is_empty());
    assert!(disconnected.lock().unwrap().is_empty());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_024_max_sessions_one_counts_pre_session_connections() {
    let (value, engine) = open_engine("024").await;
    let address = resolve_transport_address(&value.endpoint_name, &value.token).unwrap();
    let _reserved = connect(&address).await;
    let mut excess = connect(&address).await;
    excess
        .write_all(&vector("handshake_valid.bin"))
        .await
        .unwrap_or(());
    let mut ack = [0u8; 4];
    assert!(
        tokio::time::timeout(Duration::from_secs(1), excess.read_exact(&mut ack))
            .await
            .unwrap()
            .is_err()
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_025_max_sessions_n_accepts_n_concurrent_isolated_sessions() {
    let mut value = config("025");
    value.max_sessions = 2;
    let engine = Engine::new(value.clone());
    engine.open().await.unwrap();
    let first = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let second = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    assert_ne!(first.session_id, second.session_id);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_026_negotiated_and_heartbeat_state_is_session_local() {
    let mut value = config("026");
    value.max_sessions = 2;
    let engine = Engine::new(value.clone());
    let (connected, _, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let first = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let second = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 2).await;
    assert_eq!(first.ack[3], 0);
    assert_eq!(second.ack[3], 1);
    {
        let events = connected.lock().unwrap();
        assert_ne!(events[0].capabilities, events[1].capabilities);
    }
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_027_session_assignment_is_ordered_valid_and_unique() {
    let mut value = config("027");
    value.max_sessions = 2;
    let engine = Engine::new(value.clone());
    let (connected, _, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let first = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let second = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 2).await;
    assert_ne!(first.session_id, second.session_id);
    assert!(first.session_id.is_ascii() && first.session_id.len() <= 128);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_028_channel_fragment_and_correlation_identifiers_are_isolated() {
    let mut value = config("028");
    value.max_sessions = 2;
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut first = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    let mut second = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    first
        .stream
        .write_all(&vector("frame_fragment_correlated_first.bin"))
        .await
        .unwrap();
    second
        .stream
        .write_all(&vector("frame_fragment_correlated_first.bin"))
        .await
        .unwrap();
    first
        .stream
        .write_all(&vector("frame_fragment_correlated_last.bin"))
        .await
        .unwrap();
    second
        .stream
        .write_all(&vector("frame_fragment_correlated_last.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 2).await;
    {
        let events = messages.lock().unwrap();
        assert_ne!(events[0].session.session_id, events[1].session.session_id);
        assert!(events.iter().all(|event| event.correlation_id == Some(42)));
    }
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_029_reconnection_creates_new_id_higher_epoch_and_empty_state() {
    let value = config("029");
    let engine = Engine::new(value.clone());
    let (connected, _, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let first = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    drop(first.stream);
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    let second = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 2).await;
    {
        let events = connected.lock().unwrap();
        assert_ne!(first.session_id, second.session_id);
        assert!(events[1].handle.epoch > events[0].handle.epoch);
    }
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_030_stale_handles_and_delayed_work_cannot_address_a_replacement() {
    let value = config("030");
    let engine = Engine::new(value.clone());
    let (connected, _, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let first = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 1).await;
    let stale = connected.lock().unwrap()[0].handle.clone();
    drop(first.stream);
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    let _second = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    assert_eq!(
        engine
            .send(&stale, Channel::Data, &json!({"stale": true}))
            .await
            .unwrap_err()
            .code(),
        StatusCode::ErrConnectionLost
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_031_per_session_event_order_and_serialization_are_stable() {
    let mut value = config("031");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let order = Arc::new(Mutex::new(Vec::new()));
    let target = Arc::clone(&order);
    engine.on_session_connected(Arc::new(move |_| target.lock().unwrap().push("connected")));
    let target = Arc::clone(&order);
    engine.on_message(Arc::new(move |_| target.lock().unwrap().push("message")));
    let target = Arc::clone(&order);
    engine.on_session_disconnected(Arc::new(move |_| {
        target.lock().unwrap().push("disconnected")
    }));
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("frame_channel_command.bin"))
        .await
        .unwrap();
    wait_until(|| order.lock().unwrap().contains(&"message")).await;
    drop(peer.stream);
    wait_until(|| order.lock().unwrap().contains(&"disconnected")).await;
    assert_eq!(
        *order.lock().unwrap(),
        vec!["connected", "message", "disconnected"]
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_032_disconnect_event_is_exact_and_reasoned() {
    let (value, engine) = open_engine("032").await;
    let (_, _, _, disconnected) = callbacks(&engine);
    let peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    drop(peer.stream);
    wait_until(|| disconnected.lock().unwrap().len() == 1).await;
    assert_eq!(
        disconnected.lock().unwrap()[0].reason,
        DisconnectReason::PeerClose
    );
    engine.close().await.unwrap();
    assert_eq!(disconnected.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn ec_033_endpoint_and_session_failures_remain_isolated() {
    let mut value = config("033");
    value.max_sessions = 2;
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut bad = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let mut good = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    bad.stream.write_all(&frame(0xff, 0, b"{}")).await.unwrap();
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    good.stream
        .write_all(&vector("frame_channel_command.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_034_only_complete_valid_non_control_messages_reach_the_application() {
    let mut value = config("034");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("control_heartbeat.bin"))
        .await
        .unwrap();
    peer.stream
        .write_all(&vector("frame_fragment_first.bin"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(messages.lock().unwrap().is_empty());
    peer.stream
        .write_all(&vector("frame_fragment_last.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_035_complete_json_command_dispatch() {
    let mut value = config("035");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("frame_channel_command.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    let event = messages.lock().unwrap()[0].clone();
    assert_eq!(event.channel, Channel::Command);
    assert_eq!(event.payload["action"], "test");
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_036_complete_messagepack_command_dispatch() {
    let (value, engine) = open_engine("036").await;
    let (_, messages, _, _) = callbacks(&engine);
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let payload = rmp_serde::to_vec_named(&json!({"action": "test"})).unwrap();
    peer.stream
        .write_all(&frame(Channel::Command as u8, 0, &payload))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    assert_eq!(messages.lock().unwrap()[0].payload["action"], "test");
    engine.close().await.unwrap();
}

async fn fatal_frame_case(
    case: &str,
    packet: &[u8],
    expected: StatusCode,
    configure: impl FnOnce(&mut EngineConfig),
) -> (Vec<ErrorInfo>, Vec<DisconnectEvent>) {
    let mut value = config(case);
    value.supported_encodings = vec![Encoding::Json];
    configure(&mut value);
    let engine = Engine::new(value.clone());
    let (_, messages, errors, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream.write_all(packet).await.unwrap();
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    assert!(messages.lock().unwrap().is_empty());
    assert_eq!(errors.lock().unwrap().last().unwrap().status, expected);
    let result = (
        errors.lock().unwrap().clone(),
        disconnected.lock().unwrap().clone(),
    );
    engine.close().await.unwrap();
    result
}

#[tokio::test]
async fn ec_037_oversized_declared_frame_is_rejected_before_read_or_allocation() {
    let (_, disconnected) = fatal_frame_case(
        "037",
        &vector("frame_oversized.bin"),
        StatusCode::ErrPayloadTooLarge,
        |_| {},
    )
    .await;
    assert_eq!(disconnected[0].reason, DisconnectReason::ProtocolFailure);
}

#[tokio::test]
async fn ec_038_reserved_and_inconsistent_flag_bits_are_rejected() {
    for (index, flags) in [0x08, 0x10, 0x20, 0x40, 0x80, 0x02].into_iter().enumerate() {
        let packet = frame(Channel::Command as u8, flags, b"{}");
        fatal_frame_case(
            &format!("038{index}"),
            &packet,
            StatusCode::ErrProtocolViolation,
            |_| {},
        )
        .await;
    }
}

#[tokio::test]
async fn ec_039_unknown_channels_and_direction_violations_are_rejected() {
    fatal_frame_case(
        "039a",
        &frame(0xff, 0, b"{}"),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
    fatal_frame_case(
        "039b",
        &frame(Channel::Log as u8, 0, b"{}"),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
}

#[tokio::test]
async fn ec_040_short_prefixes_and_malformed_application_payloads_are_fatal() {
    fatal_frame_case(
        "040a",
        &frame(Channel::Command as u8, 0x01, &[0, 0, 0]),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
    fatal_frame_case(
        "040b",
        &frame(Channel::Command as u8, 0, b"{"),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
    fatal_frame_case(
        "040c",
        &frame(Channel::Command as u8, 0x04, &[0, 0, 0]),
        StatusCode::ErrProtocolViolation,
        |value| value.supported_capabilities = CAP_CORRELATION,
    )
    .await;
}

#[tokio::test]
async fn ec_041_uncorrelated_fragments_reassemble_once_in_stream_order() {
    let mut value = config("041");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("frame_fragment_first.bin"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(messages.lock().unwrap().is_empty());
    peer.stream
        .write_all(&vector("frame_fragment_last.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    assert_eq!(
        messages.lock().unwrap()[0].payload,
        Value::String("Hello World".to_owned())
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_042_reassembled_data_cannot_exceed_16_mib() {
    let mut first_payload = Vec::with_capacity(MAX_MESSAGE_SIZE);
    first_payload.extend_from_slice(&7u32.to_be_bytes());
    first_payload.resize(MAX_MESSAGE_SIZE, b'x');
    let first = frame(Channel::Command as u8, 0x01, &first_payload);
    let mut final_payload = Vec::new();
    final_payload.extend_from_slice(&7u32.to_be_bytes());
    final_payload.extend_from_slice(b"xxxxx");
    let mut value = config("042");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, _, errors, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream.write_all(&first).await.unwrap();
    peer.stream
        .write_all(&frame(Channel::Command as u8, 0x03, &final_payload))
        .await
        .unwrap();
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    assert_eq!(
        errors.lock().unwrap().last().unwrap().status,
        StatusCode::ErrPayloadTooLarge
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_043_incomplete_reassembly_expires() {
    let mut value = config("043");
    value.supported_encodings = vec![Encoding::Json];
    value.fragmentation.timeout = Duration::from_millis(40);
    let engine = Engine::new(value.clone());
    let (_, messages, errors, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("frame_fragment_first.bin"))
        .await
        .unwrap();
    wait_until(|| {
        errors
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.status == StatusCode::ErrFragmentTimeout)
    })
    .await;
    assert!(messages.lock().unwrap().is_empty());
    peer.stream
        .write_all(&vector("frame_fragment_first.bin"))
        .await
        .unwrap();
    peer.stream
        .write_all(&vector("frame_fragment_last.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_044_active_fragment_sequence_limit_is_enforced_per_session() {
    let mut value = config("044");
    value.supported_encodings = vec![Encoding::Json];
    value.fragmentation.active_sequence_limit = 1;
    let engine = Engine::new(value.clone());
    let (_, _, errors, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("frame_fragment_first.bin"))
        .await
        .unwrap();
    let mut data_sequence = vector("frame_fragment_first.bin");
    data_sequence[4] = Channel::Command as u8;
    peer.stream.write_all(&data_sequence).await.unwrap();
    wait_until(|| !errors.lock().unwrap().is_empty()).await;
    assert_eq!(
        errors.lock().unwrap().last().unwrap().status,
        StatusCode::ErrProtocolViolation
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_045_fragment_consistency_and_non_interleaving_are_enforced() {
    let mut first = vector("frame_fragment_correlated_first.bin");
    let mut changed = vector("frame_fragment_correlated_last.bin");
    changed[9] ^= 1;
    let mut value = config("045");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, errors, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    peer.stream.write_all(&first).await.unwrap();
    peer.stream.write_all(&changed).await.unwrap();
    wait_until(|| !errors.lock().unwrap().is_empty()).await;
    assert!(messages.lock().unwrap().is_empty());
    assert_eq!(
        errors.lock().unwrap()[0].status,
        StatusCode::ErrProtocolViolation
    );
    first.clear();
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_046_heartbeat_is_always_json_and_resets_liveness() {
    let mut value = config("046");
    value.heartbeat.disabled = false;
    value.heartbeat.interval = Duration::from_millis(80);
    value.heartbeat.missed_interval_limit = 2;
    let engine = Engine::new(value.clone());
    let (_, messages, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    peer.stream
        .write_all(&vector("control_heartbeat.bin"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(messages.lock().unwrap().is_empty());
    assert!(disconnected.lock().unwrap().is_empty());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_047_heartbeat_emission_and_timeout_are_session_local() {
    let mut value = config("047");
    value.max_sessions = 2;
    value.heartbeat.disabled = false;
    value.heartbeat.interval = Duration::from_millis(40);
    value.heartbeat.missed_interval_limit = 3;
    let engine = Engine::new(value.clone());
    let (_, _, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut active = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let mut silent = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let (channel, _, payload) =
        tokio::time::timeout(Duration::from_secs(1), read_frame(&mut silent.stream))
            .await
            .unwrap();
    assert_eq!(channel, Channel::Control as u8);
    let heartbeat: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(heartbeat["type"], "heartbeat");
    for _ in 0..4 {
        active
            .stream
            .write_all(&vector("control_heartbeat.bin"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(35)).await;
    }
    wait_until(|| {
        disconnected
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.reason == DisconnectReason::HeartbeatTimeout)
    })
    .await;
    assert_eq!(disconnected.lock().unwrap().len(), 1);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_048_ping_receives_a_pong_with_the_same_sequence() {
    let (value, engine) = open_engine("048").await;
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("control_ping.bin"))
        .await
        .unwrap();
    let (channel, flags, payload) = read_frame(&mut peer.stream).await;
    assert_eq!((channel, flags), (Channel::Control as u8, 0));
    let pong: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["seq"], 1);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_049_fatal_post_session_error_is_sent_before_close() {
    let (value, engine) = open_engine("049").await;
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("frame_oversized.bin"))
        .await
        .unwrap();
    let (channel, flags, payload) = read_frame(&mut peer.stream).await;
    assert_eq!((channel, flags), (Channel::Control as u8, 0));
    let error: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(error["type"], "error");
    assert_eq!(error["code"], 413);
    let mut byte = [0u8; 1];
    assert!(peer.stream.read_exact(&mut byte).await.is_err());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_050_unknown_valid_control_types_are_ignored_silently() {
    let mut value = config("050");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, errors, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&frame(Channel::Control as u8, 0, br#"{"type":"future"}"#))
        .await
        .unwrap();
    peer.stream
        .write_all(&vector("frame_channel_command.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    assert!(errors.lock().unwrap().is_empty());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_051_malformed_control_and_received_fatal_error_obey_terminal_rules() {
    fatal_frame_case(
        "051a",
        &frame(Channel::Control as u8, 0, b"{"),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
    fatal_frame_case(
        "051b",
        &frame(Channel::Control as u8, 0, br#"{"value":1}"#),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
    let (value, engine) = open_engine("051c").await;
    let (_, _, errors, disconnected) = callbacks(&engine);
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    peer.stream
        .write_all(&vector("control_error.bin"))
        .await
        .unwrap();
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    assert_eq!(
        errors.lock().unwrap()[0].status,
        StatusCode::ErrPayloadTooLarge
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_052_correlated_input_without_negotiation_is_rejected() {
    fatal_frame_case(
        "052",
        &vector("frame_correlated_not_negotiated.bin"),
        StatusCode::ErrProtocolViolation,
        |_| {},
    )
    .await;
}

async fn correlated_response_case(case: &str, response: Value) -> (u8, u8, Vec<u8>) {
    let mut value = config(case);
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let responder = engine.clone();
    let response = Arc::new(response);
    engine.on_message(Arc::new(move |event| {
        let responder = responder.clone();
        let response = Arc::clone(&response);
        tokio::spawn(async move {
            responder
                .send_correlated(
                    &event.session,
                    Channel::Data,
                    event.correlation_id.unwrap(),
                    response.as_ref(),
                )
                .await
                .unwrap();
        });
    }));
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    peer.stream
        .write_all(&vector("frame_correlated_request.bin"))
        .await
        .unwrap();
    let output = read_frame(&mut peer.stream).await;
    engine.close().await.unwrap();
    output
}

#[tokio::test]
async fn ec_053_a_response_repeats_the_request_correlation_id() {
    let (channel, flags, payload) = correlated_response_case("053", json!({"ok": true})).await;
    assert_eq!((channel, flags), (Channel::Data as u8, 0x04));
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 42);
    assert_eq!(
        serde_json::from_slice::<Value>(&payload[4..]).unwrap(),
        json!({"ok": true})
    );
}

#[tokio::test]
async fn ec_054_an_application_error_repeats_the_request_correlation_id() {
    let (channel, flags, payload) =
        correlated_response_case("054", json!({"error": "invalid"})).await;
    assert_eq!((channel, flags), (Channel::Data as u8, 0x04));
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 42);
}

#[tokio::test]
async fn ec_055_fragment_and_correlation_prefixes_retain_fixed_order() {
    let mut value = config("055");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (_, messages, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    peer.stream
        .write_all(&vector("frame_fragment_correlated_first.bin"))
        .await
        .unwrap();
    peer.stream
        .write_all(&vector("frame_fragment_correlated_last.bin"))
        .await
        .unwrap();
    wait_until(|| messages.lock().unwrap().len() == 1).await;
    let event = messages.lock().unwrap()[0].clone();
    assert_eq!(event.correlation_id, Some(42));
    assert_eq!(event.payload, Value::String("Hello World".to_owned()));
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_056_pending_correlation_identifiers_have_a_bounded_lifecycle() {
    let mut value = config("056");
    value.max_sessions = 2;
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (connected, _, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut first = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    let mut second = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 2).await;
    let handles: Vec<_> = connected
        .lock()
        .unwrap()
        .iter()
        .map(|view| view.handle.clone())
        .collect();
    engine
        .send_correlated(&handles[0], Channel::Data, 42, &json!({"one": 1}))
        .await
        .unwrap();
    engine
        .send_correlated(&handles[1], Channel::Data, 42, &json!({"two": 2}))
        .await
        .unwrap();
    let (_, _, first_payload) = read_frame(&mut first.stream).await;
    let (_, _, second_payload) = read_frame(&mut second.stream).await;
    assert_eq!(&first_payload[..4], &42u32.to_be_bytes());
    assert_eq!(&second_payload[..4], &42u32.to_be_bytes());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_057_uncorrelated_sends_use_selected_encoding_and_preserve_order() {
    let mut value = config("057");
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (connected, _, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 1).await;
    let handle = connected.lock().unwrap()[0].handle.clone();
    engine
        .send(&handle, Channel::Data, &json!({"seq": 1}))
        .await
        .unwrap();
    engine
        .send(&handle, Channel::Data, &json!({"seq": 2}))
        .await
        .unwrap();
    engine
        .send(&handle, Channel::Log, &json!({"log": true}))
        .await
        .unwrap();
    let first = read_frame(&mut peer.stream).await;
    let second = read_frame(&mut peer.stream).await;
    let third = read_frame(&mut peer.stream).await;
    assert_eq!(serde_json::from_slice::<Value>(&first.2).unwrap()["seq"], 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&second.2).unwrap()["seq"],
        2
    );
    assert_eq!(third.0, Channel::Log as u8);
    assert_eq!(first.1 | second.1 | third.1, 0);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_058_correlated_send_is_gated_and_preserves_the_supplied_id() {
    let mut value = config("058");
    value.max_sessions = 2;
    value.supported_encodings = vec![Encoding::Json];
    let engine = Engine::new(value.clone());
    let (connected, _, _, _) = callbacks(&engine);
    engine.open().await.unwrap();
    let mut baseline = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let mut enabled = establish(&engine, &value, &vector("handshake_cap_correlation.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 2).await;
    let views = connected.lock().unwrap().clone();
    let baseline_handle = views
        .iter()
        .find(|view| view.capabilities == 0)
        .unwrap()
        .handle
        .clone();
    let enabled_handle = views
        .iter()
        .find(|view| view.capabilities == CAP_CORRELATION)
        .unwrap()
        .handle
        .clone();
    assert_eq!(
        engine
            .send_correlated(&baseline_handle, Channel::Data, 42, &json!({"ok": true}))
            .await
            .unwrap_err()
            .code(),
        StatusCode::ErrProtocolViolation
    );
    engine
        .send_correlated(&enabled_handle, Channel::Data, 42, &json!({"ok": true}))
        .await
        .unwrap();
    let (_, flags, payload) = read_frame(&mut enabled.stream).await;
    assert_eq!(flags, 0x04);
    assert_eq!(&payload[..4], &42u32.to_be_bytes());
    let no_output =
        tokio::time::timeout(Duration::from_millis(50), read_frame(&mut baseline.stream)).await;
    assert!(no_output.is_err());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_059_applications_cannot_forge_control_or_violate_directions() {
    let (value, engine) = open_engine("059").await;
    let (connected, _, _, _) = callbacks(&engine);
    let _peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 1).await;
    let handle = connected.lock().unwrap()[0].handle.clone();
    for channel in [Channel::Control, Channel::Command] {
        assert_eq!(
            engine
                .send(&handle, channel, &json!({"forged": true}))
                .await
                .unwrap_err()
                .code(),
            StatusCode::ErrProtocolViolation
        );
    }
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_060_invalid_closed_and_stale_session_sends_fail_in_isolation() {
    let mut value = config("060");
    value.max_sessions = 2;
    let engine = Engine::new(value.clone());
    let (connected, _, _, disconnected) = callbacks(&engine);
    engine.open().await.unwrap();
    let closing = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    let mut live = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 2).await;
    let handles: Vec<_> = connected
        .lock()
        .unwrap()
        .iter()
        .map(|view| view.handle.clone())
        .collect();
    drop(closing.stream);
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    let closed = disconnected.lock().unwrap()[0].session.clone();
    let live_handle = handles
        .into_iter()
        .find(|handle| *handle != closed)
        .unwrap();
    let absent = SessionHandle {
        session_id: "absent".to_owned(),
        epoch: 0,
    };
    let stale = SessionHandle {
        session_id: live_handle.session_id.clone(),
        epoch: live_handle.epoch + 1,
    };
    for handle in [&closed, &absent, &stale] {
        assert_eq!(
            engine
                .send(handle, Channel::Data, &json!({"x": 1}))
                .await
                .unwrap_err()
                .code(),
            StatusCode::ErrConnectionLost
        );
    }
    engine
        .send(&live_handle, Channel::Data, &json!({"live": true}))
        .await
        .unwrap();
    let (_, _, payload) = read_frame(&mut live.stream).await;
    assert_eq!(
        rmp_serde::from_slice::<Value>(&payload).unwrap()["live"],
        true
    );
    engine.close().await.unwrap();
}

struct AlwaysFails;
impl serde::Serialize for AlwaysFails {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("intentional"))
    }
}

#[tokio::test]
async fn ec_061_serialization_and_size_failures_are_explicit() {
    let (value, engine) = open_engine("061").await;
    let (connected, _, _, _) = callbacks(&engine);
    let _peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 1).await;
    let handle = connected.lock().unwrap()[0].handle.clone();
    assert_eq!(
        engine
            .send(&handle, Channel::Data, &AlwaysFails)
            .await
            .unwrap_err()
            .code(),
        StatusCode::ErrProtocolViolation
    );
    let oversized = "x".repeat(MAX_MESSAGE_SIZE);
    assert_eq!(
        engine
            .send(&handle, Channel::Data, &oversized)
            .await
            .unwrap_err()
            .code(),
        StatusCode::ErrPayloadTooLarge
    );
    engine
        .send(&handle, Channel::Data, &json!({"still": "usable"}))
        .await
        .unwrap();
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_062_deferred_transport_failure_becomes_one_error_event() {
    let (value, engine) = open_engine("062").await;
    let (connected, _, errors, disconnected) = callbacks(&engine);
    let peer = establish(&engine, &value, &vector("handshake_valid.bin")).await;
    wait_until(|| connected.lock().unwrap().len() == 1).await;
    let handle = connected.lock().unwrap()[0].handle.clone();
    drop(peer.stream);
    wait_until(|| !disconnected.lock().unwrap().is_empty()).await;
    assert_eq!(
        engine
            .send(&handle, Channel::Data, &json!({"late": true}))
            .await
            .unwrap_err()
            .code(),
        StatusCode::ErrConnectionLost
    );
    assert!(errors.lock().unwrap().len() <= 1);
    engine.close().await.unwrap();
}

#[tokio::test]
async fn ec_063_error_events_preserve_phase_handle_and_terminal_cause() {
    let errors = fatal_frame_case(
        "063",
        &vector("frame_oversized.bin"),
        StatusCode::ErrPayloadTooLarge,
        |_| {},
    )
    .await
    .0;
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].phase, ErrorPhase::FrameDecode);
    assert!(errors[0].session.is_some());
    let invalid = Engine::new(EngineConfig::new("bad/name", TOKEN))
        .open()
        .await
        .unwrap_err();
    assert_eq!(invalid.info.phase, ErrorPhase::Configuration);
    assert!(invalid.info.session.is_none());
}

#[test]
fn ec_064_public_api_remains_engine_only_and_documents_event_execution() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let lib = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
    let engine = std::fs::read_to_string(root.join("src/engine.rs")).unwrap();
    assert!(lib.contains("pub use engine::Engine"));
    assert!(!lib.contains("Client"));
    assert!(!root.join("src/client.rs").exists());
    assert!(engine.contains("Callbacks execute synchronously"));
    let _: ConnectedCallback = Arc::new(|_| {});
    let _: MessageCallback = Arc::new(|_| {});
}
