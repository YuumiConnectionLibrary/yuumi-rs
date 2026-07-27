use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::protocol::{
    build_control_frame, build_frame, decode_control, decode_payload, encode_payload,
    serialization_error, ProtocolFailure, FLAG_CORRELATED, FLAG_FRAGMENT, FLAG_LAST_FRAGMENT,
    KNOWN_FLAGS,
};
use crate::transport::{resolve_transport_address, BoxStream, PlatformListener};
use crate::types::*;

#[derive(Default, Clone)]
struct Callbacks {
    connected: Option<ConnectedCallback>,
    message: Option<MessageCallback>,
    error: Option<ErrorCallback>,
    disconnected: Option<DisconnectedCallback>,
}

#[derive(Default)]
struct Lifecycle {
    open: bool,
    accept_task: Option<JoinHandle<()>>,
    maintenance_task: Option<JoinHandle<()>>,
    address: Option<std::path::PathBuf>,
}

struct Connection {
    writer: AsyncMutex<WriteHalf<BoxStream>>,
    close: Notify,
    closing: AtomicBool,
}

impl Connection {
    fn request_close(&self) {
        if !self.closing.swap(true, Ordering::AcqRel) {
            self.close.notify_one();
        }
    }
}

struct Fragment {
    id: u32,
    correlation_id: Option<u32>,
    data: Vec<u8>,
    deadline: Instant,
}

struct Session {
    connection: Arc<Connection>,
    handle: SessionHandle,
    encoding: Encoding,
    capabilities: u32,
    fragments: Mutex<HashMap<Channel, Fragment>>,
    last_activity: Mutex<Instant>,
    last_heartbeat: Mutex<Instant>,
    event_gate: Mutex<()>,
    terminal_reported: AtomicBool,
    disconnect_reason: Mutex<DisconnectReason>,
}

struct Inner {
    config: EngineConfig,
    lifecycle: AsyncMutex<Lifecycle>,
    callbacks: RwLock<Callbacks>,
    accepting: AtomicBool,
    capacity: Arc<Semaphore>,
    connections: Mutex<HashMap<u64, Arc<Connection>>>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    next_connection: AtomicU64,
    next_epoch: AtomicU64,
}

#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let capacity = config.max_sessions.max(1);
        Self {
            inner: Arc::new(Inner {
                config,
                lifecycle: AsyncMutex::new(Lifecycle::default()),
                callbacks: RwLock::new(Callbacks::default()),
                accepting: AtomicBool::new(false),
                capacity: Arc::new(Semaphore::new(capacity)),
                connections: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                workers: Mutex::new(Vec::new()),
                next_connection: AtomicU64::new(0),
                next_epoch: AtomicU64::new(0),
            }),
        }
    }

    pub fn on_session_connected(&self, callback: ConnectedCallback) {
        write_lock(&self.inner.callbacks).connected = Some(callback);
    }
    pub fn on_message(&self, callback: MessageCallback) {
        write_lock(&self.inner.callbacks).message = Some(callback);
    }
    pub fn on_error(&self, callback: ErrorCallback) {
        write_lock(&self.inner.callbacks).error = Some(callback);
    }
    pub fn on_session_disconnected(&self, callback: DisconnectedCallback) {
        write_lock(&self.inner.callbacks).disconnected = Some(callback);
    }

    pub async fn open(&self) -> Result<()> {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        if lifecycle.open {
            return Err(EngineError::new(
                ErrorCategory::Endpoint,
                StatusCode::ErrPipeFailed,
                ErrorPhase::EndpointOpen,
                "engine is already open",
                None,
            ));
        }
        validate_config(&self.inner.config)?;
        let address =
            resolve_transport_address(&self.inner.config.endpoint_name, &self.inner.config.token)?;
        let listener = PlatformListener::open(&address).await?;
        self.inner.accepting.store(true, Ordering::Release);
        let accept_inner = Arc::clone(&self.inner);
        let accept_task = tokio::spawn(async move { accept_loop(accept_inner, listener).await });
        let maintenance_inner = Arc::clone(&self.inner);
        let maintenance_task =
            tokio::spawn(async move { maintenance_loop(maintenance_inner).await });
        lifecycle.address = Some(address);
        lifecycle.accept_task = Some(accept_task);
        lifecycle.maintenance_task = Some(maintenance_task);
        lifecycle.open = true;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        if !lifecycle.open {
            return Ok(());
        }
        lifecycle.open = false;
        self.inner.accepting.store(false, Ordering::Release);
        if let Some(task) = lifecycle.accept_task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = lifecycle.maintenance_task.take() {
            task.abort();
            let _ = task.await;
        }
        let sessions: Vec<_> = lock(&self.inner.sessions).values().cloned().collect();
        for session in sessions {
            *lock(&session.disconnect_reason) = DisconnectReason::EngineClose;
        }
        let connections: Vec<_> = lock(&self.inner.connections).values().cloned().collect();
        for connection in connections {
            connection.request_close();
        }
        let workers = std::mem::take(&mut *lock(&self.inner.workers));
        for worker in workers {
            let _ = worker.await;
        }
        lock(&self.inner.connections).clear();
        lock(&self.inner.sessions).clear();
        #[cfg(unix)]
        if let Some(address) = lifecycle.address.take() {
            match std::fs::remove_file(address) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(EngineError::new(
                        ErrorCategory::Endpoint,
                        StatusCode::ErrPipeFailed,
                        ErrorPhase::Close,
                        format!("endpoint removal failed: {error}"),
                        None,
                    ))
                }
            }
        }
        #[cfg(windows)]
        {
            lifecycle.address = None;
        }
        Ok(())
    }

    pub async fn send<T: Serialize + ?Sized>(
        &self,
        session: &SessionHandle,
        channel: Channel,
        payload: &T,
    ) -> Result<()> {
        self.send_inner(session, channel, None, payload).await
    }

    pub async fn send_correlated<T: Serialize + ?Sized>(
        &self,
        session: &SessionHandle,
        channel: Channel,
        correlation_id: u32,
        payload: &T,
    ) -> Result<()> {
        self.send_inner(session, channel, Some(correlation_id), payload)
            .await
    }

    async fn send_inner<T: Serialize + ?Sized>(
        &self,
        handle: &SessionHandle,
        channel: Channel,
        correlation_id: Option<u32>,
        payload: &T,
    ) -> Result<()> {
        if !matches!(channel, Channel::Log | Channel::Data) {
            return Err(EngineError::new(
                ErrorCategory::Session,
                StatusCode::ErrProtocolViolation,
                ErrorPhase::ApplicationSend,
                "engine applications may send only Log or Data",
                Some(handle.clone()),
            ));
        }
        let session = lock(&self.inner.sessions)
            .get(&handle.session_id)
            .filter(|session| {
                session.handle == *handle && !session.connection.closing.load(Ordering::Acquire)
            })
            .cloned()
            .ok_or_else(|| {
                EngineError::new(
                    ErrorCategory::Session,
                    StatusCode::ErrConnectionLost,
                    ErrorPhase::ApplicationSend,
                    "session handle is absent, closed, or stale",
                    Some(handle.clone()),
                )
            })?;
        if correlation_id.is_some() && session.capabilities & CAP_CORRELATION == 0 {
            return Err(EngineError::new(
                ErrorCategory::Session,
                StatusCode::ErrProtocolViolation,
                ErrorPhase::ApplicationSend,
                "correlation was not negotiated",
                Some(handle.clone()),
            ));
        }
        let encoded = encode_payload(payload, session.encoding)
            .map_err(|failure| serialization_error(failure, handle.clone()))?;
        let prefix_size = usize::from(correlation_id.is_some()) * 4;
        if encoded.len() > MAX_MESSAGE_SIZE - prefix_size {
            return Err(EngineError::new(
                ErrorCategory::Serialization,
                StatusCode::ErrPayloadTooLarge,
                ErrorPhase::ApplicationSend,
                "encoded frame exceeds 16 MiB",
                Some(handle.clone()),
            ));
        }
        let mut framed_payload = Vec::with_capacity(prefix_size + encoded.len());
        if let Some(identifier) = correlation_id {
            framed_payload.extend_from_slice(&identifier.to_be_bytes());
        }
        framed_payload.extend_from_slice(&encoded);
        let packet = build_frame(
            channel,
            if correlation_id.is_some() {
                FLAG_CORRELATED
            } else {
                0
            },
            &framed_payload,
        )
        .map_err(|failure| serialization_error(failure, handle.clone()))?;
        write_packet(&self.inner, &session, &packet, ErrorPhase::FrameWrite).await
    }
}

fn validate_config(config: &EngineConfig) -> Result<()> {
    resolve_transport_address(&config.endpoint_name, &config.token)?;
    let invalid = if config.max_sessions == 0 {
        Some("max_sessions must be greater than zero")
    } else if config.supported_encodings.is_empty() {
        Some("supported_encodings must not be empty")
    } else if config
        .supported_encodings
        .iter()
        .enumerate()
        .any(|(index, value)| config.supported_encodings[..index].contains(value))
    {
        Some("supported_encodings contains a duplicate")
    } else if config.supported_capabilities & !IMPLEMENTED_CAPABILITIES != 0 {
        Some("supported_capabilities enables an unimplemented bit")
    } else if !config.heartbeat.disabled
        && (config.heartbeat.interval.is_zero() || config.heartbeat.missed_interval_limit == 0)
    {
        Some("enabled heartbeat values must be positive")
    } else if config.fragmentation.timeout.is_zero()
        || config.fragmentation.active_sequence_limit == 0
    {
        Some("fragmentation values must be positive")
    } else {
        None
    };
    if let Some(cause) = invalid {
        return Err(EngineError::new(
            ErrorCategory::Configuration,
            StatusCode::ErrProtocolViolation,
            ErrorPhase::Configuration,
            cause,
            None,
        ));
    }
    Ok(())
}

async fn accept_loop(inner: Arc<Inner>, listener: PlatformListener) {
    while inner.accepting.load(Ordering::Acquire) {
        let (stream, peer_pid) = match listener.accept().await {
            Ok(value) => value,
            Err(error) => {
                if inner.accepting.load(Ordering::Acquire) {
                    emit_endpoint_error(
                        &inner,
                        EngineError::new(
                            ErrorCategory::Endpoint,
                            StatusCode::ErrPipeFailed,
                            ErrorPhase::Accept,
                            format!("accept failed: {error}"),
                            None,
                        )
                        .info,
                    );
                }
                continue;
            }
        };
        let permit = match Arc::clone(&inner.capacity).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => continue,
        };
        let connection_id = inner.next_connection.fetch_add(1, Ordering::Relaxed);
        let (reader, writer) = tokio::io::split(stream);
        let connection = Arc::new(Connection {
            writer: AsyncMutex::new(writer),
            close: Notify::new(),
            closing: AtomicBool::new(false),
        });
        lock(&inner.connections).insert(connection_id, Arc::clone(&connection));
        let worker_inner = Arc::clone(&inner);
        let worker = tokio::spawn(async move {
            connection_worker(
                worker_inner,
                connection_id,
                connection,
                reader,
                peer_pid,
                permit,
            )
            .await;
        });
        lock(&inner.workers).push(worker);
    }
}

async fn connection_worker(
    inner: Arc<Inner>,
    connection_id: u64,
    connection: Arc<Connection>,
    mut reader: ReadHalf<BoxStream>,
    peer_pid: Option<u32>,
    _permit: OwnedSemaphorePermit,
) {
    let session = match establish_session(&inner, &connection, &mut reader, peer_pid).await {
        Ok(Some(session)) => session,
        Ok(None) | Err(()) => {
            connection.request_close();
            let _ = connection.writer.lock().await.shutdown().await;
            lock(&inner.connections).remove(&connection_id);
            return;
        }
    };
    let reason = run_session(&inner, &session, &mut reader).await;
    *lock(&session.disconnect_reason) = reason;
    connection.request_close();
    let _ = connection.writer.lock().await.shutdown().await;
    lock(&inner.sessions).remove(&session.handle.session_id);
    lock(&inner.connections).remove(&connection_id);
    lock(&session.fragments).clear();
    emit_disconnected(&inner, &session);
}

async fn establish_session(
    inner: &Arc<Inner>,
    connection: &Arc<Connection>,
    reader: &mut ReadHalf<BoxStream>,
    peer_pid: Option<u32>,
) -> std::result::Result<Option<Arc<Session>>, ()> {
    let mut handshake = [0u8; 16];
    if let Err(error) = read_exact(connection, reader, &mut handshake).await {
        if !connection.closing.load(Ordering::Acquire) {
            emit_endpoint_error(
                inner,
                EngineError::new(
                    ErrorCategory::Handshake,
                    StatusCode::ErrConnectionLost,
                    ErrorPhase::HandshakeRead,
                    format!("handshake read failed: {error}"),
                    None,
                )
                .info,
            );
        }
        return Ok(None);
    }
    let magic = u32::from_be_bytes(handshake[0..4].try_into().unwrap());
    let version = u32::from_be_bytes(handshake[4..8].try_into().unwrap());
    let pid = u32::from_be_bytes(handshake[8..12].try_into().unwrap());
    let reject = if magic != MAGIC {
        Some((StatusCode::ErrMagicMismatch, "handshake magic is invalid"))
    } else if version != PROTOCOL_VERSION {
        Some((
            StatusCode::ErrVersionMismatch,
            "protocol version is incompatible",
        ))
    } else if inner.config.expected_pid.is_some_and(|expected| {
        expected != pid || peer_pid.is_some_and(|actual| actual != expected)
    }) {
        Some((
            StatusCode::ErrPidMismatch,
            "client PID does not match expected_pid",
        ))
    } else {
        None
    };
    if let Some((status, cause)) = reject {
        emit_endpoint_error(
            inner,
            EngineError::new(
                ErrorCategory::Handshake,
                status,
                ErrorPhase::HandshakeValidate,
                cause,
                None,
            )
            .info,
        );
        return Ok(None);
    }
    let client_encodings = handshake[12];
    let selected = inner
        .config
        .supported_encodings
        .iter()
        .copied()
        .find(|encoding| client_encodings & (*encoding as u8) != 0);
    let Some(selected) = selected else {
        emit_endpoint_error(
            inner,
            EngineError::new(
                ErrorCategory::Handshake,
                StatusCode::ErrEncodingUnsupported,
                ErrorPhase::HandshakeValidate,
                "no supported encoding intersection",
                None,
            )
            .info,
        );
        return Ok(None);
    };
    let client_capabilities = u32::from_be_bytes([0, handshake[13], handshake[14], handshake[15]]);
    let capabilities =
        client_capabilities & inner.config.supported_capabilities & IMPLEMENTED_CAPABILITIES;
    let ack = [
        selected as u8,
        ((capabilities >> 16) & 0xff) as u8,
        ((capabilities >> 8) & 0xff) as u8,
        (capabilities & 0xff) as u8,
    ];
    if write_raw(connection, &ack).await.is_err() {
        emit_endpoint_error(
            inner,
            EngineError::new(
                ErrorCategory::Transport,
                StatusCode::ErrWriteFailed,
                ErrorPhase::AckWrite,
                "ACK write failed",
                None,
            )
            .info,
        );
        return Err(());
    }
    let epoch = inner.next_epoch.fetch_add(1, Ordering::Relaxed);
    let session_id = new_session_id(epoch);
    let handle = SessionHandle {
        session_id: session_id.clone(),
        epoch,
    };
    let session = Arc::new(Session {
        connection: Arc::clone(connection),
        handle: handle.clone(),
        encoding: selected,
        capabilities,
        fragments: Mutex::new(HashMap::new()),
        last_activity: Mutex::new(Instant::now()),
        last_heartbeat: Mutex::new(Instant::now()),
        event_gate: Mutex::new(()),
        terminal_reported: AtomicBool::new(false),
        disconnect_reason: Mutex::new(DisconnectReason::PeerClose),
    });
    let assignment = build_control_frame(&json!({"type": "session", "session_id": session_id}))
        .map_err(|_| ())?;
    if write_raw(connection, &assignment).await.is_err() {
        emit_endpoint_error(
            inner,
            EngineError::new(
                ErrorCategory::Transport,
                StatusCode::ErrWriteFailed,
                ErrorPhase::SessionWrite,
                "session assignment write failed",
                None,
            )
            .info,
        );
        return Err(());
    }
    lock(&inner.sessions).insert(handle.session_id.clone(), Arc::clone(&session));
    emit_connected(inner, &session);
    Ok(Some(session))
}

async fn run_session(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    reader: &mut ReadHalf<BoxStream>,
) -> DisconnectReason {
    loop {
        let mut header = [0u8; 6];
        if let Err(error) = read_exact(&session.connection, reader, &mut header).await {
            if session.connection.closing.load(Ordering::Acquire) {
                return *lock(&session.disconnect_reason);
            }
            if is_peer_close(&error) {
                return DisconnectReason::PeerClose;
            }
            report_terminal(
                inner,
                session,
                ErrorInfo {
                    category: ErrorCategory::Transport,
                    status: StatusCode::ErrConnectionLost,
                    phase: ErrorPhase::FrameRead,
                    cause: format!("frame header read failed: {error}"),
                    session: Some(session.handle.clone()),
                },
            );
            return DisconnectReason::TransportFailure;
        }
        let length = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
        if length > MAX_MESSAGE_SIZE {
            protocol_failure(
                inner,
                session,
                StatusCode::ErrPayloadTooLarge,
                ErrorPhase::FrameDecode,
                "frame payload exceeds 16 MiB",
            )
            .await;
            return DisconnectReason::ProtocolFailure;
        }
        let mut payload = vec![0u8; length];
        if let Err(error) = read_exact(&session.connection, reader, &mut payload).await {
            if session.connection.closing.load(Ordering::Acquire) {
                return *lock(&session.disconnect_reason);
            }
            report_terminal(
                inner,
                session,
                ErrorInfo {
                    category: ErrorCategory::Transport,
                    status: StatusCode::ErrConnectionLost,
                    phase: ErrorPhase::FrameRead,
                    cause: format!("frame payload read failed: {error}"),
                    session: Some(session.handle.clone()),
                },
            );
            return DisconnectReason::TransportFailure;
        }
        match process_frame(inner, session, header[4], header[5], &payload).await {
            Ok(true) => *lock(&session.last_activity) = Instant::now(),
            Ok(false) => return DisconnectReason::ProtocolFailure,
            Err(failure) => {
                protocol_failure(
                    inner,
                    session,
                    failure.status,
                    failure.phase,
                    &failure.cause,
                )
                .await;
                return DisconnectReason::ProtocolFailure;
            }
        }
    }
}

async fn process_frame(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    raw_channel: u8,
    flags: u8,
    payload: &[u8],
) -> std::result::Result<bool, ProtocolFailure> {
    let channel = Channel::try_from(raw_channel).map_err(|_| {
        ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "frame channel is unknown",
        )
    })?;
    if flags & !KNOWN_FLAGS != 0 || flags & FLAG_LAST_FRAGMENT != 0 && flags & FLAG_FRAGMENT == 0 {
        return Err(ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "frame flags are invalid",
        ));
    }
    if channel == Channel::Log {
        return Err(ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "Log is not valid client-to-engine traffic",
        ));
    }
    if channel == Channel::Control {
        if flags != 0 {
            return Err(ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::FrameDecode,
                "Control frames cannot carry application flags",
            ));
        }
        return process_control(inner, session, payload).await;
    }
    let fragmented = flags & FLAG_FRAGMENT != 0;
    let correlated = flags & FLAG_CORRELATED != 0;
    if correlated && session.capabilities & CAP_CORRELATION == 0 {
        return Err(ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "correlation was not negotiated",
        ));
    }
    let mut offset = 0;
    let fragment_id = if fragmented {
        if payload.len() < 4 {
            return Err(ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::FrameDecode,
                "fragment prefix is shorter than four bytes",
            ));
        }
        offset = 4;
        Some(u32::from_be_bytes(payload[0..4].try_into().unwrap()))
    } else {
        None
    };
    let correlation_id = if correlated {
        if payload.len() < offset + 4 {
            return Err(ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::FrameDecode,
                "correlation prefix is shorter than four bytes",
            ));
        }
        let value = u32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        Some(value)
    } else {
        None
    };
    if fragmented {
        process_fragment(
            inner,
            session,
            channel,
            flags,
            fragment_id.unwrap(),
            correlation_id,
            &payload[offset..],
        )?;
    } else {
        if lock(&session.fragments).contains_key(&channel) {
            return Err(ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::Fragmentation,
                "fragment sequences cannot be interleaved on one channel",
            ));
        }
        let decoded = decode_payload(&payload[offset..], session.encoding)?;
        emit_message(
            inner,
            session,
            MessageEvent {
                session: session.handle.clone(),
                channel,
                payload: decoded,
                correlation_id,
            },
        );
    }
    Ok(true)
}

fn process_fragment(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    channel: Channel,
    flags: u8,
    fragment_id: u32,
    correlation_id: Option<u32>,
    data: &[u8],
) -> std::result::Result<(), ProtocolFailure> {
    let complete = {
        let mut fragments = lock(&session.fragments);
        if !fragments.contains_key(&channel) {
            if fragments.len() >= inner.config.fragmentation.active_sequence_limit {
                return Err(ProtocolFailure::new(
                    StatusCode::ErrProtocolViolation,
                    ErrorPhase::Fragmentation,
                    "active fragment-sequence limit exceeded",
                ));
            }
            fragments.insert(
                channel,
                Fragment {
                    id: fragment_id,
                    correlation_id,
                    data: Vec::new(),
                    deadline: Instant::now() + inner.config.fragmentation.timeout,
                },
            );
        }
        let fragment = fragments.get_mut(&channel).unwrap();
        if fragment.id != fragment_id || fragment.correlation_id != correlation_id {
            fragments.remove(&channel);
            return Err(ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::Fragmentation,
                "fragment sequence prefixes changed or interleaved",
            ));
        }
        if fragment.data.len() > MAX_MESSAGE_SIZE - data.len() {
            fragments.remove(&channel);
            return Err(ProtocolFailure::new(
                StatusCode::ErrPayloadTooLarge,
                ErrorPhase::Fragmentation,
                "reassembled message exceeds 16 MiB",
            ));
        }
        fragment.data.extend_from_slice(data);
        if flags & FLAG_LAST_FRAGMENT != 0 {
            Some(fragments.remove(&channel).unwrap().data)
        } else {
            None
        }
    };
    if let Some(data) = complete {
        let decoded = decode_payload(&data, session.encoding)?;
        emit_message(
            inner,
            session,
            MessageEvent {
                session: session.handle.clone(),
                channel,
                payload: decoded,
                correlation_id,
            },
        );
    }
    Ok(())
}

async fn process_control(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    payload: &[u8],
) -> std::result::Result<bool, ProtocolFailure> {
    let control = decode_control(payload)?;
    match control.get("type").and_then(Value::as_str).unwrap() {
        "ping" => {
            let frame = build_control_frame(&json!({"type": "pong",
                "seq": control.get("seq").cloned().unwrap_or(Value::Null)}))?;
            write_packet(inner, session, &frame, ErrorPhase::FrameWrite)
                .await
                .map_err(|error| {
                    ProtocolFailure::new(error.code(), ErrorPhase::FrameWrite, error.info.cause)
                })?;
        }
        "error" => {
            let code = control
                .get("code")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .and_then(|value| StatusCode::try_from(value).ok())
                .ok_or_else(|| {
                    ProtocolFailure::new(
                        StatusCode::ErrProtocolViolation,
                        ErrorPhase::FrameDecode,
                        "Control error code is invalid",
                    )
                })?;
            let cause = control
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("peer reported a protocol error");
            report_terminal(
                inner,
                session,
                ErrorInfo {
                    category: ErrorCategory::Protocol,
                    status: code,
                    phase: ErrorPhase::FrameDecode,
                    cause: cause.to_owned(),
                    session: Some(session.handle.clone()),
                },
            );
            *lock(&session.disconnect_reason) = DisconnectReason::ProtocolFailure;
            session.connection.request_close();
            return Ok(false);
        }
        _ => {}
    }
    Ok(true)
}

async fn maintenance_loop(inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(25));
    loop {
        ticker.tick().await;
        if !inner.accepting.load(Ordering::Acquire) {
            return;
        }
        let sessions: Vec<_> = lock(&inner.sessions).values().cloned().collect();
        let now = Instant::now();
        for session in sessions {
            if session.connection.closing.load(Ordering::Acquire) {
                continue;
            }
            let expired = {
                let mut fragments = lock(&session.fragments);
                let before = fragments.len();
                fragments.retain(|_, fragment| fragment.deadline > now);
                before - fragments.len()
            };
            for _ in 0..expired {
                emit_session_error(
                    &inner,
                    &session,
                    ErrorInfo {
                        category: ErrorCategory::Protocol,
                        status: StatusCode::ErrFragmentTimeout,
                        phase: ErrorPhase::Fragmentation,
                        cause: "incomplete fragment sequence expired".to_owned(),
                        session: Some(session.handle.clone()),
                    },
                );
            }
            if inner.config.heartbeat.disabled {
                continue;
            }
            let deadline = inner
                .config
                .heartbeat
                .interval
                .saturating_mul(inner.config.heartbeat.missed_interval_limit);
            if now.duration_since(*lock(&session.last_activity)) >= deadline {
                report_terminal(
                    &inner,
                    &session,
                    ErrorInfo {
                        category: ErrorCategory::Transport,
                        status: StatusCode::ErrReadTimeout,
                        phase: ErrorPhase::Heartbeat,
                        cause: "session heartbeat deadline expired".to_owned(),
                        session: Some(session.handle.clone()),
                    },
                );
                *lock(&session.disconnect_reason) = DisconnectReason::HeartbeatTimeout;
                session.connection.request_close();
            } else if now.duration_since(*lock(&session.last_heartbeat))
                >= inner.config.heartbeat.interval
            {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                if let Ok(frame) =
                    build_control_frame(&json!({"type": "heartbeat", "ts": timestamp}))
                {
                    if write_packet(&inner, &session, &frame, ErrorPhase::Heartbeat)
                        .await
                        .is_ok()
                    {
                        *lock(&session.last_heartbeat) = now;
                    }
                }
            }
        }
    }
}

async fn protocol_failure(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    status: StatusCode,
    phase: ErrorPhase,
    cause: &str,
) {
    report_terminal(
        inner,
        session,
        ErrorInfo {
            category: ErrorCategory::Protocol,
            status,
            phase,
            cause: cause.to_owned(),
            session: Some(session.handle.clone()),
        },
    );
    if let Ok(frame) =
        build_control_frame(&json!({"type": "error", "code": status as u16, "message": cause}))
    {
        let _ = write_raw(&session.connection, &frame).await;
    }
    *lock(&session.disconnect_reason) = DisconnectReason::ProtocolFailure;
    session.connection.request_close();
}

async fn write_packet(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    packet: &[u8],
    phase: ErrorPhase,
) -> Result<()> {
    if session.connection.closing.load(Ordering::Acquire) {
        return Err(EngineError::new(
            ErrorCategory::Session,
            StatusCode::ErrConnectionLost,
            phase,
            "session is closing",
            Some(session.handle.clone()),
        ));
    }
    if let Err(error) = write_raw(&session.connection, packet).await {
        let failure = EngineError::new(
            ErrorCategory::Transport,
            StatusCode::ErrWriteFailed,
            phase,
            format!("transport write failed: {error}"),
            Some(session.handle.clone()),
        );
        report_terminal(inner, session, failure.info.clone());
        *lock(&session.disconnect_reason) = DisconnectReason::TransportFailure;
        session.connection.request_close();
        return Err(failure);
    }
    Ok(())
}

async fn write_raw(connection: &Connection, packet: &[u8]) -> std::io::Result<()> {
    connection.writer.lock().await.write_all(packet).await
}

async fn read_exact(
    connection: &Connection,
    reader: &mut ReadHalf<BoxStream>,
    buffer: &mut [u8],
) -> std::io::Result<()> {
    tokio::select! {
        biased;
        _ = connection.close.notified() => Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted, "connection closing")),
        result = reader.read_exact(buffer) => result.map(|_| ()),
    }
}

fn report_terminal(inner: &Arc<Inner>, session: &Arc<Session>, failure: ErrorInfo) {
    if !session.terminal_reported.swap(true, Ordering::AcqRel) {
        emit_session_error(inner, session, failure);
    }
}

fn emit_connected(inner: &Arc<Inner>, session: &Arc<Session>) {
    let _gate = lock(&session.event_gate);
    if let Some(callback) = read_lock(&inner.callbacks).connected.clone() {
        callback(SessionView {
            handle: session.handle.clone(),
            encoding: session.encoding,
            capabilities: session.capabilities,
        });
    }
}

fn emit_message(inner: &Arc<Inner>, session: &Arc<Session>, event: MessageEvent) {
    let _gate = lock(&session.event_gate);
    if let Some(callback) = read_lock(&inner.callbacks).message.clone() {
        callback(event);
    }
}

fn emit_session_error(inner: &Arc<Inner>, session: &Arc<Session>, failure: ErrorInfo) {
    let _gate = lock(&session.event_gate);
    if let Some(callback) = read_lock(&inner.callbacks).error.clone() {
        callback(failure);
    }
}

fn emit_endpoint_error(inner: &Arc<Inner>, failure: ErrorInfo) {
    if let Some(callback) = read_lock(&inner.callbacks).error.clone() {
        callback(failure);
    }
}

fn emit_disconnected(inner: &Arc<Inner>, session: &Arc<Session>) {
    let _gate = lock(&session.event_gate);
    if let Some(callback) = read_lock(&inner.callbacks).disconnected.clone() {
        callback(DisconnectEvent {
            session: session.handle.clone(),
            reason: *lock(&session.disconnect_reason),
        });
    }
}

fn new_session_id(epoch: u64) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("rs-{:x}-{epoch:x}-{timestamp:x}", std::process::id())
}

fn is_peer_close(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
    )
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/*
Callbacks execute synchronously on the Tokio task that produced the event.
Events are serialized per session; different sessions may invoke callbacks
concurrently. Same-session writes are serialized by the connection write lock.
Sequential awaited sends preserve order; concurrent sends follow lock acquisition.
*/
