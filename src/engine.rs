use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;

use crate::protocol::{
    build_control_frame, build_frame, decode_control, decode_payload, encode_payload,
    serialization_error, ProtocolFailure, FLAG_CORRELATED, FLAG_FRAGMENT, FLAG_LAST_FRAGMENT,
    KNOWN_FLAGS,
};
use crate::transport::{dial_local, resolve_transport_address, BoxStream};
use crate::types::*;

#[derive(Default, Clone)]
struct Callbacks {
    connected: Option<ConnectedCallback>,
    message: Option<MessageCallback>,
    heartbeat: Option<HeartbeatCallback>,
    error: Option<ErrorCallback>,
    disconnected: Option<DisconnectedCallback>,
}

struct Attempt {
    cancelled: AtomicBool,
    notify: Notify,
}
impl Attempt {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

struct StateData {
    state: EngineState,
    attempt: Option<Arc<Attempt>>,
}
struct Fragment {
    id: u32,
    correlation_id: Option<u32>,
    data: Vec<u8>,
    deadline: Instant,
}

enum DispatchItem {
    Connected(SessionView, bool),
    Message(MessageEvent, bool),
    Heartbeat(HeartbeatEvent, bool),
    Error(ErrorInfo, bool),
    Disconnected(DisconnectEvent),
}

impl DispatchItem {
    fn uses_capacity(&self) -> bool {
        match self {
            Self::Connected(_, value)
            | Self::Message(_, value)
            | Self::Heartbeat(_, value)
            | Self::Error(_, value) => *value,
            Self::Disconnected(_) => false,
        }
    }
}

struct DispatchState {
    items: VecDeque<DispatchItem>,
    capacity_used: usize,
    accepting: bool,
    finalized: bool,
}

struct DispatchQueue {
    state: Mutex<DispatchState>,
    notify: Notify,
    capacity: usize,
}

struct Session {
    view: SessionView,
    config: EngineConfig,
    writer: AsyncMutex<WriteHalf<BoxStream>>,
    close: Notify,
    closing: AtomicBool,
    finalized: AtomicBool,
    fragments: Mutex<HashMap<Channel, Fragment>>,
    last_activity: Mutex<Instant>,
    last_heartbeat: Mutex<Instant>,
    terminal: Mutex<Option<TerminalResult>>,
    dispatch: DispatchQueue,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl Session {
    fn request_close(&self) {
        if !self.closing.swap(true, Ordering::AcqRel) {
            self.close.notify_waiters();
        }
    }
    fn set_terminal(&self, reason: DisconnectReason, error: Option<ErrorInfo>) {
        let mut terminal = lock(&self.terminal);
        if terminal.is_none() {
            *terminal = Some(TerminalResult { reason, error });
        }
    }
}

struct Inner {
    source_config: EngineConfig,
    state: Mutex<StateData>,
    session: Mutex<Option<Arc<Session>>>,
    terminal_result: Mutex<Option<TerminalResult>>,
    callbacks: RwLock<Callbacks>,
    next_epoch: AtomicU64,
    attempt_done: Notify,
}

#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub struct Responder {
    inner: Arc<ResponderInner>,
}
struct ResponderInner {
    engine: Weak<Inner>,
    epoch: u64,
    correlation_id: u32,
    used: AtomicBool,
}

impl fmt::Debug for Responder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Responder")
            .field("epoch", &self.inner.epoch)
            .field("correlation_id", &self.inner.correlation_id)
            .finish_non_exhaustive()
    }
}

impl Responder {
    pub async fn respond<T: Serialize + ?Sized>(&self, payload: &T) -> Result<()> {
        if self.inner.used.swap(true, Ordering::AcqRel) {
            return Err(engine_error(
                ErrorKind::StaleEpoch,
                "responder is single-use",
                StatusCode::ErrProtocolViolation,
                ErrorPhase::ApplicationSend,
                Some(self.inner.epoch),
            ));
        }
        let engine = self.inner.engine.upgrade().ok_or_else(|| {
            engine_error(
                ErrorKind::StaleEpoch,
                "responder belongs to a dropped engine",
                StatusCode::ErrConnectionLost,
                ErrorPhase::ApplicationSend,
                Some(self.inner.epoch),
            )
        })?;
        respond_inner(
            &engine,
            self.inner.epoch,
            self.inner.correlation_id,
            payload,
        )
        .await
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                source_config: config,
                state: Mutex::new(StateData {
                    state: EngineState::Idle,
                    attempt: None,
                }),
                session: Mutex::new(None),
                terminal_result: Mutex::new(None),
                callbacks: RwLock::new(Callbacks::default()),
                next_epoch: AtomicU64::new(0),
                attempt_done: Notify::new(),
            }),
        }
    }

    pub fn state(&self) -> EngineState {
        lock(&self.inner.state).state
    }
    pub fn session(&self) -> Option<SessionView> {
        lock(&self.inner.session)
            .as_ref()
            .map(|session| session.view.clone())
    }
    pub fn terminal_result(&self) -> Option<TerminalResult> {
        lock(&self.inner.terminal_result).clone()
    }
    pub fn on_session_connected(&self, callback: ConnectedCallback) {
        write_lock(&self.inner.callbacks).connected = Some(callback);
    }
    pub fn on_message(&self, callback: MessageCallback) {
        write_lock(&self.inner.callbacks).message = Some(callback);
    }
    pub fn on_heartbeat(&self, callback: HeartbeatCallback) {
        write_lock(&self.inner.callbacks).heartbeat = Some(callback);
    }
    pub fn on_error(&self, callback: ErrorCallback) {
        write_lock(&self.inner.callbacks).error = Some(callback);
    }
    pub fn on_session_disconnected(&self, callback: DisconnectedCallback) {
        write_lock(&self.inner.callbacks).disconnected = Some(callback);
    }

    pub async fn connect(&self) -> Result<SessionView> {
        let config = self.inner.source_config.clone();
        if let Err(error) = validate_config(&config) {
            emit_pre_session_error(&self.inner, error.info.clone());
            return Err(error);
        }
        let address = match resolve_transport_address(&config.endpoint_name, &config.token) {
            Ok(value) => value,
            Err(error) => {
                emit_pre_session_error(&self.inner, error.info.clone());
                return Err(error);
            }
        };
        let attempt = Arc::new(Attempt {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        });
        {
            let mut state = lock(&self.inner.state);
            match state.state {
                EngineState::Idle => {
                    state.state = EngineState::Connecting;
                    state.attempt = Some(Arc::clone(&attempt));
                    *lock(&self.inner.terminal_result) = None;
                }
                EngineState::Connecting => return Err(state_error("engine is already connecting")),
                EngineState::Connected | EngineState::Closing => {
                    return Err(state_error("engine is already connected or closing"));
                }
            }
        }
        let outcome = tokio::time::timeout(
            config.connect_timeout,
            connect_attempt(&self.inner, &config, &address, &attempt),
        )
        .await;
        let result = outcome.unwrap_or_else(|_| {
            Err(engine_error(
                ErrorKind::Timeout,
                "connect and handshake attempt timed out",
                StatusCode::ErrReadTimeout,
                ErrorPhase::Dial,
                None,
            ))
        });
        match result {
            Ok((reader, writer, view)) => {
                let session = Arc::new(Session {
                    view: view.clone(),
                    config: config.clone(),
                    writer: AsyncMutex::new(writer),
                    close: Notify::new(),
                    closing: AtomicBool::new(false),
                    finalized: AtomicBool::new(false),
                    fragments: Mutex::new(HashMap::new()),
                    last_activity: Mutex::new(Instant::now()),
                    last_heartbeat: Mutex::new(Instant::now()),
                    terminal: Mutex::new(None),
                    dispatch: DispatchQueue {
                        state: Mutex::new(DispatchState {
                            items: VecDeque::new(),
                            capacity_used: 0,
                            accepting: true,
                            finalized: false,
                        }),
                        notify: Notify::new(),
                        capacity: config.application_queue_capacity,
                    },
                    workers: Mutex::new(Vec::new()),
                });
                {
                    let mut state = lock(&self.inner.state);
                    if state.state != EngineState::Connecting
                        || attempt.cancelled.load(Ordering::Acquire)
                    {
                        state.state = EngineState::Idle;
                        state.attempt = None;
                        self.inner.attempt_done.notify_waiters();
                        return Err(local_close_error());
                    }
                    state.state = EngineState::Connected;
                    state.attempt = None;
                    *lock(&self.inner.session) = Some(Arc::clone(&session));
                }
                start_session(&self.inner, &session, reader);
                self.inner.attempt_done.notify_waiters();
                Ok(view)
            }
            Err(error) => {
                let local_close = {
                    let mut state = lock(&self.inner.state);
                    let value = state.state == EngineState::Closing
                        || attempt.cancelled.load(Ordering::Acquire);
                    state.state = EngineState::Idle;
                    state.attempt = None;
                    value
                };
                self.inner.attempt_done.notify_waiters();
                if local_close {
                    Err(local_close_error())
                } else {
                    emit_pre_session_error(&self.inner, error.info.clone());
                    Err(error)
                }
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        let (attempt, session) = {
            let mut state = lock(&self.inner.state);
            match state.state {
                EngineState::Idle => return Ok(()),
                EngineState::Connecting => {
                    state.state = EngineState::Closing;
                    (state.attempt.clone(), None)
                }
                EngineState::Connected => {
                    state.state = EngineState::Closing;
                    (None, lock(&self.inner.session).clone())
                }
                EngineState::Closing => (state.attempt.clone(), lock(&self.inner.session).clone()),
            }
        };
        if let Some(attempt) = attempt {
            attempt.cancel();
            loop {
                if lock(&self.inner.state).state == EngineState::Idle {
                    return Ok(());
                }
                self.inner.attempt_done.notified().await;
            }
        }
        let Some(session) = session else {
            return Ok(());
        };
        session.set_terminal(DisconnectReason::LocalClose, None);
        stop_accepting(&session);
        session.request_close();
        join_workers(&session).await
    }

    pub async fn send<T: Serialize + ?Sized>(&self, channel: Channel, payload: &T) -> Result<()> {
        if !matches!(channel, Channel::Log | Channel::Data) {
            return Err(engine_error(
                ErrorKind::Protocol,
                "engine applications may send only Log or Data",
                StatusCode::ErrProtocolViolation,
                ErrorPhase::ApplicationSend,
                self.session().map(|session| session.epoch),
            ));
        }
        let session = require_session(&self.inner)?;
        send_packet(&self.inner, &session, channel, None, payload).await
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }
        if let Some(attempt) = lock(&self.inner.state).attempt.clone() {
            attempt.cancel();
        }
        if let Some(session) = lock(&self.inner.session).take() {
            session.set_terminal(DisconnectReason::LocalClose, None);
            stop_accepting(&session);
            session.request_close();
            for worker in std::mem::take(&mut *lock(&session.workers)) {
                worker.abort();
            }
        }
        lock(&self.inner.state).state = EngineState::Idle;
    }
}

async fn connect_attempt(
    inner: &Arc<Inner>,
    config: &EngineConfig,
    address: &std::path::Path,
    attempt: &Arc<Attempt>,
) -> Result<(ReadHalf<BoxStream>, WriteHalf<BoxStream>, SessionView)> {
    let dial = tokio::select! {
        biased;
        _ = attempt.notify.notified() => return Err(local_close_error()),
        result = dial_local(address) => result,
    };
    let (stream, peer_pid) = dial.map_err(|error| {
        engine_error(
            ErrorKind::Dial,
            format!("local endpoint dial failed: {error}"),
            StatusCode::ErrPipeFailed,
            ErrorPhase::Dial,
            None,
        )
    })?;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut handshake = [0u8; 16];
    candidate_read(attempt, &mut reader, &mut handshake)
        .await
        .map_err(|error| {
            engine_error(
                ErrorKind::Handshake,
                format!("handshake read failed: {error}"),
                StatusCode::ErrConnectionLost,
                ErrorPhase::HandshakeRead,
                None,
            )
        })?;
    let magic = u32::from_be_bytes([handshake[0], handshake[1], handshake[2], handshake[3]]);
    let version = u32::from_be_bytes([handshake[4], handshake[5], handshake[6], handshake[7]]);
    let pid = u32::from_be_bytes([handshake[8], handshake[9], handshake[10], handshake[11]]);
    if magic != MAGIC {
        return Err(engine_error(
            ErrorKind::Handshake,
            "handshake magic is invalid",
            StatusCode::ErrMagicMismatch,
            ErrorPhase::HandshakeValidate,
            None,
        ));
    }
    if version != PROTOCOL_VERSION {
        return Err(engine_error(
            ErrorKind::Handshake,
            "handshake protocol version is incompatible",
            StatusCode::ErrVersionMismatch,
            ErrorPhase::HandshakeValidate,
            None,
        ));
    }
    if config.expected_go_pid.is_some_and(|expected| {
        expected != pid || peer_pid.is_some_and(|actual| actual != expected)
    }) {
        return Err(engine_error(
            ErrorKind::Handshake,
            "Go PID does not match expected_go_pid",
            StatusCode::ErrPidMismatch,
            ErrorPhase::HandshakeValidate,
            None,
        ));
    }
    let selected = config
        .supported_encodings
        .iter()
        .copied()
        .find(|encoding| handshake[12] & (*encoding as u8) != 0)
        .ok_or_else(|| {
            engine_error(
                ErrorKind::Encoding,
                "no common encoding exists",
                StatusCode::ErrEncodingUnsupported,
                ErrorPhase::HandshakeValidate,
                None,
            )
        })?;
    let offered = u32::from_be_bytes([0, handshake[13], handshake[14], handshake[15]]);
    let capabilities = offered & config.supported_capabilities & IMPLEMENTED_CAPABILITIES;
    let ack = [
        selected as u8,
        ((capabilities >> 16) & 0xff) as u8,
        ((capabilities >> 8) & 0xff) as u8,
        (capabilities & 0xff) as u8,
    ];
    candidate_write(attempt, &mut writer, &ack)
        .await
        .map_err(|error| {
            engine_error(
                ErrorKind::Handshake,
                format!("ACK write failed: {error}"),
                StatusCode::ErrWriteFailed,
                ErrorPhase::AckWrite,
                None,
            )
        })?;
    let epoch = inner.next_epoch.fetch_add(1, Ordering::Relaxed) + 1;
    let session_id = new_session_id(epoch);
    let assignment = build_control_frame(&json!({"type":"session","session_id":session_id}))
        .map_err(|failure| {
            engine_error(
                ErrorKind::Internal,
                failure.cause,
                failure.status,
                failure.phase,
                None,
            )
        })?;
    candidate_write(attempt, &mut writer, &assignment)
        .await
        .map_err(|error| {
            engine_error(
                ErrorKind::Handshake,
                format!("session assignment write failed: {error}"),
                StatusCode::ErrWriteFailed,
                ErrorPhase::SessionWrite,
                None,
            )
        })?;
    Ok((
        reader,
        writer,
        SessionView {
            session_id,
            epoch,
            encoding: selected,
            capabilities,
        },
    ))
}

async fn candidate_read(
    attempt: &Attempt,
    reader: &mut ReadHalf<BoxStream>,
    buffer: &mut [u8],
) -> std::io::Result<()> {
    if attempt.cancelled.load(Ordering::Acquire) {
        return Err(interrupted());
    }
    tokio::select! {
        biased;
        _ = attempt.notify.notified() => Err(interrupted()),
        result = reader.read_exact(buffer) => result.map(|_| ()),
    }
}

async fn candidate_write(
    attempt: &Attempt,
    writer: &mut WriteHalf<BoxStream>,
    packet: &[u8],
) -> std::io::Result<()> {
    if attempt.cancelled.load(Ordering::Acquire) {
        return Err(interrupted());
    }
    tokio::select! {
        biased;
        _ = attempt.notify.notified() => Err(interrupted()),
        result = writer.write_all(packet) => result,
    }
}

fn start_session(inner: &Arc<Inner>, session: &Arc<Session>, reader: ReadHalf<BoxStream>) {
    let dispatch_inner = Arc::clone(inner);
    let dispatch_session = Arc::clone(session);
    let dispatcher =
        tokio::spawn(async move { dispatch_loop(dispatch_inner, dispatch_session).await });
    enqueue_application(
        inner,
        session,
        DispatchItem::Connected(session.view.clone(), true),
    );
    let reader_inner = Arc::clone(inner);
    let reader_session = Arc::clone(session);
    let reader_task =
        tokio::spawn(async move { read_loop(reader_inner, reader_session, reader).await });
    let maintenance_inner = Arc::clone(inner);
    let maintenance_session = Arc::clone(session);
    let maintenance =
        tokio::spawn(async move { maintenance_loop(maintenance_inner, maintenance_session).await });
    *lock(&session.workers) = vec![reader_task, maintenance, dispatcher];
}

async fn read_loop(inner: Arc<Inner>, session: Arc<Session>, mut reader: ReadHalf<BoxStream>) {
    loop {
        let mut header = [0u8; 6];
        if let Err(error) = session_read(&session, &mut reader, &mut header).await {
            if !session.closing.load(Ordering::Acquire) && !is_peer_close(&error) {
                session.set_terminal(
                    DisconnectReason::TransportFailure,
                    Some(error_info(
                        ErrorKind::Transport,
                        format!("frame header read failed: {error}"),
                        StatusCode::ErrConnectionLost,
                        ErrorPhase::FrameRead,
                        Some(session.view.epoch),
                    )),
                );
            }
            break;
        }
        let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if length > MAX_MESSAGE_SIZE {
            protocol_failure(
                &inner,
                &session,
                ProtocolFailure::new(
                    StatusCode::ErrPayloadTooLarge,
                    ErrorPhase::FrameDecode,
                    "frame payload exceeds 16 MiB",
                ),
            )
            .await;
            break;
        }
        let mut payload = vec![0u8; length];
        if let Err(error) = session_read(&session, &mut reader, &mut payload).await {
            if !session.closing.load(Ordering::Acquire) {
                session.set_terminal(
                    DisconnectReason::TransportFailure,
                    Some(error_info(
                        ErrorKind::Transport,
                        format!("frame payload read failed: {error}"),
                        StatusCode::ErrConnectionLost,
                        ErrorPhase::FrameRead,
                        Some(session.view.epoch),
                    )),
                );
            }
            break;
        }
        *lock(&session.last_activity) = Instant::now();
        if let Err(failure) = process_frame(&inner, &session, header[4], header[5], &payload).await
        {
            protocol_failure(&inner, &session, failure).await;
            break;
        }
        if session.closing.load(Ordering::Acquire) {
            break;
        }
    }
    finalize_session(&inner, &session).await;
}

async fn session_read(
    session: &Session,
    reader: &mut ReadHalf<BoxStream>,
    buffer: &mut [u8],
) -> std::io::Result<()> {
    if session.closing.load(Ordering::Acquire) {
        return Err(interrupted());
    }
    tokio::select! {
        biased;
        _ = session.close.notified() => Err(interrupted()),
        result = reader.read_exact(buffer) => result.map(|_| ()),
    }
}

async fn process_frame(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    raw_channel: u8,
    flags: u8,
    payload: &[u8],
) -> std::result::Result<(), ProtocolFailure> {
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
            "Log is not valid Go-to-engine traffic",
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
    if correlated && session.view.capabilities & CAP_CORRELATION == 0 {
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
        Some(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]))
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
        let id = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        offset += 4;
        Some(id)
    } else {
        None
    };
    if let Some(fragment_id) = fragment_id {
        process_fragment(
            inner,
            session,
            channel,
            flags,
            fragment_id,
            correlation_id,
            &payload[offset..],
        )
    } else {
        if lock(&session.fragments).contains_key(&channel) {
            return Err(ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::Fragmentation,
                "fragment sequences cannot be interleaved on one channel",
            ));
        }
        let value = decode_payload(&payload[offset..], session.view.encoding)?;
        deliver_message(inner, session, channel, value, correlation_id);
        Ok(())
    }
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
            if fragments.len() >= session.config.fragmentation.active_sequence_limit {
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
                    deadline: Instant::now() + session.config.fragmentation.timeout,
                },
            );
        }
        let Some(fragment) = fragments.get_mut(&channel) else {
            return Err(ProtocolFailure::new(
                StatusCode::ErrInternal,
                ErrorPhase::Fragmentation,
                "fragment state could not be created",
            ));
        };
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
            fragments.remove(&channel).map(|fragment| fragment.data)
        } else {
            None
        }
    };
    if let Some(data) = complete {
        let value = decode_payload(&data, session.view.encoding)?;
        deliver_message(inner, session, channel, value, correlation_id);
    }
    Ok(())
}

fn deliver_message(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    channel: Channel,
    payload: Value,
    correlation_id: Option<u32>,
) {
    let responder = correlation_id.map(|correlation_id| Responder {
        inner: Arc::new(ResponderInner {
            engine: Arc::downgrade(inner),
            epoch: session.view.epoch,
            correlation_id,
            used: AtomicBool::new(false),
        }),
    });
    enqueue_application(
        inner,
        session,
        DispatchItem::Message(
            MessageEvent {
                session: session.view.clone(),
                channel,
                payload,
                correlation_id,
                responder,
            },
            true,
        ),
    );
}

async fn process_control(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    payload: &[u8],
) -> std::result::Result<(), ProtocolFailure> {
    let control = decode_control(payload)?;
    match control
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "heartbeat" => {
            let timestamp = control
                .get("ts")
                .and_then(Value::as_u64)
                .unwrap_or_else(now_millis);
            enqueue_application(
                inner,
                session,
                DispatchItem::Heartbeat(
                    HeartbeatEvent {
                        session: session.view.clone(),
                        timestamp,
                    },
                    true,
                ),
            );
        }
        "ping" => {
            let packet = build_control_frame(&json!({
                "type":"pong","seq":control.get("seq").cloned().unwrap_or(Value::Null)
            }))?;
            write_packet(inner, session, &packet, ErrorPhase::FrameWrite)
                .await
                .map_err(|error| {
                    ProtocolFailure::new(
                        error.code().unwrap_or(StatusCode::ErrWriteFailed),
                        ErrorPhase::FrameWrite,
                        error.info.cause,
                    )
                })?;
        }
        "error" => {
            let status = control
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
                .unwrap_or("Go peer reported a protocol error")
                .to_owned();
            session.set_terminal(
                DisconnectReason::ProtocolFailure,
                Some(error_info(
                    ErrorKind::Protocol,
                    cause,
                    status,
                    ErrorPhase::FrameDecode,
                    Some(session.view.epoch),
                )),
            );
            stop_accepting(session);
            session.request_close();
        }
        _ => {}
    }
    Ok(())
}

async fn maintenance_loop(inner: Arc<Inner>, session: Arc<Session>) {
    let period = Duration::from_millis(25)
        .min(session.config.fragmentation.timeout)
        .min(if session.config.heartbeat.disabled {
            Duration::from_millis(25)
        } else {
            session.config.heartbeat.interval
        })
        .max(Duration::from_millis(1));
    let mut ticker = tokio::time::interval(period);
    loop {
        tokio::select! {
            biased;
            _ = session.close.notified() => return,
            _ = ticker.tick() => {}
        }
        if session.closing.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let expired = {
            let mut fragments = lock(&session.fragments);
            let before = fragments.len();
            fragments.retain(|_, fragment| fragment.deadline > now);
            before - fragments.len()
        };
        for _ in 0..expired {
            enqueue_application(
                &inner,
                &session,
                DispatchItem::Error(
                    error_info(
                        ErrorKind::Timeout,
                        "incomplete fragment sequence expired",
                        StatusCode::ErrFragmentTimeout,
                        ErrorPhase::Fragmentation,
                        Some(session.view.epoch),
                    ),
                    true,
                ),
            );
        }
        if session.config.heartbeat.disabled {
            continue;
        }
        let deadline = session
            .config
            .heartbeat
            .interval
            .saturating_mul(session.config.heartbeat.missed_interval_limit);
        if now.duration_since(*lock(&session.last_activity)) >= deadline {
            session.set_terminal(
                DisconnectReason::HeartbeatTimeout,
                Some(error_info(
                    ErrorKind::Timeout,
                    "session heartbeat deadline expired",
                    StatusCode::ErrReadTimeout,
                    ErrorPhase::Heartbeat,
                    Some(session.view.epoch),
                )),
            );
            stop_accepting(&session);
            session.request_close();
            return;
        }
        if now.duration_since(*lock(&session.last_heartbeat)) >= session.config.heartbeat.interval {
            let packet = match build_control_frame(&json!({
                "type":"heartbeat","ts":now_millis()
            })) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if write_packet(&inner, &session, &packet, ErrorPhase::Heartbeat)
                .await
                .is_err()
            {
                return;
            }
            *lock(&session.last_heartbeat) = now;
        }
    }
}

async fn protocol_failure(_inner: &Arc<Inner>, session: &Arc<Session>, failure: ProtocolFailure) {
    session.set_terminal(
        DisconnectReason::ProtocolFailure,
        Some(error_info(
            ErrorKind::Protocol,
            failure.cause.clone(),
            failure.status,
            failure.phase,
            Some(session.view.epoch),
        )),
    );
    stop_accepting(session);
    if let Ok(packet) = build_control_frame(&json!({
        "type":"error","code":failure.status as u16,"message":failure.cause
    })) {
        let _ = write_raw(session, &packet).await;
    }
    session.request_close();
}

async fn respond_inner<T: Serialize + ?Sized>(
    inner: &Arc<Inner>,
    epoch: u64,
    correlation_id: u32,
    payload: &T,
) -> Result<()> {
    let session = require_epoch(inner, epoch)?;
    if session.view.capabilities & CAP_CORRELATION == 0 {
        return Err(engine_error(
            ErrorKind::Capability,
            "correlation was not negotiated",
            StatusCode::ErrProtocolViolation,
            ErrorPhase::ApplicationSend,
            Some(epoch),
        ));
    }
    send_packet(
        inner,
        &session,
        Channel::Data,
        Some(correlation_id),
        payload,
    )
    .await
}

async fn send_packet<T: Serialize + ?Sized>(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    channel: Channel,
    correlation_id: Option<u32>,
    payload: &T,
) -> Result<()> {
    let encoded = encode_payload(payload, session.view.encoding)
        .map_err(|failure| serialization_error(failure, session.view.epoch))?;
    let prefix_size = usize::from(correlation_id.is_some()) * 4;
    if encoded.len() > MAX_MESSAGE_SIZE - prefix_size {
        return Err(engine_error(
            ErrorKind::Protocol,
            "encoded frame exceeds 16 MiB",
            StatusCode::ErrPayloadTooLarge,
            ErrorPhase::ApplicationSend,
            Some(session.view.epoch),
        ));
    }
    let mut framed = Vec::with_capacity(prefix_size + encoded.len());
    if let Some(correlation_id) = correlation_id {
        framed.extend_from_slice(&correlation_id.to_be_bytes());
    }
    framed.extend_from_slice(&encoded);
    let packet = build_frame(
        channel,
        if correlation_id.is_some() {
            FLAG_CORRELATED
        } else {
            0
        },
        &framed,
    )
    .map_err(|failure| serialization_error(failure, session.view.epoch))?;
    write_packet(inner, session, &packet, ErrorPhase::FrameWrite).await
}

async fn write_packet(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    packet: &[u8],
    phase: ErrorPhase,
) -> Result<()> {
    require_epoch(inner, session.view.epoch)?;
    if session.closing.load(Ordering::Acquire) {
        return Err(stale_error(session.view.epoch));
    }
    if let Err(error) = write_raw(session, packet).await {
        let failure = error_info(
            ErrorKind::Transport,
            format!("transport write failed: {error}"),
            StatusCode::ErrWriteFailed,
            phase,
            Some(session.view.epoch),
        );
        session.set_terminal(DisconnectReason::TransportFailure, Some(failure.clone()));
        stop_accepting(session);
        session.request_close();
        return Err(EngineError { info: failure });
    }
    Ok(())
}

async fn write_raw(session: &Session, packet: &[u8]) -> std::io::Result<()> {
    let mut writer = session.writer.lock().await;
    if session.closing.load(Ordering::Acquire) {
        return Err(interrupted());
    }
    writer.write_all(packet).await
}

fn require_session(inner: &Arc<Inner>) -> Result<Arc<Session>> {
    let session = lock(&inner.session).clone();
    match session {
        Some(session)
            if lock(&inner.state).state == EngineState::Connected
                && !session.closing.load(Ordering::Acquire) =>
        {
            Ok(session)
        }
        Some(session) => Err(stale_error(session.view.epoch)),
        None => Err(engine_error(
            ErrorKind::SessionClosed,
            "engine has no live session",
            StatusCode::ErrConnectionLost,
            ErrorPhase::ApplicationSend,
            None,
        )),
    }
}

fn require_epoch(inner: &Arc<Inner>, epoch: u64) -> Result<Arc<Session>> {
    let session = lock(&inner.session).clone();
    match session {
        Some(session)
            if session.view.epoch == epoch
                && lock(&inner.state).state == EngineState::Connected
                && !session.closing.load(Ordering::Acquire) =>
        {
            Ok(session)
        }
        _ => Err(stale_error(epoch)),
    }
}

fn enqueue_application(inner: &Arc<Inner>, session: &Arc<Session>, item: DispatchItem) -> bool {
    let mut dispatch = lock(&session.dispatch.state);
    if !dispatch.accepting || dispatch.finalized {
        return false;
    }
    if item.uses_capacity() && dispatch.capacity_used >= session.dispatch.capacity {
        dispatch.accepting = false;
        drop(dispatch);
        session.set_terminal(
            DisconnectReason::Backpressure,
            Some(error_info(
                ErrorKind::Backpressure,
                "application queue capacity exhausted",
                StatusCode::ErrInternal,
                ErrorPhase::ApplicationDispatch,
                Some(session.view.epoch),
            )),
        );
        session.request_close();
        let _ = inner;
        return false;
    }
    if item.uses_capacity() {
        dispatch.capacity_used += 1;
    }
    dispatch.items.push_back(item);
    drop(dispatch);
    session.dispatch.notify.notify_one();
    true
}

fn enqueue_terminal(session: &Session, item: DispatchItem) {
    lock(&session.dispatch.state).items.push_back(item);
    session.dispatch.notify.notify_one();
}

async fn dispatch_loop(inner: Arc<Inner>, session: Arc<Session>) {
    loop {
        let item = loop {
            let next = {
                let mut dispatch = lock(&session.dispatch.state);
                if let Some(item) = dispatch.items.pop_front() {
                    Some(Ok(item))
                } else if dispatch.finalized {
                    Some(Err(()))
                } else {
                    None
                }
            };
            match next {
                Some(Ok(item)) => break item,
                Some(Err(())) => return,
                None => session.dispatch.notify.notified().await,
            }
        };
        let uses_capacity = item.uses_capacity();
        let callback = {
            let callbacks = read_lock(&inner.callbacks);
            match &item {
                DispatchItem::Connected(_, _) => {
                    callbacks.connected.clone().map(Callback::Connected)
                }
                DispatchItem::Message(_, _) => callbacks.message.clone().map(Callback::Message),
                DispatchItem::Heartbeat(_, _) => {
                    callbacks.heartbeat.clone().map(Callback::Heartbeat)
                }
                DispatchItem::Error(_, _) => callbacks.error.clone().map(Callback::Error),
                DispatchItem::Disconnected(_) => {
                    callbacks.disconnected.clone().map(Callback::Disconnected)
                }
            }
        };
        if let Some(callback) = callback {
            let terminal_observer = matches!(
                item,
                DispatchItem::Error(_, _) | DispatchItem::Disconnected(_)
            );
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| callback.invoke(&item))) {
                if terminal_observer {
                    eprintln!(
                        "Yuumi application observer panicked: {}",
                        panic_message(payload)
                    );
                } else {
                    enqueue_terminal(
                        &session,
                        DispatchItem::Error(
                            error_info(
                                ErrorKind::Application,
                                "application callback panicked",
                                StatusCode::ErrInternal,
                                ErrorPhase::ApplicationDispatch,
                                Some(session.view.epoch),
                            ),
                            false,
                        ),
                    );
                }
            }
        }
        if uses_capacity {
            let mut dispatch = lock(&session.dispatch.state);
            dispatch.capacity_used = dispatch.capacity_used.saturating_sub(1);
        }
    }
}

enum Callback {
    Connected(ConnectedCallback),
    Message(MessageCallback),
    Heartbeat(HeartbeatCallback),
    Error(ErrorCallback),
    Disconnected(DisconnectedCallback),
}

impl Callback {
    fn invoke(&self, item: &DispatchItem) {
        match (self, item) {
            (Self::Connected(callback), DispatchItem::Connected(value, _)) => {
                callback(value.clone())
            }
            (Self::Message(callback), DispatchItem::Message(value, _)) => callback(value.clone()),
            (Self::Heartbeat(callback), DispatchItem::Heartbeat(value, _)) => {
                callback(value.clone())
            }
            (Self::Error(callback), DispatchItem::Error(value, _)) => callback(value.clone()),
            (Self::Disconnected(callback), DispatchItem::Disconnected(value)) => {
                callback(value.clone())
            }
            _ => {}
        }
    }
}

async fn finalize_session(inner: &Arc<Inner>, session: &Arc<Session>) {
    if session.finalized.swap(true, Ordering::AcqRel) {
        return;
    }
    stop_accepting(session);
    session.request_close();
    let _ = session.writer.lock().await.shutdown().await;
    lock(&session.fragments).clear();
    let terminal = lock(&session.terminal).clone().unwrap_or(TerminalResult {
        reason: DisconnectReason::PeerClose,
        error: None,
    });
    {
        let mut current = lock(&inner.session);
        if current
            .as_ref()
            .is_some_and(|value| Arc::ptr_eq(value, session))
        {
            *current = None;
        }
    }
    *lock(&inner.terminal_result) = Some(terminal.clone());
    lock(&inner.state).state = EngineState::Idle;
    if let Some(error) = terminal.error.clone() {
        enqueue_terminal(session, DispatchItem::Error(error, false));
    }
    enqueue_terminal(
        session,
        DispatchItem::Disconnected(DisconnectEvent {
            session: session.view.clone(),
            terminal,
        }),
    );
    lock(&session.dispatch.state).finalized = true;
    session.dispatch.notify.notify_waiters();
}

async fn join_workers(session: &Arc<Session>) -> Result<()> {
    let workers = std::mem::take(&mut *lock(&session.workers));
    for mut worker in workers {
        if tokio::time::timeout(Duration::from_secs(2), &mut worker)
            .await
            .is_err()
        {
            worker.abort();
            return Err(engine_error(
                ErrorKind::Timeout,
                "worker did not stop before close deadline",
                StatusCode::ErrReadTimeout,
                ErrorPhase::Close,
                Some(session.view.epoch),
            ));
        }
    }
    Ok(())
}

fn stop_accepting(session: &Session) {
    lock(&session.dispatch.state).accepting = false;
}

fn validate_config(config: &EngineConfig) -> Result<()> {
    resolve_transport_address(&config.endpoint_name, &config.token)?;
    let cause = if config.supported_encodings.is_empty() {
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
    } else if config.connect_timeout.is_zero() {
        Some("connect_timeout must be positive")
    } else if config.application_queue_capacity == 0 {
        Some("application_queue_capacity must be a positive integer")
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
    match cause {
        Some(cause) => Err(engine_error(
            ErrorKind::Configuration,
            cause,
            StatusCode::ErrProtocolViolation,
            ErrorPhase::Configuration,
            None,
        )),
        None => Ok(()),
    }
}

fn emit_pre_session_error(inner: &Arc<Inner>, error: ErrorInfo) {
    if let Some(callback) = read_lock(&inner.callbacks).error.clone() {
        if catch_unwind(AssertUnwindSafe(|| callback(error))).is_err() {
            eprintln!("Yuumi pre-session error observer panicked");
        }
    }
}

fn engine_error(
    kind: ErrorKind,
    cause: impl Into<String>,
    status: StatusCode,
    phase: ErrorPhase,
    epoch: Option<u64>,
) -> EngineError {
    EngineError::new(kind, cause, Some(status), Some(phase), epoch)
}

fn error_info(
    kind: ErrorKind,
    cause: impl Into<String>,
    status: StatusCode,
    phase: ErrorPhase,
    epoch: Option<u64>,
) -> ErrorInfo {
    ErrorInfo {
        kind,
        cause: cause.into(),
        status: Some(status),
        phase: Some(phase),
        epoch,
    }
}

fn state_error(cause: &str) -> EngineError {
    engine_error(
        ErrorKind::State,
        cause,
        StatusCode::ErrProtocolViolation,
        ErrorPhase::Dial,
        None,
    )
}

fn local_close_error() -> EngineError {
    engine_error(
        ErrorKind::SessionClosed,
        "connection attempt was closed locally",
        StatusCode::ErrConnectionLost,
        ErrorPhase::Close,
        None,
    )
}

fn stale_error(epoch: u64) -> EngineError {
    engine_error(
        ErrorKind::StaleEpoch,
        "operation belongs to an earlier or closed epoch",
        StatusCode::ErrConnectionLost,
        ErrorPhase::ApplicationSend,
        Some(epoch),
    )
}

fn interrupted() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "connection closing")
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn new_session_id(epoch: u64) -> String {
    format!(
        "rs-{:x}-{epoch:x}-{:x}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

fn is_peer_close(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
    )
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic")
        .to_owned()
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
The reader, heartbeat worker and serial application dispatcher are independent
Tokio tasks. The bounded queue counts queued and currently executing
application callbacks, while terminal error and disconnected events bypass the
capacity so backpressure remains observable. Every task and responder captures
the local epoch; a replacement session cannot be addressed by stale work.
*/
