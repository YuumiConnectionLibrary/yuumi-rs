use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

pub const MAGIC: u32 = 0x5955_4d49;
pub const PROTOCOL_VERSION: u32 = 1;
pub const CAP_CORRELATION: u32 = 0x000001;
pub const IMPLEMENTED_CAPABILITIES: u32 = CAP_CORRELATION;
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Encoding {
    Json = 0x01,
    MessagePack = 0x02,
}

impl TryFrom<u8> for Encoding {
    type Error = ();
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Json),
            0x02 => Ok(Self::MessagePack),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Channel {
    Control = 0x00,
    Command = 0x01,
    Log = 0x02,
    Data = 0x03,
}

impl TryFrom<u8> for Channel {
    type Error = ();
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Control),
            0x01 => Ok(Self::Command),
            0x02 => Ok(Self::Log),
            0x03 => Ok(Self::Data),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum StatusCode {
    HandshakeStart = 100,
    Connecting = 101,
    OkConnected = 200,
    OkMessageReceived = 201,
    OkHeartbeat = 202,
    ErrMagicMismatch = 400,
    ErrVersionMismatch = 401,
    ErrPidMismatch = 402,
    ErrProtocolViolation = 403,
    ErrFragmentTimeout = 404,
    ErrPayloadTooLarge = 413,
    ErrEncodingUnsupported = 415,
    ErrPipeFailed = 500,
    ErrReadTimeout = 501,
    ErrWriteFailed = 502,
    ErrConnectionLost = 503,
    ErrInternal = 599,
}

impl TryFrom<u16> for StatusCode {
    type Error = ();
    fn try_from(value: u16) -> std::result::Result<Self, Self::Error> {
        match value {
            100 => Ok(Self::HandshakeStart),
            101 => Ok(Self::Connecting),
            200 => Ok(Self::OkConnected),
            201 => Ok(Self::OkMessageReceived),
            202 => Ok(Self::OkHeartbeat),
            400 => Ok(Self::ErrMagicMismatch),
            401 => Ok(Self::ErrVersionMismatch),
            402 => Ok(Self::ErrPidMismatch),
            403 => Ok(Self::ErrProtocolViolation),
            404 => Ok(Self::ErrFragmentTimeout),
            413 => Ok(Self::ErrPayloadTooLarge),
            415 => Ok(Self::ErrEncodingUnsupported),
            500 => Ok(Self::ErrPipeFailed),
            501 => Ok(Self::ErrReadTimeout),
            502 => Ok(Self::ErrWriteFailed),
            503 => Ok(Self::ErrConnectionLost),
            599 => Ok(Self::ErrInternal),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Configuration,
    AddressDerivation,
    Dial,
    Timeout,
    Handshake,
    Protocol,
    Encoding,
    Capability,
    Backpressure,
    SessionClosed,
    StaleEpoch,
    Application,
    Transport,
    Internal,
    State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPhase {
    Configuration,
    AddressDerivation,
    Dial,
    HandshakeRead,
    HandshakeValidate,
    AckWrite,
    SessionWrite,
    FrameRead,
    FrameDecode,
    FrameWrite,
    Heartbeat,
    Fragmentation,
    ApplicationDispatch,
    ApplicationSend,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    LocalClose,
    PeerClose,
    HeartbeatTimeout,
    ProtocolFailure,
    TransportFailure,
    Backpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    Idle,
    Connecting,
    Connected,
    Closing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionView {
    pub session_id: String,
    pub epoch: u64,
    pub encoding: Encoding,
    pub capabilities: u32,
}

#[derive(Debug, Clone)]
pub struct MessageEvent {
    pub session: SessionView,
    pub channel: Channel,
    pub payload: Value,
    pub correlation_id: Option<u32>,
    pub responder: Option<crate::engine::Responder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatEvent {
    pub session: SessionView,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorInfo {
    pub kind: ErrorKind,
    pub cause: String,
    pub status: Option<StatusCode>,
    pub phase: Option<ErrorPhase>,
    pub epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalResult {
    pub reason: DisconnectReason,
    pub error: Option<ErrorInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisconnectEvent {
    pub session: SessionView,
    pub terminal: TerminalResult,
}

#[derive(Debug, Clone)]
pub struct HeartbeatSettings {
    pub disabled: bool,
    pub interval: Duration,
    pub missed_interval_limit: u32,
}

impl Default for HeartbeatSettings {
    fn default() -> Self {
        Self {
            disabled: false,
            interval: Duration::from_secs(30),
            missed_interval_limit: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FragmentationSettings {
    pub timeout: Duration,
    pub active_sequence_limit: usize,
}

impl Default for FragmentationSettings {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(15),
            active_sequence_limit: 16,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub endpoint_name: String,
    pub token: String,
    pub supported_encodings: Vec<Encoding>,
    pub supported_capabilities: u32,
    pub expected_go_pid: Option<u32>,
    pub connect_timeout: Duration,
    pub application_queue_capacity: usize,
    pub heartbeat: HeartbeatSettings,
    pub fragmentation: FragmentationSettings,
}

impl EngineConfig {
    pub fn new(endpoint_name: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint_name: endpoint_name.into(),
            token: token.into(),
            supported_encodings: vec![Encoding::MessagePack, Encoding::Json],
            supported_capabilities: CAP_CORRELATION,
            expected_go_pid: None,
            connect_timeout: Duration::from_secs(10),
            application_queue_capacity: 64,
            heartbeat: HeartbeatSettings::default(),
            fragmentation: FragmentationSettings::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineError {
    pub info: ErrorInfo,
}

impl EngineError {
    pub(crate) fn new(
        kind: ErrorKind,
        cause: impl Into<String>,
        status: Option<StatusCode>,
        phase: Option<ErrorPhase>,
        epoch: Option<u64>,
    ) -> Self {
        Self {
            info: ErrorInfo {
                kind,
                cause: cause.into(),
                status,
                phase,
                epoch,
            },
        }
    }
    pub fn kind(&self) -> ErrorKind {
        self.info.kind
    }
    pub fn code(&self) -> Option<StatusCode> {
        self.info.status
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "[YUUMI_ERR][{:?}] {}",
            self.info.kind, self.info.cause
        )
    }
}

impl std::error::Error for EngineError {}

pub type Result<T> = std::result::Result<T, EngineError>;
pub type ConnectedCallback = Arc<dyn Fn(SessionView) + Send + Sync>;
pub type MessageCallback = Arc<dyn Fn(MessageEvent) + Send + Sync>;
pub type HeartbeatCallback = Arc<dyn Fn(HeartbeatEvent) + Send + Sync>;
pub type ErrorCallback = Arc<dyn Fn(ErrorInfo) + Send + Sync>;
pub type DisconnectedCallback = Arc<dyn Fn(DisconnectEvent) + Send + Sync>;
