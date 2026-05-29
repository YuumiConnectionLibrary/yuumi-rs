use std::fmt;

pub const MAGIC: u32 = 0x59554D49;
pub const PROTOCOL_VERSION: u32 = 2;
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_PIPE_NAME_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    Json = 0x01,
    MsgPack = 0x02,
}

impl TryFrom<u8> for Encoding {
    type Error = ();
    fn try_from(b: u8) -> std::result::Result<Self, ()> {
        match b {
            0x01 => Ok(Self::Json),
            0x02 => Ok(Self::MsgPack),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Channel {
    Control = 0x00,
    Command = 0x01,
    Log     = 0x02,
    Data    = 0x03,
}

impl TryFrom<u8> for Channel {
    type Error = ();
    fn try_from(b: u8) -> std::result::Result<Self, ()> {
        match b {
            0 => Ok(Self::Control),
            1 => Ok(Self::Command),
            2 => Ok(Self::Log),
            3 => Ok(Self::Data),
            _ => Err(()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum StatusCode {
    HandshakeStart    = 100,
    Connecting        = 101,
    OkConnected       = 200,
    OkMessageReceived = 201,
    OkHeartbeat       = 202,
    ErrMagicMismatch     = 400,
    ErrVersionMismatch   = 401,
    ErrPidMismatch       = 402,
    ErrProtocolViolation = 403,
    ErrPipeFailed      = 500,
    ErrReadTimeout     = 501,
    ErrWriteFailed     = 502,
    ErrConnectionLost  = 503,
    ErrInternal        = 599,
}

#[derive(Debug, Clone)]
pub struct YuumiError {
    pub code: StatusCode,
    pub message: String,
}

impl YuumiError {
    pub fn new(code: StatusCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

impl fmt::Display for YuumiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[YUUMI_ERR][{}] {}", self.code as u32, self.message)
    }
}

impl std::error::Error for YuumiError {}

pub type Result<T> = std::result::Result<T, YuumiError>;

#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub max_attempts: u32,
    /// Seconds
    pub initial_delay: f64,
    /// Seconds
    pub max_delay: f64,
    /// Fraction, e.g. 0.10 for ±10 %
    pub jitter: f64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self { max_attempts: 0, initial_delay: 0.1, max_delay: 2.0, jitter: 0.10 }
    }
}
