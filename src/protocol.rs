use serde::Serialize;
use serde_json::{Map, Value};

use crate::types::{Channel, Encoding, ErrorCategory, ErrorPhase, StatusCode, MAX_MESSAGE_SIZE};

pub(crate) const FLAG_FRAGMENT: u8 = 0x01;
pub(crate) const FLAG_LAST_FRAGMENT: u8 = 0x02;
pub(crate) const FLAG_CORRELATED: u8 = 0x04;
pub(crate) const KNOWN_FLAGS: u8 = FLAG_FRAGMENT | FLAG_LAST_FRAGMENT | FLAG_CORRELATED;

#[derive(Debug)]
pub(crate) struct ProtocolFailure {
    pub status: StatusCode,
    pub phase: ErrorPhase,
    pub cause: String,
}

impl ProtocolFailure {
    pub fn new(status: StatusCode, phase: ErrorPhase, cause: impl Into<String>) -> Self {
        Self {
            status,
            phase,
            cause: cause.into(),
        }
    }
}

pub(crate) fn encode_payload<T: Serialize + ?Sized>(
    value: &T,
    encoding: Encoding,
) -> std::result::Result<Vec<u8>, ProtocolFailure> {
    match encoding {
        Encoding::Json => serde_json::to_vec(value).map_err(|error| {
            ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::ApplicationSend,
                format!("payload serialization failed: {error}"),
            )
        }),
        Encoding::MessagePack => rmp_serde::to_vec_named(value).map_err(|error| {
            ProtocolFailure::new(
                StatusCode::ErrProtocolViolation,
                ErrorPhase::ApplicationSend,
                format!("payload serialization failed: {error}"),
            )
        }),
    }
}

pub(crate) fn decode_payload(
    payload: &[u8],
    encoding: Encoding,
) -> std::result::Result<Value, ProtocolFailure> {
    let failure = || {
        ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "application payload cannot be decoded with the negotiated encoding",
        )
    };
    match encoding {
        Encoding::Json => serde_json::from_slice(payload).map_err(|_| failure()),
        Encoding::MessagePack => rmp_serde::from_slice(payload).map_err(|_| failure()),
    }
}

pub(crate) fn decode_control(
    payload: &[u8],
) -> std::result::Result<Map<String, Value>, ProtocolFailure> {
    let value: Value = serde_json::from_slice(payload).map_err(|_| {
        ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "Control payload is not valid JSON",
        )
    })?;
    let object = value.as_object().ok_or_else(|| {
        ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "Control payload must be an object with a string type",
        )
    })?;
    if !object.get("type").is_some_and(Value::is_string) {
        return Err(ProtocolFailure::new(
            StatusCode::ErrProtocolViolation,
            ErrorPhase::FrameDecode,
            "Control payload must be an object with a string type",
        ));
    }
    Ok(object.clone())
}

pub(crate) fn build_frame(
    channel: Channel,
    flags: u8,
    payload: &[u8],
) -> std::result::Result<Vec<u8>, ProtocolFailure> {
    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(ProtocolFailure::new(
            StatusCode::ErrPayloadTooLarge,
            ErrorPhase::ApplicationSend,
            "frame payload exceeds 16 MiB",
        ));
    }
    let mut frame = Vec::with_capacity(6 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.push(channel as u8);
    frame.push(flags);
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub(crate) fn build_control_frame<T: Serialize + ?Sized>(
    value: &T,
) -> std::result::Result<Vec<u8>, ProtocolFailure> {
    let payload = encode_payload(value, Encoding::Json)?;
    build_frame(Channel::Control, 0, &payload)
}

pub(crate) fn serialization_error(
    failure: ProtocolFailure,
    session: crate::types::SessionHandle,
) -> crate::types::EngineError {
    crate::types::EngineError::new(
        ErrorCategory::Serialization,
        failure.status,
        failure.phase,
        failure.cause,
        Some(session),
    )
}
