use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::types::{
    Channel, Encoding, StatusCode, YuumiError, MAGIC, MAX_MESSAGE_SIZE, PROTOCOL_VERSION,
};

/// Build the 16-byte handshake packet (Big-Endian).
pub fn build_handshake_packet(pid: u32) -> [u8; 16] {
    let mut p = [0u8; 16];
    p[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    p[4..8].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    p[8..12].copy_from_slice(&pid.to_be_bytes());
    p[12] = 0x03; // EncodingCaps: JSON | MsgPack
                  // p[13..16] = 0 (reserved)
    p
}

pub fn encode_payload(value: &Value, encoding: Encoding) -> crate::types::Result<Vec<u8>> {
    match encoding {
        Encoding::Json => serde_json::to_vec(value)
            .map_err(|e| YuumiError::new(StatusCode::ErrInternal, e.to_string())),
        Encoding::MsgPack => rmp_serde::to_vec_named(value)
            .map_err(|e| YuumiError::new(StatusCode::ErrInternal, e.to_string())),
    }
}

pub fn decode_payload(bytes: &[u8], encoding: Encoding) -> crate::types::Result<Value> {
    match encoding {
        Encoding::Json => serde_json::from_slice(bytes)
            .map_err(|e| YuumiError::new(StatusCode::ErrInternal, e.to_string())),
        Encoding::MsgPack => rmp_serde::from_slice(bytes)
            .map_err(|e| YuumiError::new(StatusCode::ErrInternal, e.to_string())),
    }
}

pub fn build_frame(value: &Value, channel: Channel, encoding: Encoding) -> crate::types::Result<Vec<u8>> {
    let payload = encode_payload(value, encoding)?;
    let mut frame = Vec::with_capacity(6 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.push(channel as u8);
    frame.push(0u8); // flags
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    encoding: Encoding,
) -> crate::types::Result<(Value, Channel)> {
    let mut header = [0u8; 6];
    reader.read_exact(&mut header).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof || e.kind() == std::io::ErrorKind::ConnectionReset {
            YuumiError::new(StatusCode::ErrConnectionLost, "connection closed")
        } else {
            YuumiError::new(StatusCode::ErrConnectionLost, e.to_string())
        }
    })?;

    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let channel_byte = header[4];
    let flags = header[5];

    if flags != 0 {
        return Err(YuumiError::new(
            StatusCode::ErrProtocolViolation,
            format!("frame flags must be 0x00, got 0x{flags:02x}"),
        ));
    }
    if length > MAX_MESSAGE_SIZE {
        return Err(YuumiError::new(StatusCode::ErrProtocolViolation, "payload too large"));
    }

    let channel = Channel::try_from(channel_byte).map_err(|_| {
        YuumiError::new(StatusCode::ErrProtocolViolation, format!("unknown channel: {channel_byte}"))
    })?;

    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await.map_err(|e| {
        YuumiError::new(StatusCode::ErrConnectionLost, e.to_string())
    })?;

    let value = decode_payload(&body, encoding)?;
    Ok((value, channel))
}

/// Send the handshake and read the 4-byte ACK. Returns the negotiated `Encoding`.
pub async fn perform_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> crate::types::Result<Encoding> {
    let pid = std::process::id();
    let packet = build_handshake_packet(pid);

    stream.write_all(&packet).await.map_err(|e| {
        YuumiError::new(StatusCode::ErrWriteFailed, format!("handshake write failed: {e}"))
    })?;

    let mut ack = [0u8; 4];
    stream.read_exact(&mut ack).await.map_err(|e| {
        YuumiError::new(StatusCode::ErrMagicMismatch, format!("ACK not received: {e}"))
    })?;

    if ack[1] != 0 || ack[2] != 0 || ack[3] != 0 {
        return Err(YuumiError::new(
            StatusCode::ErrProtocolViolation,
            "ACK reserved bytes must be 0x00",
        ));
    }

    Encoding::try_from(ack[0]).map_err(|_| {
        YuumiError::new(
            StatusCode::ErrProtocolViolation,
            format!("unknown encoding in ACK: 0x{:02x}", ack[0]),
        )
    })
}
