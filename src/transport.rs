use std::path::PathBuf;

use crate::types::{StatusCode, YuumiError, MAX_PIPE_NAME_BYTES};

/// Resolve the socket file path for a given pipe name.
/// The name is truncated to `MAX_PIPE_NAME_BYTES` UTF-8 bytes, then `.sock` is appended.
pub fn resolve_transport_address(pipe_name: &str) -> PathBuf {
    let bytes = pipe_name.as_bytes();
    let safe_name = if bytes.len() <= MAX_PIPE_NAME_BYTES {
        pipe_name.to_string()
    } else {
        let mut end = MAX_PIPE_NAME_BYTES;
        while end > 0 && !pipe_name.is_char_boundary(end) {
            end -= 1;
        }
        pipe_name[..end].to_string()
    };
    std::env::temp_dir().join(format!("{safe_name}.sock"))
}

#[cfg(unix)]
pub async fn dial_transport(pipe_name: &str) -> crate::types::Result<tokio::net::UnixStream> {
    let address = resolve_transport_address(pipe_name);
    tokio::net::UnixStream::connect(&address).await.map_err(|e| {
        YuumiError::new(StatusCode::ErrPipeFailed, format!("dial failed: {e}"))
    })
}
