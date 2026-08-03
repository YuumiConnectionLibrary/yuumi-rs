use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::types::{EngineError, ErrorKind, ErrorPhase, Result, StatusCode};

pub(crate) trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> TransportStream for T {}
pub(crate) type BoxStream = Box<dyn TransportStream>;

pub(crate) fn resolve_transport_address(endpoint_name: &str, token: &str) -> Result<PathBuf> {
    if !valid_endpoint_name(endpoint_name) {
        return Err(config_error(
            "endpoint_name must match [A-Za-z0-9][A-Za-z0-9_-]{0,31}",
        ));
    }
    if !valid_token(token) {
        return Err(config_error(
            "token must contain exactly 32 lowercase hexadecimal characters",
        ));
    }
    #[cfg(windows)]
    let address = PathBuf::from(format!(r"\\.\pipe\yuumi-{endpoint_name}-{token}"));
    #[cfg(unix)]
    let address = {
        let digest = address_digest(endpoint_name, token);
        std::env::temp_dir().join(format!("yuumi-{digest}.sock"))
    };
    #[cfg(unix)]
    validate_unix_address(&address)?;
    Ok(address)
}

pub(crate) async fn dial_local(address: &Path) -> std::io::Result<(BoxStream, Option<u32>)> {
    #[cfg(unix)]
    {
        let stream = tokio::net::UnixStream::connect(address).await?;
        let peer_pid = stream
            .peer_cred()
            .ok()
            .and_then(|credentials| credentials.pid())
            .and_then(|pid| u32::try_from(pid).ok());
        Ok((Box::new(stream), peer_pid))
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        loop {
            match ClientOptions::new().read(true).write(true).open(address) {
                Ok(stream) => {
                    let peer_pid = named_pipe_server_pid(&stream);
                    return Ok((Box::new(stream), peer_pid));
                }
                Err(error) if error.raw_os_error() == Some(231) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn valid_endpoint_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=32).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_token(value: &str) -> bool {
    value.len() == 32
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn config_error(cause: impl Into<String>) -> EngineError {
    EngineError::new(
        ErrorKind::Configuration,
        cause,
        Some(StatusCode::ErrProtocolViolation),
        Some(ErrorPhase::Configuration),
        None,
    )
}

#[cfg(unix)]
fn validate_unix_address(address: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    #[cfg(target_os = "macos")]
    const MAX_BYTES: usize = 103;
    #[cfg(not(target_os = "macos"))]
    const MAX_BYTES: usize = 107;
    if address.as_os_str().as_bytes().len() > MAX_BYTES {
        return Err(EngineError::new(
            ErrorKind::AddressDerivation,
            "canonical Unix socket address exceeds the platform byte bound",
            Some(StatusCode::ErrPipeFailed),
            Some(ErrorPhase::AddressDerivation),
            None,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn address_digest(endpoint_name: &str, token: &str) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut input = Vec::with_capacity(128);
    input.extend_from_slice(b"yuumi\0");
    input.extend_from_slice(endpoint_name.as_bytes());
    input.push(0);
    input.extend_from_slice(token.as_bytes());
    let bit_length = (input.len() as u64) * 8;
    input.push(0x80);
    while input.len() % 64 != 56 {
        input.push(0);
    }
    input.extend_from_slice(&bit_length.to_be_bytes());
    let mut hash = [
        0x6a09e667u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    for chunk in input.chunks_exact(64) {
        let mut words = [0u32; 64];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            let offset = index * 4;
            *word = u32::from_be_bytes([
                chunk[offset],
                chunk[offset + 1],
                chunk[offset + 2],
                chunk[offset + 3],
            ]);
        }
        for index in 16..64 {
            let first = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let second = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(first)
                .wrapping_add(words[index - 7])
                .wrapping_add(second);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = hash;
        for index in 0..64 {
            let upper = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ (!e & g);
            let first = h
                .wrapping_add(upper)
                .wrapping_add(choose)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let lower = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let second = lower.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        for (value, addition) in hash.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *value = value.wrapping_add(addition);
        }
    }
    hash[..4].iter().map(|word| format!("{word:08x}")).collect()
}

#[cfg(windows)]
fn named_pipe_server_pid(stream: &tokio::net::windows::named_pipe::NamedPipeClient) -> Option<u32> {
    use std::os::windows::io::AsRawHandle;
    let mut pid = 0u32;
    let ok = unsafe { GetNamedPipeServerProcessId(stream.as_raw_handle(), &mut pid) };
    (ok != 0).then_some(pid)
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetNamedPipeServerProcessId(pipe: std::os::windows::io::RawHandle, pid: *mut u32) -> i32;
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn canonical_digest_matches_every_distinct_vector() {
        for (name, token, digest) in [
            (
                "a",
                "000102030405060708090a0b0c0d0e0f",
                "c3da2f7decb02b7a24e054711453a85c",
            ),
            (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345",
                "f0e0d0c0b0a090807060504030201000",
                "a9ce40da7198349aab024f107b698ef4",
            ),
            (
                "a",
                "100102030405060708090a0b0c0d0e0f",
                "8710f11234486007b59a4333c6f01736",
            ),
            (
                "b",
                "000102030405060708090a0b0c0d0e0f",
                "4c724cfb8212cb82d0857d699cdc7647",
            ),
        ] {
            let path = resolve_transport_address(name, token).unwrap();
            assert_eq!(
                path.file_name().unwrap(),
                format!("yuumi-{digest}.sock").as_str()
            );
        }
    }
}
