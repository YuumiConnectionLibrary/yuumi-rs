use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use yuumi::{Engine, EngineConfig, SessionView, CAP_CORRELATION, MAGIC, PROTOCOL_VERSION};

pub const TOKEN: &str = "0123456789abcdef0123456789abcdef";
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub trait PeerIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> PeerIo for T {}
pub type PeerStream = Box<dyn PeerIo>;

pub fn config() -> EngineConfig {
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut config = EngineConfig::new(format!("t{:x}{sequence:x}", std::process::id()), TOKEN);
    config.heartbeat.disabled = true;
    config
}

pub fn handshake(encodings: u8, capabilities: u32) -> [u8; 16] {
    let mut packet = [0u8; 16];
    packet[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    packet[4..8].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    packet[8..12].copy_from_slice(&std::process::id().to_be_bytes());
    packet[12] = encodings;
    packet[13..16].copy_from_slice(&capabilities.to_be_bytes()[1..4]);
    packet
}

pub fn frame(channel: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(6 + payload.len());
    packet.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    packet.push(channel);
    packet.push(flags);
    packet.extend_from_slice(payload);
    packet
}

pub async fn read_frame(stream: &mut PeerStream) -> (u8, u8, Vec<u8>) {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await.unwrap();
    (header[4], header[5], payload)
}

pub struct Established {
    pub stream: PeerStream,
    pub ack: [u8; 4],
    pub assignment: (u8, u8, Vec<u8>),
    pub view: SessionView,
}

pub async fn establish(engine: &Engine, config: &EngineConfig) -> Established {
    establish_with(engine, config, handshake(3, CAP_CORRELATION)).await
}

pub async fn establish_with(
    engine: &Engine,
    config: &EngineConfig,
    packet: [u8; 16],
) -> Established {
    let listener = TestListener::bind(config).await;
    let peer = tokio::spawn(async move {
        let mut stream = listener.accept().await;
        stream.write_all(&packet).await.unwrap();
        let mut ack = [0u8; 4];
        stream.read_exact(&mut ack).await.unwrap();
        let assignment = read_frame(&mut stream).await;
        (stream, ack, assignment)
    });
    let view = engine.connect().await.unwrap();
    let (stream, ack, assignment) = peer.await.unwrap();
    Established {
        stream,
        ack,
        assignment,
        view,
    }
}

#[cfg(unix)]
pub struct TestListener {
    inner: tokio::net::UnixListener,
    address: PathBuf,
}

#[cfg(unix)]
impl TestListener {
    pub async fn bind(config: &EngineConfig) -> Self {
        let address = address(config);
        let _ = std::fs::remove_file(&address);
        let inner = tokio::net::UnixListener::bind(&address).unwrap();
        Self { inner, address }
    }

    pub async fn accept(self) -> PeerStream {
        let (stream, _) = self.inner.accept().await.unwrap();
        Box::new(stream)
    }
}

#[cfg(unix)]
impl Drop for TestListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.address);
    }
}

#[cfg(windows)]
pub struct TestListener {
    inner: tokio::net::windows::named_pipe::NamedPipeServer,
}

#[cfg(windows)]
impl TestListener {
    pub async fn bind(config: &EngineConfig) -> Self {
        use tokio::net::windows::named_pipe::{PipeMode, ServerOptions};
        let inner = ServerOptions::new()
            .pipe_mode(PipeMode::Byte)
            .create(address(config))
            .unwrap();
        Self { inner }
    }

    pub async fn accept(self) -> PeerStream {
        self.inner.connect().await.unwrap();
        Box::new(self.inner)
    }
}

pub fn address(config: &EngineConfig) -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(format!(
            r"\\.\pipe\yuumi-{}-{}",
            config.endpoint_name, config.token
        ))
    }
    #[cfg(unix)]
    {
        std::env::temp_dir().join(format!(
            "yuumi-{}.sock",
            digest(&config.endpoint_name, &config.token)
        ))
    }
}

pub async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !predicate() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

pub fn vector(name: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../yuumi-spec/test-vectors")
            .join(name),
    )
    .unwrap()
}

#[cfg(unix)]
fn digest(endpoint_name: &str, token: &str) -> String {
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
    let mut input = b"yuumi\0".to_vec();
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
            *word = u32::from_be_bytes(chunk[index * 4..index * 4 + 4].try_into().unwrap());
        }
        for index in 16..64 {
            let a = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let b = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(a)
                .wrapping_add(words[index - 7])
                .wrapping_add(b);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = hash;
        for index in 0..64 {
            let first = h
                .wrapping_add(e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25))
                .wrapping_add((e & f) ^ (!e & g))
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let second = (a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22))
                .wrapping_add((a & b) ^ (a & c) ^ (b & c));
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
