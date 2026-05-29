use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::UnixStream;

use crate::protocol::{build_frame, perform_handshake, read_frame};
use crate::transport::{dial_transport, resolve_transport_address};
use crate::types::{Channel, Encoding, ReconnectPolicy, Result, StatusCode, YuumiError};

pub type MessageHandler   = Arc<dyn Fn(Value, Channel) + Send + Sync>;
pub type HeartbeatHandler = Arc<dyn Fn(u64)            + Send + Sync>;
pub type ErrorHandler     = Arc<dyn Fn(YuumiError)     + Send + Sync>;

struct Handlers {
    on_message:   Option<MessageHandler>,
    on_heartbeat: Option<HeartbeatHandler>,
    on_error:     Option<ErrorHandler>,
}

pub struct Client {
    writer:    tokio::sync::Mutex<WriteHalf<UnixStream>>,
    reader:    tokio::sync::Mutex<Option<ReadHalf<UnixStream>>>,
    encoding:  Encoding,
    handlers:  Arc<Mutex<Handlers>>,
    read_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Client {
    /// Low-level constructor — no handshake performed (use in tests).
    pub fn raw(stream: UnixStream, encoding: Encoding) -> Self {
        let (read, write) = tokio::io::split(stream);
        Self {
            writer:    tokio::sync::Mutex::new(write),
            reader:    tokio::sync::Mutex::new(Some(read)),
            encoding,
            handlers:  Arc::new(Mutex::new(Handlers {
                on_message:   None,
                on_heartbeat: None,
                on_error:     None,
            })),
            read_task: tokio::sync::Mutex::new(None),
        }
    }

    // ── Constructors ─────────────────────────────────────────────────────────

    pub async fn connect(pipe_name: &str) -> Result<Self> {
        let mut stream = dial_transport(pipe_name).await?;
        let encoding = perform_handshake(&mut stream).await?;
        Ok(Self::raw(stream, encoding))
    }

    pub async fn connect_with_policy(pipe_name: &str, policy: &ReconnectPolicy) -> Result<Self> {
        if policy.max_attempts == 0 {
            return Self::connect(pipe_name).await;
        }
        let mut last_err = YuumiError::new(StatusCode::ErrInternal, "unreachable");
        for attempt in 1..=policy.max_attempts {
            match Self::connect(pipe_name).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    last_err = e;
                    if attempt < policy.max_attempts {
                        let delay = backoff_delay(attempt, policy);
                        tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
                    }
                }
            }
        }
        Err(last_err)
    }

    // ── Callbacks ─────────────────────────────────────────────────────────────

    pub fn on_message<F>(&self, f: F)
    where F: Fn(Value, Channel) + Send + Sync + 'static {
        self.handlers.lock().unwrap().on_message = Some(Arc::new(f));
    }

    pub fn on_heartbeat<F>(&self, f: F)
    where F: Fn(u64) + Send + Sync + 'static {
        self.handlers.lock().unwrap().on_heartbeat = Some(Arc::new(f));
    }

    pub fn on_error<F>(&self, f: F)
    where F: Fn(YuumiError) + Send + Sync + 'static {
        self.handlers.lock().unwrap().on_error = Some(Arc::new(f));
    }

    // ── I/O ──────────────────────────────────────────────────────────────────

    pub async fn listen(&self) {
        let mut reader_guard = self.reader.lock().await;
        let Some(read) = reader_guard.take() else { return };
        drop(reader_guard);

        let handlers = Arc::clone(&self.handlers);
        let encoding = self.encoding;

        let task = tokio::spawn(read_loop(read, handlers, encoding));
        *self.read_task.lock().await = Some(task);
    }

    pub async fn send(&self, value: Value, channel: Channel) -> Result<()> {
        let frame = build_frame(&value, channel, self.encoding)?;
        self.writer.lock().await.write_all(&frame).await.map_err(|e| {
            YuumiError::new(StatusCode::ErrWriteFailed, e.to_string())
        })
    }

    /// Read one frame directly (only valid before `listen()` is called).
    pub async fn receive(&self) -> Result<(Value, Channel)> {
        let mut guard = self.reader.lock().await;
        match guard.as_mut() {
            None => Err(YuumiError::new(StatusCode::ErrInternal, "listen() already called")),
            Some(read) => read_frame(read, self.encoding).await,
        }
    }

    pub async fn close(&self) {
        if let Some(task) = self.read_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        let _ = self.writer.lock().await.shutdown().await;
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.read_task.try_lock() {
            if let Some(task) = guard.take() {
                task.abort();
            }
        }
    }
}

// ── Read loop ─────────────────────────────────────────────────────────────────

fn heartbeat_ts(value: &Value) -> Option<u64> {
    let obj = value.as_object()?;
    if obj.get("type")?.as_str()? != "heartbeat" { return None; }
    obj.get("ts")?.as_u64()
}

async fn read_loop(
    mut read: ReadHalf<UnixStream>,
    handlers: Arc<Mutex<Handlers>>,
    encoding: Encoding,
) {
    loop {
        match read_frame(&mut read, encoding).await {
            Err(e) => {
                let handler = handlers.lock().unwrap().on_error.clone();
                if let Some(h) = handler { h(e); }
                return;
            }
            Ok((value, channel)) => {
                if channel == Channel::Control {
                    if let Some(ts) = heartbeat_ts(&value) {
                        let handler = handlers.lock().unwrap().on_heartbeat.clone();
                        if let Some(h) = handler { h(ts); }
                        continue;
                    }
                }
                let handler = handlers.lock().unwrap().on_message.clone();
                if let Some(h) = handler { h(value, channel); }
            }
        }
    }
}

// ── Backoff ───────────────────────────────────────────────────────────────────

fn backoff_delay(attempt: u32, policy: &ReconnectPolicy) -> f64 {
    let base = (policy.initial_delay * 2_f64.powi((attempt - 1) as i32)).min(policy.max_delay);
    // deterministic ±jitter: use attempt parity to avoid rand dependency
    let sign = if attempt % 2 == 0 { 1.0 } else { -1.0 };
    (base + sign * base * policy.jitter * 0.5).max(0.0)
}

// ── Public connect helper ─────────────────────────────────────────────────────

pub async fn connect(pipe_name: &str, policy: Option<&ReconnectPolicy>) -> Result<Client> {
    match policy {
        Some(p) if p.max_attempts > 0 => Client::connect_with_policy(pipe_name, p).await,
        _ => Client::connect(pipe_name).await,
    }
}
