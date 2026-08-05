use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::Notify;
use yuumi::{
    Channel, Engine, EngineConfig, ErrorKind, FragmentationSettings, HeartbeatSettings, Responder,
    StatusCode,
};

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn event(name: &str, engine: &str) -> Value {
    json!({"event": name, "engine": engine})
}

fn kind_name(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::StaleEpoch => "stale_epoch",
        ErrorKind::Protocol => "protocol",
        ErrorKind::SessionClosed => "session_closed",
        _ => "unexpected",
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let endpoint = env::var("YUUMI_ENDPOINT_NAME").map_err(|_| "missing endpoint")?;
    let token = env::var("YUUMI_TOKEN").map_err(|_| "missing token")?;
    let scenario = Arc::new(env::var("YUUMI_INTEROP_SCENARIO").map_err(|_| "missing scenario")?);
    let engine_name =
        Arc::new(env::var("YUUMI_INTEROP_ENGINE_NAME").map_err(|_| "missing engine name")?);
    let mut config = EngineConfig::new(endpoint, token);
    config.connect_timeout = Duration::from_secs(5);
    config.application_queue_capacity = if scenario.as_str() == "backpressure" {
        1
    } else {
        64
    };
    config.heartbeat = if scenario.as_str() == "slow" {
        HeartbeatSettings {
            disabled: false,
            interval: Duration::from_millis(50),
            missed_interval_limit: 20,
        }
    } else {
        HeartbeatSettings {
            disabled: true,
            ..HeartbeatSettings::default()
        }
    };
    config.fragmentation = FragmentationSettings {
        timeout: Duration::from_millis(150),
        active_sequence_limit: 16,
    };

    let engine = Engine::new(config);
    let finished = Arc::new(Notify::new());
    let failure = Arc::new(Mutex::new(None::<String>));
    let captured = Arc::new(Mutex::new(None::<Responder>));
    let connection_count = Arc::new(AtomicUsize::new(0));
    let terminal_kind = Arc::new(Mutex::new(None::<ErrorKind>));

    {
        let engine = engine.clone();
        let scenario = Arc::clone(&scenario);
        let engine_name = Arc::clone(&engine_name);
        let captured = Arc::clone(&captured);
        let connection_count = Arc::clone(&connection_count);
        let failure = Arc::clone(&failure);
        let finished = Arc::clone(&finished);
        engine.clone().on_session_connected(Arc::new(move |_| {
            let count = connection_count.fetch_add(1, Ordering::AcqRel) + 1;
            if scenario.as_str() != "reconnect" || count != 2 {
                return;
            }
            let engine = engine.clone();
            let engine_name = Arc::clone(&engine_name);
            let responder = lock(&captured).clone();
            let failure = Arc::clone(&failure);
            let finished = Arc::clone(&finished);
            tokio::spawn(async move {
                let responder_kind = match responder {
                    Some(value) => match value.respond(&json!({"unexpected": true})).await {
                        Err(error) => kind_name(error.kind()).to_owned(),
                        Ok(()) => "not_rejected".to_owned(),
                    },
                    None => "missing".to_owned(),
                };
                let send_kind = match engine
                    .send(Channel::Command, &json!({"unexpected": true}))
                    .await
                {
                    Err(error) => kind_name(error.kind()).to_owned(),
                    Ok(()) => "not_rejected".to_owned(),
                };
                let payload = json!({
                    "event": "stale", "engine": engine_name.as_str(),
                    "responder": responder_kind, "send": send_kind
                });
                if let Err(error) = engine.send(Channel::Data, &payload).await {
                    *lock(&failure) = Some(error.to_string());
                    finished.notify_one();
                }
            });
        }));
    }

    {
        let engine = engine.clone();
        let engine_name = Arc::clone(&engine_name);
        let captured = Arc::clone(&captured);
        engine.clone().on_message(Arc::new(move |message| {
            if let Some(text) = message.payload.as_str() {
                let engine = engine.clone();
                let payload = json!({
                    "event": "limit", "engine": engine_name.as_str(), "size": text.len()
                });
                tokio::spawn(async move {
                    let _ = engine.send(Channel::Data, &payload).await;
                });
                return;
            }
            let operation = message.payload.get("op").and_then(Value::as_str);
            match operation {
                Some("roundtrip") => {
                    let engine = engine.clone();
                    let engine_name = Arc::clone(&engine_name);
                    tokio::spawn(async move {
                        if let Some(responder) = message.responder {
                            let response = json!({
                                "event": "response", "engine": engine_name.as_str(),
                                "opaque": message.payload["opaque"].clone()
                            });
                            let _ = responder.respond(&response).await;
                        }
                        let _ = engine
                            .send(Channel::Log, &event("uncorrelated", &engine_name))
                            .await;
                    });
                }
                Some("fragmented") => {
                    let engine = engine.clone();
                    let size = message.payload["payload"].as_str().map_or(0, str::len);
                    let payload = json!({
                        "event": "fragmented", "engine": engine_name.as_str(), "size": size
                    });
                    tokio::spawn(async move {
                        let _ = engine.send(Channel::Data, &payload).await;
                    });
                }
                Some("slow") => {
                    std::thread::sleep(Duration::from_millis(300));
                    tokio::spawn(async move {
                        if let Some(responder) = message.responder {
                            let _ = responder.respond(&json!({"event": "slow_complete"})).await;
                        }
                    });
                }
                Some("hold") => std::thread::sleep(Duration::from_millis(500)),
                Some("capture") => {
                    *lock(&captured) = message.responder;
                    let engine = engine.clone();
                    let payload = event("captured", &engine_name);
                    tokio::spawn(async move {
                        let _ = engine.send(Channel::Data, &payload).await;
                    });
                }
                Some("reuse") => {
                    let engine = engine.clone();
                    let engine_name = Arc::clone(&engine_name);
                    tokio::spawn(async move {
                        if let Some(responder) = message.responder {
                            let _ = responder.respond(&json!({"event": "reuse_response"})).await;
                            let kind = responder
                                .respond(&json!({"unexpected": true}))
                                .await
                                .err()
                                .map(|error| kind_name(error.kind()).to_owned())
                                .unwrap_or_else(|| "not_rejected".to_owned());
                            let payload = json!({
                                "event": "responder_reuse",
                                "engine": engine_name.as_str(), "kind": kind
                            });
                            let _ = engine.send(Channel::Data, &payload).await;
                        }
                    });
                }
                Some("engine_close") => {
                    let engine = engine.clone();
                    tokio::spawn(async move {
                        if let Some(responder) = message.responder {
                            let _ = responder.respond(&json!({"event": "closing"})).await;
                        }
                        let _ = engine.close().await;
                    });
                }
                _ => {}
            }
        }));
    }

    {
        let engine = engine.clone();
        let engine_name = Arc::clone(&engine_name);
        let terminal_kind = Arc::clone(&terminal_kind);
        engine.clone().on_error(Arc::new(move |error| {
            *lock(&terminal_kind) = Some(error.kind);
            if error.status == Some(StatusCode::ErrFragmentTimeout) {
                let engine = engine.clone();
                let payload = json!({
                    "event": "fragment_timeout",
                    "engine": engine_name.as_str(),
                    "code": StatusCode::ErrFragmentTimeout as u32
                });
                tokio::spawn(async move {
                    let _ = engine.send(Channel::Data, &payload).await;
                });
            }
        }));
    }

    {
        let engine = engine.clone();
        let scenario = Arc::clone(&scenario);
        let connection_count = Arc::clone(&connection_count);
        let terminal_kind = Arc::clone(&terminal_kind);
        let failure = Arc::clone(&failure);
        let finished = Arc::clone(&finished);
        engine.clone().on_session_disconnected(Arc::new(move |_| {
            if scenario.as_str() == "reconnect" && connection_count.load(Ordering::Acquire) == 1 {
                let engine = engine.clone();
                let failure = Arc::clone(&failure);
                let finished = Arc::clone(&finished);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    if let Err(error) = engine.connect().await {
                        *lock(&failure) = Some(error.to_string());
                        finished.notify_one();
                    }
                });
                return;
            }
            let observed = *lock(&terminal_kind);
            if scenario.as_str() == "oversize" && observed != Some(ErrorKind::Protocol) {
                *lock(&failure) = Some(format!("oversize terminal kind was {observed:?}"));
            } else if scenario.as_str() == "backpressure"
                && observed != Some(ErrorKind::Backpressure)
            {
                *lock(&failure) = Some(format!("backpressure terminal kind was {observed:?}"));
            }
            finished.notify_one();
        }));
    }

    engine.connect().await.map_err(|error| error.to_string())?;
    finished.notified().await;
    engine.close().await.map_err(|error| error.to_string())?;
    let outcome = match lock(&failure).take() {
        Some(error) => Err(error),
        None => Ok(()),
    };
    outcome
}

/*
This private fixture supplies test-application scenario policy only. It uses
the public Rust Engine API and keeps reconnect explicit in the fixture.
*/
