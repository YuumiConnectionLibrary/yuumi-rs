use std::sync::Arc;

use serde_json::json;
use yuumi::{Engine, EngineConfig, MessageEvent, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let engine = Engine::new(EngineConfig::new(
        "example",
        "0123456789abcdef0123456789abcdef",
    ));

    engine.on_session_connected(Arc::new(|session| {
        println!("connected: {} epoch={}", session.session_id, session.epoch);
    }));

    engine.on_message(Arc::new(|event: MessageEvent| {
        if let Some(responder) = event.responder {
            tokio::spawn(async move {
                if let Err(error) = responder.respond(&json!({"ok": true})).await {
                    eprintln!("response failed: {error}");
                }
            });
        }
    }));

    engine.on_error(Arc::new(|error| {
        eprintln!("{:?}: {}", error.kind, error.cause);
    }));

    engine.on_session_disconnected(Arc::new(|event| {
        println!("disconnected: {:?}", event.terminal.reason);
    }));

    engine.connect().await?;
    run_application_loop().await;
    engine.close().await
}

async fn run_application_loop() {}
