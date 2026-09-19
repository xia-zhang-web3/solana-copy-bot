//! Network-free persistent consumer for Python worker boundary tests.
#[path = "forward_capture/fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_config::IngestionConfig;
use copybot_ingestion::capture_replay::CaptureReplay;
use prost::Message;
use serde_json::{json, Value};
use std::io::{BufRead, Write};

#[tokio::test]
#[ignore = "explicit task-owned database and stdin controller required"]
async fn forward_capture_worker_harness() -> Result<()> {
    let path = std::env::var("FORWARD_CAPTURE_DB")?;
    let mut config = IngestionConfig::default();
    config.source = "yellowstone_grpc".into();
    config.yellowstone_delivery_mode = "legacy".into();
    config.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.yellowstone_x_token = "offline-fixture".into();
    config.capture_scope_db = Some(path);
    let mut consumer = CaptureReplay::open(&config).await?;
    println!("FORWARD_RESULT={}", json!({"status":"READY"}));
    std::io::stdout().flush()?;
    for line in std::io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line?)?;
        let result = match request["action"].as_str().unwrap() {
            "ack" => { consumer.accept_pending().await?; json!({"status":"ACK"}) }
            "restart" => { drop(consumer); consumer = CaptureReplay::open(&config).await?; json!({"status":"RESTARTED"}) }
            "push" => {
                let update = fixture::update(&request["fixture"]);
                let event = consumer.push(&update.encode_to_vec()).await?;
                json!({"status":"PUSHED","event":event})
            }
            "stop" => break,
            _ => anyhow::bail!("unknown offline command"),
        };
        println!("FORWARD_RESULT={result}");
        std::io::stdout().flush()?;
    }
    Ok(())
}
