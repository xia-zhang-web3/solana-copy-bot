use super::durable_transport_fixture::Fixture;
use crate::{DeliveryReceiver, TransportClass, TransportStage};
use anyhow::{Context, Result};
use copybot_config::{AssociationDeliveryConfig, DeliveryBudget};
use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
use futures_util::stream;
use prost::Message;
use std::{
    path::Path,
    process::{Child, Command},
    time::Duration,
};
use yellowstone_grpc_proto::prelude::*;

struct LocalRelay(Child);
impl Drop for LocalRelay {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn case(code: Option<tonic::Code>, count: usize) -> Result<serde_json::Value> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let messages = (1..=count)
        .map(|i| SubscribeUpdate {
            // Real framed protobuf pressure across HTTP2 and two 64KiB relay buffers.
            filters: vec!["load".repeat(4096)],
            update_oneof: Some(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
                slot: i as u64 + 100,
                parent_slot: i as u64 + 99,
                blockhash: bs58::encode(sha2::Sha256::digest((i as u64 + 100).to_le_bytes()))
                    .into_string(),
                parent_blockhash: bs58::encode(sha2::Sha256::digest((i as u64 + 99).to_le_bytes()))
                    .into_string(),
                ..Default::default()
            })),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let bytes = messages.iter().map(Message::encoded_len).sum::<usize>();
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Fixture {
                messages,
                terminal: code,
                reject_subscribe: None,
            }))
            .serve_with_incoming(incoming),
    );
    let directory = tempfile::tempdir()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let python = std::env::var("COPYBOT_LOCAL_PYTHON").unwrap_or_else(|_| "python3".into());
    let mut relay = LocalRelay(
        Command::new(python)
            .arg("-B")
            .arg(root.join("tools/tests/relay_loopback_fixture.py"))
            .arg("--directory")
            .arg(directory.path())
            .arg("--target-port")
            .arg(port.to_string())
            .spawn()?,
    );
    let ready = directory.path().join("ready.json");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("two local relays ready")?;
    let ready: serde_json::Value = serde_json::from_slice(&std::fs::read(ready)?)?;
    let mut c = copybot_config::IngestionConfig::default();
    c.yellowstone_x_token = "synthetic-local-only".into();
    c.yellowstone_program_ids = vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".into()];
    c.subscribe_program_ids = c.yellowstone_program_ids.clone();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = format!("http://127.0.0.1:{}", ready["port"].as_u64().unwrap());
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    c.yellowstone_reconnect_initial_ms = 10_000;
    c.yellowstone_reconnect_max_ms = 10_000;
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(16, 1 << 20),
        blocks: b(4096, 64 << 20),
        history: b(4096, 8 << 20),
        outputs: b(16, 1 << 20),
        queue: b(32, 4 << 20),
        inbox: b(64, 4 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 8 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1_000,
        sqlite_busy_ms: 100,
    });
    let mut receiver = DeliveryReceiver::start(&c, "local-two-relays".into(), None)?;
    let started = std::time::Instant::now();
    let parents = tokio::time::timeout(Duration::from_secs(30), async {
        let mut parents = 0;
        loop {
            let envelope = receiver
                .next()
                .await?
                .context("local delivery before terminal")?;
            match envelope.delivery.event {
                DeliveryEvent::Parent(parent) => {
                    parents += 1;
                    assert_eq!(parent.child.slot, parents + 100);
                }
                DeliveryEvent::Session(SessionGap::Transport | SessionGap::End) => break,
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(parents)
    })
    .await??;
    let snapshot = receiver.ingress_snapshot();
    receiver.stop();
    server.abort();
    assert_eq!(parents as usize, count);
    assert_eq!(snapshot.received_blocks, count as u64);
    let class = match code {
        Some(tonic::Code::Internal) => TransportClass::Internal,
        Some(tonic::Code::DataLoss) => TransportClass::DataLoss,
        None => TransportClass::End,
        _ => unreachable!(),
    };
    let stage = if code.is_some() {
        TransportStage::Stream
    } else {
        TransportStage::End
    };
    assert_eq!(snapshot.reconnect_classes[class as usize], 1);
    assert_eq!(snapshot.reconnect_stages[stage as usize], 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    std::fs::write(directory.path().join("DONE"), b"")?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while relay.0.try_wait()?.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let statuses = ["front", "backend"]
        .into_iter()
        .map(|role| -> Result<serde_json::Value> {
            Ok(serde_json::from_slice(&std::fs::read(
                directory
                    .path()
                    .join(role)
                    .join(format!("{role}-status.json")),
            )?)?)
        })
        .collect::<Result<Vec<_>>>()?;
    if let Ok(path) = std::env::var("RUN15_TRANSPORT_EVIDENCE") {
        let path = std::path::PathBuf::from(path).with_extension(format!("{:?}.json", code));
        std::fs::write(
            path,
            serde_json::to_vec_pretty(
                &serde_json::json!({"code": format!("{code:?}"), "parents": parents, "relays": statuses}),
            )?,
        )?;
    }
    assert!(
        statuses
            .iter()
            .all(|s| s["connections"] == 1 && s["failures"] == 0),
        "local relay terminal statuses: {statuses:?}"
    );
    assert!(statuses[1]["upstream_received_bytes"].as_u64().unwrap() >= bytes as u64);
    Ok(
        serde_json::json!({"code":format!("{code:?}"), "parents": parents,
        "protobuf_bytes":bytes, "duration_ms":started.elapsed().as_millis(), "relays":statuses,
        "external_provider_calls":0, "synthetic_local_stream":true}),
    )
}

use sha2::Digest;
#[tokio::test]
async fn genuine_tonic_two_relays_high_flow_preserves_frames_and_exact_terminal_codes() -> Result<()>
{
    let mut results = Vec::new();
    for (code, count) in [
        (Some(tonic::Code::Internal), 2048),
        (Some(tonic::Code::DataLoss), 32),
        (None, 32),
    ] {
        results.push(case(code, count).await?);
    }
    if let Ok(path) = std::env::var("RUN15_TRANSPORT_EVIDENCE") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({"status":"PASS","cases":results}))?,
        )?;
    }
    Ok(())
}
