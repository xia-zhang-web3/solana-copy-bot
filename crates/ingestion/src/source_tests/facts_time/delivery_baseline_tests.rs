use super::*;
use futures_util::stream;
use prost::Message;
use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
};
use yellowstone_grpc_proto::prelude::*;
struct ScriptedStreams {
    first: Vec<SubscribeUpdate>,
    second: Vec<SubscribeUpdate>,
    subscriptions: AtomicUsize,
}
#[derive(Clone)]
struct Fixture(
    Arc<Mutex<Vec<SubscribeUpdate>>>,
    bool,
    bool,
    Option<Arc<ScriptedStreams>>,
);
#[tonic::async_trait]
impl geyser_server::Geyser for Fixture {
    type SubscribeStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;
    async fn subscribe(
        &self,
        request: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let req = request.into_inner().message().await?.unwrap();
        assert_eq!(req.commitment, Some(CommitmentLevel::Confirmed as i32));
        assert!(req.accounts.is_empty() && req.blocks_meta.is_empty() && req.entry.is_empty());
        let selected = req.transactions.values().next().unwrap();
        assert!(!selected.account_include.is_empty());
        if self.1 {
            assert_eq!(req.blocks.len(), 1);
            let block = req.blocks.values().next().unwrap();
            assert_eq!(block.include_transactions, Some(true));
            assert_eq!(block.include_accounts, Some(false));
            assert_eq!(block.include_entries, Some(false));
            let mut tx = selected.account_include.clone();
            tx.sort();
            let mut blocks = block.account_include.clone();
            blocks.sort();
            assert_eq!(tx, blocks);
        } else {
            assert!(req.blocks.is_empty());
        }
        let (messages, failure, pace_blocks) = if let Some(script) = self.3.as_ref() {
            if script.subscriptions.fetch_add(1, Ordering::Relaxed) == 0 {
                (script.first.clone(), true, false)
            } else {
                (script.second.clone(), false, true)
            }
        } else {
            (self.0.lock().unwrap().clone(), self.2, false)
        };
        let failure = failure.then(|| Err(tonic::Status::unavailable("fixture stream boundary")));
        Ok(tonic::Response::new(Box::pin(
            stream::iter(messages.into_iter().map(Ok).chain(failure))
                .then(move |item| async move {
                    if pace_blocks
                        && matches!(
                            &item,
                            Ok(SubscribeUpdate {
                                update_oneof: Some(subscribe_update::UpdateOneof::Block(_)),
                                ..
                            })
                        )
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                    }
                    item
                })
                .chain(stream::pending()),
        )))
    }
    async fn subscribe_replay_info(
        &self,
        _: tonic::Request<SubscribeReplayInfoRequest>,
    ) -> Result<tonic::Response<SubscribeReplayInfoResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn ping(
        &self,
        _: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PongResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_latest_blockhash(
        &self,
        _: tonic::Request<GetLatestBlockhashRequest>,
    ) -> Result<tonic::Response<GetLatestBlockhashResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_block_height(
        &self,
        _: tonic::Request<GetBlockHeightRequest>,
    ) -> Result<tonic::Response<GetBlockHeightResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_slot(
        &self,
        _: tonic::Request<GetSlotRequest>,
    ) -> Result<tonic::Response<GetSlotResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn is_blockhash_valid(
        &self,
        _: tonic::Request<IsBlockhashValidRequest>,
    ) -> Result<tonic::Response<IsBlockhashValidResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_version(
        &self,
        _: tonic::Request<GetVersionRequest>,
    ) -> Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
}
use futures_util::StreamExt;
#[tokio::test]
async fn b89_legacy_known_time_service_control() -> Result<()> {
    let mut missing = cases::base(true, false);
    missing.created_at = None;
    let mut healthy = cases::base(true, false);
    let subscribe_update::UpdateOneof::Transaction(tx) = healthy.update_oneof.as_mut().unwrap()
    else {
        panic!()
    };
    tx.transaction.as_mut().unwrap().signature = vec![89; 64];
    let control = bs58::encode(
        transaction(&healthy)
            .transaction
            .as_ref()
            .unwrap()
            .signature
            .clone(),
    )
    .into_string();
    let expected = bs58::encode(
        transaction(&missing)
            .transaction
            .as_ref()
            .unwrap()
            .signature
            .clone(),
    )
    .into_string();
    if let Ok(dir) = std::env::var("B89_EVIDENCE") {
        std::fs::write(
            format!("{dir}/baseline-missing.pb"),
            missing.encode_to_vec(),
        )?;
        std::fs::write(format!("{dir}/baseline-known.pb"), healthy.encode_to_vec())?;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Fixture(
                Arc::new(Mutex::new(vec![missing, healthy])),
                false,
                false,
                None,
            )))
            .serve_with_incoming(incoming),
    );
    let mut c = config();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = format!("http://{addr}");
    c.reorder_hold_ms = 0;
    let mut service = crate::IngestionService::build(&c)?;
    let swap = tokio::time::timeout(std::time::Duration::from_secs(5), service.next_swap())
        .await??
        .unwrap();
    server.abort();
    assert_eq!(swap.signature, control);
    assert_ne!(swap.signature,expected,"RED: accepted88 actual subscription -> IngestionService drops missing-time SELL; only known-time control reaches app boundary");
    Ok(())
}

#[tokio::test]
#[ignore = "coordinated local transport/app fixture; no external endpoints"]
async fn b89_loopback_server_fixture() -> Result<()> {
    let dir = std::path::PathBuf::from(std::env::var("B89_FIXTURE_DIR")?);
    let decode = |base: &std::path::Path, name: &str| -> Result<SubscribeUpdate> {
        Ok(SubscribeUpdate::decode(
            std::fs::read(base.join(format!("{name}.pb")))?.as_slice(),
        )?)
    };
    let scripted = std::env::var_os("B136_LOOPBACK_STREAMS_DIR")
        .map(|base| -> Result<_> {
            let base = std::path::PathBuf::from(base);
            let pre_source = if base.join("pre-source.pb").is_file() {
                "pre-source"
            } else {
                "source"
            };
            Ok(Arc::new(ScriptedStreams {
                first: [pre_source, "block-100"]
                    .into_iter()
                    .map(|name| decode(&base, name))
                    .collect::<Result<_>>()?,
                second: [
                    "source",
                    "block-100",
                    "cohort-our",
                    "cohort-block-120",
                    "sell",
                    "block-150",
                ]
                .into_iter()
                .map(|name| decode(&base, name))
                .collect::<Result<_>>()?,
                subscriptions: AtomicUsize::new(0),
            }))
        })
        .transpose()?;
    let messages = if scripted.is_some() {
        Vec::new()
    } else {
        ["missing", "block"]
            .into_iter()
            .map(|name| decode(&dir, name))
            .collect::<Result<_>>()?
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Fixture(
                Arc::new(Mutex::new(messages)),
                true,
                false,
                scripted,
            )))
            .serve_with_incoming(incoming),
    );
    std::fs::write(dir.join("loopback-url.txt"), format!("http://{addr}"))?;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !dir.join("loopback-done").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn durable_loopback_reports_funnel_parent_and_safe_stream_failure() -> Result<()> {
    use crate::{DeliveryReceiver, TransportClass, TransportStage};
    use copybot_config::{AssociationDeliveryConfig, DeliveryBudget};
    use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
    let update = cases::base(false, false);
    let tx = transaction(&update);
    let slot = tx.slot;
    let block = SubscribeUpdateBlock {
        slot: tx.slot,
        blockhash: bs58::encode([80u8; 32]).into_string(),
        parent_slot: tx.slot - 1,
        parent_blockhash: bs58::encode([79u8; 32]).into_string(),
        transactions: vec![tx.transaction.as_ref().unwrap().clone()],
        ..Default::default()
    };
    let block = SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(block)),
        ..Default::default()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Fixture(
                Arc::new(Mutex::new(vec![update, block])),
                true,
                true,
                None,
            )))
            .serve_with_incoming(incoming),
    );
    let mut c = config();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = format!("http://{addr}");
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(16, 1 << 20),
        blocks: b(8, 8 << 20),
        history: b(32, 2 << 20),
        outputs: b(8, 1 << 20),
        queue: b(16, 4 << 20),
        inbox: b(64, 4 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 8 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1_000,
        sqlite_busy_ms: 100,
    });
    let mut receiver = DeliveryReceiver::start(&c, "loopback".into(), None)?;
    let (admissions, parents) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut admissions = 0;
        let mut parents = 0;
        loop {
            let e = receiver.next().await?.expect("delivery before failure");
            match e.delivery.event {
                DeliveryEvent::Admission(_) => admissions += 1,
                DeliveryEvent::Parent(_) => parents += 1,
                DeliveryEvent::Session(SessionGap::Transport) => break,
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>((admissions, parents))
    })
    .await??;
    let s = receiver.ingress_snapshot();
    receiver.stop();
    server.abort();
    assert_eq!((admissions, parents), (1, 1));
    assert_eq!(
        (s.received_transactions, s.received_blocks, s.admissions),
        (1, 1, 1)
    );
    assert_eq!(s.last_parent_slot, slot);
    assert_eq!(s.last_received_block_slot, slot);
    assert_eq!(s.reconnect_stages[TransportStage::Stream as usize], 1);
    assert_eq!(s.reconnect_classes[TransportClass::Unavailable as usize], 1);
    Ok(())
}
