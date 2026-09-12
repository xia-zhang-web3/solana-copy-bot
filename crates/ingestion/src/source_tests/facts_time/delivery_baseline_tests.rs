use super::*;
use futures_util::stream;
use prost::Message;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use yellowstone_grpc_proto::prelude::*;
#[derive(Clone)]
struct Fixture(Arc<Mutex<Vec<SubscribeUpdate>>>, bool);
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
        let messages = self.0.lock().unwrap().clone();
        Ok(tonic::Response::new(Box::pin(
            stream::iter(messages.into_iter().map(Ok)).chain(stream::pending()),
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
    let messages = ["missing", "block"]
        .into_iter()
        .map(|name| {
            SubscribeUpdate::decode(
                std::fs::read(dir.join(format!("{name}.pb")))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap()
        })
        .collect();
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
