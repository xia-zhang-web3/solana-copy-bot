//! Local tonic fixture with a precise terminal boundary, no external service.
use futures_util::stream;
use prost::Message;
use sha2::{Digest, Sha256};
use std::pin::Pin;
use yellowstone_grpc_proto::prelude::*;
#[derive(Clone)]
pub(super) struct Fixture {
    pub corpus: std::sync::Arc<super::corpus::Corpus>,
    pub subscriptions: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub offered: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}
#[tonic::async_trait]
impl geyser_server::Geyser for Fixture {
    type SubscribeStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;
    type SubscribeDeshredStream = Pin<
        Box<dyn futures_util::Stream<Item = Result<SubscribeUpdateDeshred, tonic::Status>> + Send>,
    >;
    async fn subscribe(
        &self,
        request: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let req = request.into_inner().message().await?.unwrap();
        assert_eq!(req.commitment, Some(CommitmentLevel::Confirmed as i32));
        assert_eq!(req.blocks.len(), 1);
        assert_eq!(req.from_slot, None, "HTTP mode never replay-gRPC");
        let number = self
            .subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let corpus = self.corpus.clone();
        let offered = self.offered.clone();
        let first = if number == 0 {
            super::corpus::FIRST
        } else {
            super::corpus::FIRST + 16
        };
        // Test-only finite queue: producer deadlines and exact protobuf bytes are
        // independent of HTTP/2 polling/consumer ACK. At 120 padded blocks the
        // declared fixture maximum is ~500 MB; it is not runtime queue headroom.
        let (send, receive) = tokio::sync::mpsc::channel(
            ((super::corpus::LAST - super::corpus::FIRST + 1) * 2) as usize,
        );
        tokio::spawn(async move {
            for slot in first..=super::corpus::LAST {
                let due = corpus.created(slot);
                if let Ok(wait) = (due - chrono::Utc::now()).to_std() {
                    tokio::time::sleep(wait).await;
                }
                if number == 0 && slot == super::corpus::FIRST + 9 {
                    offered.lock().unwrap().push(serde_json::json!({
                        "subscription":number,"slot":slot,"kind":"controlled_disconnect",
                        "created_unix_ns":due.timestamp_nanos_opt().unwrap(),
                        "offered_unix_ns":chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                        "code":"DataLoss","synthetic_historical_internal_not_reproduced":true
                    }));
                    let _ = send
                        .send(Err(tonic::Status::data_loss(
                            "synthetic controlled disconnect",
                        )))
                        .await;
                    break;
                }
                let selected = [super::corpus::OLD, super::corpus::FRESH].contains(&slot);
                let mut updates = vec![];
                if selected {
                    updates.push(("tx", corpus.transaction(slot)));
                }
                updates.push(("block", corpus.update(slot)));
                for (kind, update) in updates {
                    let bytes = update.encode_to_vec();
                    offered.lock().unwrap().push(serde_json::json!({
                        "subscription":number,"slot":slot,"kind":kind,
                        "created_unix_ns":due.timestamp_nanos_opt().unwrap(),
                        "offered_unix_ns":chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                        "protobuf_bytes":bytes.len(),
                        "protobuf_sha256":format!("{:x}",Sha256::digest(&bytes))
                    }));
                    if send.send(Ok(update)).await.is_err() {
                        return;
                    }
                }
            }
        });
        let stream = stream::unfold(receive, |mut receive| async move {
            match receive.recv().await {
                Some(update) => Some((update, receive)),
                None => {
                    std::future::pending::<()>().await;
                    None
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn subscribe_deshred(
        &self,
        _: tonic::Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeDeshredStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn subscribe_replay_info(
        &self,
        _: tonic::Request<SubscribeReplayInfoRequest>,
    ) -> Result<tonic::Response<SubscribeReplayInfoResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn ping(
        &self,
        _: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PongResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_latest_blockhash(
        &self,
        _: tonic::Request<GetLatestBlockhashRequest>,
    ) -> Result<tonic::Response<GetLatestBlockhashResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_block_height(
        &self,
        _: tonic::Request<GetBlockHeightRequest>,
    ) -> Result<tonic::Response<GetBlockHeightResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_slot(
        &self,
        _: tonic::Request<GetSlotRequest>,
    ) -> Result<tonic::Response<GetSlotResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn is_blockhash_valid(
        &self,
        _: tonic::Request<IsBlockhashValidRequest>,
    ) -> Result<tonic::Response<IsBlockhashValidResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_version(
        &self,
        _: tonic::Request<GetVersionRequest>,
    ) -> Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
}
