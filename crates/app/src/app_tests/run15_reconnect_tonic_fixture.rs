//! Loopback transport with an injected break after a durable follower BUY anchor.
//! Source Info is captured; skipped slots/foreign filler/receipts are explicit models.
use super::run15_full_path_frames as frames;
use anyhow::{Context, Result};
use futures_util::stream;
use prost::Message;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use yellowstone_grpc_proto::prelude::*;
#[derive(Default)]
pub(super) struct Control {
    pub requests: Mutex<Vec<Option<u64>>>,
    pub bought: AtomicBool,
    pub anchor_durable: AtomicBool,
    pub done: AtomicBool,
    pub tail: Mutex<Vec<SubscribeUpdate>>,
}
#[derive(Clone)]
pub(super) struct Fixture {
    pub control: Arc<Control>,
    pub source: SubscribeUpdate,
    pub late_tx: SubscribeUpdate,
    pub middle: Vec<SubscribeUpdate>,
    pub follower: SubscribeUpdate,
    pub sell: SubscribeUpdate,
    pub http_mode: bool,
}
impl Fixture {
    pub fn blocks(&self) -> Vec<SubscribeUpdateBlock> {
        let mut updates = vec![self.source.clone()];
        updates.extend(self.middle.clone());
        updates.push(self.follower.clone());
        updates.push(self.sell.clone());
        updates.extend(self.control.tail.lock().unwrap().clone());
        updates
            .into_iter()
            .filter_map(|u| match u.update_oneof {
                Some(subscribe_update::UpdateOneof::Block(b)) => Some(b),
                _ => None,
            })
            .collect()
    }
    pub fn build(input: &std::path::Path, model: &serde_json::Value) -> Result<Self> {
        let (tx, source) = frames::source_buy(input)?;
        let mut source = SubscribeUpdate::decode(source.as_slice())?;
        let Some(subscribe_update::UpdateOneof::Block(b)) = source.update_oneof.as_mut() else {
            unreachable!()
        };
        let captured = b.transactions.pop().context("captured source Info")?;
        assert_eq!(captured.index, 695);
        // Complete fixture cardinality, not a claim that these 695 votes were captured.
        for index in 0..695 {
            let mut filler = captured.clone();
            filler.index = index;
            filler.is_vote = true;
            let mut signature = vec![77; 64];
            signature[..8].copy_from_slice(&index.to_le_bytes());
            filler.signature = signature;
            if let Some(message) = filler.transaction.as_mut().and_then(|t| t.message.as_mut()) {
                message.account_keys[0] = vec![77; 32];
            }
            b.transactions.push(filler);
        }
        b.transactions.push(captured);
        let (_, sell, _, parent_hash) = frames::mixed(input)?;
        let (_, follower) = frames::follower(model, &parent_hash)?;
        let middle = [
            frames::block(
                frames::SOURCE_SLOT + 1,
                frames::hash(frames::SOURCE_SLOT + 1),
                frames::hash(frames::SOURCE_SLOT),
                vec![],
                0,
            ),
            frames::block(
                frames::BOT_SLOT - 1,
                frames::hash(frames::BOT_SLOT - 1),
                frames::hash(frames::SOURCE_SLOT + 1),
                vec![],
                0,
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, raw)| -> Result<_> {
            let mut u = SubscribeUpdate::decode(raw.as_slice())?;
            if i == 1 {
                let Some(subscribe_update::UpdateOneof::Block(b)) = u.update_oneof.as_mut() else {
                    unreachable!()
                };
                b.parent_slot = frames::SOURCE_SLOT + 1;
            }
            Ok(u)
        })
        .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            control: Arc::new(Control::default()),
            http_mode: false,
            source,
            late_tx: SubscribeUpdate::decode(tx.as_slice())?,
            middle,
            follower: SubscribeUpdate::decode(follower.as_slice())?,
            sell: SubscribeUpdate::decode(sell.as_slice())?,
        })
    }
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
    ) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let request = request.into_inner().message().await?.unwrap();
        assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
        let block = request.blocks.values().next().unwrap();
        assert!(block.account_include.is_empty());
        assert_eq!(block.include_transactions, Some(true));
        let connection = {
            let mut requests = self.control.requests.lock().unwrap();
            requests.push(request.from_slot);
            requests.len()
        };
        let fixture = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            let send = |u| async { tx.send(Ok(u)).await };
            if connection == 1 {
                assert_eq!(request.from_slot, None);
                if send(fixture.source.clone()).await.is_err() {
                    return;
                }
                if send(fixture.late_tx.clone()).await.is_err() {
                    return;
                }
                while !fixture.control.bought.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                for u in fixture
                    .middle
                    .iter()
                    .chain(std::iter::once(&fixture.follower))
                {
                    if send(u.clone()).await.is_err() {
                        return;
                    }
                }
                while !fixture.control.anchor_durable.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                let _ = tx
                    .send(Err(tonic::Status::internal(
                        "controlled break after committed follower BUY",
                    )))
                    .await;
                return;
            }
            let floor = if fixture.http_mode {
                assert_eq!(
                    request.from_slot, None,
                    "HTTP mode sent rejected replay parameter"
                );
                frames::SELL_SLOT + fixture.control.tail.lock().unwrap().len() as u64 + 1
            } else {
                request
                    .from_slot
                    .expect("durable checkpoint must survive reconnect/restart")
            };
            if connection == 2 && !fixture.http_mode {
                assert_eq!(
                    floor,
                    frames::BOT_SLOT - 1,
                    "replay did not use committed BUY parent"
                );
            }
            for u in std::iter::once(&fixture.source)
                .chain(fixture.middle.iter())
                .chain(std::iter::once(&fixture.follower))
                .chain(std::iter::once(&fixture.sell))
            {
                let Some(subscribe_update::UpdateOneof::Block(b)) = u.update_oneof.as_ref() else {
                    unreachable!()
                };
                if b.slot >= floor && send(u.clone()).await.is_err() {
                    return;
                }
            }
            let old = fixture.control.tail.lock().unwrap().clone();
            for u in old {
                let Some(subscribe_update::UpdateOneof::Block(b)) = u.update_oneof.as_ref() else {
                    unreachable!()
                };
                if b.slot >= floor && send(u).await.is_err() {
                    return;
                }
            }
            let mut slot = frames::SELL_SLOT + fixture.control.tail.lock().unwrap().len() as u64;
            let Some(subscribe_update::UpdateOneof::Block(b)) = fixture.sell.update_oneof.as_ref()
            else {
                unreachable!()
            };
            let mut parent = if slot == frames::SELL_SLOT {
                b.blockhash.clone()
            } else {
                frames::hash(slot)
            };
            while !fixture.control.done.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
                slot += 1;
                let hash = frames::hash(slot);
                let u = SubscribeUpdate::decode(
                    frames::block(slot, hash.clone(), parent, vec![], 0).as_slice(),
                )
                .unwrap();
                fixture.control.tail.lock().unwrap().push(u.clone());
                parent = hash;
                if send(u).await.is_err() {
                    return;
                }
            }
            // Finish is controlled by the consumer closing, not an unrelated EOF retry.
            tx.closed().await;
        });
        Ok(tonic::Response::new(Box::pin(stream::unfold(
            rx,
            |mut rx| async { rx.recv().await.map(|v| (v, rx)) },
        ))))
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
