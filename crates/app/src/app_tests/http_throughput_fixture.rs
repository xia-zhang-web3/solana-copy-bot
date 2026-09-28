//! Loopback-only sustained source: captured bodies, modeled slots and cadence.
use anyhow::{Context, Result};
use futures_util::stream;
use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use yellowstone_grpc_proto::prelude::*;

pub(super) const COUNT: u64 = 60;
pub(super) const START_SLOT: u64 = 8_000_000;
pub(super) const INTERVAL_US: u64 = 333_333;
#[derive(Default)]
pub(super) struct Control {
    pub produced: AtomicU64,
    pub overflow: AtomicBool,
    pub done: AtomicBool,
    pub requests: AtomicU64,
    pub produced_at: Mutex<Vec<Instant>>,
}
#[derive(Clone)]
pub(super) struct Fixture {
    pub control: Arc<Control>,
    pub bodies: Vec<SubscribeUpdateBlock>,
    pub programs: Vec<Vec<u8>>,
}
fn hash(slot: u64) -> String {
    let mut bytes = [38; 32];
    bytes[..8].copy_from_slice(&slot.to_le_bytes());
    bs58::encode(bytes).into_string()
}
fn envelope(update: subscribe_update::UpdateOneof) -> SubscribeUpdate {
    let now = chrono::Utc::now();
    SubscribeUpdate {
        update_oneof: Some(update),
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        }),
        ..Default::default()
    }
}
impl Fixture {
    pub(super) fn selected(&self, info: &SubscribeUpdateTransactionInfo) -> bool {
        if info.is_vote || info.meta.as_ref().is_some_and(|m| m.err.is_some()) {
            return false;
        }
        let Some(message) = info.transaction.as_ref().and_then(|t| t.message.as_ref()) else {
            return false;
        };
        let loaded = info.meta.iter().flat_map(|m| {
            m.loaded_writable_addresses
                .iter()
                .chain(&m.loaded_readonly_addresses)
        });
        message
            .account_keys
            .iter()
            .chain(loaded)
            .any(|key| self.programs.contains(key))
    }
    fn batch(&self, ordinal: u64) -> Vec<SubscribeUpdate> {
        let mut block = self.bodies[ordinal as usize % self.bodies.len()].clone();
        block.slot = START_SLOT + ordinal;
        block.parent_slot = block.slot - 1;
        block.blockhash = hash(block.slot);
        block.parent_blockhash = hash(block.parent_slot);
        // Transaction metadata/signatures stay captured. Header identities and
        // repeated body cadence are explicit throughput models, never market facts.
        let mut batch = block
            .transactions
            .iter()
            .filter(|info| self.selected(info))
            .map(|info| {
                envelope(subscribe_update::UpdateOneof::Transaction(
                    SubscribeUpdateTransaction {
                        transaction: Some(info.clone()),
                        slot: block.slot,
                    },
                ))
            })
            .collect::<Vec<_>>();
        batch.push(envelope(subscribe_update::UpdateOneof::Block(block)));
        batch
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
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let request = request.into_inner().message().await?.unwrap();
        assert_eq!(request.from_slot, None);
        assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
        let filter = request.transactions.values().next().unwrap();
        assert_eq!(filter.vote, Some(false));
        assert_eq!(filter.failed, Some(false));
        let mut subscribed = filter
            .account_include
            .iter()
            .map(|key| bs58::decode(key).into_vec().unwrap())
            .collect::<Vec<_>>();
        let mut expected = self.programs.clone();
        subscribed.sort();
        expected.sort();
        assert_eq!(subscribed, expected);
        assert!(request
            .blocks
            .values()
            .next()
            .unwrap()
            .account_include
            .is_empty());
        self.control.requests.fetch_add(1, Ordering::SeqCst);
        // Four source block batches, a fixed 1.33s jitter budget. try_send keeps
        // the independent 3/s production clock honest: backpressure cannot slow it.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let fixture = self.clone();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now();
            for ordinal in 0..COUNT {
                tokio::time::sleep_until(start + Duration::from_micros(INTERVAL_US * ordinal))
                    .await;
                let batch = fixture.batch(ordinal);
                fixture
                    .control
                    .produced_at
                    .lock()
                    .unwrap()
                    .push(Instant::now());
                fixture.control.produced.fetch_add(1, Ordering::SeqCst);
                if tx.try_send(batch).is_err() {
                    fixture.control.overflow.store(true, Ordering::SeqCst);
                    break;
                }
            }
            fixture.control.done.store(true, Ordering::SeqCst);
            tx.closed().await;
        });
        let stream = stream::unfold(
            (rx, Vec::<SubscribeUpdate>::new().into_iter()),
            |(mut rx, mut current)| async move {
                loop {
                    if let Some(update) = current.next() {
                        return Some((Ok(update), (rx, current)));
                    }
                    current = rx.recv().await?.into_iter();
                }
            },
        );
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

pub(super) struct Relays {
    child: std::process::Child,
    directory: PathBuf,
    pub url: String,
}
impl Drop for Relays {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Relays {
    pub(super) async fn start(root: &Path, port: u16) -> Result<Self> {
        let directory = root.join("two-relays");
        let child = std::process::Command::new(std::env::var("COPYBOT_LOCAL_PYTHON")?)
            .arg("-B")
            .arg(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../tools/tests/relay_loopback_fixture.py"),
            )
            .arg("--directory")
            .arg(&directory)
            .arg("--target-port")
            .arg(port.to_string())
            .arg("--seconds")
            .arg("60")
            .arg("--socket-path")
            .arg(std::env::temp_dir().join(format!(
                "cb-throughput-{}-{}.sock",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            )))
            .spawn()?;
        let ready = directory.join("ready.json");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("throughput canonical relay startup")?;
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(ready)?)?;
        Ok(Self {
            child,
            directory,
            url: format!("http://127.0.0.1:{}", v["port"].as_u64().unwrap()),
        })
    }
    pub(super) async fn finish(&mut self) -> Result<serde_json::Value> {
        std::fs::write(self.directory.join("DONE"), b"")?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.child.try_wait()?.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        let mut states = serde_json::Map::new();
        for role in ["front", "backend"] {
            states.insert(
                role.into(),
                serde_json::from_slice(&std::fs::read(
                    self.directory
                        .join(role)
                        .join(format!("{role}-status.json")),
                )?)?,
            );
        }
        Ok(states.into())
    }
}

pub(super) fn bodies_from_http(directory: &Path) -> Result<Vec<SubscribeUpdateBlock>> {
    ["response-737.json", "response-779.json"]
        .iter()
        .map(|name| {
            let mut record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(directory.join(name))?)?;
            // These historical requests disabled block rewards. Preserve the
            // capture itself; this throughput-only header projection explicitly
            // models empty rewards and does not claim missing facts were zero.
            record["result"]["rewards"] = serde_json::json!([]);
            record["result"]["numRewardPartitions"] = serde_json::Value::Null;
            copybot_ingestion::normalize_confirmed_http_block(
                record["params"][0]
                    .as_u64()
                    .context("saved getBlock slot")?,
                &record["result"],
            )
        })
        .collect()
}
