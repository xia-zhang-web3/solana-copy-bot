//! Opt-in scoped durability before legacy dedupe/reorder/best-effort persistence.
//! A single awaited blocking task owns each receive; no unbounded work queue.
use super::{
    yellowstone::parse_yellowstone_update, YellowstoneParsedUpdate, YellowstoneRuntimeConfig,
};
use anyhow::{ensure, Result};
use copybot_storage_core::capture_scope::CaptureStore;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
    prost::Message,
};

pub(crate) struct ScopedCapture {
    store: Mutex<CaptureStore>,
    failed: AtomicBool,
}
impl ScopedCapture {
    pub fn open(path: &str) -> Result<Self> {
        Ok(Self {
            store: Mutex::new(CaptureStore::open(path.as_ref())?),
            failed: AtomicBool::new(false),
        })
    }
    pub fn healthy(&self) -> Result<()> {
        ensure!(
            !self.failed.load(Ordering::SeqCst),
            "scoped capture stopped; coverage incomplete"
        );
        Ok(())
    }
    async fn fail(self: &Arc<Self>) {
        self.failed.store(true, Ordering::SeqCst);
        let owner = self.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(mut store) = owner.store.lock() {
                let _ = store.gap("capture_io_or_integrity_failure", true);
            }
        })
        .await;
    }
}
fn timestamp() -> f64 {
    chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0
}

pub(crate) async fn restore(runtime: Arc<YellowstoneRuntimeConfig>) -> Result<()> {
    let Some(capture) = runtime.capture.clone() else {
        return Ok(());
    };
    let owner = capture.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        owner.healthy()?;
        let mut store = owner
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("capture lock poisoned"))?;
        store.start()?;
        for (seq, bytes, _source_at) in store.pending()? {
            let update = SubscribeUpdate::decode(bytes.as_slice())?;
            finish(
                &mut store,
                seq,
                &parse_yellowstone_update(update, &runtime),
                &runtime,
            )?;
        }
        Ok(())
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|value| value);
    if result.is_err() {
        capture.fail().await;
    }
    result
}

pub(crate) async fn refresh(runtime: Arc<YellowstoneRuntimeConfig>) -> Result<()> {
    let Some(capture) = runtime.capture.clone() else {
        return Ok(());
    };
    let owner = capture.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        owner.healthy()?;
        owner
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("capture lock poisoned"))?
            .accept_pending(timestamp())
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|value| value);
    if result.is_err() {
        capture.fail().await;
    }
    result
}

pub(crate) async fn gap(runtime: Arc<YellowstoneRuntimeConfig>) -> Result<()> {
    let Some(capture) = runtime.capture.clone() else {
        return Ok(());
    };
    let owner = capture.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        owner
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("capture lock poisoned"))?
            .gap("provider_stream_discontinuity", false)
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|value| value);
    if result.is_err() {
        capture.fail().await;
    }
    result
}

/// Same entrypoint used by the real stream and the offline saved-fixture replay.
pub(crate) async fn process(
    update: SubscribeUpdate,
    runtime: Arc<YellowstoneRuntimeConfig>,
) -> Result<Result<Option<YellowstoneParsedUpdate>>> {
    let Some(capture) = runtime.capture.clone() else {
        return Ok(parse_yellowstone_update(update, &runtime));
    };
    let received_at = timestamp();
    let owner = capture.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_> {
        owner.healthy()?;
        let mut store = owner
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("capture lock poisoned"))?;
        let mut receipt = None;
        if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() {
            let info = tx.transaction.as_ref();
            let wallet = info
                .and_then(|i| i.transaction.as_ref())
                .and_then(|t| t.message.as_ref())
                .and_then(|m| m.account_keys.first())
                .filter(|k| k.len() == 32)
                .map(|k| bs58::encode(k).into_string());
            if let Some(wallet) = wallet {
                let sig = info.and_then(super::yellowstone_proto::decode_signature_from_proto);
                let source_at = update
                    .created_at
                    .as_ref()
                    .map(|t| t.seconds as f64 + t.nanos as f64 / 1e9);
                ensure!(
                    update.encoded_len() <= 8_388_608,
                    "capture envelope size bound exceeded"
                );
                receipt = store.receive(
                    sig.as_deref(),
                    &wallet,
                    tx.slot,
                    &update.encode_to_vec(),
                    &format!("{:x}", Sha256::digest(tx.encode_to_vec())),
                    received_at,
                    source_at,
                )?;
            } else {
                // Cannot prove this unidentified frame is outside a protected wallet.
                store.gap("unidentified_received_transaction", false)?;
            }
        }
        let parsed = parse_yellowstone_update(update, &runtime);
        if let Some(receipt) = receipt {
            if receipt.stage == "RECEIVED" {
                finish(&mut store, receipt.seq, &parsed, &runtime)?;
            }
        }
        Ok(parsed)
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|value| value);
    if result.is_err() {
        capture.fail().await;
    }
    result
}
fn finish(
    store: &mut CaptureStore,
    seq: i64,
    parsed: &Result<Option<YellowstoneParsedUpdate>>,
    runtime: &YellowstoneRuntimeConfig,
) -> Result<()> {
    let event = match parsed {
        Ok(Some(YellowstoneParsedUpdate::Observation(raw))) => crate::parser::SwapParser::new(
            runtime.raydium_program_ids.iter().cloned().collect(),
            runtime.pumpswap_program_ids.iter().cloned().collect(),
        )
        .parse(raw.clone()),
        _ => None,
    };
    let reason = match parsed {
        Err(_) => "decoder_error",
        Ok(None) => "decoder_rejected_or_unavailable_time",
        _ if event.is_none() => "swap_parser_rejected",
        _ => "decoded_and_committed",
    };
    store.finish(seq, event.as_ref(), reason)
}
