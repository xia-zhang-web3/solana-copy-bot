//! Synthetic local identities and timestamps; saved facts only seed transaction shape.
use super::rpc;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::*;
use yellowstone_grpc_proto::prost_types::Timestamp;
pub const FIRST: u64 = 1000;
pub const OLD: u64 = 1001;
pub const RECOVERED: u64 = 1012;
pub const FRESH: u64 = 1100;
pub const LAST: u64 = 1119;
pub const STEP_NS: u64 = 263_157_895;
pub struct Corpus {
    pub template: SubscribeUpdateBlock,
    pub raw: Value,
    pub seed: Value,
    pub wallet: String,
    pub started: DateTime<Utc>,
}
impl Corpus {
    pub fn load() -> Result<Self> {
        let dir = std::path::PathBuf::from(std::env::var("INGRESS_CORPUS_DIR")?);
        let mut raw: Value = serde_json::from_slice(&std::fs::read(dir.join("anchor.json"))?)?;
        if raw.get("result").is_some() {
            raw = raw["result"].clone();
        }
        // Every background signer becomes a non-admitted synthetic identity.
        let foreign = bs58::encode([248u8; 32]).into_string();
        for (index, t) in raw["transactions"]
            .as_array_mut()
            .context("anchor txs")?
            .iter_mut()
            .enumerate()
        {
            let n = t["transaction"]["message"]["header"]["numRequiredSignatures"]
                .as_u64()
                .unwrap() as usize;
            for key in t["transaction"]["message"]["accountKeys"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .take(n)
            {
                *key = json!(foreign);
            }
            for (sig_n, sig) in t["transaction"]["signatures"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .enumerate()
            {
                let mut bytes = [248u8; 64];
                bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
                bytes[8..16].copy_from_slice(&(sig_n as u64).to_le_bytes());
                *sig = json!(bs58::encode(bytes).into_string());
            }
        }
        raw["parentSlot"] = json!(FIRST - 1);
        raw["previousBlockhash"] = json!(hash(FIRST - 1));
        raw["blockhash"] = json!(hash(FIRST));
        raw["blockHeight"] = json!(FIRST);
        let template = crate::source::http_recovery::normalize_confirmed_http_block(FIRST, &raw)?;
        let seed: Value = serde_json::from_slice(&std::fs::read(dir.join("buy.json"))?)?;
        ensure!(
            seed["meta"]["err"].is_null(),
            "saved BUY must be successful"
        );
        let wallet = seed["transaction"]["message"]["accountKeys"][0]
            .as_str()
            .context("seed wallet")?
            .to_owned();
        Ok(Self {
            template,
            raw,
            seed,
            wallet,
            started: match std::env::var("INGRESS_COMMON_START_UNIX_NS") {
                Ok(ns) => DateTime::from_timestamp_nanos(ns.parse::<i64>()?),
                Err(_) => Utc::now() + chrono::Duration::seconds(2),
            },
        })
    }
    pub fn latest(&self) -> u64 {
        let elapsed = (Utc::now() - self.started)
            .num_nanoseconds()
            .unwrap_or(0)
            .max(0) as u64;
        FIRST.saturating_add(elapsed / STEP_NS).min(LAST)
    }
    pub fn created(&self, slot: u64) -> DateTime<Utc> {
        self.started + chrono::Duration::nanoseconds(((slot - FIRST) * STEP_NS) as i64)
    }
    pub fn buy_raw(&self, slot: u64) -> Value {
        let mut value = self.seed.clone();
        value["slot"] = json!(slot);
        value["blockTime"] = json!(self.created(slot).timestamp());
        value["transactionIndex"] = json!(self.template.transactions.len());
        let mut signature = [247u8; 64];
        signature[..8].copy_from_slice(&slot.to_le_bytes());
        value["transaction"]["signatures"][0] = json!(bs58::encode(signature).into_string());
        value
    }
    pub fn transaction(&self, slot: u64) -> SubscribeUpdate {
        let mut tx = rpc::update(&self.buy_raw(slot)).unwrap();
        tx.transaction.as_mut().unwrap().index = self.template.transactions.len() as u64;
        let created = if slot == OLD {
            self.created(slot) - chrono::Duration::seconds(121)
        } else {
            self.created(slot)
        };
        SubscribeUpdate {
            created_at: Some(stamp(created)),
            update_oneof: Some(subscribe_update::UpdateOneof::Transaction(tx)),
            ..Default::default()
        }
    }
    pub fn block(&self, slot: u64) -> SubscribeUpdateBlock {
        let mut block = self.template.clone();
        block.slot = slot;
        block.parent_slot = slot - 1;
        block.blockhash = hash(slot);
        block.parent_blockhash = hash(slot - 1);
        block.block_height = Some(BlockHeight { block_height: slot });
        block.block_time = Some(UnixTimestamp {
            timestamp: self.created(slot).timestamp(),
        });
        if [OLD, RECOVERED, FRESH].contains(&slot) {
            let tx = rpc::update(&self.buy_raw(slot)).unwrap();
            let mut info = tx.transaction.unwrap();
            info.index = block.transactions.len() as u64;
            block.transactions.push(info);
            block.executed_transaction_count += 1;
        }
        block
    }
    pub fn raw(&self, slot: u64) -> Value {
        let mut raw = self.raw.clone();
        raw["parentSlot"] = json!(slot - 1);
        raw["previousBlockhash"] = json!(hash(slot - 1));
        raw["blockhash"] = json!(hash(slot));
        raw["blockHeight"] = json!(slot);
        raw["blockTime"] = json!(self.created(slot).timestamp());
        if [OLD, RECOVERED, FRESH].contains(&slot) {
            raw["transactions"]
                .as_array_mut()
                .unwrap()
                .push(self.buy_raw(slot));
        }
        raw
    }
    pub fn update(&self, slot: u64) -> SubscribeUpdate {
        use prost::Message;
        let mut value = SubscribeUpdate {
            created_at: Some(stamp(self.created(slot))),
            update_oneof: Some(subscribe_update::UpdateOneof::Block(self.block(slot))),
            ..Default::default()
        };
        // 07 measured 291133175 received bytes/70 blocks; preserve real tx shape
        // and add declared synthetic wire padding to that observed 4.16MB envelope.
        let needed = 4_160_000usize.saturating_sub(value.encoded_len());
        value.filters.push("X".repeat(needed));
        value
    }
    pub fn signature(&self, slot: u64) -> String {
        self.buy_raw(slot)["transaction"]["signatures"][0]
            .as_str()
            .unwrap()
            .into()
    }
}
pub fn stamp(t: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: t.timestamp(),
        nanos: t.timestamp_subsec_nanos() as i32,
    }
}
pub fn hash(slot: u64) -> String {
    let mut b = [246u8; 32];
    b[..8].copy_from_slice(&slot.to_le_bytes());
    bs58::encode(b).into_string()
}
