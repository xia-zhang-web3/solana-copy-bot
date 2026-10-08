//! Immutable consumed09 inputs. Only modeled predecessor/tail are synthetic.
use anyhow::{ensure, Context, Result};
use copybot_config::{AppConfig, AssociationDeliveryConfig, DeliveryBudget, HttpRecoveryConfig};
use copybot_core_types::{association_delivery::*, association_parent::*};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    SqliteStore,
};
use prost::Message as _;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashSet},
    path::{Path, PathBuf},
};
use yellowstone_grpc_proto::prelude::*;

pub(super) const SLOT: u64 = 452055099;
pub(super) struct Saved {
    pub grpc: SubscribeUpdateBlock,
    pub http: SubscribeUpdateBlock,
    pub raw_http: Vec<u8>,
    pub bindings: Value,
}
pub(super) fn load() -> Result<Saved> {
    let path = PathBuf::from(std::env::var("COPYBOT_INDEX_PAIR09_DIR")?);
    let pins = [
        (
            "grpc_typed.pb",
            3091570,
            "26a1e19720861669417e68edceefa20b6a9bdef4281cfaf020055121d1566db1",
        ),
        (
            "grpc_ui_amount_bits.bin",
            87805,
            "49406a6fd4d71ea4be53799c27672e026d4f771c36b14af2293522da26a130b6",
        ),
        (
            "http_normalized.pb",
            3091564,
            "accec111484223849e31c32ca10551c34c5d8477fe135ae98101f68e7ca7eb9d",
        ),
        (
            "http_ui_amount_bits.bin",
            87805,
            "06bdb7c2f01d9ffccebc86e441e1260d8eb3518c31557b38614a5f392589d9a3",
        ),
        (
            "http_response.json",
            5022101,
            "16a6351214455f7c553592d0c2e41a980744e2dc148fad9e932ed50d31571c6c",
        ),
        (
            "http_attempt_1.json",
            5022101,
            "16a6351214455f7c553592d0c2e41a980744e2dc148fad9e932ed50d31571c6c",
        ),
        (
            "manifest.json",
            1458,
            "702392b443e93cf52749a0e8fa78242e38eed1f29b45094346fda09b0f89b975",
        ),
    ];
    let mut bindings = serde_json::Map::new();
    // All hashes are checked before any producer, broker or transport starts.
    for (file, size, hash) in pins {
        let bytes = std::fs::read(path.join(file))?;
        ensure!(
            bytes.len() == size && format!("{:x}", Sha256::digest(&bytes)) == hash,
            "saved09_input_binding:{file}"
        );
        bindings.insert(file.into(), json!({"bytes":size,"sha256":hash}));
    }
    let mut grpc =
        SubscribeUpdateBlock::decode(std::fs::read(path.join("grpc_typed.pb"))?.as_slice())?;
    let mut http =
        SubscribeUpdateBlock::decode(std::fs::read(path.join("http_normalized.pb"))?.as_slice())?;
    restore(
        &mut grpc,
        &std::fs::read(path.join("grpc_ui_amount_bits.bin"))?,
    )?;
    restore(
        &mut http,
        &std::fs::read(path.join("http_ui_amount_bits.bin"))?,
    )?;
    ensure!(
        grpc.slot == SLOT
            && http.slot == SLOT
            && grpc.transactions.len() == 1243
            && http.transactions.len() == 1243,
        "saved09_header_cardinality"
    );
    let raw_http = std::fs::read(path.join("http_response.json"))?;
    // Check the daemon's accepted normalizer against its saved normalized side,
    // after sidecar restoration, without replacing the runtime comparison.
    let raw: Value = serde_json::from_slice(&raw_http)?;
    let normalized = copybot_ingestion::normalize_confirmed_http_block(SLOT, &raw["result"])?;
    ensure!(
        normalized == http && floats(&normalized) == floats(&http),
        "saved09_http_normalizer_identity"
    );
    Ok(Saved {
        grpc,
        http,
        raw_http,
        bindings: Value::Object(bindings),
    })
}
fn floats(block: &SubscribeUpdateBlock) -> Vec<u64> {
    block
        .transactions
        .iter()
        .flat_map(|i| {
            i.meta.iter().flat_map(|m| {
                m.pre_token_balances
                    .iter()
                    .chain(&m.post_token_balances)
                    .filter_map(|b| b.ui_token_amount.as_ref().map(|u| u.ui_amount.to_bits()))
            })
        })
        .collect()
}
fn restore(block: &mut SubscribeUpdateBlock, bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() % 17 == 0, "float_sidecar_length");
    let mut seen = BTreeSet::new();
    for row in bytes.chunks_exact(17) {
        let pos = u32::from_le_bytes(row[..4].try_into()?) as usize;
        let side = row[4];
        let index = u32::from_le_bytes(row[5..9].try_into()?) as usize;
        let bits = u64::from_le_bytes(row[9..17].try_into()?);
        ensure!(
            side <= 1 && seen.insert((pos, side, index)),
            "float_sidecar_duplicate_or_side"
        );
        let meta = block
            .transactions
            .get_mut(pos)
            .and_then(|i| i.meta.as_mut())
            .context("float_sidecar_position")?;
        let balances = if side == 0 {
            &mut meta.pre_token_balances
        } else {
            &mut meta.post_token_balances
        };
        let amount = balances
            .get_mut(index)
            .and_then(|b| b.ui_token_amount.as_mut())
            .context("float_sidecar_row")?;
        let wire = amount.ui_amount.to_bits();
        // Typed prost reencoding may omit -0.0. The pinned saved09 corpus has
        // no negative zero; nonzero contradictions are always refused.
        ensure!(
            wire == bits || (wire == 0 && bits == (-0.0f64).to_bits()),
            "float_sidecar_contradiction"
        );
        amount.ui_amount = f64::from_bits(bits);
    }
    let expected = block
        .transactions
        .iter()
        .enumerate()
        .flat_map(|(pos, i)| {
            i.meta.iter().flat_map(move |m| {
                [&m.pre_token_balances, &m.post_token_balances]
                    .into_iter()
                    .enumerate()
                    .flat_map(move |(side, rows)| {
                        rows.iter().enumerate().filter_map(move |(row, b)| {
                            b.ui_token_amount.as_ref().map(|_| (pos, side as u8, row))
                        })
                    })
            })
        })
        .collect::<BTreeSet<_>>();
    ensure!(
        seen == expected && seen.len() == 5165,
        "float_sidecar_complete_coverage"
    );
    Ok(())
}
pub(super) fn empty_child(parent: &SubscribeUpdateBlock) -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot: parent.slot + 1,
        parent_slot: parent.slot,
        parent_blockhash: parent.blockhash.clone(),
        blockhash: bs58::encode([parent.slot as u8; 32]).into_string(),
        block_time: parent.block_time.as_ref().map(|t| UnixTimestamp {
            timestamp: t.timestamp + 1,
        }),
        block_height: parent.block_height.as_ref().map(|h| BlockHeight {
            block_height: h.block_height + 1,
        }),
        rewards: Some(Rewards::default()),
        ..Default::default()
    }
}
pub(super) fn predecessor(anchor: &SubscribeUpdateBlock) -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot: anchor.parent_slot,
        blockhash: anchor.parent_blockhash.clone(),
        parent_slot: anchor.parent_slot - 1,
        parent_blockhash: bs58::encode([241; 32]).into_string(),
        rewards: Some(Rewards::default()),
        ..Default::default()
    }
}
pub(super) fn config(grpc: &str, http: &str, evidence: &Path) -> AppConfig {
    let mut app = AppConfig::default();
    app.execution.canary_entry_submit_enabled = false;
    let c = &mut app.ingestion;
    c.source = "yellowstone_grpc".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    c.yellowstone_grpc_url = grpc.into();
    c.yellowstone_x_token = "offline-fixture-only".into();
    c.yellowstone_replay_wallets = vec![bs58::encode([242; 32]).into_string()];
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: budget(16, 16 << 20),
        blocks: budget(16, 64 << 20),
        history: budget(16, 8 << 20),
        outputs: budget(16, 8 << 20),
        queue: budget(16, 32 << 20),
        inbox: budget(10000, 32 << 20),
        input_bytes: 16 << 20,
        metadata_bytes: 16 << 20,
        pending_ttl_ms: 60000,
        block_ttl_ms: 60000,
        history_ttl_ms: 120000,
        tick_ms: 100,
        sqlite_busy_ms: 1000,
    });
    c.yellowstone_http_recovery = Some(HttpRecoveryConfig {
        raw_window_blocks: None,
        broker_url: http.into(),
        broker_token: String::new(),
        range_slots: 64,
        max_response_bytes: 16 << 20,
        timeout_ms: 2000,
        fetch_concurrency: 1,
        anchor_evidence_dir: Some(evidence.to_string_lossy().into()),
    });
    app
}
fn budget(count: usize, bytes: usize) -> DeliveryBudget {
    DeliveryBudget { count, bytes }
}
pub(super) fn initialize(
    path: &Path,
    app: &AppConfig,
    anchor: &SubscribeUpdateBlock,
) -> Result<()> {
    SqliteStore::open(path)?
        .run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    let scope = copybot_ingestion::replay_scope(
        &app.ingestion,
        &HashSet::from_iter(app.ingestion.yellowstone_replay_wallets.clone()),
    )?;
    let mut inbox = AssociationInbox::open_ordered_sell_consumer(
        path,
        InboxLimits {
            count: 10000,
            bytes: 32 << 20,
            busy_ms: 1000,
        },
    )?;
    inbox.configure_replay_scope(&scope)?;
    // Explicit modeled seed using the captured anchor's exact predecessor key.
    let predecessor = copybot_core_types::association_recovery::BlockCheckpoint {
        scope,
        observation: ParentObservation {
            child: BlockKey {
                slot: anchor.parent_slot,
                hash: anchor.parent_blockhash.clone(),
            },
            parent: BlockKey {
                slot: anchor.parent_slot - 1,
                hash: bs58::encode([241; 32]).into_string(),
            },
            issue: None,
        },
        executed_transaction_count: 0,
        supplied_transaction_count: 0,
        claims: vec![],
    };
    inbox.persist(
        &Delivery {
            session: "modeled-predecessor-only".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::ParentCheckpoint(predecessor),
        },
        &CandidateGeneration::Unknown,
    )?;
    Ok(())
}
