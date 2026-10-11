//! Current, explicitly modeled protobuf operands, never relabeled historical data.
//! SOURCE daemon/signing and ingress are real local paths; external facts are mocks.
#[path = "../../../ingestion/src/source_tests/jupiter_raydium_fixture.rs"]
mod modeled;
use anyhow::{Context, Result};
use copybot_config::{AppConfig, NativeFreshBuyConfig, TechnicalCohortConfig};
use copybot_core_types::association_delivery::AdmissionFacts;
use prost::Message as _;
use serde_json::json;
use yellowstone_grpc_proto::prelude::*;
pub(super) const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub(super) const SOLD_SOL: u64 = 9_950_000;
pub(super) const RECEIVED: u64 = super::owner_roundtrip_fixture::RECEIVED;
fn bytes(s: &str) -> Vec<u8> {
    bs58::decode(s).into_vec().unwrap()
}
fn amount(row: &mut TokenBalance, n: u64, decimals: u32) {
    let a = row.ui_token_amount.as_mut().unwrap();
    a.amount = n.to_string();
    a.decimals = decimals;
    a.ui_amount = n as f64 / 10_f64.powi(decimals as i32);
    a.ui_amount_string = a.ui_amount.to_string();
}
fn roles(t: &mut SubscribeUpdateTransaction, wallet: &str) {
    use crate::execution_pumpswap_accounts::{
        associated_token_address, parse_pubkey, token_program_id,
    };
    let key = parse_pubkey(wallet, "model_wallet").unwrap();
    let mint = parse_pubkey(USDC, "model_mint").unwrap();
    let sol = parse_pubkey(modeled::SOL, "model_sol").unwrap();
    let tx = t.transaction.as_mut().unwrap();
    let keys = &mut tx
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .account_keys;
    keys[0] = bytes(wallet);
    keys[1] = associated_token_address(&key, &mint, &token_program_id()).to_vec();
    keys[2] = associated_token_address(&key, &sol, &token_program_id()).to_vec();
    keys[3] = bytes(USDC);
    let meta = tx.meta.as_mut().unwrap();
    for row in meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances)
    {
        if row.owner == bs58::encode([11; 32]).into_string() {
            row.owner = wallet.into();
        }
        if row.mint == modeled::MINT {
            row.mint = USDC.into();
            let n = row
                .ui_token_amount
                .as_ref()
                .unwrap()
                .amount
                .parse()
                .unwrap();
            amount(row, n, 6);
        }
    }
    for group in &mut meta.inner_instructions {
        for ix in &mut group.instructions {
            if ix.program_id_index == 5 && ix.data.first() == Some(&18) {
                ix.data[1..].copy_from_slice(&bytes(wallet));
            }
        }
    }
}
pub(super) fn buy(wallet: &str, signature: &str, slot: u64) -> SubscribeUpdateTransaction {
    // The decoded operands are deliberately modeled, not an on-chain message claim.
    // Their wallet/signature/slot/actual quantity are tied to the local OWNER receipt.
    let mut t = modeled::buy(true, true, 7, 10_000_000);
    roles(&mut t, wallet);
    t.slot = slot;
    let tx = t.transaction.as_mut().unwrap();
    tx.signature = bytes(signature);
    tx.transaction.as_mut().unwrap().signatures = vec![bytes(signature)];
    let meta = tx.meta.as_mut().unwrap();
    for row in &mut meta.post_token_balances {
        if row.mint == USDC {
            amount(
                row,
                if row.account_index == 1 {
                    RECEIVED
                } else {
                    500_000_000 - RECEIVED
                },
                6,
            );
        }
    }
    let group = meta
        .inner_instructions
        .iter_mut()
        .find(|g| g.index == 4)
        .unwrap();
    group.instructions[0].data[9..].copy_from_slice(&(RECEIVED * 9950 / 10000).to_le_bytes());
    group.instructions[2].data[1..].copy_from_slice(&RECEIVED.to_le_bytes());
    let event = &mut group.instructions[3].data;
    event[88..120].copy_from_slice(&bytes(USDC));
    event[120..128].copy_from_slice(&RECEIVED.to_le_bytes());
    tx.transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .instructions[4]
        .data[24..32]
        .copy_from_slice(&RECEIVED.to_le_bytes());
    t
}
pub(super) fn sell(wallet: &str, slot: u64) -> SubscribeUpdateTransaction {
    let mut t = modeled::buy(false, true, 105, 10_000_000);
    roles(&mut t, wallet);
    t.slot = slot;
    let tx = t.transaction.as_mut().unwrap();
    tx.signature = vec![43; 64];
    tx.transaction.as_mut().unwrap().signatures = vec![vec![43; 64]];
    let message = tx.transaction.as_mut().unwrap().message.as_mut().unwrap();
    let mut route = message.instructions[4].clone();
    // Reverse vault orientation: coin WSOL, pc USDC; SOURCE owns only the USDC ATA.
    route.accounts = vec![5, 0, 1, 2, 8, 4, 8, 9, 8, 10, 5, 11, 12, 14, 15, 1, 2, 0];
    route.data[16..24].copy_from_slice(&RECEIVED.to_le_bytes());
    route.data[24..32].copy_from_slice(&SOLD_SOL.to_le_bytes());
    let mut floor = message.instructions[2].clone();
    floor.accounts = vec![0, 0];
    floor.data[4..].copy_from_slice(&985_000_000_u64.to_le_bytes());
    message.instructions = vec![
        message.instructions[0].clone(),
        route,
        message.instructions[5].clone(),
        floor,
    ];
    let meta = tx.meta.as_mut().unwrap();
    let mut creation = meta.inner_instructions[0].clone();
    creation.index = 0;
    let mut swap = meta
        .inner_instructions
        .iter()
        .find(|g| g.index == 4)
        .unwrap()
        .clone();
    swap.index = 1;
    swap.instructions[0].accounts = vec![5, 11, 12, 14, 15, 1, 2, 0];
    swap.instructions[0].data[1..9].copy_from_slice(&RECEIVED.to_le_bytes());
    swap.instructions[0].data[9..].copy_from_slice(&(SOLD_SOL * 9950 / 10000).to_le_bytes());
    swap.instructions[1].accounts = vec![1, 15, 0];
    swap.instructions[1].data[1..].copy_from_slice(&RECEIVED.to_le_bytes());
    swap.instructions[2].accounts = vec![14, 2, 12];
    swap.instructions[2].data[1..].copy_from_slice(&SOLD_SOL.to_le_bytes());
    let event = &mut swap.instructions[3].data;
    event[48..80].copy_from_slice(&bytes(USDC));
    event[80..88].copy_from_slice(&RECEIVED.to_le_bytes());
    event[88..120].copy_from_slice(&bytes(modeled::SOL));
    event[120..128].copy_from_slice(&SOLD_SOL.to_le_bytes());
    meta.inner_instructions = vec![creation, swap];
    let mut own = meta
        .pre_token_balances
        .iter()
        .find(|r| r.account_index == 1)
        .unwrap()
        .clone();
    amount(&mut own, RECEIVED, 6);
    let mut coin = meta
        .pre_token_balances
        .iter()
        .find(|r| r.account_index == 14)
        .unwrap()
        .clone();
    coin.mint = modeled::SOL.into();
    amount(&mut coin, 1_000_000_000, 9);
    let mut pc = meta
        .pre_token_balances
        .iter()
        .find(|r| r.account_index == 15)
        .unwrap()
        .clone();
    pc.mint = USDC.into();
    amount(&mut pc, 500_000_000, 6);
    meta.pre_token_balances = vec![own.clone(), coin.clone(), pc.clone()];
    amount(&mut own, 0, 6);
    amount(&mut coin, 1_000_000_000 - SOLD_SOL, 9);
    amount(&mut pc, 500_000_000 + RECEIVED, 6);
    meta.post_token_balances = vec![own, coin, pc];
    meta.pre_balances.fill(0);
    meta.post_balances.fill(0);
    meta.pre_balances[0] = 1_000_000_000;
    meta.post_balances[0] = 1_000_000_000 + SOLD_SOL - meta.fee;
    for n in [1, 15] {
        meta.pre_balances[n] = 2_039_280;
        meta.post_balances[n] = 2_039_280;
    }
    meta.pre_balances[14] = 1_002_039_280;
    meta.post_balances[14] = 1_002_039_280 - SOLD_SOL;
    t
}
pub(super) fn frames(t: SubscribeUpdateTransaction) -> Vec<Vec<u8>> {
    let slot = t.slot;
    let info = t.transaction.clone().unwrap();
    let update = SubscribeUpdate {
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        }),
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(t)),
        ..Default::default()
    };
    vec![
        update.encode_to_vec(),
        super::run15_full_path_frames::block(
            slot,
            super::run15_full_path_frames::hash(slot),
            super::run15_full_path_frames::hash(slot - 1),
            vec![info],
            1,
        ),
    ]
}
pub(super) fn config(case: &super::owner_source_gate_fixture::GateCase) -> Result<AppConfig> {
    let p = case.source.config.owner_technical_buy.as_ref().unwrap();
    let g = p.source_copy_exit_gate.as_ref().unwrap();
    let mut c = super::association_fixture::config(&json!({"programs":[modeled::AMM,modeled::JUP],
        "raydium":[modeled::AMM],"pumpswap":[]}));
    c.execution = case.source.config.clone();
    let e = &mut c.execution;
    e.owner_technical_buy = None;
    e.canary_wallet_pubkey = g.bot_wallet_pubkey.clone();
    e.execution_signer_pubkey = g.bot_wallet_pubkey.clone();
    e.execution_signer_keypair_path = "never-load-mock-bot-key".into();
    e.canary_kill_switch_path = g.bot_kill_switch_path.clone();
    e.native_fresh_buy = Some(NativeFreshBuyConfig {
        policy: copybot_config::PROCESSED_SLOT_FENCE_AVAILABILITY_V1.into(),
    });
    e.canary_entry_submit_enabled = true;
    e.tiny_experiment.activate = true;
    e.tiny_experiment.id = Some(g.bot_run_id.clone());
    e.technical_cohort = Some(TechnicalCohortConfig {
        policy: copybot_config::TECHNICAL_COHORT_V1.into(),
        activate: true,
        run_id: g.bot_run_id.clone(),
        wallet_ids: vec![p.wallet_pubkey.clone()],
        mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
        route: e.canary_route.clone(),
        activated_at: p.activated_at.clone(),
        deadline: p.expires_at.clone(),
        max_wait_seconds: u64::try_from(
            chrono::DateTime::parse_from_rfc3339(&p.expires_at)?
                .signed_duration_since(chrono::DateTime::parse_from_rfc3339(&p.activated_at)?)
                .num_seconds(),
        )?,
        max_buy_count: 1,
        max_source_sell_count: 1,
    });
    copybot_config::validate_technical_cohort(e)?;
    copybot_config::validate_association_delivery(&c)?;
    Ok(c)
}
pub(super) fn admission(
    case: &super::owner_source_gate_fixture::GateCase,
    signature: &str,
) -> Result<Option<AdmissionFacts>> {
    use rusqlite::OptionalExtension;
    let raw: Option<String> = case
        .sql()?
        .query_row(
            "SELECT admission FROM association_inbox_identities WHERE signature=?1",
            [signature],
            |r| r.get(0),
        )
        .optional()?;
    raw.map(|s| serde_json::from_str(&s).context("decoded admission"))
        .transpose()
}

pub(super) async fn negative_replay(
    case: &super::owner_source_gate_fixture::GateCase,
    transaction: SubscribeUpdateTransaction,
) -> Result<()> {
    use crate::association_consumer::AssociationConsumer;
    use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
    use copybot_ingestion::{IngestionService, ReplayInput};
    let c = config(case)?;
    let authority = crate::execution_technical_cohort::authority(&c.execution)?.unwrap();
    let scope = crate::execution_technical_cohort::admission_wallets(&authority, &c.execution)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut ingestion = IngestionService::with_replay_scoped(
        &c,
        receiver,
        "controlled-negative-model".into(),
        Some(scope),
    )?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut ingestion,
        &c.ingestion,
        &c.execution,
        &case.path.to_string_lossy(),
    )
    .await?
    .context("normal scoped consumer")?;
    let mut rpc = Parsed(|request: serde_json::Value| async move {
        let result = match request["method"].as_str() {
            Some("getGenesisHash") => json!(super::owner_buy_fixture::GENESIS),
            Some("getSlot") => json!(119),
            other => anyhow::bail!("unexpected mock method {other:?}"),
        };
        Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    });
    consumer
        .poll_with_transport(&case.bot, Some(&mut rpc))
        .await?;
    consumer.next_fence_at = Some(tokio::time::Instant::now());
    consumer
        .poll_with_transport(&case.bot, Some(&mut rpc))
        .await?;
    for (n, payload) in frames(transaction).into_iter().enumerate() {
        sender
            .send(ReplayInput::Update {
                offset_ns: n as u64 + 1,
                payload,
            })
            .await?;
    }
    sender.send(ReplayInput::End(3)).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match consumer
                .poll_with_transport(&case.bot, Some(&mut rpc))
                .await
            {
                Ok(()) => (),
                Err(e) if e.to_string() == "association delivery stopped" => break,
                Err(e) => return Err(e),
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}
