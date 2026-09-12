//! Offline receipt operands; financial mutations only in explicit seed/settle steps.
use super::{association_fixture as f, association_parent_fixture as p};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, SwapEvent};
use copybot_storage_core::*;
use serde_json::{json, Value};
use std::path::Path;

pub fn at() -> DateTime<Utc> {
    "2026-09-09T00:00:10Z".parse().unwrap()
}
pub fn open(path: &Path) -> Result<f::Db> {
    Ok(f::Db {
        path: path.into(),
        store: SqliteStore::open(path)?,
        sql: rusqlite::Connection::open(path)?,
    })
}
pub async fn seeded(name: &str) -> Result<(f::Db, Value)> {
    let root = super::b93_local_fixture::begin(name)?;
    let inputs = super::b93_local_fixture::inputs();
    let m: Value = serde_json::from_slice(&std::fs::read(inputs.join("chain.json"))?)?;
    assert_eq!(m["oracle"]["explicit_synthetic_only"], true);
    let mut db = open(&root.join("case.sqlite"))?;
    db.store.run_migrations(
        &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"),
    )?;
    super::b93_buy_fixture::seed(&db, &m)?;
    super::association_observation_fixture::stage(&db, &m, name, inputs).await?;
    let r = p::read(&db, &m)?;
    assert_eq!(
        serde_json::to_value(r.current.selected_chain)?,
        json!("ProviderOrderedAcrossBlocks")
    );
    assert_eq!(raw(&db, &m)?, 7000);
    Ok((db, m))
}
pub fn token(m: &Value) -> &str {
    m["our"]["token_out"].as_str().unwrap()
}
pub fn raw(db: &f::Db, m: &Value) -> Result<u64> {
    Ok(db
        .store
        .load_execution_canary_open_position(token(m))?
        .unwrap()
        .qty_exact
        .unwrap()
        .raw())
}
pub fn additional_buy(db: &f::Db, m: &Value, name: &str, leader: &str, time: &str) -> Result<()> {
    let mut m = m.clone();
    m["source"]["signature"] = json!(format!("source-{name}"));
    m["source"]["signer"] = json!(leader);
    m["our"]["signature"] = json!(format!("receipt-{name}"));
    m["our"]["amount_out"] = json!(2.0);
    m["our"]["exact_amounts"]["amount_out_raw"] = json!("2000");
    m["oracle"]["buy_time"] = json!(time);
    super::b93_buy_fixture::seed(db, &m)
}
pub fn receipt(
    db: &f::Db,
    m: &Value,
    name: &str,
    sold: u64,
) -> Result<ExecutionCanaryReceiptFacts> {
    receipt_signature(db, m, name, sold, &format!("synthetic-{name}"))
}
pub fn receipt_signature(
    db: &f::Db,
    m: &Value,
    name: &str,
    sold: u64,
    sig: &str,
) -> Result<ExecutionCanaryReceiptFacts> {
    let time: DateTime<Utc> = "2026-09-09T00:00:05Z".parse()?;
    let sig = sig.to_owned();
    let signal = format!("b93-{name}");
    db.store.insert_copy_signal(&CopySignalRow {
        signal_id: signal.clone(),
        wallet_id: m["source"]["signer"].as_str().unwrap().into(),
        token: token(m).into(),
        side: "sell".into(),
        notional_sol: 0.1,
        notional_lamports: Some(Lamports::new(100_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: time,
        status: "shadow_recorded".into(),
    })?;
    // Settlement fixture precondition, not a parallel daemon admission claim.
    let id = db
        .store
        .reserve_execution_canary_order(&signal, "tiny", time)?
        .order
        .order_id;
    db.store.mark_execution_canary_built(&id, time)?;
    db.store.mark_execution_canary_simulated(
        &id,
        time,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    db.store.mark_execution_canary_submitted(&id, time, &sig)?;
    let wallet = m["our"]["signer"].as_str().unwrap();
    db.store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: sig.clone(),
            wallet_pubkey: wallet.into(),
            token: token(m).into(),
            side: "sell".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(140),
            confirmed_at: time,
            reason: "synthetic_receipt_pending".into(),
        },
        time,
    )?;
    Ok(ExecutionCanaryReceiptFacts {
        order_id: id,
        tx_signature: sig,
        wallet_pubkey: wallet.into(),
        token: token(m).into(),
        side: "sell".into(),
        slot: 140,
        wallet_native_pre: Lamports::new(1_000_000_000),
        wallet_native_post: Lamports::new(1_100_000_000),
        wallet_native_delta: SignedLamports::new(100_000_000),
        transaction_fee: Some(Lamports::new(10000)),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some(wallet.into()),
        token_delta: Some(ReceiptTokenDelta {
            raw: -i128::from(sold),
            decimals: 3,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Unresolved,
        block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    })
}
pub fn settle(
    db: &f::Db,
    facts: &ExecutionCanaryReceiptFacts,
) -> Result<ExecutionCanaryCashSettlementResult> {
    db.store
        .record_execution_canary_receipt_facts(facts, at())?;
    let plan = db
        .store
        .plan_execution_canary_sell_settlement(&facts.order_id)?;
    ensure!(
        matches!(plan, ExecutionCanarySellSettlement::Ready(_)),
        "{plan:?}"
    );
    let applied = db
        .store
        .apply_execution_canary_sell_settlement(facts, at())?;
    write(
        &format!("settlement-{}", facts.order_id),
        json!({"plan":format!("{plan:?}"),"applied":format!("{applied:?}"),"db":db.path}),
    )?;
    Ok(applied)
}
pub fn legacy_event(m: &Value) -> Result<SwapEvent> {
    let v = &m["sell"];
    Ok(SwapEvent {
        signature: v["signature"].as_str().unwrap().into(),
        wallet: v["signer"].as_str().unwrap().into(),
        dex: "pumpswap".into(),
        token_in: token(m).into(),
        token_out: v["token_out"].as_str().unwrap().into(),
        amount_in: v["amount_in"].as_f64().unwrap(),
        amount_out: v["amount_out"].as_f64().unwrap(),
        slot: v["slot"].as_u64().unwrap(),
        ts_utc: at(),
        exact_amounts: Some(serde_json::from_value(v["exact_amounts"].clone())?),
    })
}
pub fn legacy(db: &f::Db, m: &Value) -> Result<CopySignalRow> {
    let event = legacy_event(m)?;
    db.store.insert_observed_swap(&event)?;
    let position = db
        .store
        .load_execution_canary_open_position(token(m))?
        .unwrap();
    let ExecutionSourceSellOutcome::Inserted(staged) = db
        .store
        .stage_execution_source_sell_intent(&event, &position.position_id)?
    else {
        anyhow::bail!("legacy stage refused")
    };
    let ExecutionSourceSellPromotionOutcome::Inserted(binding) = db
        .store
        .promote_execution_source_sell_intent(&staged.intent_id)?
    else {
        anyhow::bail!("legacy promote refused")
    };
    Ok(db
        .store
        .load_copy_signal_by_signal_id(&binding.signal_id)?
        .unwrap())
}
pub fn state(db: &f::Db, m: &Value) -> Result<Value> {
    let p = p::read(db, m)?;
    let pos = db.store.load_execution_canary_open_position(token(m))?;
    let original: (String,i64)=db.sql.query_row("SELECT token_delta_raw,token_decimals FROM execution_canary_receipt_facts WHERE tx_signature=?1",[m["our"]["signature"].as_str().unwrap()],|r|Ok((r.get(0)?,r.get(1)?)))?;
    Ok(
        json!({"db":db.path,"preparation":{"first":p.first,"historical":p.historical_initial,"current":p.current},
        "original_buy_raw":original.0,"original_buy_decimals":original.1,"position":format!("{pos:?}"),
        "raw":pos.as_ref().and_then(|p|p.qty_exact.map(|q|q.raw())),"generation":pos.as_ref().map(|p|&p.position_id),
        "attribution":format!("{:?}",db.store.load_execution_canary_buy_attribution(token(m))?)}),
    )
}
pub fn write(name: &str, v: Value) -> Result<()> {
    let name = name.replace([':', '/', '\\'], "_");
    let p = super::b93_local_fixture::results().join(format!("{name}.json"));
    std::fs::create_dir_all(p.parent().unwrap())?;
    std::fs::write(p, serde_json::to_vec_pretty(&v)?)?;
    Ok(())
}

pub fn config(url: &str) -> copybot_config::ExecutionConfig {
    let mut c = super::source_write_off_fixture::config(url);
    // Public synthetic payer only. No private key is created or loaded.
    c.canary_wallet_pubkey = bs58::encode([94u8; 32]).into_string();
    c.execution_signer_pubkey = c.canary_wallet_pubkey.clone();
    c.execution_signer_keypair_path = super::b93_local_fixture::root()
        .join("absent-signer.json")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!std::path::Path::new(&c.execution_signer_keypair_path).exists());
    c.quote_canary_enabled = true;
    c.priority_fee_canary_enabled = true;
    c.priority_fee_canary_rpc_url = url.into();
    c.swap_instructions_dry_run_enabled = true;
    c.pretrade_max_priority_fee_lamports = 500_000;
    c.max_confirm_seconds = 1;
    c
}
