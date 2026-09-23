//! Actual strict runner orchestration with only external responses substituted.
#[path = "native_buy_route_fixture.rs"]
mod fixture;
use crate::execution_canary::ExecutionCanaryRunner;
use crate::execution_canary_route::{
    NativeBuyMockAdapter, NativeBuyMockCounts, NativeBuyMockIo, NativeBuyRunnerOperands,
};
use crate::execution_quote_canary_helpers::{PriorityFeeSample, QuoteSample};
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits}, ordered_sell_quote::QuoteClaimStep,
    ExecutionCanaryReceiptProof, SqliteStore,
};
use fixture::{admission, delivery, GENESIS, LEADER, MINT, SOL, SOURCE_SIGNATURE};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc, Mutex};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Case {
    config: ExecutionConfig,
    path: String,
    store: SqliteStore,
    sql: Connection,
    wallet: String,
    signature: String,
    now: chrono::DateTime<Utc>,
    io: Arc<NativeBuyMockIo>,
}

fn quote(now: chrono::DateTime<Utc>, out: &str) -> QuoteSample {
    let route = json!([{"swapInfo":{"label":"Metis"}}]);
    let threshold = if out == "900" { "855" } else { "950" };
    let body = json!({"inputMint":SOL,"outputMint":MINT,
        "inAmount":"1000000","outAmount":out,"otherAmountThreshold":threshold,
        "swapMode":"ExactIn","slippageBps":500,"routePlan":route,"priceImpactPct":"0"});
    QuoteSample {
        http_request_started_ts: Some(now), quote_response_available_ts: Some(now),
        in_amount: "1000000".into(), out_amount: out.into(),
        response_json: body.to_string(), price_impact_pct: Some(0.0),
        route_plan_json: Some(route.to_string()), in_decimals: Some(9),
        out_decimals: Some(3), latency_ms: 0,
    }
}

async fn setup(fresh_out: &str, pending: bool) -> Result<Case> {
    let (payload, signature, payer) = fixture::signed_payload_for_lamports(1_000_000)?;
    let wallet = bs58::encode(payer).into_string();
    let mut config = fixture::config(&wallet);
    config.canary_buy_size_sol = 0.001;
    config.quote_canary_buy_size_sol = 0.001;
    let path = format!("file:native-runner-{}-{}?mode=memory&cache=shared",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations")))?;
    copybot_storage_core::ensure_discovery_v2_schema(&store)?;
    let sql = Connection::open(&path)?;
    let now = Utc::now();
    let window = "2026-09-22T00:00:00+00:00";
    sql.execute("INSERT INTO followlist(wallet_id,added_at,active) VALUES(?1,?2,1)",
        params![LEADER,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_candidate_sources(wallet_id,source_cohort,window_start,updated_at) VALUES(?1,'candidate',?2,?3)",
        params![LEADER,window,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_strategy_state(id,publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json,updated_at) VALUES(1,'healthy',?1,?2,'policy',?3,?4)",
        params![(now-Duration::seconds(3)).to_rfc3339(),window,json!([LEADER]).to_string(),now.to_rfc3339()])?;
    fixture::actual_source_replay(&store, &path, &config).await?;
    assert_eq!(store.list_native_buy_pending(1)?.len(), 1);
    store.activate_tiny_experiment("fractional-test", &wallet, now)?;
    let decoded = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    let facts = crate::execution_native_rpc::synthetic_classic_funding(
        &payload, payer, 1_000_000_000, 19_000, 120)?;
    let receipt = json!({"jsonrpc":"2.0","result":{"slot":120,
        "transaction":{"signatures":[signature],"message":{"accountKeys":[
            {"pubkey":wallet,"signer":true,"writable":true},
            {"pubkey":bs58::encode([42;32]).into_string(),"signer":false,"writable":true}]}},
        "meta":{"err":null,"fee":19000,"preBalances":[1_000_000_000,2_039_280],
            "postBalances":[998_981_000,2_039_280],
            "preTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,
                "uiTokenAmount":{"amount":"0","decimals":3}}],
            "postTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,
                "uiTokenAmount":{"amount":"1000","decimals":3}}]}}});
    let counts = Arc::new(Mutex::new(NativeBuyMockCounts::default()));
    let io = Arc::new(NativeBuyMockIo {
        runner: Some(NativeBuyRunnerOperands {
            finalized_genesis: GENESIS.into(),
            finalized_transaction: json!({"slot":100,"meta":{"err":null,
                "postTokenBalances":[{"mint":MINT,"owner":LEADER}]},
                "transaction":{"signatures":[SOURCE_SIGNATURE],"message":{"accountKeys":[
                    {"pubkey":LEADER,"signer":true}]}}}),
            mint_account: json!({"context":{"slot":100},"value":{
                "owner":copybot_storage_core::native_buy::SPL_TOKEN_PROGRAM}}),
            initial_quote: quote(now, "1000"), fresh_quote: quote(now, fresh_out),
            priority: PriorityFeeSample { status: "ok".into(), lamports: Some(2000),
                json: Some(crate::app_tests::priority_fee_fixture::total_json(2000)), error: None },
            adapter: NativeBuyMockAdapter { config: config.clone(), payload,
                signature: signature.clone(), counts: counts.clone() },
        }),
        initial_sol: facts, fee_lamports: 19_000, fee_slot: 120,
        expected_message_sha256: decoded.binding.message_sha256,
        submit_signature: Some(signature.clone()),
        confirmation: if pending { json!({"result":{"value":[null]}}) } else {
            json!({"result":{"value":[{"err":null,"slot":120,
                "confirmationStatus":"confirmed"}]}}) },
        receipt, counts,
    });
    Ok(Case { config, path, store, sql, wallet, signature, now, io })
}

async fn tick(case: &Case, config: &ExecutionConfig, store: &SqliteStore,
    io: Arc<NativeBuyMockIo>) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    let chain: Value = serde_json::from_slice(&std::fs::read(
        crate::app_tests::b135_fixture::inputs().join("chain.json"))?)?;
    let mut app = crate::app_tests::association_fixture::config(&chain);
    app.execution = config.clone();
    let runner = ExecutionCanaryRunner::new(config.clone())
        .for_ingestion(&app.ingestion, &case.path)?.with_native_buy_mock(io);
    runner.process_tick(store, Utc::now()).await
}

#[tokio::test]
async fn native_buy_runner_actual_tick_receipt_h1000_fractional_sell_h750() -> Result<()> {
    let mut case = setup("1000", false).await?;
    let summary = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(summary.state_machine_reserved, 1);
    assert_eq!(summary.quote_entry_inserted, 1);
    let c = case.io.counts.lock().unwrap();
    assert_eq!((c.source_finality,c.priority,c.initial_quote,c.fresh_quote,
        c.unsigned_build,c.signing_envelope,c.initial_sol,c.fee,c.send,c.confirmation,c.receipt),
        (1,1,1,1,1,1,1,1,1,1,1));
    drop(c);
    assert_eq!(case.store.list_native_buy_pending(1)?.len(), 0);
    let signal_id = format!("native-buy-v1:{SOURCE_SIGNATURE}");
    let event = case.store.load_execution_quote_canary_event_by_id(
        &format!("quote:entry:{signal_id}"))?.expect("initial quote event");
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));
    let metadata = crate::execution_build_plan_metadata::load_execution_build_plan_metadata(
        &case.store, &signal_id)?;
    assert_eq!(metadata.quote_in_amount_raw.as_deref(), Some("1000000"));
    assert_eq!(metadata.quote_out_amount_raw.as_deref(), Some("1000"));
    let position = case.store.load_execution_canary_open_position(MINT)?.expect("canonical BUY H");
    assert_eq!(position.qty_exact.unwrap().raw(), 1000);
    assert_eq!(position.cost_lamports.unwrap().as_u64(), 1_019_000);
    sell_250(&mut case).await?;
    Ok(())
}

#[tokio::test]
async fn native_buy_runner_changed_fresh_quote_blocks_before_reserve() -> Result<()> {
    let case = setup("900", false).await?;
    let summary = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(summary.state_machine_entry_gate_blocked, 1);
    assert_eq!(summary.state_machine_skipped_reason,
        Some(crate::execution_build_plan_refresh::FRESH_SUBMIT_QUOTE_SLIPPAGE_ABOVE_LIMIT));
    let c = case.io.counts.lock().unwrap();
    assert_eq!((c.source_finality,c.initial_quote,c.fresh_quote,c.initial_sol,c.send),
        (1,1,1,0,0));
    drop(c);
    let signal_id = format!("native-buy-v1:{SOURCE_SIGNATURE}");
    let event = case.store.load_execution_quote_canary_event_by_id(
        &format!("quote:entry:{signal_id}"))?.expect("initial quote event");
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));
    assert!(case.store.load_execution_canary_order_by_signal(&signal_id)?.is_none());
    assert!(case.store.load_execution_canary_open_position(MINT)?.is_none());
    Ok(())
}

#[tokio::test]
async fn native_buy_runner_disabled_authority_recovers_pending_after_ram_reopen_once() -> Result<()> {
    let case = setup("1000", true).await?;
    tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert!(case.store.load_execution_canary_open_position(MINT)?.is_none());
    let signal_id = format!("native-buy-v1:{SOURCE_SIGNATURE}");
    let order = case.store.load_execution_canary_order_by_signal(&signal_id)?.expect("pending order");
    let before: (Option<String>,Option<String>) = case.sql.query_row(
        "SELECT outcome,reconciled_at FROM execution_tiny_reservations WHERE order_id=?1",
        [&order.order_id], |r| Ok((r.get(0)?,r.get(1)?)))?;
    assert_eq!(before, (None,None));
    let reopened = SqliteStore::open(&case.path)?;
    let mut disabled = case.config.clone();
    disabled.native_fresh_buy = None;
    let pending_tick = tick(&case, &disabled, &reopened, case.io.clone()).await?;
    assert_eq!(pending_tick.state_machine_reserved, 0);
    assert!(reopened.load_execution_canary_open_position(MINT)?.is_none());
    let after_pending: (Option<String>,Option<String>) = case.sql.query_row(
        "SELECT outcome,reconciled_at FROM execution_tiny_reservations WHERE order_id=?1",
        [&order.order_id], |r| Ok((r.get(0)?,r.get(1)?)))?;
    assert_eq!(before, after_pending);
    let success_io = Arc::new(NativeBuyMockIo {
        runner: None, initial_sol: case.io.initial_sol.clone(),
        fee_lamports: case.io.fee_lamports, fee_slot: case.io.fee_slot,
        expected_message_sha256: case.io.expected_message_sha256.clone(),
        submit_signature: case.io.submit_signature.clone(),
        confirmation: json!({"result":{"value":[{"err":null,"slot":120,
            "confirmationStatus":"confirmed"}]}}),
        receipt: case.io.receipt.clone(), counts: case.io.counts.clone(),
    });
    let settled_tick = tick(&case, &disabled, &reopened, success_io.clone()).await?;
    assert_eq!(settled_tick.state_machine_reserved, 0);
    assert_eq!(reopened.load_execution_canary_open_position(MINT)?.unwrap()
        .qty_exact.unwrap().raw(), 1000);
    tick(&case, &disabled, &reopened, success_io).await?;
    let c = case.io.counts.lock().unwrap();
    assert_eq!((c.initial_quote,c.fresh_quote,c.send), (1,1,1));
    let fills: i64 = case.sql.query_row("SELECT count(*) FROM fills WHERE order_id=?1",
        [&order.order_id], |r| r.get(0))?;
    assert_eq!(fills, 1);
    let orders: i64 = case.sql.query_row("SELECT count(*) FROM orders WHERE signal_id=?1",
        [&signal_id], |r| r.get(0))?;
    assert_eq!(orders, 1);
    Ok(())
}

async fn sell_250(case: &mut Case) -> Result<()> {
    let native = crate::app_tests::fractional::fractional_fixture::native_binding().await?;
    let mut evidence = crate::app_tests::fractional::fractional_tests::evidence()?;
    let mut inbox = AssociationInbox::open_ordered_sell_consumer(&case.path,
        InboxLimits { count: 1000, bytes: 8 << 20, busy_ms: 100 })?;
    let mut own = admission();
    own.facts.signature = case.signature.clone();
    own.facts.slot = 120;
    own.facts.wallet = case.wallet.clone();
    own.facts.amount_in_bits = 0.001f64.to_bits();
    own.facts.amount_out_bits = 1.0f64.to_bits();
    let exact = own.facts.exact_amounts.as_mut().unwrap();
    exact.amount_in_raw = "1000000".into();
    exact.amount_out_raw = "1000".into();
    inbox.persist_at(&delivery(3, copybot_core_types::association_delivery::DeliveryEvent::Admission(own.clone())),
        &copybot_core_types::association_delivery::CandidateGeneration::Unknown, case.now)?;
    let blockhash = native["anchors"][1]["terminal"]["ProviderAsserted"]["blockhash"]
        .as_str().unwrap().to_owned();
    inbox.persist_at(&delivery(4, copybot_core_types::association_delivery::DeliveryEvent::Terminal {
        signature: case.signature.clone(), expected: own,
        result: copybot_core_types::association_delivery::Terminal::ProviderAsserted(
            copybot_core_types::association_delivery::ProviderAssertion {
                slot: 120, blockhash, signature: case.signature.clone(),
                transaction_index: 0, block_time: copybot_core_types::association_delivery::BlockTime::Missing,
            }),
    }), &copybot_core_types::association_delivery::CandidateGeneration::Unknown, case.now)?;
    let sell: copybot_core_types::association_delivery::AdmissionFacts =
        serde_json::from_value(native["anchors"][2]["identity"]["admission"].clone())?;
    let generation = case.store.association_candidate(&sell.facts);
    inbox.persist_at(&delivery(5, copybot_core_types::association_delivery::DeliveryEvent::Admission(sell.clone())),
        &generation, case.now)?;
    let sell_blockhash = native["anchors"][2]["terminal"]["ProviderAsserted"]["blockhash"]
        .as_str().unwrap().to_owned();
    inbox.persist_at(&delivery(6, copybot_core_types::association_delivery::DeliveryEvent::Terminal {
        signature: sell.facts.signature.clone(), expected: sell.clone(),
        result: copybot_core_types::association_delivery::Terminal::ProviderAsserted(
            copybot_core_types::association_delivery::ProviderAssertion {
                slot: 150, blockhash: sell_blockhash, signature: sell.facts.signature.clone(),
                transaction_index: 0, block_time: copybot_core_types::association_delivery::BlockTime::Missing,
            }),
    }), &generation, case.now)?;
    for (i, path) in native["parent_paths"].as_array().unwrap().iter().take(2).enumerate() {
        let edge = &path["edges"][0];
        let parent = copybot_core_types::association_parent::ParentObservation {
            child: serde_json::from_value(edge["child"].clone())?,
            parent: serde_json::from_value(edge["parent"].clone())?, issue: None,
        };
        inbox.persist_at(&delivery(7+i as u64,
            copybot_core_types::association_delivery::DeliveryEvent::Parent(parent)),
            &copybot_core_types::association_delivery::CandidateGeneration::Unknown, case.now)?;
    }
    for _ in 0..20 {
        if !inbox.has_sell_preparation_work()? { break; }
        inbox.recover_sell_preparation()?;
    }
    assert!(inbox.sell_preparation(&sell.facts.signature)?.is_some());
    drop(inbox);
    let mut meta: Value = serde_json::from_slice(&std::fs::read(
        crate::app_tests::b135_fixture::inputs().join("chain.json"))?)?;
    meta["our"]["signer"] = json!(case.wallet);
    meta["our"]["signature"] = json!(case.signature);
    let db = crate::app_tests::association_fixture::Db {
        path: case.path.clone().into(), sql: Connection::open(&case.path)?,
        store: SqliteStore::open(&case.path)?,
    };
    let mut f = crate::app_tests::fractional::fractional_fixture::Fixture::from_parts(db, meta)?;
    let step = f.db.store.claim_strict_sell_quote_for_owned_preparation(
        crate::app_tests::association_parent_fixture::limits(),
        "http://127.0.0.1:1/", Utc::now)?;
    let QuoteClaimStep::Claimed(initial) = step else { anyhow::bail!("owned SELL not claimable") };
    assert_eq!(initial.binding.raw, 1000);
    evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["owner"] =
        json!(case.wallet);
    let claim = crate::app_tests::fractional::fractional_tests::bind(&mut f, initial, evidence).await?;
    assert_eq!(claim.binding.raw, 250);
    let before = f.db.store.load_execution_canary_open_position(MINT)?.unwrap();
    let prepared = crate::app_tests::fractional::fractional_financial_fixture::prepare(&f, &claim)?;
    let dispatch = crate::app_tests::fractional::fractional_financial_fixture::dispatch(&f, &prepared)?;
    let sold = crate::app_tests::fractional::fractional_financial_fixture::receipt(&dispatch, 250);
    f.db.store.mark_execution_canary_confirmed_unreconciled(&dispatch.order_id,
        &ExecutionCanaryReceiptProof { tx_signature: dispatch.tx_signature.clone(),
            wallet_pubkey: dispatch.wallet.clone(), token: dispatch.token.clone(), side: "sell".into(),
            confirmation_status: "confirmed".into(), slot: Some(151), confirmed_at: Utc::now(),
            reason: "mocked canonical receipt".into() }, Utc::now())?;
    f.db.store.record_execution_canary_receipt_facts(&sold, Utc::now())?;
    f.db.store.apply_execution_canary_sell_settlement(&sold, Utc::now())?;
    let reopened = SqliteStore::open(&case.path)?;
    reopened.apply_execution_canary_sell_settlement(&sold, Utc::now())?;
    let after = reopened.load_execution_canary_open_position(MINT)?.unwrap();
    assert_eq!(after.qty_exact.unwrap().raw(), 750);
    let basis = before.cost_lamports.unwrap().as_u64();
    let allocated = (basis * 250).div_ceil(1000);
    assert_eq!((basis, allocated), (1_019_000, 254_750));
    assert_eq!(after.cost_lamports.unwrap().as_u64(), basis-allocated);
    assert_eq!(after.cost_lamports.unwrap().as_u64(), 764_250);
    let cash = reopened.load_execution_canary_cash_settlement(&dispatch.order_id)?.unwrap();
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(cash.allocated_entry_basis.as_u64(), allocated);
    assert_eq!(cash.remaining_entry_basis.as_u64(), basis-allocated);
    let fills: i64 = f.db.sql.query_row("SELECT count(*) FROM fills WHERE order_id=?1",
        [&dispatch.order_id], |r| r.get(0))?;
    assert_eq!(fills, 1);
    let fee: Option<String> = f.db.sql.query_row(
        "SELECT transaction_fee FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&dispatch.order_id], |r| r.get(0))?;
    assert_eq!(fee.as_deref(), Some("19000"));
    Ok(())
}
