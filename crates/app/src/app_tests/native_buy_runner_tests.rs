//! Actual strict runner orchestration with only external responses substituted.
#[path = "native_buy_route_fixture.rs"]
pub(super) mod fixture;
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

pub(super) struct Case {
    pub(super) config: ExecutionConfig,
    pub(super) path: String,
    pub(super) store: SqliteStore,
    sql: Connection,
    wallet: String,
    signature: String,
    now: chrono::DateTime<Utc>,
    pub(super) io: Arc<NativeBuyMockIo>,
    buy_lamports: u64,
    payer_server: Option<crate::app_tests::native_rpc_fixture::Fixture>,
    payer: Option<Arc<Mutex<crate::app_tests::initial_sol_rpc_fixture::FundingRpc>>>,
}

fn quote_for_amount(now: chrono::DateTime<Utc>, out: &str, amount: &str) -> QuoteSample {
    let route = json!([{"swapInfo":{"label":"Metis"}}]);
    let threshold = match out { "900" => "855", "10000" => "9500", _ => "950" };
    let body = json!({"inputMint":SOL,"outputMint":MINT,
        "inAmount":amount,"outAmount":out,"otherAmountThreshold":threshold,
        "swapMode":"ExactIn","slippageBps":500,"routePlan":route,"priceImpactPct":"0"});
    QuoteSample {
        http_request_started_ts: Some(now), quote_response_available_ts: Some(now),
        in_amount: amount.into(), out_amount: out.into(),
        response_json: body.to_string(), price_impact_pct: Some(0.0),
        route_plan_json: Some(route.to_string()), in_decimals: Some(9),
        out_decimals: Some(3), latency_ms: 0,
    }
}

async fn setup(fresh_out: &str, pending: bool) -> Result<Case> {
    setup_case(fresh_out, pending, false, false, false).await
}
pub(super) async fn setup_case(fresh_out: &str, pending: bool, protected: bool, activate: bool, cohort: bool) -> Result<Case> {
    let buy_lamports = if protected { 10_000_000 } else { 1_000_000 };
    let output_raw = if protected { "10000" } else { "1000" };
    let (payload, signature, wallet_bytes) = if protected {
        fixture::signed_payload_for_lamports_and_floor(buy_lamports, 985_000_000)?
    } else {
        fixture::signed_payload_for_lamports(buy_lamports)?
    };
    let wallet = bs58::encode(wallet_bytes).into_string();
    let mut config = fixture::config(&wallet);
    config.canary_buy_size_sol = buy_lamports as f64 / 1e9;
    config.quote_canary_buy_size_sol = config.canary_buy_size_sol;
    if protected {
        config.tiny_experiment.policy_mode = copybot_config::TinyPolicyMode::ProtectedNativeCapital;
    }
    config.tiny_experiment.activate = activate;
    if cohort {
        let activated = Utc::now() - Duration::seconds(180);
        config.canary_route = "jupiter_swap_instructions".into();
        config.tiny_experiment.id = Some("technical-cohort-test".into());
        config.pretrade_min_sol_reserve = 0.160_200_031;
        config.technical_cohort = Some(copybot_config::TechnicalCohortConfig {
            policy: copybot_config::TECHNICAL_COHORT_V1.into(), activate,
            run_id: "technical-cohort-test".into(), wallet_ids: vec![LEADER.into()],
            mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
            route: "jupiter_swap_instructions".into(),
            activated_at: activated.to_rfc3339(),
            deadline: (activated + Duration::seconds(3600)).to_rfc3339(),
            max_wait_seconds: 3600, max_buy_count: 1, max_source_sell_count: 1,
        });
    }
    let (payer_server, payer) = if protected {
        let state = Arc::new(Mutex::new(crate::app_tests::initial_sol_rpc_fixture::FundingRpc::default()));
        let server = crate::app_tests::initial_sol_rpc_fixture::FundingRpc::server(state.clone()).await?;
        config.submit_adapter_http_url = server.endpoint.clone();
        config.owned_sell_preparation.as_mut().unwrap().rpc_url = server.endpoint.clone();
        (Some(server), Some(state))
    } else { (None, None) };
    let path = format!("file:native-runner-{}-{}?mode=memory&cache=shared",
        std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations")))?;
    copybot_storage_core::ensure_discovery_v2_schema(&store)?;
    let sql = Connection::open(&path)?;
    let now = Utc::now();
    let window = "2026-09-22T00:00:00+00:00";
    if !cohort {
    sql.execute("INSERT INTO followlist(wallet_id,added_at,active) VALUES(?1,?2,1)",
        params![LEADER,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_candidate_sources(wallet_id,source_cohort,window_start,updated_at) VALUES(?1,'candidate',?2,?3)",
        params![LEADER,window,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_strategy_state(id,publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json,updated_at) VALUES(1,'healthy',?1,?2,'policy',?3,?4)",
        params![(now-Duration::seconds(3)).to_rfc3339(),window,json!([LEADER]).to_string(),now.to_rfc3339()])?;
    }
    // Replay runs under the same explicit activation config used by the tick.
    fixture::actual_source_replay(&store, &path, &config).await?;
    assert_eq!(store.list_native_buy_pending(1)?.len(), 1);
    if !protected {
        store.activate_tiny_experiment("fractional-test", &wallet, now)?;
    }
    let decoded = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    let facts = crate::execution_native_rpc::synthetic_classic_funding(
        &payload, wallet_bytes, 1_000_000_000, 19_000, 120)?;
    let post_native = 1_000_000_000 - buy_lamports - 19_000;
    let receipt = json!({"jsonrpc":"2.0","result":{"slot":120,
        "transaction":{"signatures":[signature],"message":{"accountKeys":[
            {"pubkey":wallet,"signer":true,"writable":true},
            {"pubkey":bs58::encode([42;32]).into_string(),"signer":false,"writable":true}]}},
        "meta":{"err":null,"fee":19000,"preBalances":[1_000_000_000,2_039_280],
            "postBalances":[post_native,2_039_280],
            "preTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,
                "uiTokenAmount":{"amount":"0","decimals":3}}],
            "postTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,
                "uiTokenAmount":{"amount":output_raw,"decimals":3}}]}}});
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
            initial_quote: quote_for_amount(now, output_raw, &buy_lamports.to_string()),
            fresh_quote: quote_for_amount(now, fresh_out, &buy_lamports.to_string()),
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
    Ok(Case { config, path, store, sql, wallet, signature, now, io,
        buy_lamports, payer_server, payer })
}

pub(super) async fn tick(case: &Case, config: &ExecutionConfig, store: &SqliteStore,
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
    sell_quarter(&mut case).await?;
    Ok(())
}

#[tokio::test]
async fn native_buy_first_protected_activation_from_empty_db_through_tick() -> Result<()> {
    let off = setup_case("10000", false, true, false, false).await?;
    assert!(off.store.load_tiny_experiment(Utc::now())?.is_none());
    let denied = tick(&off, &off.config, &off.store, off.io.clone()).await?;
    assert_eq!((denied.state_machine_reserved, denied.state_machine_failed), (1, 1), "{denied:?}");
    assert!(off.store.load_tiny_experiment(Utc::now())?.is_none());
    assert_eq!(off.io.counts.lock().unwrap().send, 0);

    let mut case = setup_case("10000", false, true, true, false).await?;
    assert!(case.store.load_tiny_experiment(Utc::now())?.is_none());
    assert_eq!(case.sql.query_row("SELECT count(*) FROM execution_tiny_native_policy", [],
        |r| r.get::<_, i64>(0))?, 0);
    assert_eq!(case.sql.query_row("SELECT count(*) FROM execution_canary_dispatch", [],
        |r| r.get::<_, i64>(0))?, 0);
    assert!(case.store.load_execution_canary_open_position(MINT)?.is_none());
    let admitted = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(admitted.state_machine_reserved, 1, "{admitted:?}");
    assert_eq!(case.io.counts.lock().unwrap().send, 1, "{admitted:?}");
    let policy = case.store.tiny_native_policy("fractional-test", &case.wallet, Utc::now())?;
    assert_eq!((policy.initial_lamports, policy.original_reserve,
        policy.floor_lamports, policy.allowance),
        (1_000_000_000, 50_000_001, 985_000_000, 15_000_000));
    let owned = case.store.load_execution_canary_open_position(MINT)?.expect("receipt-owned H");
    assert_eq!(owned.qty_exact.unwrap().raw(), 10000);
    assert_eq!(owned.cost_lamports.unwrap().as_u64(), 10_019_000);
    let reopened = SqliteStore::open(&case.path)?;
    let repeated = tick(&case, &case.config, &reopened, case.io.clone()).await?;
    assert_eq!(repeated.state_machine_reserved, 0);
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    assert_eq!(reopened.tiny_native_policy("fractional-test", &case.wallet, Utc::now())?, policy);
    sell_quarter(&mut case).await?;
    let remaining = case.store.load_execution_canary_open_position(MINT)?.unwrap();
    assert_eq!((remaining.qty_exact.unwrap().raw(), remaining.cost_lamports.unwrap().as_u64()), (7500, 7_514_250));
    let payer_calls = case.payer_server.take().unwrap().finish().await?;
    assert_eq!(payer_calls.iter().filter(|c| c.request["method"] == "getMultipleAccounts").count(), 1);
    Ok(())
}

#[tokio::test]
async fn native_buy_first_protected_activation_refuses_wrong_identity_and_payer() -> Result<()> {
    for change in ["wallet", "id", "payer"] {
        let case = setup_case("10000", false, true, true, false).await?;
        let mut config = case.config.clone();
        match change {
            "wallet" => config.canary_wallet_pubkey = bs58::encode([3; 32]).into_string(),
            "id" => config.tiny_experiment.id = Some("other-experiment".into()),
            "payer" => {
                case.payer.as_ref().unwrap().lock().unwrap()
                    .rows.insert(case.wallet.clone(), Value::Null);
            }
            _ => unreachable!(),
        }
        let result = tick(&case, &config, &case.store, case.io.clone()).await;
        if change == "wallet" {
            assert!(result.unwrap_err().to_string().contains("owned_sell_wallet_identity"));
        } else {
            result?;
        }
        assert!(case.store.load_tiny_experiment(Utc::now())?.is_none(), "{change}");
        assert_eq!(case.io.counts.lock().unwrap().send, 0, "{change}");
    }
    Ok(())
}

#[tokio::test]
async fn native_buy_first_protected_activation_rechecks_fence_after_payer_await() -> Result<()> {
    let case = setup_case("10000", false, true, true, false).await?;
    let seen = {
        let mut payer = case.payer.as_ref().unwrap().lock().unwrap();
        payer.delay_ms = 150;
        payer.seen_accounts.clone()
    };
    let change_fence = async {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while seen.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }).await?;
        case.sql.execute("UPDATE native_buy_session_state SET active_session=NULL WHERE id=1", [])?;
        Ok::<_, anyhow::Error>(())
    };
    let (result, mutation) = tokio::join!(tick(&case, &case.config, &case.store, case.io.clone()), change_fence);
    mutation?;
    let summary = result?;
    assert_eq!(summary.state_machine_reserved, 1, "{summary:?}");
    assert!(case.store.load_tiny_experiment(Utc::now())?.is_none());
    assert_eq!(case.io.counts.lock().unwrap().send, 0);
    Ok(())
}

#[tokio::test]
async fn native_buy_first_protected_activation_concurrent_ticks_do_not_rearm() -> Result<()> {
    let case = setup_case("10000", false, true, true, false).await?;
    let reopened = SqliteStore::open(&case.path)?;
    let (first, second) = tokio::join!(
        tick(&case, &case.config, &case.store, case.io.clone()),
        tick(&case, &case.config, &reopened, case.io.clone()),
    );
    first?;
    second?;
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    assert_eq!(case.sql.query_row("SELECT count(*) FROM execution_tiny_native_policy", [],
        |r| r.get::<_, i64>(0))?, 1);
    assert_eq!(case.sql.query_row("SELECT count(*) FROM execution_canary_dispatch", [],
        |r| r.get::<_, i64>(0))?, 1);
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

#[tokio::test]
async fn native_buy_protected_pending_reconciles_after_activation_disabled() -> Result<()> {
    let case = setup_case("10000", true, true, true, false).await?;
    let first = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(first.state_machine_reserved, 1, "{first:?}");
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    assert!(case.store.load_execution_canary_open_position(MINT)?.is_none());
    let mut disabled = case.config.clone();
    disabled.tiny_experiment.activate = false;
    disabled.native_fresh_buy = None;
    let reopened = SqliteStore::open(&case.path)?;
    let pending = tick(&case, &disabled, &reopened, case.io.clone()).await?;
    assert_eq!(pending.state_machine_reserved, 0);
    assert!(reopened.load_execution_canary_open_position(MINT)?.is_none());
    let success_io = Arc::new(NativeBuyMockIo {
        runner: None, initial_sol: case.io.initial_sol.clone(),
        fee_lamports: case.io.fee_lamports, fee_slot: case.io.fee_slot,
        expected_message_sha256: case.io.expected_message_sha256.clone(),
        submit_signature: case.io.submit_signature.clone(),
        confirmation: json!({"result":{"value":[{"err":null,"slot":120,
            "confirmationStatus":"confirmed"}]}}),
        receipt: case.io.receipt.clone(), counts: case.io.counts.clone(),
    });
    tick(&case, &disabled, &reopened, success_io.clone()).await?;
    tick(&case, &disabled, &reopened, success_io).await?;
    assert_eq!(reopened.load_execution_canary_open_position(MINT)?.unwrap()
        .qty_exact.unwrap().raw(), 10000);
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    let order = reopened.load_execution_canary_order_by_signal(
        &format!("native-buy-v1:{SOURCE_SIGNATURE}"))?.unwrap();
    let fills: i64 = case.sql.query_row("SELECT count(*) FROM fills WHERE order_id=?1",
        [&order.order_id], |r| r.get(0))?;
    assert_eq!(fills, 1);
    Ok(())
}

#[path = "native_buy_runner_sell_fixture.rs"]
mod sell_fixture;
pub(super) use sell_fixture::sell_quarter;
