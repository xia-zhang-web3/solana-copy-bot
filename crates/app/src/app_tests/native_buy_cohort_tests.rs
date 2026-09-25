//! Source producer to native BUY receipt and source-owned fractional SELL.
use super::native_buy_runner_tests::{setup_case, setup_case_with_config, sell_quarter, tick};
use crate::app_tests::b136_server::{NativeCohort, Server};
use crate::execution_canary::ExecutionCanaryRunner;
use crate::execution_canary_route::NativeBuyMockIo;
use anyhow::{Context, Result};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::SqliteStore;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

static COHORT_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn cohort_source_sell_runs_through_actual_daemon_tick_and_settles_once() -> Result<()> {
    let _only_cohort = COHORT_TEST.lock().await;
    let (_, signature, wallet_bytes) =
        super::native_buy_runner_tests::fixture::signed_payload_for_lamports_and_floor(
            10_000_000, 985_000_000)?;
    let wallet = bs58::encode(wallet_bytes).into_string();
    let old: Value = serde_json::from_slice(&std::fs::read(
        crate::app_tests::b136_fixture::inputs().join("our-rpc.json"))?)?;
    let historical_wallet = old["transaction"]["message"]["accountKeys"][0]["pubkey"]
        .as_str().context("historical wallet")?;
    let mut buy_rpc: Value = serde_json::from_str(
        &old.to_string().replace(historical_wallet, &wallet))?;
    buy_rpc["transaction"]["signatures"][0] = json!(signature);
    buy_rpc["meta"]["fee"] = json!(19_000);
    buy_rpc["meta"]["preBalances"][0] = json!(1_000_000_000_u64);
    let native_after_buy = 1_000_000_000_u64 - 10_000_000 - 19_000;
    buy_rpc["meta"]["postBalances"][0] = json!(native_after_buy);
    buy_rpc["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("10000");
    let mut evidence = super::fractional_tests::evidence()?;
    evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["owner"] =
        json!(wallet);
    evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"] = json!("10000");
    let server = Server::for_native_cohort(NativeCohort {
        buy_signature: signature.clone(), buy_rpc, evidence,
        owned_raw: 10_000, sold_raw: 2_500, native_after_buy,
    }).await?;
    let root = crate::app_tests::temporary_output_fixture::OutputRoot::new("cohort-sell-tick")?;
    let key = ed25519_dalek::SigningKey::from_bytes(&[11; 32]);
    let signer = root.path().join("synthetic-test-key.json");
    std::fs::write(&signer, serde_json::to_vec(&key.to_keypair_bytes().as_slice())?)?;
    let quote_url = format!("{}/swap/v1", server.url);
    let rpc_url = server.url.clone();
    let kill = root.path().join("stop").to_string_lossy().into_owned();
    let signer = signer.to_string_lossy().into_owned();
    let path = root.path().join("cohort-state.db").to_string_lossy().into_owned();
    let case = setup_case_with_config("10000", false, true, true, true, Some(path), move |c| {
        c.execution_signer_keypair_path = signer;
        c.canary_kill_switch_path = kill;
        c.quote_canary_base_url = quote_url;
        c.submit_adapter_http_url = rpc_url.clone();
        c.owned_sell_preparation.as_mut().unwrap().rpc_url = rpc_url;
        c.quote_canary_timeout_ms = 4000;
        c.submit_timeout_ms = 200;
        c.max_confirm_seconds = 1;
    }).await?;
    assert_eq!((case.signature.as_str(), case.wallet.as_str()),
        (signature.as_str(), wallet.as_str()));
    let bought = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(bought.state_machine_reserved, 1, "{bought:?}");
    assert_eq!(case.io.counts.lock().unwrap().send, 1, "{bought:?}");
    let funding_calls = server.calls.lock().unwrap().iter()
        .filter(|c| c["method"] == "getMultipleAccounts").count();
    assert_eq!(funding_calls, 1, "initial native BUY funding proof");
    replay_cohort_sell_after_buy(&case).await?;
    let config = case.config.clone();
    let chain: Value = serde_json::from_slice(&std::fs::read(
        super::b135_fixture::inputs().join("chain.json"))?)?;
    let mut app = super::association_fixture::config(&chain);
    app.execution = config.clone();
    copybot_config::validate_association_delivery(&app)?;
    let runner = ExecutionCanaryRunner::new(config.clone())
        .for_ingestion(&app.ingestion, &case.path)?
        .with_native_buy_mock(case.io.clone());
    let db = Connection::open(&case.path)?;
    let sell_id = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            server.check()?;
            let summary = runner.process_tick(&case.store, chrono::Utc::now()).await?;
            let id: Option<String> = db.query_row(
                "SELECT order_id FROM rpc_owned_sell_dispatches LIMIT 1", [], |r| r.get(0),
            ).optional()?;
            if let Some(id) = id {
                if case.store.load_execution_canary_cash_settlement(&id)?.is_some() {
                    return Ok::<_, anyhow::Error>(id);
                }
            }
            anyhow::ensure!(summary.last_error.is_none(), "{summary:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.context("cohort SELL did not settle via daemon tick")??;
    let cash = case.store.load_execution_canary_cash_settlement(&sell_id)?
        .context("SELL cash settlement")?;
    assert_eq!((cash.sold_quantity.raw(), cash.remaining_quantity.raw()), (2500, 7500));
    assert_eq!(cash.allocated_entry_basis.as_u64(), 2_504_750);
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981_000);
    let remaining = case.store.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.unwrap();
    assert_eq!((remaining.qty_exact.unwrap().raw(),
        remaining.cost_lamports.unwrap().as_u64()), (7500, 7_514_250));
    let calls = server.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|c| c["method"] == "getMultipleAccounts").count(),
        funding_calls, "SELL must not reuse the initial BUY balance snapshot");
    assert_eq!(calls.iter().find(|c| c["method"] == "quote")
        .context("strict SELL quote")?["params"]["amount"], "2500");
    for method in ["getBlock", "getTokenAccountsByOwner", "quote", "simulateTransaction",
        "sendTransaction", "getTransaction"] {
        assert!(calls.iter().any(|c| c["method"] == method), "missing {method}");
    }
    assert_eq!(calls.iter().filter(|c| c["method"] == "sendTransaction").count(), 1);
    drop(calls);
    drop(runner);
    let reopened = SqliteStore::open(&case.path)?;
    let restarted = ExecutionCanaryRunner::new(config).for_ingestion(&app.ingestion, &case.path)?
        .with_native_buy_mock(case.io.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..10 {
            let summary = restarted.process_tick(&reopened, chrono::Utc::now()).await?;
            anyhow::ensure!(summary.last_error.is_none(), "restart tick: {summary:?}");
            // Let each background strict job claim or observe the durable prior
            // outcome, then reap it on the next real daemon tick.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<_, anyhow::Error>(())
    }).await.context("restart strict jobs did not drain")??;
    let handoffs: i64 = db.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [],
        |r| r.get(0))?;
    assert_eq!(handoffs, 1);
    assert_eq!(server.calls.lock().unwrap().iter()
        .filter(|c| c["method"] == "sendTransaction").count(), 1);
    assert_eq!(server.calls.lock().unwrap().iter()
        .filter(|c| c["method"] == "getMultipleAccounts").count(), funding_calls);
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    Ok(())
}

fn replace_equal(mut wire: Vec<u8>, old: &[u8], new: &[u8]) -> Result<Vec<u8>> {
    anyhow::ensure!(old.len() == new.len(), "fixture replacement length");
    let mut count = 0;
    for at in 0..=wire.len() - old.len() {
        if &wire[at..at + old.len()] == old {
            wire[at..at + old.len()].copy_from_slice(new);
            count += 1;
        }
    }
    anyhow::ensure!(count > 0, "fixture replacement missing");
    Ok(wire)
}

fn rewrite_identity(wire: Vec<u8>, old_wallet: &str, wallet: &str,
    old_signature: &str, signature: &str) -> Result<Vec<u8>> {
    let old_raw_wallet = bs58::decode(old_wallet).into_vec()?;
    let raw_wallet = bs58::decode(wallet).into_vec()?;
    let old_raw_signature = bs58::decode(old_signature).into_vec()?;
    let raw_signature = bs58::decode(signature).into_vec()?;
    let wire = replace_equal(wire, &old_raw_wallet, &raw_wallet)?;
    let wire = replace_equal(wire, old_wallet.as_bytes(), wallet.as_bytes())?;
    replace_equal(wire, &old_raw_signature, &raw_signature)
}

async fn replay_cohort_sell_after_buy(case: &super::native_buy_runner_tests::Case) -> Result<()> {
    let input = crate::app_tests::b136_fixture::inputs();
    let chain: Value = serde_json::from_slice(&std::fs::read(input.join("chain.json"))?)?;
    let mut app = super::association_fixture::config(&chain);
    app.execution = case.config.clone();
    copybot_config::validate_association_delivery(&app)?;
    let authority = crate::execution_technical_cohort::authority(&case.config)?
        .context("active test cohort")?;
    let scope = crate::execution_technical_cohort::admission_wallets(&authority, &case.config)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut ingestion = IngestionService::with_replay_scoped(
        &app, receiver, "cohort-source-sell".into(), Some(scope))?;
    let mut consumer = crate::association_consumer::AssociationConsumer::start_with_execution(
        &mut ingestion, &app.ingestion, &app.execution, &case.path).await?
        .context("cohort ingress consumer")?;
    let inbox_limits = copybot_storage_core::association_inbox::InboxLimits {
        count: 20_000, bytes: 128 << 20, busy_ms: 100,
    };
    let before_usage = copybot_storage_core::association_inbox::AssociationInbox::open_ordered_sell_consumer(
        &case.path, inbox_limits)?.usage()?;
    let source_wallet = chain["source"]["signer"].as_str().context("source wallet")?.to_owned();
    let source_signature = chain["source"]["signature"].as_str().context("source signature")?.to_owned();
    let old_wallet = chain["our"]["signer"].as_str().context("fixture bot wallet")?;
    let old_signature = chain["our"]["signature"].as_str().context("fixture bot signature")?;
    let source = std::fs::read(input.join("source.pb"))?;
    let our = rewrite_identity(std::fs::read(input.join("cohort-our.pb"))?,
        old_wallet, &case.wallet, old_signature, &case.signature)?;
    let block_our = rewrite_identity(std::fs::read(input.join("cohort-block-120.pb"))?,
        old_wallet, &case.wallet, old_signature, &case.signature)?;
    let frames = [source.clone(), our, std::fs::read(input.join("sell.pb"))?,
        std::fs::read(input.join("block-100.pb"))?, block_our,
        std::fs::read(input.join("block-150.pb"))?];
    let source_signature_for_replay = source_signature.clone();
    let producer = tokio::spawn(async move {
        for n in 0..742u16 {
            let foreign_wallet = bs58::encode([2u8; 32]).into_string();
            let mut raw_signature = [91u8; 64];
            raw_signature[..2].copy_from_slice(&n.to_le_bytes());
            let foreign_signature = bs58::encode(raw_signature).into_string();
            let payload = rewrite_identity(source.clone(), &source_wallet, &foreign_wallet,
                &source_signature_for_replay, &foreign_signature)?;
            sender.send(ReplayInput::Update { offset_ns: u64::from(n) + 1, payload }).await?;
        }
        for (n, payload) in frames.into_iter().enumerate() {
            sender.send(ReplayInput::Update { offset_ns: 743 + n as u64, payload }).await?;
        }
        sender.send(ReplayInput::End(750)).await?;
        Ok::<(), anyhow::Error>(())
    });
    let mut rpc = crate::execution_owned_sell_rpc::fractional::transport::Parsed(
        |request: Value| async move {
            let result = match request["method"].as_str() {
                Some("getGenesisHash") => json!("11111111111111111111111111111111"),
                Some("getSlot") => json!(99),
                other => anyhow::bail!("unexpected cohort fence method: {other:?}"),
            };
            Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
        });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match consumer.poll_with_transport(&case.store, Some(&mut rpc)).await {
                Ok(()) => {},
                Err(error) if error.to_string() == "association delivery stopped" => break,
                Err(error) => return Err(error),
            }
        }
        Ok::<(), anyhow::Error>(())
    }).await??;
    producer.await??;
    let db = Connection::open(&case.path)?;
    let foreign: i64 = db.query_row("SELECT count(*) FROM association_inbox_identities WHERE signature NOT IN (?1,?2,?3)",
        rusqlite::params![source_signature, case.signature, chain["sell"]["signature"].as_str()], |r| r.get(0))?;
    assert_eq!(foreign, 0, "foreign swaps reached durable identity storage");
    let inbox = copybot_storage_core::association_inbox::AssociationInbox::open_ordered_sell_consumer(
        &case.path, inbox_limits)?;
    let after_usage = inbox.usage()?;
    assert!(after_usage.0 <= before_usage.0 + 100
        && after_usage.1 <= before_usage.1 + (8 << 20),
        "742 foreign swaps grew the logical inbox: {before_usage:?} -> {after_usage:?}");
    let preparation = inbox.sell_preparation(chain["sell"]["signature"].as_str().unwrap())?;
    assert!(preparation.is_some(),
        "streamed source SELL lacks preparation");
    Ok(())
}

#[tokio::test]
async fn streamed_sell_without_bot_anchor_or_parent_edge_has_no_trade_authority() -> Result<()> {
    use super::association_parent_fixture as parent;
    use crate::app_tests::b136_fixture::{self, Fixture};
    for omitted in ["our", "block-120"] {
        let fixture = Fixture::new().await?;
        let mut frames = parent::frames(&fixture.meta);
        frames.retain(|name| name != omitted);
        parent::stage_at(&fixture.db, b136_fixture::inputs(), &fixture.meta,
            frames, "missing-cohort-proof", false).await?;
        let proof = parent::read(&fixture.db, &fixture.meta)?;
        assert_eq!(proof.current.trade_authority, "trade_authority_none", "{omitted}");
        assert!(proof.current.anchors.iter().any(|anchor| anchor.conflict
            || anchor.terminal.is_none()) || proof.current.parent_paths.len() < 2, "{omitted}");
    }
    Ok(())
}

#[test]
fn cohort_native_buy_zero_fee_uses_real_jupiter_assembler() -> Result<()> {
    use crate::execution_instruction_bundle_binding::BundleRequest;
    use crate::execution_submit_adapter::{
        ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
        JupiterMetisDryRunExecutionAdapter,
    };
    use serde_json::Value;
    let quote: Value = serde_json::from_str(include_str!(
        "generic_buy_fixtures/owner-sol-usdc-20260924-quote.json"))?;
    let bundle: Value = serde_json::from_str(include_str!(
        "generic_buy_fixtures/owner-sol-usdc-20260924-instructions.json"))?;
    let wallet = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
    let mut c = super::super::generic_buy_fixture::config("http://127.0.0.1:9");
    c.canary_route = "jupiter_swap_instructions".into();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    c.pretrade_max_priority_fee_lamports = 50_000;
    c.technical_cohort = Some(copybot_config::TechnicalCohortConfig {
        policy: copybot_config::TECHNICAL_COHORT_V1.into(), activate: true,
        run_id: "assembler-fixture".into(), wallet_ids: vec!["leader".into()],
        mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
        route: c.canary_route.clone(), activated_at: "fixture".into(),
        deadline: "fixture".into(), max_wait_seconds: 3600,
        max_buy_count: 1, max_source_sell_count: 1,
    });
    let request = ExecutionSubmitRequest {
        order_id: "exec-canary:native-buy-v1:fixture".into(),
        signal_id: "native-buy-v1:fixture".into(),
        client_order_id: "copybot:native-buy-v1:fixture".into(),
        attempt: 1, route: c.canary_route.clone(), wallet_id: "leader".into(),
        token: quote["outputMint"].as_str().unwrap().into(), side: "buy".into(),
        buy_size_sol: 0.01, slippage_tolerance_bps: 50,
        wallet_pubkey: wallet.into(), entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_event_id: Some("cohort-fixture-quote".into()),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("10000000".into()),
            quote_out_amount_raw: quote["outAmount"].as_str().map(str::to_owned),
            quote_response_json: Some(quote.to_string()),
            route_plan_json: Some(quote["routePlan"].to_string()),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(0),
            priority_fee_json: Some(super::super::priority_fee_fixture::total_json(0)),
            ..Default::default()
        },
    };
    let plan = JupiterMetisDryRunExecutionAdapter::new(c.clone())
        .build_transaction_plan(&request)?;
    let bound = BundleRequest::capture(&plan)?.bind(&bundle)?;
    let payload = crate::execution_guarded_generic_buy::assemble(
        &c, &plan, &bound, 50_000_001)?.serialized_transaction_base64;
    let fee = crate::execution_priority_fee_wire::decode_priority_fee(&payload)?;
    assert_eq!((fee.price, fee.total), (0, 0));
    Ok(())
}

#[tokio::test]
async fn cohort_actual_producer_after_wait_buys_once_and_source_sell_keeps_remainder() -> Result<()> {
    let _only_cohort = COHORT_TEST.lock().await;
    let mut case = setup_case("10000", false, true, true, true).await?;
    let db = Connection::open(&case.path)?;
    let count: i64 = db.query_row("SELECT count(*) FROM native_buy_fence_epochs", [], |r| r.get(0))?;
    assert_eq!(count, 2, "startup and periodic request-bound fences");
    for table in ["followlist", "discovery_candidate_sources", "discovery_strategy_state"] {
        let count: i64 = db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        assert_eq!(count, 0, "{table} must not grant cohort authority");
    }
    let first = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(first.state_machine_reserved, 1, "{first:?}");
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    let owned = case.store.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.expect("receipt-owned position");
    assert_eq!(owned.qty_exact.unwrap().raw(), 10000);
    let reopened = SqliteStore::open(&case.path)?;
    let repeated = tick(&case, &case.config, &reopened, case.io.clone()).await?;
    assert_eq!(repeated.state_machine_reserved, 0);
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    sell_quarter(&mut case).await?;
    let remaining = reopened.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.expect("partial remains open");
    assert_eq!(remaining.qty_exact.unwrap().raw(), 7500);
    let sells: i64 = db.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r.get(0))?;
    assert_eq!(sells, 1);
    Ok(())
}

#[tokio::test]
async fn cohort_unknown_buy_reconciles_after_restart_without_second_send() -> Result<()> {
    let _only_cohort = COHORT_TEST.lock().await;
    let case = setup_case("10000", true, true, true, true).await?;
    let first = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(first.state_machine_reserved, 1, "{first:?}");
    assert!(case.store.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.is_none());
    let reopened = SqliteStore::open(&case.path)?;
    let settled_io = Arc::new(NativeBuyMockIo {
        runner: None, initial_sol: case.io.initial_sol.clone(),
        fee_lamports: case.io.fee_lamports, fee_slot: case.io.fee_slot,
        expected_message_sha256: case.io.expected_message_sha256.clone(),
        submit_signature: case.io.submit_signature.clone(),
        confirmation: json!({"result":{"value":[{"err":null,"slot":120,
            "confirmationStatus":"confirmed"}]}}),
        receipt: case.io.receipt.clone(), counts: case.io.counts.clone(),
    });
    tick(&case, &case.config, &reopened, settled_io.clone()).await?;
    tick(&case, &case.config, &reopened, settled_io).await?;
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    assert_eq!(reopened.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.unwrap().qty_exact.unwrap().raw(), 10000);
    let db = Connection::open(&case.path)?;
    let count: i64 = db.query_row("SELECT count(*) FROM execution_canary_dispatch WHERE side='buy'", [], |r| r.get(0))?;
    assert_eq!(count, 1);
    Ok(())
}
