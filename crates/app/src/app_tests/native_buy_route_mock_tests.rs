//! Causal native admission to the existing tiny BUY route and canonical receipt.
#[path = "native_buy_route_fixture.rs"]
mod fixture;
use crate::execution_canary_route::{
    process_native_buy_with_mock_quote_and_adapter, NativeBuyGuard, NativeBuyMockCounts,
    NativeBuyMockIo,
};
use crate::execution_native_buy_rpc;
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::association_delivery::{
    AdmissionFacts, BlockTime, CandidateGeneration, DeliveryEvent, ProviderAssertion,
    Terminal,
};
use copybot_core_types::association_parent::ParentObservation;
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    native_buy::SPL_TOKEN_PROGRAM,
    ordered_sell_quote::QuoteClaimStep,
    ExecutionCanaryReceiptProof, SqliteStore,
};
use fixture::{
    admission, config, delivery, quote, signed_payload, SignedAdapter, GENESIS, LEADER, MINT,
    SOURCE_SIGNATURE,
};
use rusqlite::params;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn native_buy_admission_route_receipt_opens_real_owned_h1000() -> Result<()> {
    native_buy_case(CaseMode::Positive).await
}

#[tokio::test]
async fn native_buy_cohort_substitution_after_simulation_blocks_send_and_restart() -> Result<()> {
    native_buy_case(CaseMode::Substitute).await
}

#[tokio::test]
async fn native_buy_failed_receipt_books_fee_once_without_h() -> Result<()> {
    native_buy_case(CaseMode::FailedReceipt).await
}

#[tokio::test]
async fn native_buy_unknown_send_retains_dispatch_and_no_h() -> Result<()> {
    native_buy_case(CaseMode::UnknownSend).await
}

#[tokio::test]
async fn native_buy_disabled_authority_reconciles_pending_dispatch_after_reopen() -> Result<()> {
    native_buy_case(CaseMode::PendingConfirmation).await
}

#[tokio::test]
async fn native_buy_receipt_mismatch_keeps_unresolved_hold_after_reopen() -> Result<()> {
    native_buy_case(CaseMode::ReceiptMismatch).await
}

#[tokio::test]
async fn native_buy_source_price_slippage_blocks_bad_quote_before_reserve() -> Result<()> {
    native_buy_case(CaseMode::BadPrice).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CaseMode {
    Positive,
    Substitute,
    FailedReceipt,
    UnknownSend,
    PendingConfirmation,
    ReceiptMismatch,
    BadPrice,
}

async fn native_buy_case(mode: CaseMode) -> Result<()> {
    let native = crate::app_tests::fractional::fractional_fixture::native_binding().await?;
    let mut evidence = crate::app_tests::fractional::fractional_tests::evidence()?;
    let (payload, signature, payer) = signed_payload()?;
    let wallet = bs58::encode(payer).into_string();
    let config = config(&wallet);
    let path = format!(
        "file:native-buy-route-{}-{}?mode=memory&cache=shared",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    copybot_storage_core::ensure_discovery_v2_schema(&store)?;
    let sql = rusqlite::Connection::open(&path)?;
    let now = Utc::now();
    let window = "2026-09-22T00:00:00+00:00";
    sql.execute(
        "INSERT INTO followlist(wallet_id,added_at,active) VALUES(?1,?2,1)",
        params![LEADER, (now - Duration::seconds(3)).to_rfc3339()],
    )?;
    sql.execute("INSERT INTO discovery_candidate_sources(wallet_id,source_cohort,window_start,updated_at) VALUES(?1,'candidate',?2,?3)",
        params![LEADER,window,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_strategy_state(id,publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json,updated_at) VALUES(1,'healthy',?1,?2,'policy',?3,?4)",
        params![(now-Duration::seconds(3)).to_rfc3339(),window,json!([LEADER]).to_string(),now.to_rfc3339()])?;
    fixture::actual_source_replay(&store, &path, &config).await?;
    assert_eq!(store.list_native_buy_pending(1)?.len(), 1);
    let mut inbox = AssociationInbox::open_ordered_sell_consumer(
        &path,
        InboxLimits { count: 1000, bytes: 8 << 20, busy_ms: 100 },
    )?;
    let mut rpc = Parsed(|request: Value| async move {
        let result = match request["method"].as_str().unwrap() {
            "getGenesisHash" => json!(GENESIS),
            "getTransaction" => json!({"slot":100,"meta":{"err":null,
                "postTokenBalances":[{"mint":MINT,"owner":LEADER}]},
                "transaction":{"signatures":[SOURCE_SIGNATURE],"message":{"accountKeys":[
                    {"pubkey":LEADER,"signer":true}]}}}),
            "getAccountInfo" => json!({"context":{"slot":100},"value":{"owner":SPL_TOKEN_PROGRAM}}),
            _ => panic!("unexpected source proof request"),
        };
        Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    });
    execution_native_buy_rpc::finalized_source(
        &mut rpc,
        &config,
        SOURCE_SIGNATURE,
        100,
        LEADER,
        MINT,
        &mut || Ok(()),
    )
    .await?;
    assert!(store.native_buy_record_finalized(
        SOURCE_SIGNATURE,
        100,
        SPL_TOKEN_PROGRAM,
        Utc::now()
    )?);
    let candidate = store
        .native_buy_ready(
            SOURCE_SIGNATURE,
            Utc::now(),
            config.canary_max_signal_age_seconds,
        )?
        .unwrap();
    let signal = store
        .load_copy_signal_by_signal_id(&candidate.signal_id)?
        .unwrap();
    assert!(store.load_execution_canary_observed_leg_by_signature(SOURCE_SIGNATURE)?.is_none());
    let fresh = quote(&store, &signal, &config, &candidate.decision_id, Utc::now(),
        mode == CaseMode::BadPrice)?;
    let decoded = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    let facts = crate::execution_native_rpc::synthetic_classic_funding(
        &payload,
        payer,
        1_000_000_000,
        19_000,
        120,
    )?;
    let mut receipt = json!({"jsonrpc":"2.0","result":{"slot":120,
        "transaction":{"signatures":[signature],"message":{"accountKeys":[
            {"pubkey":wallet,"signer":true,"writable":true},
            {"pubkey":bs58::encode([42;32]).into_string(),"signer":false,"writable":true}]}},
        "meta":{"err":null,"fee":19000,"preBalances":[1_000_000_000,2_039_280],
            "postBalances":[989_981_000,2_039_280],
            "preTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,"uiTokenAmount":{"amount":"0","decimals":3}}],
            "postTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,"uiTokenAmount":{"amount":"1000","decimals":3}}]}}});
    if mode == CaseMode::FailedReceipt {
        receipt["result"]["meta"]["err"] = json!({"InstructionError":[0,"GenericError"]});
        receipt["result"]["meta"]["postBalances"][0] = json!(999_981_000u64);
        receipt["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("0");
    }
    if mode == CaseMode::ReceiptMismatch {
        receipt["result"]["transaction"]["signatures"][0] = json!("different-signature");
    }
    let counts = Arc::new(Mutex::new(NativeBuyMockCounts::default()));
    let io = Arc::new(NativeBuyMockIo {
        runner: None,
        initial_sol: facts,
        fee_lamports: 19_000,
        fee_slot: 120,
        expected_message_sha256: decoded.binding.message_sha256.clone(),
        submit_signature: (mode != CaseMode::UnknownSend).then(|| signature.clone()),
        confirmation: if matches!(mode, CaseMode::UnknownSend | CaseMode::PendingConfirmation) {
            json!({"result":{"value":[null]}})
        } else {
            json!({"result":{"value":[{"err":null,"slot":120,"confirmationStatus":"confirmed"}]}})
        },
        receipt,
        counts: counts.clone(),
    });
    store.activate_tiny_experiment("fractional-test", &wallet, now)?;
    let guard = NativeBuyGuard::new(&config, &candidate.signal_id, &candidate.decision_id, now)?
        .with_mock_io(io.clone());
    assert!(guard.check(&store)?);
    let adapter = SignedAdapter {
        config: config.clone(),
        payload,
        signature,
        substitute_cohort_at_simulation: (mode == CaseMode::Substitute).then(|| path.clone()),
    };
    let outcome = process_native_buy_with_mock_quote_and_adapter(
        &config, &store, &signal, now, &guard, &adapter, fresh,
    )
    .await?;
    if mode == CaseMode::BadPrice {
        assert_eq!(outcome.entry_gate_blocked, 1);
        assert_eq!(counts.lock().unwrap().send, 0);
        assert!(store.load_execution_canary_order_by_signal(&signal.signal_id)?.is_none());
        assert!(store.load_execution_canary_open_position(MINT)?.is_none());
        return Ok(());
    }
    if mode == CaseMode::Substitute {
        assert_eq!(outcome.skipped_reason, Some("native_buy_decision_changed"));
        assert_eq!(counts.lock().unwrap().send, 0);
        assert!(store.load_execution_canary_open_position(MINT)?.is_none());
        let reopened = SqliteStore::open(&path)?;
        let again = process_native_buy_with_mock_quote_and_adapter(
            &config,
            &reopened,
            &signal,
            now,
            &guard,
            &adapter,
            crate::execution_build_plan_metadata::load_execution_build_plan_metadata(
                &reopened,
                &signal.signal_id,
            )?,
        )
        .await?;
        assert_eq!(again.existing, 1);
        assert_eq!(counts.lock().unwrap().send, 0);
        return Ok(());
    }
    if mode == CaseMode::PendingConfirmation {
        assert_eq!(counts.lock().unwrap().send, 1);
        let order_id = outcome.last_order_id.as_deref().expect("pending BUY order");
        let unresolved: (Option<String>, Option<String>) = sql.query_row(
            "SELECT outcome,reconciled_at FROM execution_tiny_reservations WHERE order_id=?1",
            [order_id], |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        assert_eq!(unresolved, (None,None));
        assert!(store.load_execution_canary_open_position(MINT)?.is_none());
        let reopened = SqliteStore::open(&path)?;
        let mut disabled = config.clone();
        disabled.native_fresh_buy = None;
        disabled.canary_wallet_pubkey = "different-current-wallet".into();
        let pending = crate::execution_canary_route::
            process_native_buy_receipt_recovery_for_route_with_mock(
                &disabled, &reopened, Utc::now(), &io,
            ).await?;
        assert_eq!(pending.existing, 1);
        assert!(reopened.load_execution_canary_open_position(MINT)?.is_none());
        assert_eq!(counts.lock().unwrap().send, 1);
        let still_unresolved: (Option<String>, Option<String>) = sql.query_row(
            "SELECT outcome,reconciled_at FROM execution_tiny_reservations WHERE order_id=?1",
            [order_id], |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        assert_eq!(still_unresolved, unresolved);
        let success_io = NativeBuyMockIo {
            runner: None,
            initial_sol: io.initial_sol.clone(), fee_lamports: io.fee_lamports,
            fee_slot: io.fee_slot,
            expected_message_sha256: io.expected_message_sha256.clone(),
            submit_signature: io.submit_signature.clone(),
            confirmation: json!({"result":{"value":[{"err":null,"slot":120,
                "confirmationStatus":"confirmed"}]}}),
            receipt: io.receipt.clone(), counts: counts.clone(),
        };
        let settled = crate::execution_canary_route::
            process_native_buy_receipt_recovery_for_route_with_mock(
                &disabled, &reopened, Utc::now(), &success_io,
            ).await?;
        assert_eq!(settled.existing, 1);
        assert_eq!(reopened.load_execution_canary_open_position(MINT)?
            .unwrap().qty_exact.unwrap().raw(), 1000);
        let again = crate::execution_canary_route::
            process_native_buy_receipt_recovery_for_route_with_mock(
                &disabled, &reopened, Utc::now(), &success_io,
            ).await?;
        assert_eq!(again.existing, 0);
        assert_eq!(counts.lock().unwrap().send, 1);
        return Ok(());
    }
    if mode == CaseMode::ReceiptMismatch {
        let order_id = outcome.last_order_id.as_deref().expect("mismatched BUY order");
        assert!(store.load_execution_canary_open_position(MINT)?.is_none());
        let before: (Option<String>, Option<String>) = sql.query_row(
            "SELECT outcome,reconciled_at FROM execution_tiny_reservations WHERE order_id=?1",
            [order_id], |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        assert_eq!(before, (None,None));
        let mut disabled = config.clone();
        disabled.native_fresh_buy = None;
        let reopened = SqliteStore::open(&path)?;
        let repeated = crate::execution_canary_route::
            process_native_buy_receipt_recovery_for_route_with_mock(
                &disabled, &reopened, Utc::now(), &io,
            ).await?;
        assert_eq!(repeated.existing, 1);
        assert!(reopened.load_execution_canary_open_position(MINT)?.is_none());
        let after: (Option<String>, Option<String>) = sql.query_row(
            "SELECT outcome,reconciled_at FROM execution_tiny_reservations WHERE order_id=?1",
            [order_id], |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        assert_eq!(before, after);
        assert_eq!(counts.lock().unwrap().send, 1);
        return Ok(());
    }
    if mode == CaseMode::UnknownSend {
        assert_eq!(counts.lock().unwrap().send, 1);
        assert!(store.load_execution_canary_open_position(MINT)?.is_none());
        fixture::persist_unowned_sell(&mut inbox, &native, now)?;
        assert!(matches!(
            store.claim_strict_sell_quote_for_owned_preparation(
                crate::app_tests::association_parent_fixture::limits(),
                "http://127.0.0.1:1/",
                Utc::now
            )?,
            QuoteClaimStep::Empty
        ));
        let reopened = SqliteStore::open(&path)?;
        let mut disabled = config.clone();
        disabled.native_fresh_buy = None;
        let pending = crate::execution_canary_route::
            process_native_buy_receipt_recovery_for_route_with_mock(
                &disabled, &reopened, Utc::now(), &io,
            ).await?;
        assert_eq!(pending.existing, 1);
        let pending_again = crate::execution_canary_route::
            process_native_buy_receipt_recovery_for_route_with_mock(
                &disabled, &reopened, Utc::now(), &io,
            ).await?;
        assert_eq!(pending_again.existing, 1);
        let dispatches: i64 =
            sql.query_row("SELECT count(*) FROM execution_canary_dispatch", [], |r| {
                r.get(0)
            })?;
        assert_eq!(dispatches, 1);
        assert!(reopened
            .load_execution_canary_open_position(MINT)?
            .is_none());
        let unresolved: Option<String> = sql.query_row(
            "SELECT outcome FROM execution_tiny_reservations LIMIT 1", [], |r|r.get(0))?;
        assert_eq!(unresolved, None);
        assert_eq!(counts.lock().unwrap().send, 1);
        return Ok(());
    }
    if mode == CaseMode::FailedReceipt {
        assert_eq!(counts.lock().unwrap().send, 1);
        assert!(store.load_execution_canary_open_position(MINT)?.is_none());
        let order_id = outcome.last_order_id.as_deref().expect("failed BUY order");
        let fee: (String,String) = sql.query_row("SELECT transaction_fee_lamports,wallet_fee_lamports FROM execution_failed_expense_ledger WHERE order_id=?1",[order_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
        assert_eq!(fee, ("19000".into(), "19000".into()));
        let ledger_rows: i64 = sql.query_row(
            "SELECT count(*) FROM execution_failed_expense_ledger WHERE order_id=?1",
            [order_id],
            |r| r.get(0),
        )?;
        assert_eq!(ledger_rows, 1);
        let reopened = SqliteStore::open(&path)?;
        let mut disabled = config.clone();
        disabled.native_fresh_buy = None;
        for _ in 0..2 {
            let _ = crate::execution_canary_route::
                process_native_buy_receipt_recovery_for_route_with_mock(
                    &disabled, &reopened, Utc::now(), &io,
                ).await?;
        }
        assert!(reopened
            .load_execution_canary_open_position(MINT)?
            .is_none());
        let ledger_after: i64 = sql.query_row(
            "SELECT count(*) FROM execution_failed_expense_ledger WHERE order_id=?1",
            [order_id], |r|r.get(0))?;
        assert_eq!(ledger_after, 1);
        assert_eq!(counts.lock().unwrap().send, 1);
        return Ok(());
    }
    assert_eq!(outcome.reserved, 1, "{outcome:?}");
    let counts = counts.lock().unwrap();
    assert_eq!(
        (
            counts.quote,
            counts.initial_sol,
            counts.fee,
            counts.send,
            counts.confirmation,
            counts.receipt
        ),
        (1, 1, 1, 1, 1, 1)
    );
    let position = store
        .load_execution_canary_open_position(MINT)?
        .expect("canonical BUY H");
    assert_eq!(position.qty_exact.expect("raw H").raw(), 1000);
    drop(counts);

    // Source → executed own BUY → source SELL: actual admission and receipt identities.
    let mut own = admission();
    own.facts.signature = adapter.signature.clone();
    own.facts.slot = 120;
    own.facts.wallet = wallet.clone();
    own.facts.amount_out_bits = 1.0f64.to_bits();
    own.facts.exact_amounts.as_mut().unwrap().amount_out_raw = "1000".into();
    inbox.persist_at(
        &delivery(3, DeliveryEvent::Admission(own.clone())),
        &CandidateGeneration::Unknown,
        now,
    )?;
    inbox.persist_at(
        &delivery(
            4,
            DeliveryEvent::Terminal {
                signature: adapter.signature.clone(),
                expected: own,
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: 120,
                    blockhash: native["anchors"][1]["terminal"]["ProviderAsserted"]["blockhash"]
                        .as_str()
                        .unwrap()
                        .into(),
                    signature: adapter.signature.clone(),
                    transaction_index: 0,
                    block_time: BlockTime::Missing,
                }),
            },
        ),
        &CandidateGeneration::Unknown,
        now,
    )?;
    let sell: AdmissionFacts =
        serde_json::from_value(native["anchors"][2]["identity"]["admission"].clone())?;
    let generation = store.association_candidate(&sell.facts);
    inbox.persist_at(
        &delivery(5, DeliveryEvent::Admission(sell.clone())),
        &generation,
        now,
    )?;
    inbox.persist_at(
        &delivery(
            6,
            DeliveryEvent::Terminal {
                signature: sell.facts.signature.clone(),
                expected: sell.clone(),
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: 150,
                    blockhash: native["anchors"][2]["terminal"]["ProviderAsserted"]["blockhash"]
                        .as_str()
                        .unwrap()
                        .into(),
                    signature: sell.facts.signature.clone(),
                    transaction_index: 0,
                    block_time: BlockTime::Missing,
                }),
            },
        ),
        &generation,
        now,
    )?;
    for (i, path) in native["parent_paths"]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
        .enumerate()
    {
        let edge = &path["edges"][0];
        let parent = ParentObservation {
            child: serde_json::from_value(edge["child"].clone())?,
            parent: serde_json::from_value(edge["parent"].clone())?,
            issue: None,
        };
        inbox.persist_at(
            &delivery(7 + i as u64, DeliveryEvent::Parent(parent)),
            &CandidateGeneration::Unknown,
            now,
        )?;
    }
    for _ in 0..20 {
        if !inbox.has_sell_preparation_work()? {
            break;
        }
        inbox.recover_sell_preparation()?;
    }
    assert!(inbox.sell_preparation(&sell.facts.signature)?.is_some());
    drop(inbox);
    let mut meta: Value = serde_json::from_slice(&std::fs::read(
        crate::app_tests::b135_fixture::inputs().join("chain.json"),
    )?)?;
    meta["our"]["signer"] = json!(wallet);
    meta["our"]["signature"] = json!(adapter.signature);
    let db = crate::app_tests::association_fixture::Db {
        path: path.into(),
        sql,
        store,
    };
    let mut f = crate::app_tests::fractional::fractional_fixture::Fixture::from_parts(db, meta)?;
    let step = f.db.store.claim_strict_sell_quote_for_owned_preparation(
        crate::app_tests::association_parent_fixture::limits(),
        "http://127.0.0.1:1/",
        Utc::now,
    )?;
    let QuoteClaimStep::Claimed(initial) = step else {
        anyhow::bail!("native BUY did not enable dependent SELL claim")
    };
    assert_eq!(initial.binding.raw, 1000);
    evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["owner"] =
        json!(wallet);
    let claim =
        crate::app_tests::fractional::fractional_tests::bind(&mut f, initial, evidence).await?;
    assert_eq!(claim.binding.raw, 250);
    let before =
        f.db.store
            .load_execution_canary_open_position(MINT)?
            .unwrap();
    let prepared = crate::app_tests::fractional::fractional_financial_fixture::prepare(&f, &claim)?;
    let dispatch =
        crate::app_tests::fractional::fractional_financial_fixture::dispatch(&f, &prepared)?;
    let sold = crate::app_tests::fractional::fractional_financial_fixture::receipt(&dispatch, 250);
    f.db.store.mark_execution_canary_confirmed_unreconciled(
        &dispatch.order_id,
        &ExecutionCanaryReceiptProof {
            tx_signature: dispatch.tx_signature.clone(),
            wallet_pubkey: dispatch.wallet.clone(),
            token: dispatch.token.clone(),
            side: "sell".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(151),
            confirmed_at: Utc::now(),
            reason: "mocked canonical receipt".into(),
        },
        Utc::now(),
    )?;
    f.db.store
        .record_execution_canary_receipt_facts(&sold, Utc::now())?;
    f.db.store
        .apply_execution_canary_sell_settlement(&sold, Utc::now())?;
    let reopened = SqliteStore::open(&f.db.path)?;
    reopened.apply_execution_canary_sell_settlement(&sold, Utc::now())?;
    let after = reopened.load_execution_canary_open_position(MINT)?.unwrap();
    assert_eq!(after.qty_exact.unwrap().raw(), 750);
    let basis = before.cost_lamports.unwrap().as_u64();
    let allocated = (basis * 250).div_ceil(1000);
    assert_eq!(after.cost_lamports.unwrap().as_u64(), basis - allocated);
    let cash = reopened
        .load_execution_canary_cash_settlement(&dispatch.order_id)?
        .unwrap();
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(cash.allocated_entry_basis.as_u64(), allocated);
    assert_eq!(cash.remaining_entry_basis.as_u64(), basis - allocated);
    let fills: i64 = f.db.sql.query_row(
        "SELECT count(*) FROM fills WHERE order_id=?1",
        [&dispatch.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(fills, 1);
    let fee: Option<String> = f.db.sql.query_row(
        "SELECT transaction_fee FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&dispatch.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(fee.as_deref(), Some("19000"));
    Ok(())
}
