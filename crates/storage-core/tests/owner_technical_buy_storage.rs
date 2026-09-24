use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use copybot_storage_core::{
    BuyAttributionIssue, ExecutionCanaryBuyAttribution, ExecutionCanaryDispatch,
    ExecutionCanaryRecordOutcome, ExecutionDispatchClaim, ExecutionOrderOrigin,
    OwnerTechnicalBuyIntent, OwnerTechnicalBuyIntentRecordOutcome, SqliteStore, TinyBudgetClaim,
    EXECUTION_SIMULATION_STATUS_PASSED,
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
};

fn migrations() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
}
fn fixture(path: &Path) -> Result<SqliteStore> {
    let mut s = SqliteStore::open(path)?;
    s.run_migrations(&migrations())?;
    Ok(s)
}
fn intent() -> OwnerTechnicalBuyIntent {
    let activated_at = Utc.with_ymd_and_hms(2026, 9, 24, 12, 0, 0).unwrap();
    OwnerTechnicalBuyIntent {
        intent_id: "technical-1".into(),
        run_id: "run-1".into(),
        wallet: "synthetic-wallet".into(),
        signer: "synthetic-wallet".into(),
        genesis_hash: "synthetic-genesis".into(),
        mint: "synthetic-mint".into(),
        amount_lamports: 10_000_000,
        route: "metis-canary".into(),
        activated_at,
        expires_at: activated_at + Duration::minutes(10),
        authority_sha256: "a".repeat(64),
        max_priority_fee_lamports: 50_000,
        min_reserve_lamports: 10_000_000,
        max_slippage_bps: 100,
        max_daily_loss_lamports: 10_000_000,
        max_open_positions: 1,
        max_buy_count: 1,
    }
}
fn prepared(
    s: &SqliteStore,
    i: &OwnerTechnicalBuyIntent,
) -> Result<(
    copybot_storage_core::ExecutionCanaryOrder,
    ExecutionCanaryDispatch,
    TinyBudgetClaim,
)> {
    let now = i.activated_at + Duration::seconds(1);
    let order = s
        .reserve_owner_technical_buy_order(&i.intent_id, || Ok(now))?
        .order;
    s.mark_execution_canary_built(&order.order_id, now)?;
    let order = s.mark_execution_canary_simulated(
        &order.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    let dispatch = ExecutionCanaryDispatch {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        route: order.route.clone(),
        attempt: 1,
        wallet: i.wallet.clone(),
        token: i.mint.clone(),
        side: "buy".into(),
        tx_signature: "synthetic-signature".into(),
        transaction_sha256: "b".repeat(64),
        message_sha256: "c".repeat(64),
    };
    let budget = TinyBudgetClaim {
        experiment_id: i.run_id.clone(),
        wallet: i.wallet.clone(),
        tx_signature: dispatch.tx_signature.clone(),
        message_sha256: dispatch.message_sha256.clone(),
        transaction_sha256: dispatch.transaction_sha256.clone(),
        buy_lamports: Some(i.amount_lamports),
        protected_capital: None,
        total_fee: 50_000,
        priority_fee: 20_000,
        fee_slot: 1,
    };
    Ok((order, dispatch, budget))
}

#[test]
fn owner_origin_is_immutable_distinct_and_one_order_only() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let s = fixture(&dir.path().join("db"))?;
    let i = intent();
    assert_eq!(
        s.register_owner_technical_buy_intent(&i)?,
        OwnerTechnicalBuyIntentRecordOutcome::Inserted
    );
    assert_eq!(
        s.register_owner_technical_buy_intent(&i)?,
        OwnerTechnicalBuyIntentRecordOutcome::Existing
    );
    assert_eq!(
        s.load_owner_technical_buy_intent(&i.intent_id)?,
        Some(i.clone())
    );
    assert_eq!(s.list_owner_technical_buy_intents(1)?, vec![i.clone()]);
    let mut changed = i.clone();
    changed.mint = "different".into();
    assert!(s.register_owner_technical_buy_intent(&changed).is_err());
    let order = s.reserve_owner_technical_buy_order(&i.intent_id, || Ok(i.activated_at))?;
    assert_eq!(order.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert_eq!(
        s.execution_order_origin(&order.order.order_id)?,
        Some(ExecutionOrderOrigin::OwnerTechnicalBuy {
            intent_id: i.intent_id.clone()
        })
    );
    assert_eq!(
        s.execution_receipt_token_side(&order.order.order_id)?,
        (i.mint.clone(), "buy".into())
    );
    assert_eq!(
        s.reserve_owner_technical_buy_order(&i.intent_id, || Ok(i.activated_at))?
            .outcome,
        ExecutionCanaryRecordOutcome::Existing
    );
    let c = rusqlite::Connection::open(dir.path().join("db"))?;
    assert!(c.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id) VALUES('other','owner-buy:technical-1','metis','2026-09-24T12:00:00Z','candidate','other-client')",[]).is_err());
    assert!(c
        .execute("UPDATE owner_technical_buy_intents SET mint='other'", [])
        .is_err());
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}

#[test]
fn owner_source_upgrade_preserves_old_orders_and_custom_ddl() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let old = dir.path().join("old");
    std::fs::create_dir(&old)?;
    for entry in std::fs::read_dir(migrations())? {
        let p = entry?.path();
        if p.extension().is_some_and(|v| v == "sql")
            && p.file_name().unwrap().to_str().unwrap() < "0085_owner_technical_buy_intent.sql"
        {
            std::fs::copy(&p, old.join(p.file_name().unwrap()))?;
        }
    }
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&old)?;
    let c = rusqlite::Connection::open(&path)?;
    c.execute_batch("PRAGMA foreign_keys=ON;
      INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status)
      VALUES('old-copy','leader','buy','mint',0.01,'2026-09-24T12:00:00Z','shadow_recorded');
      INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id)
      VALUES('exec-canary:old-copy','old-copy','metis','2026-09-24T12:00:00Z','execution_canary_candidate','client');
      ALTER TABLE execution_order_sources ADD COLUMN deployed_extra BLOB;
      UPDATE execution_order_sources SET deployed_extra=x'00ff';
      CREATE INDEX custom_owner_source_idx ON execution_order_sources(deployed_extra);")?;
    let prior: Vec<u8> = c.query_row(
        "SELECT deployed_extra FROM execution_order_sources WHERE identity_id='old-copy'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(s.run_migrations(&migrations())?, 1);
    assert_eq!(s.run_migrations(&migrations())?, 0);
    assert_eq!(
        c.query_row(
            "SELECT deployed_extra FROM execution_order_sources WHERE identity_id='old-copy'",
            [],
            |r| r.get::<_, Vec<u8>>(0)
        )?,
        prior
    );
    assert!(c
        .prepare("SELECT 1 FROM sqlite_master WHERE name='custom_owner_source_idx'")?
        .exists([])?);
    assert_eq!(
        s.execution_order_origin("exec-canary:old-copy")?,
        Some(ExecutionOrderOrigin::Copy {
            signal_id: "old-copy".into()
        })
    );
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}

#[test]
fn owner_source_failed_upgrade_rolls_back_and_restores_foreign_keys() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let old = dir.path().join("old");
    let all = dir.path().join("all");
    std::fs::create_dir(&old)?;
    std::fs::create_dir(&all)?;
    for entry in std::fs::read_dir(migrations())? {
        let p = entry?.path();
        if p.extension().is_some_and(|v| v == "sql") {
            std::fs::copy(&p, all.join(p.file_name().unwrap()))?;
            if p.file_name().unwrap().to_str().unwrap() < "0085_owner_technical_buy_intent.sql" {
                std::fs::copy(&p, old.join(p.file_name().unwrap()))?;
            }
        }
    }
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&old)?;
    let c = rusqlite::Connection::open(&path)?;
    let before: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE name='execution_order_sources'",
        [],
        |r| r.get(0),
    )?;
    let migration = all.join("0085_owner_technical_buy_intent.sql");
    let original = std::fs::read_to_string(&migration)?;
    std::fs::write(
        &migration,
        format!("{original}\nSELECT * FROM deliberate_missing_table;"),
    )?;
    assert!(s.run_migrations(&all).is_err());
    assert_eq!(
        c.query_row(
            "SELECT sql FROM sqlite_master WHERE name='execution_order_sources'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        before
    );
    assert!(!c
        .prepare("SELECT 1 FROM sqlite_master WHERE name='owner_technical_buy_intents'")?
        .exists([])?);
    assert!(s
        .reserve_execution_canary_order("missing-source", "metis", intent().activated_at)
        .is_err());
    std::fs::write(&migration, original)?;
    assert_eq!(s.run_migrations(&all)?, 1);
    Ok(())
}

#[test]
fn owner_dispatch_is_single_use_and_unknown_survives_restart_and_expiry() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let s = fixture(&path)?;
    let i = intent();
    s.register_owner_technical_buy_intent(&i)?;
    s.activate_tiny_experiment(&i.run_id, &i.wallet, i.activated_at)?;
    let (order, dispatch, budget) = prepared(&s, &i)?;
    let now = i.activated_at + Duration::seconds(2);
    assert_eq!(
        s.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || Ok(now))?,
        ExecutionDispatchClaim::New
    );
    for n in 0..101 {
        let mut later = i.clone();
        later.intent_id = format!("later-intent-{n}");
        later.run_id = format!("later-run-{n}");
        later.activated_at += Duration::seconds(i64::from(n + 1));
        later.expires_at += Duration::seconds(i64::from(n + 1));
        s.register_owner_technical_buy_intent(&later)?;
    }
    assert!(!s.list_owner_technical_buy_intents(100)?.contains(&i));
    assert_eq!(s.list_owner_technical_buy_recovery_intents()?, vec![i.clone()]);
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert_eq!(
        s.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || Ok(
            i.expires_at + Duration::hours(1)
        ))?,
        ExecutionDispatchClaim::Existing
    );
    assert!(s.execution_canary_unresolved_buy()?);
    assert_eq!(
        s.load_execution_canary_dispatch(&order.order_id)?,
        Some(dispatch.clone())
    );
    let mut different = dispatch.clone();
    different.tx_signature = "another".into();
    assert!(s
        .claim_owner_technical_buy_dispatch(&order, &different, &budget, || Ok(now))
        .is_err());
    let c = rusqlite::Connection::open(&path)?;
    let claims: i64 = c.query_row("SELECT COUNT(*) FROM execution_canary_dispatch", [], |r| {
        r.get(0)
    })?;
    assert_eq!(claims, 1);
    Ok(())
}

#[test]
fn copy_buy_cannot_spend_an_owner_activated_tiny_budget() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let s = fixture(&path)?;
    let i = intent();
    s.register_owner_technical_buy_intent(&i)?;
    let now = i.activated_at + Duration::seconds(1);
    s.activate_tiny_experiment(&i.run_id, &i.wallet, now)?;
    let sql = rusqlite::Connection::open(&path)?;
    sql.execute("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status)
        VALUES('competing-copy','leader','buy',?1,0.01,?2,'shadow_recorded')",
        rusqlite::params![i.mint,now.to_rfc3339()])?;
    let signal = s.load_copy_signal_by_signal_id("competing-copy")?.expect("copy signal");
    let order = s.reserve_execution_canary_order(&signal.signal_id, &i.route, now)?.order;
    s.mark_execution_canary_built(&order.order_id, now)?;
    let order = s.mark_execution_canary_simulated(&order.order_id, now,
        EXECUTION_SIMULATION_STATUS_PASSED, None)?;
    let dispatch = ExecutionCanaryDispatch {
        order_id: order.order_id.clone(), signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(), route: order.route.clone(),
        attempt: order.attempt, wallet: i.wallet.clone(), token: i.mint.clone(),
        side: "buy".into(), tx_signature: "competing-signature".into(),
        transaction_sha256: "b".repeat(64), message_sha256: "c".repeat(64),
    };
    let budget = TinyBudgetClaim {
        experiment_id: i.run_id.clone(), wallet: i.wallet.clone(),
        tx_signature: dispatch.tx_signature.clone(),
        message_sha256: dispatch.message_sha256.clone(),
        transaction_sha256: dispatch.transaction_sha256.clone(),
        buy_lamports: Some(i.amount_lamports), protected_capital: None,
        total_fee: 50_000, priority_fee: 20_000, fee_slot: 1,
    };
    let refusal = s.claim_tiny_experiment_dispatch(&order, &signal, &dispatch, &budget, now)
        .expect_err("copy must not spend owner authority");
    assert!(format!("{refusal:#}").contains("owner_buy_budget_exclusive"), "{refusal:#}");
    assert!(s.load_execution_canary_dispatch(&order.order_id)?.is_none());
    assert!(s.list_owner_technical_buy_recovery_intents()?.is_empty());
    Ok(())
}

#[test]
fn owner_confirmed_fill_uses_owner_origin_without_copy_signal() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let s = fixture(&dir.path().join("db"))?;
    let i = intent();
    s.register_owner_technical_buy_intent(&i)?;
    s.activate_tiny_experiment(&i.run_id, &i.wallet, i.activated_at)?;
    let (order, dispatch, budget) = prepared(&s, &i)?;
    let now = i.activated_at + Duration::seconds(2);
    assert_eq!(
        s.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || Ok(now))?,
        ExecutionDispatchClaim::New
    );
    s.mark_execution_canary_confirmed(&order.order_id, now + Duration::seconds(1))?;
    let fill = s.record_execution_canary_confirmed_buy_fill(
        &order.order_id,
        &i.mint,
        10.0,
        None,
        0.01,
        now + Duration::seconds(1),
    )?;
    assert_eq!(fill.position.token, i.mint);
    assert!(s.execution_canary_fill_exists(&order.order_id)?);
    let attribution=s.load_execution_canary_buy_attribution(&i.mint)?;
    let ExecutionCanaryBuyAttribution::Open(attribution)=attribution else { panic!("position missing"); };
    assert!(attribution.proven_contributors.is_empty());
    assert!(attribution.unproven_links.iter().any(|v|
        v.reason==BuyAttributionIssue::OwnerTechnicalBuyHasNoSourceWallet));
    let c = rusqlite::Connection::open(dir.path().join("db"))?;
    let fake_copy: i64 = c.query_row(
        "SELECT COUNT(*) FROM copy_signals WHERE signal_id=?1",
        [&order.signal_id],
        |r| r.get(0),
    )?;
    assert_eq!(fake_copy, 0);
    Ok(())
}

#[test]
fn owner_dispatch_rejects_changed_identity_budget_and_expiry() -> Result<()> {
    for change in ["wallet", "mint", "amount", "priority", "expiry"] {
        let dir = tempfile::tempdir()?;
        let s = fixture(&dir.path().join("db"))?;
        let i = intent();
        s.register_owner_technical_buy_intent(&i)?;
        s.activate_tiny_experiment(&i.run_id, &i.wallet, i.activated_at)?;
        let (order, mut dispatch, mut budget) = prepared(&s, &i)?;
        let mut now = i.activated_at + Duration::seconds(2);
        match change {
            "wallet" => dispatch.wallet = "wrong".into(),
            "mint" => dispatch.token = "wrong".into(),
            "amount" => budget.buy_lamports = Some(1),
            "priority" => budget.priority_fee = 50_001,
            "expiry" => now = i.expires_at,
            _ => unreachable!(),
        }
        assert!(
            s.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || Ok(now))
                .is_err(),
            "{change}"
        );
        assert!(
            s.load_execution_canary_dispatch(&order.order_id)?.is_none(),
            "{change}"
        );
    }
    Ok(())
}

#[test]
fn concurrent_connections_only_one_owner_dispatch_claim() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let s = fixture(&path)?;
    let i = intent();
    s.register_owner_technical_buy_intent(&i)?;
    s.activate_tiny_experiment(&i.run_id, &i.wallet, i.activated_at)?;
    let (order, dispatch, budget) = prepared(&s, &i)?;
    let gate = Arc::new(Barrier::new(3));
    let mut workers = vec![];
    for _ in 0..2 {
        let (path, order, dispatch, budget, gate) = (
            path.clone(),
            order.clone(),
            dispatch.clone(),
            budget.clone(),
            gate.clone(),
        );
        workers.push(std::thread::spawn(move || {
            let s = SqliteStore::open(path).unwrap();
            gate.wait();
            s.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || {
                Ok(order.submit_ts + Duration::seconds(1))
            })
        }));
    }
    gate.wait();
    let results = workers
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(ExecutionDispatchClaim::New)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Ok(ExecutionDispatchClaim::Existing)))
            .count(),
        1
    );
    Ok(())
}

#[test]
fn owner_failed_order_with_pending_expense_remains_in_recovery_after_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let s = fixture(&path)?;
    let i = intent();
    s.register_owner_technical_buy_intent(&i)?;
    s.activate_tiny_experiment(&i.run_id, &i.wallet, i.activated_at)?;
    let (order, dispatch, budget) = prepared(&s, &i)?;
    s.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget,
        || Ok(i.activated_at + Duration::seconds(2)))?;
    s.detect_failed_expense(&order.order_id, &i.wallet, "signature_status", "confirmed",
        Some(10), &serde_json::json!({"InstructionError":[0,{"Custom":1}]}),
        i.activated_at + Duration::seconds(3))?;
    assert_eq!(s.load_execution_canary_order(&order.order_id)?.unwrap().status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED);
    drop(s);
    let reopened = SqliteStore::open(path)?;
    assert_eq!(reopened.list_owner_technical_buy_recovery_intents()?, vec![i]);
    Ok(())
}
