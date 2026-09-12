#[path = "common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;
use rusqlite::params;

#[test]
fn signed_apply_full_and_partial_is_exact_and_replay_safe() -> Result<()> {
    for sold in [6, 7] {
        for native in [-13, 0, 29] {
            let db = Db::new(7, 23, -2, sold, native)?;
            let p = db.ready()?;
            let out = db
                .store
                .apply_execution_canary_sell_settlement(&db.facts(sold, native), db.now)?;
            assert!(!out.already_accounted);
            let s = out.settlement;
            assert_eq!(s.allocated_entry_basis, p.allocated_entry_basis);
            assert_eq!(s.remaining_quantity.raw(), 7 - sold);
            assert_eq!(
                s.cash_result_delta.as_i128(),
                native - i128::from(s.allocated_entry_basis.as_u64())
            );
            assert_eq!(
                s.accumulated_cash_result.as_i128(),
                s.cash_result_delta.as_i128() - 2
            );
            assert!(s.swap_price.is_none());
            assert_eq!(s.decomposition, ReceiptDecomposition::Unresolved);
            assert!(!db.store.execution_canary_accounting_pending()?);
            let before = snapshot(&db.conn()?)?;
            let reopened = SqliteStore::open(&db.path)?;
            assert!(
                reopened
                    .apply_execution_canary_sell_settlement(&db.facts(sold, native), db.now)?
                    .already_accounted
            );
            assert_eq!(snapshot(&db.conn()?)?, before);
            let fields:(String,String,String,i64) = db.conn()?.query_row(
                "SELECT typeof(pnl_lamports),typeof(cost_lamports),state,pnl_lamports FROM positions",[],
                |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            assert_eq!(fields.0, "integer");
            assert_eq!(fields.1, "integer");
            assert_eq!(fields.2, if sold == 7 { "closed" } else { "open" });
            assert_eq!(i128::from(fields.3), s.accumulated_cash_result.as_i128());
        }
    }
    Ok(())
}

#[test]
fn sequential_partial_applications_conserve_odd_basis_and_one_raw() -> Result<()> {
    let db = Db::new(7, 23, 0, 2, 5)?;
    let mut allocated = 0;
    let mut native_sum = 0;
    for (i, (sold, native, remaining, expected_basis)) in
        [(2, 5, 5, 7), (4, -3, 1, 13), (1, 0, 0, 3)]
            .into_iter()
            .enumerate()
    {
        let mut fresh = db.facts(sold, native);
        if i > 0 {
            fresh.order_id = format!("exec-canary:partial-{i}");
            fresh.tx_signature = format!("partial-signature-{i}");
            let conn = db.conn()?;
            conn.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,tx_signature,client_order_id,simulation_status,attempt,err_code)
                VALUES(?1,'sell-signal','tiny',?2,?3,?4,?1,'passed',1,?5)",
                params![fresh.order_id,db.now.to_rfc3339(),EXECUTION_STATUS_CANARY_SUBMITTED,fresh.tx_signature,EXECUTION_ACCOUNTING_PENDING_REASON])?;
            db.store.mark_execution_canary_confirmed_unreconciled(
                &fresh.order_id,
                &ExecutionCanaryReceiptProof {
                    tx_signature: fresh.tx_signature.clone(),
                    wallet_pubkey: "wallet".into(),
                    token: "mint".into(),
                    side: "sell".into(),
                    confirmation_status: "confirmed".into(),
                    slot: Some(42),
                    confirmed_at: db.now,
                    reason: "awaiting".into(),
                },
                db.now,
            )?;
            db.store
                .record_execution_canary_receipt_facts(&fresh, db.now)?;
        }
        let s = db
            .store
            .apply_execution_canary_sell_settlement(&fresh, db.now)?
            .settlement;
        allocated += s.allocated_entry_basis.as_u64();
        native_sum += native;
        assert_eq!(s.allocated_entry_basis.as_u64(), expected_basis);
        assert_eq!(s.remaining_quantity.raw(), remaining);
        assert_eq!(s.remaining_entry_basis.as_u64() + allocated, 23);
        assert_eq!(
            s.accumulated_cash_result.as_i128(),
            native_sum - i128::from(allocated)
        );
    }
    assert_eq!(
        db.conn()?
            .query_row("SELECT count(*) FROM fills", [], |r| r.get::<_, i64>(0))?,
        3
    );
    Ok(())
}

#[test]
fn current_partial_cannot_borrow_saved_operands_and_conflicts_fail_on_replay() -> Result<()> {
    let db = Db::new(7, 23, 0, 7, 0)?;
    let mut partial = db.facts(7, 0);
    partial.token_delta = None;
    partial.token_coverage = ReceiptTokenCoverage::Unresolved;
    partial.token_coverage_reason = Some("receipt_token_balance_unresolved".into());
    db.store
        .record_execution_canary_receipt_facts(&partial, db.now)?;
    let before = snapshot(&db.conn()?)?;
    assert!(db
        .store
        .apply_execution_canary_sell_settlement(&partial, db.now)
        .is_err());
    assert_eq!(snapshot(&db.conn()?)?, before);
    let mut full = db.facts(7, 0);
    full.block_time = None; // Unknown fresh metadata does not erase durable known metadata.
    db.store
        .apply_execution_canary_sell_settlement(&full, db.now)?;
    let before = snapshot(&db.conn()?)?;
    for field in ["wallet", "signature", "slot", "raw", "native", "partial"] {
        let mut bad = db.facts(7, 0);
        match field {
            "wallet" => bad.wallet_pubkey = "other".into(),
            "signature" => bad.tx_signature = "other".into(),
            "slot" => bad.slot = 43,
            "raw" => bad.token_delta.as_mut().unwrap().raw = -6,
            "native" => bad = db.facts(7, 1),
            "partial" => bad = partial.clone(),
            _ => unreachable!(),
        }
        assert!(
            db.store
                .apply_execution_canary_sell_settlement(&bad, db.now)
                .is_err(),
            "{field}"
        );
        assert_eq!(snapshot(&db.conn()?)?, before);
    }
    db.conn()?
        .execute("UPDATE copy_signals SET token='changed'", [])?;
    assert!(db
        .store
        .validate_execution_canary_cash_settlement_replay(ORDER, "wallet")
        .is_err());
    Ok(())
}

#[test]
fn failure_at_every_accounting_write_rolls_back_inventory_marker_and_completion() -> Result<()> {
    for (table, event) in [
        ("positions", "UPDATE"),
        ("fills", "INSERT"),
        ("orders", "UPDATE"),
        ("execution_canary_receipt_proofs", "UPDATE"),
    ] {
        let db = Db::new(7, 23, 0, 7, -13)?;
        db.conn()?.execute_batch(&format!("CREATE TRIGGER fail_cash BEFORE {event} ON {table} BEGIN SELECT RAISE(ABORT,'injected accounting failure'); END;"))?;
        let before = snapshot(&db.conn()?)?;
        assert!(
            db.store
                .apply_execution_canary_sell_settlement(&db.facts(7, -13), db.now)
                .is_err(),
            "{table}"
        );
        assert_eq!(snapshot(&db.conn()?)?, before, "{table}");
        assert!(db.store.execution_canary_accounting_pending()?);
        assert!(!db.store.execution_canary_fill_exists(ORDER)?);
        assert!(db
            .store
            .load_execution_canary_receipt_facts(ORDER)?
            .is_some());
        db.conn()?.execute_batch("DROP TRIGGER fail_cash")?;
        db.store
            .apply_execution_canary_sell_settlement(&db.facts(7, -13), db.now)?;
        assert!(!db.store.execution_canary_accounting_pending()?);
    }
    Ok(())
}

#[test]
fn apply_replans_current_inventory_instead_of_using_a_stale_ready() -> Result<()> {
    let db = Db::new(7, 23, 0, 3, 0)?;
    assert_eq!(db.ready()?.allocated_entry_basis.as_u64(), 10);
    db.conn()?.execute(
        "UPDATE positions SET qty_raw='4',cost_lamports=11,pnl_lamports=-5",
        [],
    )?;
    let s = db
        .store
        .apply_execution_canary_sell_settlement(&db.facts(3, 0), db.now)?
        .settlement;
    assert_eq!(s.remaining_quantity.raw(), 1);
    assert_eq!(s.allocated_entry_basis.as_u64(), 9);
    assert_eq!(s.accumulated_cash_result.as_i128(), -14);
    let db = Db::new(7, 23, 0, 7, 0)?;
    db.ready()?;
    db.conn()?.execute("UPDATE positions SET qty_raw='6'", [])?;
    let before = snapshot(&db.conn()?)?;
    assert!(db
        .store
        .apply_execution_canary_sell_settlement(&db.facts(7, 0), db.now)
        .is_err());
    assert_eq!(snapshot(&db.conn()?)?, before);
    Ok(())
}

#[test]
fn sqlite_signed_domain_never_promotes_money_to_real() -> Result<()> {
    for (cost, acc, cash, ok) in [
        (0, 0, i128::from(i64::MAX), true),
        (0, 0, i128::from(i64::MIN), true),
        (0, 0, i128::from(i64::MAX) + 1, false),
        (0, 0, i128::from(i64::MIN) - 1, false),
        (1, 0, i128::from(i64::MIN), false),
        (0, 1, i128::from(i64::MAX), false),
        (0, -1, i128::from(i64::MIN), false),
    ] {
        let db = Db::new(7, cost, acc, 7, cash)?;
        let before = snapshot(&db.conn()?)?;
        let result = db
            .store
            .apply_execution_canary_sell_settlement(&db.facts(7, cash), db.now);
        assert_eq!(result.is_ok(), ok, "{cost}/{acc}/{cash}");
        if !ok {
            assert_eq!(snapshot(&db.conn()?)?, before);
        } else {
            assert_eq!(
                db.conn()?
                    .query_row("SELECT typeof(pnl_lamports) FROM positions", [], |r| r
                        .get::<_, String>(
                        0
                    ))?,
                "integer"
            );
        }
    }
    Ok(())
}

#[test]
fn concurrent_apply_has_one_winner_and_one_completion() -> Result<()> {
    let db = Db::new(7, 23, 0, 7, -13)?;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let (path, fresh, now, b) = (db.path.clone(), db.facts(7, -13), db.now, barrier.clone());
        handles.push(std::thread::spawn(move || -> Result<bool> {
            let store = SqliteStore::open(&path)?;
            b.wait();
            Ok(store
                .apply_execution_canary_sell_settlement(&fresh, now)?
                .already_accounted)
        }));
    }
    let mut results = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    results.sort();
    assert_eq!(results, [false, true]);
    assert_eq!(
        db.conn()?
            .query_row("SELECT count(*) FROM fills", [], |r| r.get::<_, i64>(0))?,
        1
    );
    assert_eq!(
        db.conn()?
            .query_row("SELECT pnl_lamports FROM positions", [], |r| r
                .get::<_, i64>(0))?,
        -36
    );
    Ok(())
}

#[test]
fn identity_changed_after_ready_is_rechecked_before_first_write() -> Result<()> {
    for sql in [
        "UPDATE orders SET tx_signature='changed'",
        "UPDATE execution_canary_receipt_proofs SET slot='43'",
        "UPDATE copy_signals SET token='changed'",
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey='changed'",
    ] {
        let db = Db::new(7, 23, 0, 7, 0)?;
        db.ready()?;
        db.conn()?.execute(sql, [])?;
        let before = snapshot(&db.conn()?)?;
        assert!(db
            .store
            .apply_execution_canary_sell_settlement(&db.facts(7, 0), db.now)
            .is_err());
        assert_eq!(snapshot(&db.conn()?)?, before);
        assert!(db.store.execution_canary_accounting_pending()?);
    }
    Ok(())
}
