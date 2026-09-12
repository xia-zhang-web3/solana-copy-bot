#[path = "common/receipt_facts_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;
fn bundle(db: &Db) -> ReceiptObservationBundle {
    let facts = db.facts();
    let mut native = NativeAccountObservations::empty(&facts);
    let known = |s: &str| NativeObservation::known(s, ObservationSource::RpcTokenBalance);
    let end = NativeTokenEndpoint {
        mint: known("mint"),
        token_owner: known("wallet"),
        token_program: known("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
        decimals: known("3"),
        raw: known("0"),
    };
    native.accounts.push(NativeAccountObservation {
        account_index: 1,
        pubkey: "account".into(),
        native_pre: NativeObservation::known("0", ObservationSource::RpcNativeBalance),
        native_post: NativeObservation::known(
            "9007199254740993",
            ObservationSource::RpcNativeBalance,
        ),
        native_delta: NativeObservation::known(
            "9007199254740993",
            ObservationSource::RpcNativeBalance,
        ),
        pre_token: end.clone(),
        post_token: end,
        relevance: vec!["target_mint".into()],
    });
    ReceiptObservationBundle { facts, native }
}
#[test]
fn native_observations_atomic_writes_abort_ignore_readback_and_reopen() -> Result<()> {
    for table in [
        "execution_canary_receipt_facts",
        "execution_receipt_native_observations",
    ] {
        for action in ["RAISE(ABORT,'synthetic local write')", "RAISE(IGNORE)"] {
            let mut db = Db::new(Some(42))?;
            let b = bundle(&db);
            db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER reject_write BEFORE INSERT ON {table} BEGIN SELECT {action}; END;"
            ))?;
            assert!(db
                .store
                .record_receipt_observation_bundle(&b, db.now)
                .is_err());
            db.reopen()?;
            assert!(db
                .store
                .load_execution_canary_receipt_facts(&db.id)?
                .is_none());
            assert!(db.store.load_receipt_native_observations(&db.id)?.is_none());
            assert!(db.store.execution_canary_accounting_pending()?);
            db.conn()?.execute_batch("DROP TRIGGER reject_write")?;
            db.store.record_receipt_observation_bundle(&b, db.now)?;
            db.reopen()?;
            assert_eq!(
                db.store.load_receipt_native_observations(&db.id)?,
                Some(b.native)
            );
        }
    }
    let mut db = Db::new(Some(42))?;
    let b = bundle(&db);
    db.conn()?.execute_batch("CREATE TRIGGER corrupt_write AFTER INSERT ON execution_receipt_native_observations BEGIN UPDATE execution_receipt_native_observations SET tx_signature='changed' WHERE order_id=NEW.order_id; END;")?;
    assert!(db
        .store
        .record_receipt_observation_bundle(&b, db.now)
        .is_err());
    db.reopen()?;
    assert!(db
        .store
        .load_execution_canary_receipt_facts(&db.id)?
        .is_none());
    assert!(db.store.load_receipt_native_observations(&db.id)?.is_none());
    Ok(())
}
#[test]
fn native_observations_pending_enrichment_conflict_is_sticky_and_keeps_original() -> Result<()> {
    let mut db = Db::new(Some(42))?;
    let full = bundle(&db);
    let mut partial = full.clone();
    partial.native.accounts[0].post_token.raw =
        NativeObservation::unknown(ObservationCoverage::Missing);
    partial.native.accounts_coverage = ObservationCoverage::Missing;
    db.store
        .record_receipt_observation_bundle(&partial, db.now)?;
    db.reopen()?;
    db.store.record_receipt_observation_bundle(&full, db.now)?;
    db.store
        .record_receipt_observation_bundle(&partial, db.now)?;
    assert_eq!(
        db.store.load_receipt_native_observations(&db.id)?,
        Some(full.native.clone())
    );
    let mut conflict = full.clone();
    conflict.native.accounts[0].post_token.raw.value = Some("1".into());
    assert!(db
        .store
        .record_receipt_observation_bundle(&conflict, db.now)
        .is_err());
    db.reopen()?;
    assert!(db
        .store
        .record_receipt_observation_bundle(&partial, db.now)
        .is_err());
    assert_eq!(
        db.store.load_receipt_native_observations(&db.id)?,
        Some(full.native)
    );
    let r = db.store.receipt_native_observations_report(
        db.now - chrono::Duration::seconds(1),
        db.now + chrono::Duration::seconds(1),
        1,
    )?;
    assert_eq!(r.conflict_orders, "1");
    assert_eq!(r.rows[0].coverage, "conflict");
    Ok(())
}
#[test]
fn native_observations_completed_immutable_exact_window_limit_and_retention() -> Result<()> {
    let mut db = Db::new(Some(42))?;
    let b = bundle(&db);
    db.store
        .record_receipt_observation_bundle(&b, db.now + chrono::Duration::days(10))?;
    db.account()?;
    let money = snapshot(&db.conn()?)?;
    db.reopen()?;
    db.store.record_receipt_observation_bundle(&b, db.now)?;
    let mut changed = b.clone();
    changed.native.accounts[0].post_token.raw.value = Some("7".into());
    assert!(db
        .store
        .record_receipt_observation_bundle(&changed, db.now)
        .is_err());
    let cutoff = db.now + chrono::Duration::days(50);
    db.store.apply_history_retention(
        HistoryRetentionCutoffs {
            risk_events_before: cutoff,
            copy_signals_before: cutoff,
            orders_before: cutoff,
            shadow_closed_trades_before: cutoff,
            execution_quote_canary_before: cutoff,
        },
        true,
    )?;
    db.reopen()?;
    assert_eq!(snapshot(&db.conn()?)?, money);
    assert_eq!(
        db.store.load_receipt_native_observations(&db.id)?,
        Some(b.native)
    );
    let since = db.now - chrono::Duration::seconds(1);
    let end = db.now + chrono::Duration::seconds(1);
    for limit in [0, 1, 1000] {
        let r = db
            .store
            .receipt_native_observations_report(since, end, limit)?;
        assert_eq!(
            (r.total_orders.as_str(), r.account_rows.as_str()),
            ("1", "1")
        );
        assert_eq!(r.coverage, "covered_observations");
        assert_eq!(r.decomposition, "unresolved");
        assert_eq!(r.rows.len(), usize::from(limit > 0));
        let json = serde_json::to_string(&r)?;
        if limit > 0 {
            assert!(json.contains("\"9007199254740993\""));
        }
    }
    assert_eq!(
        db.store
            .receipt_native_observations_report(end, cutoff, 1)?
            .total_orders,
        "0"
    );
    db.conn()?.execute_batch("PRAGMA foreign_keys=ON")?;
    assert!(db
        .conn()?
        .execute("DELETE FROM execution_canary_receipt_facts", [])
        .is_err());
    Ok(())
}
#[test]
fn native_observations_bounds_canonical_numbers_and_binding_reject_before_write() -> Result<()> {
    for case in [
        "slot",
        "signature",
        "wallet",
        "amount",
        "duplicate",
        "count",
        "reason",
    ] {
        let db = Db::new(Some(42))?;
        let mut b = bundle(&db);
        match case {
            "slot" => b.native.slot = "43".into(),
            "signature" => b.native.tx_signature = "foreign".into(),
            "wallet" => b.native.wallet_pubkey = "foreign".into(),
            "amount" => {
                b.native.accounts[0].pre_token.raw.value = Some("18446744073709551616".into())
            }
            "duplicate" => b.native.accounts.push(b.native.accounts[0].clone()),
            "count" => b.native.accounts = vec![b.native.accounts[0].clone(); 65],
            _ => b.native.reasons.push("x".repeat(100)),
        }
        assert!(
            b.native.validate().is_err()
                || db
                    .store
                    .record_receipt_observation_bundle(&b, db.now)
                    .is_err(),
            "{case}"
        );
        assert!(db
            .store
            .load_execution_canary_receipt_facts(&db.id)?
            .is_none());
    }
    Ok(())
}
