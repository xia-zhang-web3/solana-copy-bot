#[path = "common/receipt_facts_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::*;
use fixture::*;
use rusqlite::Connection;
use tempfile::tempdir;

#[test]
fn receipt_facts_canonical_domains_and_replay_survive_reopen_without_accounting() -> Result<()> {
    for (pre, post, raw) in [
        (0, u64::MAX, i128::MAX),
        (u64::MAX, 0, i128::MIN),
        ((1_u64 << 53) + 1, (1_u64 << 53) + 2, 0),
        (0, 0, -7000),
    ] {
        let mut db = Db::new(None)?;
        let initial = snapshot(&db.conn()?)?;
        let mut facts = db.facts();
        facts.slot = u64::MAX;
        facts.wallet_native_pre = Lamports::new(pre);
        facts.wallet_native_post = Lamports::new(post);
        facts.wallet_native_delta = SignedLamports::new(i128::from(post) - i128::from(pre));
        facts.transaction_fee = Some(Lamports::new(u64::MAX));
        facts.token_delta.as_mut().unwrap().raw = raw;
        assert_eq!(
            db.store
                .record_execution_canary_receipt_facts(&facts, db.now)?,
            ReceiptFactsRecordOutcome::Inserted
        );
        assert_eq!(snapshot(&db.conn()?)?, initial);
        db.reopen()?;
        assert_eq!(
            db.store.load_execution_canary_receipt_facts(&db.id)?,
            Some(facts.clone())
        );
        assert_eq!(
            db.store.record_execution_canary_receipt_facts(
                &facts,
                db.now + chrono::Duration::hours(1)
            )?,
            ReceiptFactsRecordOutcome::Existing
        );
        let stored: (String, String, String, String, String, String, String) = db.conn()?.query_row(
            "SELECT slot, wallet_native_pre, wallet_native_post, wallet_native_delta, transaction_fee, token_delta_raw, recorded_at FROM execution_canary_receipt_facts",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)))?;
        assert_eq!(
            stored,
            (
                u64::MAX.to_string(),
                pre.to_string(),
                post.to_string(),
                facts.wallet_native_delta.as_i128().to_string(),
                u64::MAX.to_string(),
                raw.to_string(),
                db.now.to_rfc3339()
            )
        );
        assert_eq!(snapshot(&db.conn()?)?, initial);
        assert!(db.store.execution_canary_accounting_pending()?);
        assert!(!db.store.execution_canary_fill_exists(&db.id)?);
    }
    Ok(())
}

#[test]
fn receipt_facts_atomic_identity_and_known_conflicts_cannot_overwrite() -> Result<()> {
    let db = Db::new(Some(42))?;
    let facts = db.facts();
    db.store
        .record_execution_canary_receipt_facts(&facts, db.now)?;
    let initial = snapshot(&db.conn()?)?;
    for field in [
        "order",
        "signature",
        "wallet",
        "token",
        "side",
        "slot",
        "pre",
        "post",
        "delta",
        "fee",
        "payer",
        "raw",
        "decimals",
        "coverage",
        "block_time",
    ] {
        let mut bad = facts.clone();
        match field {
            "order" => bad.order_id = "other".into(),
            "signature" => bad.tx_signature = "other".into(),
            "wallet" => bad.wallet_pubkey = "other".into(),
            "token" => bad.token = "other".into(),
            "side" => bad.side = "sell".into(),
            "slot" => bad.slot = 43,
            "pre" => {
                bad.wallet_native_pre = Lamports::new(2001);
                bad.wallet_native_delta = SignedLamports::new(-1001);
            }
            "post" => {
                bad.wallet_native_post = Lamports::new(999);
                bad.wallet_native_delta = SignedLamports::new(-1001);
            }
            "delta" => bad.wallet_native_delta = SignedLamports::new(-999),
            "fee" => bad.transaction_fee = Some(Lamports::new(0)),
            "payer" => bad.fee_payer = Some("sponsor".into()),
            "raw" => bad.token_delta.as_mut().unwrap().raw = 6999,
            "decimals" => bad.token_delta.as_mut().unwrap().decimals = 9,
            "coverage" => bad.token_coverage = ReceiptTokenCoverage::ProvenLifecycle,
            "block_time" => bad.block_time = bad.block_time.map(|t| t + 1),
            _ => unreachable!(),
        }
        assert!(
            db.store
                .record_execution_canary_receipt_facts(&bad, db.now)
                .is_err(),
            "{field}"
        );
        assert_eq!(
            db.store.load_execution_canary_receipt_facts(&db.id)?,
            Some(facts.clone())
        );
        assert_eq!(snapshot(&db.conn()?)?, initial);
    }
    // Check the durable order/proof, not just the caller's facts or existing facts row.
    db.conn()?
        .execute("UPDATE copy_signals SET token = 'changed-durable-mint'", [])?;
    assert!(db
        .store
        .record_execution_canary_receipt_facts(&facts, db.now)
        .is_err());
    assert_eq!(
        db.store.load_execution_canary_receipt_facts(&db.id)?,
        Some(facts)
    );
    Ok(())
}

#[test]
fn receipt_facts_enrichment_is_monotonic_and_all_or_nothing() -> Result<()> {
    let mut db = Db::new(Some(42))?;
    let full = db.facts();
    let mut partial = full.clone();
    partial.transaction_fee = None;
    partial.fee_coverage = ReceiptFeeCoverage::Invalid;
    partial.fee_payer = None;
    partial.token_delta = None;
    partial.token_coverage = ReceiptTokenCoverage::Unresolved;
    partial.token_coverage_reason = Some("receipt_token_creation_unproven".into());
    partial.block_time = None;
    db.store
        .record_execution_canary_receipt_facts(&partial, db.now)?;
    let initial = snapshot(&db.conn()?)?;
    // No known fact may change just because the same input also enriches unknown fields.
    let mut conflict = full.clone();
    conflict.wallet_native_pre = Lamports::new(3000);
    conflict.wallet_native_delta = SignedLamports::new(-2000);
    assert!(db
        .store
        .record_execution_canary_receipt_facts(&conflict, db.now)
        .is_err());
    assert_eq!(
        db.store.load_execution_canary_receipt_facts(&db.id)?,
        Some(partial.clone())
    );
    let mut enriched = full.clone();
    enriched.wsol_coverage = ReceiptWsolCoverage::Observed;
    assert_eq!(
        db.store
            .record_execution_canary_receipt_facts(&enriched, db.now)?,
        ReceiptFactsRecordOutcome::Enriched
    );
    db.reopen()?;
    assert_eq!(
        db.store.load_execution_canary_receipt_facts(&db.id)?,
        Some(enriched.clone())
    );
    assert_eq!(
        db.store
            .record_execution_canary_receipt_facts(&partial, db.now)?,
        ReceiptFactsRecordOutcome::Existing
    );
    assert_eq!(
        db.store.load_execution_canary_receipt_facts(&db.id)?,
        Some(enriched)
    );
    assert_eq!(snapshot(&db.conn()?)?, initial);
    Ok(())
}

#[test]
fn receipt_facts_write_faults_roll_back_insert_and_enrichment() -> Result<()> {
    let db = Db::new(Some(42))?;
    let full = db.facts();
    let initial = snapshot(&db.conn()?)?;
    db.conn()?.execute_batch(
        "CREATE TABLE sentinel (value INTEGER);
        CREATE TRIGGER fail_insert AFTER INSERT ON execution_canary_receipt_facts BEGIN
        INSERT INTO sentinel VALUES (1); SELECT RAISE(ABORT, 'synthetic'); END;",
    )?;
    assert!(db
        .store
        .record_execution_canary_receipt_facts(&full, db.now)
        .is_err());
    assert!(db
        .store
        .load_execution_canary_receipt_facts(&db.id)?
        .is_none());
    assert_eq!(
        db.conn()?
            .query_row("SELECT COUNT(*) FROM sentinel", [], |r| r.get::<_, i64>(0))?,
        0
    );
    db.conn()?.execute_batch("DROP TRIGGER fail_insert")?;
    let mut partial = full.clone();
    partial.transaction_fee = None;
    partial.fee_coverage = ReceiptFeeCoverage::Missing;
    db.store
        .record_execution_canary_receipt_facts(&partial, db.now)?;
    db.conn()?.execute_batch(
        "CREATE TRIGGER fail_update AFTER UPDATE ON execution_canary_receipt_facts BEGIN
        INSERT INTO sentinel VALUES (2); SELECT RAISE(ABORT, 'synthetic'); END;",
    )?;
    assert!(db
        .store
        .record_execution_canary_receipt_facts(&full, db.now)
        .is_err());
    assert_eq!(
        db.store.load_execution_canary_receipt_facts(&db.id)?,
        Some(partial)
    );
    assert_eq!(
        db.conn()?
            .query_row("SELECT COUNT(*) FROM sentinel", [], |r| r.get::<_, i64>(0))?,
        0
    );
    assert_eq!(snapshot(&db.conn()?)?, initial);
    Ok(())
}

#[test]
fn receipt_facts_rejects_malformed_or_out_of_domain_sql_values() -> Result<()> {
    for (field, bad) in [
        ("wallet_native_pre", "18446744073709551616"),
        ("wallet_native_pre", "-1"),
        ("wallet_native_pre", "02000"),
        ("wallet_native_post", "1e3"),
        ("wallet_native_delta", "-0"),
        ("transaction_fee", "0.0"),
        ("token_delta_raw", "170141183460469231731687303715884105728"),
        (
            "token_delta_raw",
            "-170141183460469231731687303715884105729",
        ),
        ("slot", "+42"),
    ] {
        let db = Db::new(Some(42))?;
        let facts = db.facts();
        db.store
            .record_execution_canary_receipt_facts(&facts, db.now)?;
        db.conn()?.execute(
            &format!("UPDATE execution_canary_receipt_facts SET {field} = ?1"),
            [bad],
        )?;
        assert!(
            db.store
                .load_execution_canary_receipt_facts(&db.id)
                .is_err(),
            "{field}: {bad}"
        );
        assert!(db
            .store
            .record_execution_canary_receipt_facts(&facts, db.now)
            .is_err());
        assert!(!db.store.execution_canary_fill_exists(&db.id)?);
    }
    for case in ["unknown_fee", "unknown_token", "reason", "payer"] {
        let db = Db::new(Some(42))?;
        let mut facts = db.facts();
        match case {
            "unknown_fee" => facts.fee_coverage = ReceiptFeeCoverage::Missing,
            "unknown_token" => facts.token_coverage = ReceiptTokenCoverage::Unresolved,
            "reason" => facts.token_coverage_reason = Some("payload".into()),
            "payer" => facts.fee_payer = Some(" ".into()),
            _ => unreachable!(),
        }
        assert!(db
            .store
            .record_execution_canary_receipt_facts(&facts, db.now)
            .is_err());
        assert!(db
            .store
            .load_execution_canary_receipt_facts(&db.id)?
            .is_none());
    }
    Ok(())
}

#[test]
fn receipt_facts_never_backfills_or_enriches_accounted_history() -> Result<()> {
    for with_facts in [false, true] {
        let db = Db::new(Some(42))?;
        let mut facts = db.facts();
        facts.transaction_fee = None;
        facts.fee_coverage = ReceiptFeeCoverage::Missing;
        if with_facts {
            db.store
                .record_execution_canary_receipt_facts(&facts, db.now)?;
        }
        db.account()?;
        let historical = snapshot(&db.conn()?)?;
        if with_facts {
            assert_eq!(
                db.store
                    .record_execution_canary_receipt_facts(&facts, db.now)?,
                ReceiptFactsRecordOutcome::Existing
            );
            let mut enriched = facts.clone();
            enriched.transaction_fee = Some(Lamports::ZERO);
            enriched.fee_coverage = ReceiptFeeCoverage::Known;
            assert!(db
                .store
                .record_execution_canary_receipt_facts(&enriched, db.now)
                .is_err());
            assert_eq!(
                db.store.load_execution_canary_receipt_facts(&db.id)?,
                Some(facts)
            );
        } else {
            assert!(db
                .store
                .record_execution_canary_receipt_facts(&facts, db.now)
                .is_err());
            assert!(db
                .store
                .load_execution_canary_receipt_facts(&db.id)?
                .is_none());
        }
        assert_eq!(snapshot(&db.conn()?)?, historical);
    }
    Ok(())
}

#[test]
fn receipt_facts_0054_upgrade_from_0053_does_not_rewrite_history() -> Result<()> {
    let dir = tempdir()?;
    let old = dir.path().join("pre-0054");
    std::fs::create_dir(&old)?;
    let mut expected_upgrade_versions = Vec::new();
    for entry in std::fs::read_dir(migrations())? {
        let entry = entry?;
        let name = entry.file_name();
        let filename = name.to_string_lossy();
        if filename.ends_with(".sql") {
            if filename.as_ref() < "0054" {
                std::fs::copy(entry.path(), old.join(&name))?;
            } else {
                expected_upgrade_versions.push(filename.into_owned());
            }
        }
    }
    expected_upgrade_versions.sort();
    for required in [
        "0054_execution_canary_receipt_facts.sql",
        "0062_execution_source_sell_staging_cursor.sql",
    ] {
        assert!(expected_upgrade_versions
            .iter()
            .any(|name| name == required));
    }
    let path = dir.path().join("historical.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let (id, now) = seed(&store, Some(42))?;
    store.confirm_execution_canary_buy_fill(
        &id,
        "mint",
        7.0,
        Some(copybot_core_types::TokenQuantity::new(7000, 3)),
        0.000001,
        now,
        now,
        Some(Lamports::new(1000)),
    )?;
    let before = snapshot(&Connection::open(&path)?)?;
    assert_eq!(
        store.run_migrations(migrations())?,
        expected_upgrade_versions.len()
    );
    let recorded_versions = {
        let conn = Connection::open(&path)?;
        let mut versions = conn.prepare(
            "SELECT version FROM schema_migrations WHERE version >= '0054' ORDER BY version",
        )?;
        let rows = versions
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    assert_eq!(recorded_versions, expected_upgrade_versions);
    assert_eq!(store.run_migrations(migrations())?, 0);
    assert_eq!(snapshot(&Connection::open(&path)?)?, before);
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(migrations())?, 0);
    assert_eq!(snapshot(&Connection::open(&path)?)?, before);
    assert!(store.load_execution_canary_receipt_facts(&id)?.is_none());
    assert_eq!(
        Connection::open(path)?.query_row(
            "SELECT COUNT(*) FROM execution_canary_receipt_facts",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}

#[test]
fn receipt_facts_requires_durable_identity_before_first_insert() -> Result<()> {
    for case in [
        "wallet",
        "signature",
        "token",
        "side",
        "slot",
        "missing_proof",
        "confirmation",
        "order_status",
    ] {
        let db = Db::new(Some(42))?;
        let mut facts = db.facts();
        match case {
            "wallet" => facts.wallet_pubkey = "other".into(),
            "signature" => facts.tx_signature = "other".into(),
            "token" => facts.token = "other".into(),
            "side" => facts.side = "sell".into(),
            "slot" => facts.slot = 43,
            "missing_proof" => {
                db.conn()?
                    .execute("DELETE FROM execution_canary_receipt_proofs", [])?;
            }
            "confirmation" => {
                db.conn()?.execute(
                    "UPDATE execution_canary_receipt_proofs SET confirmation_status = 'processed'",
                    [],
                )?;
            }
            "order_status" => {
                db.conn()?.execute(
                    "UPDATE orders SET status = ?1",
                    [EXECUTION_STATUS_CANARY_SUBMITTED],
                )?;
            }
            _ => unreachable!(),
        }
        let initial = snapshot(&db.conn()?)?;
        assert!(
            db.store
                .record_execution_canary_receipt_facts(&facts, db.now)
                .is_err(),
            "{case}"
        );
        assert!(db
            .store
            .load_execution_canary_receipt_facts(&db.id)?
            .is_none());
        assert_eq!(snapshot(&db.conn()?)?, initial);
    }
    Ok(())
}
