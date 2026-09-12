#[path = "common/legacy_sell_ownership_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{ExecutionCanaryReceiptProof, SqliteStore};
use fixture::*;
use std::path::Path;

#[test]
fn recorded_migration_missing_table_and_views_refuse_without_writes() -> Result<()> {
    for (table, migration) in [
        (
            "execution_canary_receipt_proofs",
            "0051_execution_canary_receipt_proofs.sql",
        ),
        (
            "execution_canary_receipt_facts",
            "0054_execution_canary_receipt_facts.sql",
        ),
    ] {
        for view in [false, true] {
            let mut db = database(3)?;
            let id = seed(&db, "current", Api::Confirmed)?;
            assert_eq!(
                db.conn()?.query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version=?1",
                    [migration],
                    |r| r.get::<_, i64>(0)
                )?,
                1
            );
            db.conn()?
                .execute_batch(&format!("ALTER TABLE {table} RENAME TO hidden_receipt"))?;
            if view {
                db.conn()?.execute_batch(&format!(
                    "CREATE VIEW {table} AS SELECT * FROM hidden_receipt"
                ))?;
            }
            db.reopen()?;
            let before = snapshot(&db)?;
            let error =
                apply(&db, Api::Confirmed, &id).expect_err("unavailable modern receipt table");
            assert!(format!("{error:#}").contains(table));
            if !view {
                assert!(format!("{error:#}").contains(migration));
            }
            assert_eq!(snapshot(&db)?, before);
            assert!(!db.store.execution_canary_fill_exists(&id)?);
        }
    }
    Ok(())
}

#[test]
fn migration_metadata_and_receipt_schema_failures_stay_sql_errors() -> Result<()> {
    for corruption in [
        "missing-ledger",
        "broken-ledger-view",
        "missing-column",
        "blob-current",
    ] {
        let db = database(3)?;
        let id = seed(&db, "current", Api::Confirmed)?;
        match corruption {
            "missing-column" => db.conn()?.execute_batch(
                "ALTER TABLE execution_canary_receipt_facts RENAME COLUMN wallet_pubkey TO hidden_wallet")?,
            "blob-current" => { db.conn()?.execute(
                "UPDATE execution_canary_receipt_facts SET wallet_pubkey=x'FF' WHERE order_id=?1", [&id])?; }
            _ => {
                db.conn()?.execute_batch(
                    "ALTER TABLE execution_canary_receipt_facts RENAME TO hidden_facts;
                     ALTER TABLE schema_migrations RENAME TO hidden_migrations;")?;
                if corruption == "broken-ledger-view" {
                    db.conn()?.execute_batch("CREATE VIEW schema_migrations AS SELECT version FROM missing_metadata")?;
                }
                // Keep the existing connection: open() would recreate a missing migration ledger.
            }
        }
        let before = snapshot(&db)?;
        let error =
            apply(&db, Api::Confirmed, &id).expect_err("SQL failure cannot become no claim");
        assert!(
            error.downcast_ref::<rusqlite::Error>().is_some(),
            "{corruption}: {error:#}"
        );
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn recorded_fill_replay_precedes_new_table_metadata_checks() -> Result<()> {
    for api in [Api::Confirm, Api::Confirmed] {
        let db = database(3)?;
        let id = seed(&db, "current", api)?;
        apply(&db, api, &id)?;
        db.conn()?.execute_batch(
            "ALTER TABLE execution_canary_receipt_facts RENAME TO hidden_facts;
             ALTER TABLE schema_migrations RENAME TO hidden_migrations;",
        )?;
        let before = snapshot(&db)?;
        assert_eq!(apply(&db, api, &id)?.close_status, "no_position");
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

fn old_database(before_proofs: bool) -> Result<Db> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("legacy.db");
    let migrations = dir.path().join("migrations");
    std::fs::create_dir(&migrations)?;
    for entry in std::fs::read_dir(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))? {
        let entry = entry?;
        let name = entry.file_name().into_string().unwrap();
        let include = if before_proofs {
            // Supported sparse historical ledger: later versions do not prove 0051 ran.
            !name.starts_with("0051") && !name.starts_with("0061")
        } else {
            name.as_str() < "0054"
        };
        if include && name.ends_with(".sql") {
            std::fs::copy(entry.path(), migrations.join(name))?;
        }
    }
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&migrations)?;
    let db = Db {
        dir,
        path,
        store,
        now: "2026-09-07T12:00:00Z".parse()?,
    };
    // Existing public no-order import API, with no fabricated receipt owner.
    db.store.record_execution_canary_open_position(
        "imported-inventory",
        "mint",
        21.0,
        Some(TokenQuantity::new(21000, 3)),
        0.000003,
        db.now,
    )?;
    Ok(db)
}

#[test]
fn real_old_schemas_keep_no_receipt_and_proof_only_accounting() -> Result<()> {
    for before_proofs in [true, false] {
        let mut db = old_database(before_proofs)?;
        let missing = if before_proofs {
            "0051_execution_canary_receipt_proofs.sql"
        } else {
            "0054_execution_canary_receipt_facts.sql"
        };
        assert_eq!(
            db.conn()?.query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=?1",
                [missing],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        if before_proofs {
            assert!(
                db.conn()?
                    .query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r
                        .get::<_, String>(
                        0
                    ))?
                    > missing.to_owned()
            );
        }
        let id = legacy_order(&db, "old-sell", Api::Confirm)?;
        if !before_proofs {
            db.store.mark_execution_canary_confirmed_unreconciled(
                &id,
                &ExecutionCanaryReceiptProof {
                    tx_signature: "shared-signature".into(),
                    wallet_pubkey: "execution-wallet".into(),
                    token: "mint".into(),
                    side: "sell".into(),
                    confirmation_status: "confirmed".into(),
                    slot: Some(42),
                    confirmed_at: db.now,
                    reason: "receipt_not_fetched".into(),
                },
                db.now,
            )?;
        }
        let result = apply(&db, Api::Confirm, &id)?;
        assert_eq!(result.close_status, "partial");
        assert_eq!(
            result.remaining_qty_exact,
            Some(TokenQuantity::new(14000, 3))
        );
        db.reopen()?;
        let before = snapshot(&db)?;
        assert_eq!(apply(&db, Api::Confirm, &id)?.close_status, "no_position");
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}
