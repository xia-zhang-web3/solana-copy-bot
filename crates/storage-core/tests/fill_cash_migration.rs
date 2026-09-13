#[path = "common/historical_migration_fixture.rs"]
mod historical;
use anyhow::Result;
use copybot_storage_core::SqliteStore;
use rusqlite::{params, Connection};
use std::path::Path;
use tempfile::tempdir;

const MIGRATIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations");

#[test]
fn nonempty_0054_upgrade_preserves_values_ids_sequence_indexes_triggers_and_foreign_keys(
) -> Result<()> {
    let dir = tempdir()?;
    let old = dir.path().join("old");
    std::fs::create_dir(&old)?;
    for file in std::fs::read_dir(MIGRATIONS)? {
        let file = file?;
        if file.file_name().to_string_lossy().as_ref() < "0055" {
            std::fs::copy(file.path(), old.join(file.file_name()))?;
        }
    }
    let path = dir.path().join("history.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let conn = Connection::open(&path)?;
    conn.execute_batch("PRAGMA foreign_keys=ON;
        INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status)
            VALUES('s','w','mint','sell',1,'2026-09-05T00:00:00Z','shadow_recorded');
        CREATE INDEX deployed_fill_token ON fills(token,id);
        CREATE TABLE fill_audit (id INTEGER);
        CREATE TRIGGER deployed_fill_insert AFTER INSERT ON fills BEGIN INSERT INTO fill_audit VALUES(NEW.id); END;
        CREATE TABLE fill_child_restrict (fill_id INTEGER REFERENCES fills(id) ON DELETE RESTRICT);
        CREATE TABLE fill_child_cascade (fill_id INTEGER REFERENCES fills(id) ON DELETE CASCADE);")?;
    for (id, price, fee) in [(11, 0.031, Some(0.000005)), (900, 1.25, Some(0.0))] {
        let order = format!("order-{id}");
        conn.execute(
            "INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt)
            VALUES(?1,'s','legacy','2026-09-05T00:00:00Z','execution_canary_confirmed',?1,1)",
            [&order],
        )?;
        conn.execute("INSERT INTO fills(id,order_id,token,qty,avg_price,fee,slippage_bps,qty_raw,qty_decimals,notional_lamports,fee_lamports)
            VALUES(?1,?2,'mint',3,?3,?4,7.5,'300',2,777,?5)",params![id,order,price,fee,fee.map(|_|5000)])?;
    }
    conn.execute_batch(
        "DELETE FROM fills WHERE id=900;
        INSERT INTO fill_child_restrict VALUES(11); INSERT INTO fill_child_cascade VALUES(11);",
    )?;
    let query="SELECT id,order_id,token,qty,avg_price,fee,slippage_bps,notional_lamports,fee_lamports,qty_raw,qty_decimals FROM fills ORDER BY id";
    let before = values(&conn, query)?;
    let audit = values(&conn, "SELECT * FROM fill_audit")?;
    let through61 = dir.path().join("through61");
    historical::prefix(&through61, "0062")?;
    assert_eq!(store.run_migrations(&through61)?, 7);
    assert_eq!(values(&conn, query)?, before);
    assert_eq!(
        values(&conn, "SELECT * FROM fill_audit")?,
        audit,
        "rebuild must not fire historical insert triggers"
    );
    for table in ["fill_child_restrict", "fill_child_cascade"] {
        assert_eq!(
            conn.query_row(&format!("SELECT fill_id FROM {table}"), [], |r| r
                .get::<_, i64>(0))?,
            11
        );
    }
    assert_eq!(conn.query_row("SELECT count(*) FROM sqlite_master WHERE name IN('deployed_fill_token','deployed_fill_insert')",[],|r|r.get::<_,i64>(0))?,2);
    assert_eq!(
        conn.query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='fills'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        900
    );
    assert_eq!(
        conn.query_row("SELECT accounting_basis FROM fills", [], |r| r
            .get::<_, String>(0))?,
        "legacy_unclassified"
    );
    assert!(conn
        .execute(
            "INSERT INTO fills(order_id,token,qty) VALUES('order-11','mint',3)",
            []
        )
        .is_err());
    assert!(conn
        .execute(
            "INSERT INTO fills(order_id,token,qty) VALUES('nonexistent','mint',3)",
            []
        )
        .is_err());
    assert!(conn
        .execute("DELETE FROM orders WHERE order_id='order-11'", [])
        .is_err());
    conn.execute("INSERT INTO fills(order_id,token,qty,avg_price,fee,slippage_bps) VALUES('order-900','mint',3,NULL,NULL,NULL)",[])?;
    assert_eq!(conn.last_insert_rowid(), 901);
    assert_eq!(
        conn.query_row("SELECT max(id) FROM fill_audit", [], |r| r.get::<_, i64>(0))?,
        901
    );
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    assert_eq!(store.run_migrations(&through61)?, 0);
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(&through61)?, 0);
    store.ensure_history_retention_tables()?;
    for field in ["avg_price", "fee", "slippage_bps"] {
        assert_eq!(
            conn.query_row(
                "SELECT \"notnull\" FROM pragma_table_info('fills') WHERE name=?1",
                [field],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
    }
    Ok(())
}

#[test]
fn fresh_cash_schema_rejects_incomplete_or_real_money_and_retains_single_marker() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("new.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(Path::new(MIGRATIONS))?;
    let conn = Connection::open(&path)?;
    conn.execute_batch("PRAGMA foreign_keys=ON;
        INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status) VALUES('s','w','mint','sell',1,'t','x');
        INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt) VALUES('o','s','r','t','x','c',1);")?;
    assert!(conn.execute("INSERT INTO fills(order_id,token,qty,accounting_basis) VALUES('o','mint',1,'receipt_native_cash')",[]).is_err());
    let sql="INSERT INTO fills(order_id,token,qty,avg_price,fee,slippage_bps,qty_raw,qty_decimals,accounting_basis,position_id,
        wallet_native_delta_lamports,entry_basis_lamports,cash_result_delta_lamports,accumulated_cash_result_lamports,
        remaining_qty_raw,remaining_cost_lamports,settlement_ts)
        VALUES('o','mint',1,NULL,NULL,NULL,'1',0,'receipt_native_cash','p',?1,1,?2,?2,'0',0,'t')";
    assert!(conn.execute(sql, params![0.5, -0.5]).is_err());
    conn.execute(sql, params![0_i64, -1_i64])?;
    for assignment in [
        "wallet_native_delta_lamports=9223372036854775808.0",
        "entry_basis_lamports=-1",
        "cash_result_delta_lamports=NULL",
        "accumulated_cash_result_lamports=0.5",
        "qty_decimals=NULL",
        "avg_price=0",
        "fee=0",
        "slippage_bps=0",
        "notional_lamports=0",
        "position_id=NULL",
    ] {
        assert!(
            conn.execute(&format!("UPDATE fills SET {assignment}"), [])
                .is_err(),
            "{assignment}"
        );
    }
    store.ensure_history_retention_tables()?;
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}

fn values(conn: &Connection, sql: &str) -> Result<Vec<Vec<rusqlite::types::Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let n = stmt.column_count();
    let rows = stmt
        .query_map([], |r| {
            (0..n)
                .map(|i| r.get(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[test]
fn unsupported_raw_migration_runner_fails_before_changing_history() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE fills(id INTEGER PRIMARY KEY,order_id TEXT,avg_price REAL NOT NULL);
        INSERT INTO fills VALUES(7,'old',0.5);",
    )?;
    let before = values(&conn, "SELECT * FROM fills")?;
    let sql = std::fs::read_to_string(
        Path::new(MIGRATIONS).join("0055_execution_receipt_cash_settlement.sql"),
    )?;
    assert!(conn.execute_batch(&sql).is_err());
    assert_eq!(values(&conn, "SELECT * FROM fills")?, before);
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='fills_cash_upgrade'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}
