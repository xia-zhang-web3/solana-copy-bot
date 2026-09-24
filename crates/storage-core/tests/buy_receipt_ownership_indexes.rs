#[path = "common/buy_attribution_review_fixture.rs"]
mod fixture;
#[path = "common/buy_receipt_history.rs"]
mod history;
#[allow(dead_code)]
#[path = "../src/buy_receipt_ownership.rs"]
mod ownership;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::Db;
use rusqlite::{params, Connection, StatementStatus};
use std::path::Path;

// This integration crate includes ownership only to exercise its exact claim_sql.
// Its unused verifier branch cannot name a crate-private library helper. Keep a
// fail-loud shim here; receipt behavior is exercised through SqliteStore below.
mod rpc_owned_sell_handoff {
    pub mod dispatch {
        pub mod identity {
            pub(crate) fn token_side(
                _: &rusqlite::Connection,
                _: &str,
            ) -> anyhow::Result<(String, String)> {
                unreachable!("index query probe must not execute the receipt verifier")
            }
        }
    }
}

const MIGRATIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations");
const INDEXES: [&str; 3] = [
    "idx_buy_receipt_proofs_signature",
    "idx_buy_receipt_facts_signature",
    "idx_buy_receipt_orders_signature",
];

fn before_indexes() -> Result<Db> {
    let dir = tempfile::tempdir()?;
    let old = dir.path().join("pre-0061");
    std::fs::create_dir(&old)?;
    for entry in std::fs::read_dir(MIGRATIONS)? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|e| e == "sql")
            && !entry.file_name().to_string_lossy().starts_with("0061_")
        {
            std::fs::copy(entry.path(), old.join(entry.file_name()))?;
        }
    }
    let path = dir.path().join("upgrade.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    Ok(Db {
        dir,
        path,
        store,
        now: "2026-09-07T12:00:00Z".parse()?,
    })
}

#[test]
fn indexes_upgrade_reopen_preserves_existing_duplicate_money_and_replays() -> Result<()> {
    for duplicates in [false, true] {
        let mut db = before_indexes()?;
        let a = db.seed("dup-a", "source-a", "buy")?;
        let first = db.buy(&a)?;
        if duplicates {
            let b = db.seed("dup-b", "source-b", "buy")?;
            history::buy(&db, &b, "mint")?;
        }
        let before = db.snapshot()?;
        assert_eq!(db.store.run_migrations(Path::new(MIGRATIONS))?, 1);
        assert_eq!(
            db.snapshot()?,
            before,
            "nonunique indexes cannot repair or reject old money"
        );
        db.reopen()?;
        assert_eq!(db.store.run_migrations(Path::new(MIGRATIONS))?, 0);
        assert_eq!(db.snapshot()?, before);
        let conn = db.conn()?;
        for index in INDEXES {
            let unique: i64 = conn.query_row(
                "SELECT il.\"unique\" FROM sqlite_master m, pragma_index_list(m.tbl_name) il WHERE m.name=?1 AND il.name=m.name",
                [index], |r| r.get(0))?;
            assert_eq!(unique, 0);
        }
        let ids = if duplicates {
            vec![a.clone(), "exec-canary:dup-b".into()]
        } else {
            vec![a.clone()]
        };
        for id in ids {
            assert_eq!(
                db.buy(&id)?.position.position_id,
                first.position.position_id
            );
        }
        assert_eq!(db.snapshot()?, before);
        let c = db.seed("dup-c", "source-c", "buy")?;
        let before_reject = db.snapshot()?;
        assert!(db.buy(&c).is_err());
        assert_eq!(db.snapshot()?, before_reject);
        println!(
            "BUY_INDEX_UPGRADE duplicates={duplicates} money_unchanged=true replay_preserved=true"
        );
    }
    Ok(())
}

fn work(conn: &Connection) -> Result<(i32, Vec<String>)> {
    let mut steps = 0;
    let mut plans = Vec::new();
    for table in [
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        // The exact production statement and binding, not an equivalent test query.
        let sql = ownership::claim_sql(table);
        plans.extend(
            conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                .query_map(["sig:exec-canary:a"], |r| r.get::<_, String>(3))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
        let mut stmt = conn.prepare(&sql)?;
        {
            let mut rows = stmt.query(["sig:exec-canary:a"])?;
            let mut count = 0;
            while rows.next()?.is_some() {
                count += 1;
            }
            assert_eq!(
                count, 2,
                "one owner appears in direct and linked-order probes"
            );
        }
        steps += stmt.get_status(StatementStatus::VmStep);
        assert_eq!(stmt.get_status(StatementStatus::AutoIndex), 0);
    }
    Ok((steps, plans))
}

fn grow(conn: &mut Connection, start: usize, count: usize) -> Result<()> {
    let tx = conn.transaction()?;
    for table in [
        "orders",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        let columns: Vec<String> = tx
            .prepare(&format!("PRAGMA table_info({table})"))?
            .query_map([], |r| r.get(1))?
            .collect::<rusqlite::Result<_>>()?;
        let select = columns
            .iter()
            .map(|c| match c.as_str() {
                "order_id" | "client_order_id" => "?1".to_owned(),
                "tx_signature" => "?2".to_owned(),
                _ => format!("\"{c}\""),
            })
            .collect::<Vec<_>>()
            .join(",");
        let mut stmt = tx.prepare(&format!(
            "INSERT INTO {table} SELECT {select} FROM {table} WHERE order_id='exec-canary:a'"
        ))?;
        for i in start..start + count {
            stmt.execute(params![
                format!("unrelated:{i}"),
                format!("unrelated-sig:{i}")
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[test]
fn actual_lookup_vm_work_stays_bounded_with_large_same_wallet_history() -> Result<()> {
    let db = Db::new()?;
    db.seed("a", "source-a", "buy")?;
    let mut conn = db.conn()?;
    let (small, plans) = work(&conn)?;
    assert!(plans.iter().all(|p| !p.starts_with("SCAN ")), "{plans:?}");
    for index in INDEXES {
        assert!(plans.iter().any(|p| p.contains(index)), "{plans:?}");
    }
    // Explicit stress data: clone a valid pending receipt/order shape, changing
    // only order IDs and signatures. All 30,000 rows share the execution wallet.
    grow(&mut conn, 0, 3000)?;
    let medium = work(&conn)?.0;
    grow(&mut conn, 3000, 27000)?;
    let large = work(&conn)?.0;
    // A one-row index can end iteration earlier than a populated index. Bound
    // that constant overhead, then require no growth from 3,000 to 30,000 rows.
    assert!(medium <= small + 16);
    assert_eq!(medium, large);
    assert!(large < 200, "unexpected per-BUY VM work: {large}");
    for index in INDEXES {
        conn.execute_batch(&format!("DROP INDEX {index}"))?;
    }
    let (unindexed, old_plans) = work(&conn)?;
    assert!(
        unindexed > large * 1000,
        "index control must detect full history work"
    );
    println!("BUY_LOOKUP_VM small={small} rows3000={medium} rows30000={large} unindexed={unindexed} plans={plans:?} old_plans={old_plans:?}");
    Ok(())
}
