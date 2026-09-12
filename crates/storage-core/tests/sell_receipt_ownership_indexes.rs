#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "../src/sell_receipt_ownership.rs"]
mod ownership;
use anyhow::Result;
use copybot_storage_core::SellSettlementUnsupported;
use fixture::*;
use rusqlite::{params, Connection, StatementStatus};
const INDEXES: [&str; 3] = [
    "idx_buy_receipt_proofs_signature",
    "idx_buy_receipt_facts_signature",
    "idx_buy_receipt_orders_signature",
];

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
                .query_map(["signature"], |r| r.get::<_, String>(3))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
        let mut stmt = conn.prepare(&sql)?;
        {
            let mut rows = stmt.query(["signature"])?;
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
            "INSERT INTO {table} SELECT {select} FROM {table} WHERE order_id='exec-canary:settlement'"
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
    let db = Db::new(3, 9, 0, 1, 0)?;
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
    assert!(large < 200, "unexpected per-SELL VM work: {large}");
    for index in INDEXES {
        conn.execute_batch(&format!("DROP INDEX {index}"))?;
    }
    let (unindexed, old_plans) = work(&conn)?;
    assert!(
        unindexed > large * 1000,
        "index control must detect full history work"
    );
    println!("SELL_LOOKUP_VM small={small} rows3000={medium} rows30000={large} unindexed={unindexed} plans={plans:?} old_plans={old_plans:?}");
    Ok(())
}
