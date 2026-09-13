#[path = "common/historical_retry_fixture.rs"]
mod historical_retry;
use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{
    SqliteStore, EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use rusqlite::{params, Connection};

const ROUTE: &str = "metis-swap-instructions-dry-run";
const UNKNOWN: &str = "retry_after_unknown_submit_timeout";
const NOT_SENT: &str = "retry_after_rpc_submit_not_sent";

struct Fixture {
    store: SqliteStore,
    dir: tempfile::TempDir,
}

fn combined(
    f: &Fixture,
    reason: &str,
    second: Option<&str>,
    limit: u32,
    guard: Option<u32>,
) -> Result<Vec<String>> {
    Ok(f.store
        .list_reconcilable_execution_canary_orders_for_retry_reasons(
            ROUTE, reason, second, limit, guard,
        )?
        .into_iter()
        .map(|o| o.order_id)
        .collect())
}

#[test]
fn recovery_groups_share_receipt_attempt_age_without_duplicate_orders() -> Result<()> {
    let f = Fixture::new()?;
    f.retry("blocked-buy", "buy", 0, NOT_SENT)?;
    let pending = f.retry("known", "buy", 5, UNKNOWN)?;
    let sell = f.retry(
        "sell-not-sent",
        "sell",
        10,
        &format!("{NOT_SENT}:rpc_error"),
    )?;
    let unknown = f.retry("sell-unknown", "sell", 20, UNKNOWN)?;
    let conn = f.conn()?;
    conn.execute(
        "UPDATE orders SET status=?2, tx_signature='known' WHERE order_id=?1",
        params![pending, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED],
    )?;
    conn.execute("INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,token,side,confirmation_status,confirmed_at,last_attempt_at,reason) VALUES (?1,'known','wallet','Mint','buy','confirmed','2026-09-05T12:00:05+00:00','2026-09-05T12:00:05+00:00','receipt_unavailable')", [&pending])?;
    let orders_before = f
        .store
        .list_reconcilable_execution_canary_orders_for_route(ROUTE, UNKNOWN, 20)?;
    assert_eq!(
        combined(&f, UNKNOWN, Some(NOT_SENT), 1, Some(2))?,
        [pending.clone()]
    );
    conn.execute("UPDATE execution_canary_receipt_proofs SET last_attempt_at='2026-09-05T12:00:30+00:00' WHERE order_id=?1", [&pending])?;
    assert_eq!(
        combined(&f, UNKNOWN, Some(NOT_SENT), 1, Some(2))?,
        [sell.clone()]
    );
    assert_eq!(
        combined(&f, UNKNOWN, Some(NOT_SENT), 10, Some(2))?,
        [sell.clone(), unknown.clone(), pending.clone()]
    );
    // Migration 0064 keeps legacy Unknown in reconciliation regardless of retry
    // reason. Duplicating NOT_SENT must not hide its hold or duplicate any row.
    assert_eq!(
        combined(&f, NOT_SENT, Some(NOT_SENT), 10, Some(2))?,
        [sell, unknown, pending]
    );
    for before in orders_before {
        assert_eq!(
            f.store.load_execution_canary_order(&before.order_id)?,
            Some(before)
        );
    }
    Ok(())
}

#[test]
fn recovery_groups_none_preserves_both_single_reason_apis_and_read_state() -> Result<()> {
    let f = Fixture::new()?;
    for (n, reason) in [UNKNOWN, NOT_SENT, "retry_after_rpc_submit_not_sent:error"]
        .into_iter()
        .enumerate()
    {
        f.retry(&format!("buy-{n}"), "buy", n as i64 * 20, reason)?;
        f.retry(&format!("sell-{n}"), "sell", n as i64 * 20 + 10, reason)?;
    }
    let before = combined(&f, UNKNOWN, Some(NOT_SENT), 20, None)?
        .into_iter()
        .map(|id| f.store.load_execution_canary_order(&id))
        .collect::<Result<Vec<_>>>()?;
    for reason in [UNKNOWN, NOT_SENT] {
        for limit in [0, 1, 10] {
            for guard in [None, Some(2)] {
                assert_eq!(
                    combined(&f, reason, None, limit, guard)?,
                    f.selected(reason, limit, guard)?
                );
                if guard.is_none() {
                    assert_eq!(
                        f.store
                            .list_reconcilable_execution_canary_orders_for_route(
                                ROUTE, reason, limit
                            )?,
                        f.store
                            .list_reconcilable_execution_canary_orders_for_retry_reasons(
                                ROUTE, reason, None, limit, guard
                            )?
                    );
                }
            }
        }
    }
    for order in before.into_iter().flatten() {
        assert_eq!(
            f.store.load_execution_canary_order(&order.order_id)?,
            Some(order)
        );
    }
    Ok(())
}

#[test]
fn recovery_groups_keep_route_status_signature_budget_and_limit_boundaries() -> Result<()> {
    let f = Fixture::new()?;
    let conn = f.conn()?;
    f.retry("blocked", "buy", -20, NOT_SENT)?;
    let sell = f.retry("sell", "sell", 0, NOT_SENT)?;
    let unknown = f.retry("unknown", "sell", 10, UNKNOWN)?;
    let exhausted = f.retry("exhausted-buy", "buy", 20, NOT_SENT)?;
    conn.execute(
        "UPDATE orders SET attempt=3 WHERE order_id=?1",
        [&exhausted],
    )?;
    let mut expected = vec![sell, unknown, exhausted];
    for (i, status) in [
        EXECUTION_STATUS_CANARY_SUBMITTED,
        EXECUTION_STATUS_CANARY_CONFIRMED,
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
    ]
    .into_iter()
    .enumerate()
    {
        let id = f.retry(&format!("receipt-{i}"), "buy", 30 + 10 * i as i64, NOT_SENT)?;
        conn.execute(
            "UPDATE orders SET status=?2, tx_signature='known' WHERE order_id=?1",
            params![id, status],
        )?;
        expected.push(id);
    }
    for kind in ["route", "signed", "terminal", "filled", "reason"] {
        let id = f.retry(kind, "sell", -10, NOT_SENT)?;
        match kind {
            "route" => {
                conn.execute("UPDATE orders SET route='other' WHERE order_id=?1", [&id])?;
            }
            "signed" => {
                conn.execute(
                    "UPDATE orders SET tx_signature='known' WHERE order_id=?1",
                    [&id],
                )?;
            }
            "terminal" => {
                conn.execute(
                    "UPDATE orders SET status=?2 WHERE order_id=?1",
                    params![id, copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED],
                )?;
            }
            "filled" => {
                conn.execute(
                    "UPDATE orders SET status=?2, tx_signature='known' WHERE order_id=?1",
                    params![id, EXECUTION_STATUS_CANARY_CONFIRMED],
                )?;
                conn.execute(
                    "INSERT INTO fills(order_id,token,qty) VALUES (?1,'Mint',1)",
                    [&id],
                )?;
            }
            _ => {
                conn.execute(
                    "UPDATE orders SET simulation_error='unsupported' WHERE order_id=?1",
                    [&id],
                )?;
            }
        }
    }
    assert_eq!(
        combined(&f, UNKNOWN, Some(NOT_SENT), 20, Some(2))?,
        expected
    );
    assert_eq!(
        combined(&f, UNKNOWN, Some(NOT_SENT), 1, Some(2))?,
        expected[..1]
    );
    assert!(combined(&f, UNKNOWN, Some(NOT_SENT), 0, Some(2))?.is_empty());
    Ok(())
}

#[test]
fn recovery_groups_binding_read_failure_stays_an_error() -> Result<()> {
    let f = Fixture::new()?;
    f.retry("buy", "buy", 0, NOT_SENT)?;
    f.conn()?
        .execute_batch("ALTER TABLE copy_signals RENAME TO unavailable_copy_signals")?;
    assert!(combined(&f, UNKNOWN, Some(NOT_SENT), 1, Some(2)).is_err());
    Ok(())
}

impl Fixture {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let mut store = SqliteStore::open(dir.path().join("queue.db"))?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        Ok(Self { store, dir })
    }

    fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(self.dir.path().join("queue.db"))?)
    }

    fn retry(&self, name: &str, side: &str, offset: i64, reason: &str) -> Result<String> {
        let now = Utc.with_ymd_and_hms(2026, 9, 5, 12, 0, 0).unwrap() + Duration::seconds(offset);
        self.store.insert_copy_signal(&CopySignalRow {
            signal_id: name.into(),
            wallet_id: "leader".into(),
            side: side.into(),
            token: "Mint".into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "shadow_recorded".into(),
        })?;
        let order = self
            .store
            .reserve_execution_canary_order(name, ROUTE, now)?
            .order;
        self.store
            .mark_execution_canary_built(&order.order_id, now)?;
        self.store.mark_execution_canary_simulated(
            &order.order_id,
            now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        if reason == UNKNOWN {
            historical_retry::import_simulated_history(
                &self.store,
                &self.conn()?,
                &order.order_id,
                now + Duration::seconds(2),
                reason,
            )?;
        } else {
            self.store
                .mark_execution_canary_retry_after_submit_not_sent(&order.order_id, now, reason)?;
        }
        Ok(order.order_id)
    }

    fn selected(&self, reason: &str, limit: u32, guard: Option<u32>) -> Result<Vec<String>> {
        Ok(self
            .store
            .list_reconcilable_execution_canary_orders_with_buy_retry_guard(
                ROUTE, reason, limit, guard,
            )?
            .into_iter()
            .map(|o| o.order_id)
            .collect())
    }
}
