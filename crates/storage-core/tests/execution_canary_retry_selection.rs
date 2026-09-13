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

#[test]
fn retry_selection_filters_only_buy_before_limit_and_keeps_old_api() -> Result<()> {
    for reason in [
        UNKNOWN.to_string(),
        NOT_SENT.to_string(),
        format!("{NOT_SENT}:rpc_error"),
    ] {
        let f = Fixture::new()?;
        let buy = f.retry("old-buy", "BUY", 0, &reason)?;
        let sell = f.retry("later-sell", "SELL", 10, &reason)?;
        let pending = f.retry("known-buy", "buy", 20, &reason)?;
        let now = Utc.with_ymd_and_hms(2026, 9, 5, 12, 1, 0).unwrap();
        f.store
            .mark_execution_canary_submitted(&pending, now, "known-signature")?;
        let query_reason = if reason == UNKNOWN { UNKNOWN } else { NOT_SENT };
        let old = f
            .store
            .list_reconcilable_execution_canary_orders_for_route(ROUTE, query_reason, 10)?;
        let new = f
            .store
            .list_reconcilable_execution_canary_orders_with_buy_retry_guard(
                ROUTE,
                query_reason,
                10,
                None,
            )?;
        assert_eq!(old, new);
        assert_eq!(f.selected(query_reason, 1, None)?, [buy]);
        assert_eq!(f.selected(query_reason, 1, Some(2))?, [sell.clone()]);
        assert_eq!(f.selected(query_reason, 10, Some(2))?, [sell, pending]);
        assert!(f.selected(query_reason, 0, Some(2))?.is_empty());
        // Repeated reads do not move timestamps, advance attempts, or mutate orders.
        assert_eq!(
            f.store
                .list_reconcilable_execution_canary_orders_for_route(ROUTE, query_reason, 10)?,
            old
        );
    }
    Ok(())
}

#[test]
fn retry_selection_preserves_expired_budget_and_unrelated_recovery_statuses() -> Result<()> {
    for reason in [UNKNOWN, NOT_SENT] {
        let f = Fixture::new()?;
        let buy = f.retry("budget-buy", "buy", 0, reason)?;
        assert!(f.selected(reason, 1, Some(2))?.is_empty());
        assert_eq!(f.selected(reason, 1, Some(1))?, [buy.clone()]);
        assert_eq!(f.selected(reason, 1, Some(0))?, [buy.clone()]);
        for (index, status) in [
            EXECUTION_STATUS_CANARY_SUBMITTED,
            EXECUTION_STATUS_CANARY_CONFIRMED,
            EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
        ]
        .into_iter()
        .enumerate()
        {
            let id = f.retry(
                &format!("status-{index}"),
                "buy",
                10 + index as i64 * 10,
                reason,
            )?;
            f.conn()?.execute(
                "UPDATE orders SET status=?2, tx_signature='known' WHERE order_id=?1",
                params![id, status],
            )?;
        }
        let selected = f.selected(reason, 10, Some(2))?;
        assert_eq!(selected.len(), 3);
        assert!(!selected.contains(&buy));
        assert!(selected.iter().all(|id| f
            .store
            .load_execution_canary_order(id)
            .unwrap()
            .unwrap()
            .tx_signature
            .as_deref()
            == Some("known")));
    }
    Ok(())
}

#[test]
fn retry_selection_does_not_expand_route_reason_or_signature_eligibility() -> Result<()> {
    let f = Fixture::new()?;
    let sell = f.retry("sell", "sell", 0, NOT_SENT)?;
    let other_route = f.retry("other-route", "sell", 10, NOT_SENT)?;
    let signed_simulated = f.retry("signed-simulated", "sell", 20, NOT_SENT)?;
    f.conn()?.execute(
        "UPDATE orders SET route='other-route' WHERE order_id=?1",
        [&other_route],
    )?;
    f.conn()?.execute(
        "UPDATE orders SET tx_signature='known' WHERE order_id=?1",
        [&signed_simulated],
    )?;
    assert_eq!(f.selected(NOT_SENT, 10, Some(2))?, [sell]);
    assert!(f.selected(UNKNOWN, 10, Some(2))?.is_empty());
    Ok(())
}

#[test]
fn retry_selection_binding_read_error_is_not_an_empty_success() -> Result<()> {
    let f = Fixture::new()?;
    f.retry("buy", "buy", 0, NOT_SENT)?;
    f.conn()?
        .execute_batch("ALTER TABLE copy_signals RENAME TO unavailable_copy_signals")?;
    assert!(f.selected(NOT_SENT, 1, Some(2)).is_err());
    Ok(())
}
