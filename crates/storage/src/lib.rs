use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

pub use copybot_core_types::{
    CopySignalRow, ExecutionConfirmStateSnapshot, ExecutionOrderRow,
    FinalizeExecutionConfirmOutcome, InsertExecutionOrderPendingOutcome, TokenQualityCacheRow,
    TokenQualityRpcRow, WalletMetricRow, WalletUpsertRow,
    EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
};

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];
const DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS: i64 = 3;
pub const STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES: i64 = 30;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL: f64 = 0.05;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES: usize = 3;
pub const STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES: usize = 60;
const LIVE_UNREALIZED_RELIABLE_PRICE_WINDOW_MINUTES: i64 = 30;
const LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SOL_NOTIONAL: f64 = 0.05;
const LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SAMPLES: usize = 1;
const LIVE_UNREALIZED_RELIABLE_PRICE_MAX_SAMPLES: usize = 60;
static SQLITE_WRITE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static SQLITE_BUSY_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

mod discovery;
mod execution_orders;
mod market_data;
mod migrations;
mod pricing;
mod risk_metrics;
mod shadow;
mod sqlite_retry;
mod system_events;

pub use execution_orders::{MarkOrderDroppedOutcome, ScheduleOrderRetryOutcome};
pub use sqlite_retry::is_retryable_sqlite_anyhow_error;

pub struct SqliteStore {
    conn: Connection,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SqliteContentionSnapshot {
    pub write_retry_total: u64,
    pub busy_error_total: u64,
}

pub fn sqlite_contention_snapshot() -> SqliteContentionSnapshot {
    SqliteContentionSnapshot {
        write_retry_total: SQLITE_WRITE_RETRY_TOTAL.load(Ordering::Relaxed),
        busy_error_total: SQLITE_BUSY_ERROR_TOTAL.load(Ordering::Relaxed),
    }
}

pub fn note_sqlite_write_retry() {
    SQLITE_WRITE_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn note_sqlite_busy_error() {
    SQLITE_BUSY_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FollowlistUpdateResult {
    pub activated: usize,
    pub deactivated: usize,
}

#[derive(Debug, Clone)]
pub struct ShadowLotRow {
    pub id: i64,
    pub wallet_id: String,
    pub token: String,
    pub qty: f64,
    pub cost_sol: f64,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowCloseOutcome {
    pub closed_qty: f64,
    pub realized_pnl_sol: f64,
    pub has_open_lots_after: bool,
}

#[derive(Debug, Clone, Default)]
pub struct TokenMarketStats {
    pub first_seen: Option<DateTime<Utc>>,
    pub holders_proxy: u64,
    pub liquidity_sol_proxy: f64,
    pub volume_5m_sol: f64,
    pub unique_traders_5m: u64,
}

#[derive(Debug, Clone)]
pub struct DiscoveryRuntimeCursor {
    pub ts_utc: DateTime<Utc>,
    pub slot: u64,
    pub signature: String,
}

impl SqliteStore {
    fn live_execution_state_snapshot_on_conn(
        conn: &Connection,
        token: &str,
    ) -> Result<ExecutionConfirmStateSnapshot> {
        let total_exposure_sol: f64 = conn
            .query_row(
                "SELECT COALESCE(SUM(cost_sol), 0.0)
                 FROM positions
                 WHERE state = 'open'",
                [],
                |row| row.get(0),
            )
            .context("failed querying live open exposure in finalize confirm transaction")?;
        let token_exposure_sol: f64 = conn
            .query_row(
                "SELECT COALESCE(SUM(cost_sol), 0.0)
                 FROM positions
                 WHERE state = 'open'
                   AND token = ?1",
                params![token],
                |row| row.get(0),
            )
            .context(
                "failed querying live open exposure by token in finalize confirm transaction",
            )?;
        let open_positions: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM positions
                 WHERE state = 'open'",
                [],
                |row| row.get(0),
            )
            .context("failed querying live open positions count in finalize confirm transaction")?;

        Ok(ExecutionConfirmStateSnapshot {
            total_exposure_sol: total_exposure_sol.max(0.0),
            token_exposure_sol: token_exposure_sol.max(0.0),
            open_positions: open_positions.max(0) as u64,
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create sqlite parent dir: {}", parent.display())
            })?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db: {}", path.display()))?;
        conn.busy_timeout(StdDuration::from_secs(5))
            .context("failed to set sqlite busy_timeout")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to set sqlite journal mode WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("failed to set sqlite synchronous NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable sqlite foreign keys")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            );",
        )
        .context("failed to create schema_migrations table")?;

        Ok(Self { conn })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finalize_execution_confirmed_order(
        &self,
        order_id: &str,
        signal_id: &str,
        token: &str,
        side: &str,
        qty: f64,
        notional_sol: f64,
        avg_price: f64,
        fee: f64,
        slippage_bps: f64,
        confirmed_ts: DateTime<Utc>,
    ) -> Result<FinalizeExecutionConfirmOutcome> {
        const CONFIRM_ACTION: &str = "marking order confirmed in finalize confirm transaction";
        const CONFIRM_EXPECTED: &[&str] = &[
            "execution_submitted",
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
        ];
        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            if let Err(error) = self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION") {
                let error = anyhow!(error).context("failed to open execution confirm transaction");
                let retryable = is_retryable_sqlite_anyhow_error(&error);
                if retryable {
                    note_sqlite_busy_error();
                }
                if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                    note_sqlite_write_retry();
                    std::thread::sleep(StdDuration::from_millis(
                        SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                    ));
                    continue;
                }
                return Err(error).context("failed to finalize confirmed order");
            }
            let tx_result = (|| -> Result<FinalizeExecutionConfirmOutcome> {
                let status: Option<String> = self
                    .conn
                    .query_row(
                        "SELECT status FROM orders WHERE order_id = ?1 LIMIT 1",
                        params![order_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed reading order status before finalize confirm")?;
                let Some(status) = status else {
                    return Err(anyhow!(
                        "failed finalizing confirmed order: order_id={} not found",
                        order_id
                    ));
                };
                if status == "execution_confirmed" {
                    return Ok(FinalizeExecutionConfirmOutcome::AlreadyConfirmed);
                }
                if !matches!(
                    status.as_str(),
                    "execution_submitted" | EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS
                ) {
                    return Err(anyhow!(
                        "failed finalizing confirmed order: order_id={} has unexpected status={}",
                        order_id,
                        status
                    ));
                }

                self.conn
                    .execute(
                        "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![order_id, token, qty, avg_price, fee, slippage_bps],
                    )
                    .context("failed inserting execution fill in finalize confirm transaction")?;

                Self::apply_execution_fill_to_positions_on_conn(
                    &self.conn,
                    token,
                    side,
                    qty,
                    notional_sol,
                    fee,
                    confirmed_ts,
                )?;

                let changed_order = self
                    .conn
                    .execute(
                        "UPDATE orders
                         SET status = 'execution_confirmed',
                             confirm_ts = ?1,
                             err_code = NULL
                         WHERE order_id = ?2
                           AND status IN ('execution_submitted', ?3)",
                        params![
                            confirmed_ts.to_rfc3339(),
                            order_id,
                            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS
                        ],
                    )
                    .context("failed marking order confirmed in finalize confirm transaction")?;
                if changed_order == 0 {
                    return Err(self.unexpected_order_status_error(
                        order_id,
                        CONFIRM_ACTION,
                        CONFIRM_EXPECTED,
                    )?);
                }

                let changed_signal = self
                    .conn
                    .execute(
                        "UPDATE copy_signals
                         SET status = 'execution_confirmed'
                         WHERE signal_id = ?1",
                        params![signal_id],
                    )
                    .context(
                        "failed updating copy signal status in finalize confirm transaction",
                    )?;
                if changed_signal == 0 {
                    return Err(anyhow!(
                        "failed updating copy signal status in finalize confirm transaction: signal_id={} not found",
                        signal_id
                    ));
                }

                let snapshot = Self::live_execution_state_snapshot_on_conn(&self.conn, token)?;
                Ok(FinalizeExecutionConfirmOutcome::Applied(snapshot))
            })();

            match tx_result {
                Ok(outcome) => {
                    if let Err(error) = self.conn.execute_batch("COMMIT") {
                        let error = anyhow!(error)
                            .context("failed to commit execution confirm transaction");
                        let _ = self.conn.execute_batch("ROLLBACK");
                        let retryable = is_retryable_sqlite_anyhow_error(&error);
                        if retryable {
                            note_sqlite_busy_error();
                        }
                        if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                            note_sqlite_write_retry();
                            std::thread::sleep(StdDuration::from_millis(
                                SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                            ));
                            continue;
                        }
                        return Err(error).context("failed to finalize confirmed order");
                    }
                    return Ok(outcome);
                }
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    let retryable = is_retryable_sqlite_anyhow_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        note_sqlite_write_retry();
                        std::thread::sleep(StdDuration::from_millis(
                            SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                        ));
                        continue;
                    }
                    return Err(error).context("failed to finalize confirmed order");
                }
            }
        }

        unreachable!("retry loop must return on success or terminal error");
    }

    pub fn insert_execution_fill(
        &self,
        order_id: &str,
        token: &str,
        qty: f64,
        avg_price: f64,
        fee: f64,
        slippage_bps: f64,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![order_id, token, qty, avg_price, fee, slippage_bps],
            )
        })
        .context("failed inserting execution fill")?;
        Ok(())
    }

    pub fn live_open_exposure_sol(&self) -> Result<f64> {
        let value: f64 = self
            .conn
            .query_row(
                "SELECT COALESCE(SUM(cost_sol), 0.0)
                 FROM positions
                 WHERE state = 'open'",
                [],
                |row| row.get(0),
            )
            .context("failed querying live open exposure")?;
        Ok(value.max(0.0))
    }

    pub fn live_open_exposure_sol_for_token(&self, token: &str) -> Result<f64> {
        let value: f64 = self
            .conn
            .query_row(
                "SELECT COALESCE(SUM(cost_sol), 0.0)
                 FROM positions
                 WHERE state = 'open'
                   AND token = ?1",
                params![token],
                |row| row.get(0),
            )
            .context("failed querying live open exposure by token")?;
        Ok(value.max(0.0))
    }

    pub fn live_open_positions_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM positions
                 WHERE state = 'open'",
                [],
                |row| row.get(0),
            )
            .context("failed querying live open positions count")?;
        Ok(count.max(0) as u64)
    }

    pub fn live_has_open_position(&self, token: &str) -> Result<bool> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                 LIMIT 1",
                params![token],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking live open position by token")?;
        Ok(exists.is_some())
    }

    pub fn live_open_position_qty_cost(&self, token: &str) -> Result<Option<(f64, f64)>> {
        let row: Option<(f64, f64)> = self
            .conn
            .query_row(
                "SELECT qty, cost_sol
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                 LIMIT 1",
                params![token],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("failed querying live open position qty/cost by token")?;
        Ok(row.filter(|(qty, cost)| {
            qty.is_finite() && *qty > 0.0 && cost.is_finite() && *cost >= 0.0
        }))
    }

    pub fn apply_execution_fill_to_positions(
        &self,
        token: &str,
        side: &str,
        qty: f64,
        notional_sol: f64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        const EPS: f64 = 1e-12;

        if qty <= EPS || notional_sol <= 0.0 || !qty.is_finite() || !notional_sol.is_finite() {
            return Ok(());
        }
        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            if let Err(error) = self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION") {
                let error = anyhow!(error).context("failed to open execution position transaction");
                let retryable = is_retryable_sqlite_anyhow_error(&error);
                if retryable {
                    note_sqlite_busy_error();
                }
                if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                    note_sqlite_write_retry();
                    std::thread::sleep(StdDuration::from_millis(
                        SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                    ));
                    continue;
                }
                return Err(error).context("failed to apply execution fill to positions");
            }

            let update_result = Self::apply_execution_fill_to_positions_on_conn(
                &self.conn,
                token,
                side,
                qty,
                notional_sol,
                0.0,
                ts,
            );

            match update_result {
                Ok(()) => {
                    if let Err(error) = self.conn.execute_batch("COMMIT") {
                        let error = anyhow!(error)
                            .context("failed to commit execution position transaction");
                        let _ = self.conn.execute_batch("ROLLBACK");
                        let retryable = is_retryable_sqlite_anyhow_error(&error);
                        if retryable {
                            note_sqlite_busy_error();
                        }
                        if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                            note_sqlite_write_retry();
                            std::thread::sleep(StdDuration::from_millis(
                                SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                            ));
                            continue;
                        }
                        return Err(error).context("failed to apply execution fill to positions");
                    }
                    return Ok(());
                }
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    let retryable = is_retryable_sqlite_anyhow_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        note_sqlite_write_retry();
                        std::thread::sleep(StdDuration::from_millis(
                            SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                        ));
                        continue;
                    }
                    return Err(error).context("failed to apply execution fill to positions");
                }
            }
        }

        unreachable!("retry loop must return on success or terminal error");
    }

    fn apply_execution_fill_to_positions_on_conn(
        conn: &Connection,
        token: &str,
        side: &str,
        qty: f64,
        notional_sol: f64,
        fee_sol: f64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        const EPS: f64 = 1e-12;

        if !fee_sol.is_finite() || fee_sol < 0.0 {
            return Err(anyhow!(
                "invalid execution fill fee token={} side={} fee_sol={}",
                token,
                side,
                fee_sol
            ));
        }

        let side_norm = side.trim().to_ascii_lowercase();
        let existing: Option<(String, f64, f64, Option<f64>)> = conn
            .query_row(
                "SELECT position_id, qty, cost_sol, pnl_sol
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                 ORDER BY opened_ts ASC
                 LIMIT 1",
                params![token],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .context("failed querying live open position row")?;

        match side_norm.as_str() {
            "buy" => {
                let effective_cost = notional_sol + fee_sol;
                if let Some((position_id, current_qty, current_cost, current_pnl)) = existing {
                    conn.execute(
                        "UPDATE positions
                         SET qty = ?1,
                             cost_sol = ?2,
                             pnl_sol = ?3,
                             state = 'open',
                             closed_ts = NULL
                         WHERE position_id = ?4",
                        params![
                            current_qty + qty,
                            current_cost + effective_cost,
                            current_pnl.unwrap_or(0.0),
                            position_id,
                        ],
                    )
                    .context("failed updating open live position for buy fill")?;
                } else {
                    let position_id = format!("live:{}:{}", token, uuid::Uuid::new_v4().simple());
                    conn.execute(
                        "INSERT INTO positions(
                            position_id,
                            token,
                            qty,
                            cost_sol,
                            opened_ts,
                            state,
                            pnl_sol
                        ) VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0.0)",
                        params![position_id, token, qty, effective_cost, ts.to_rfc3339()],
                    )
                    .context("failed inserting new live position for buy fill")?;
                }
            }
            "sell" => {
                let Some((position_id, current_qty, current_cost, current_pnl)) = existing else {
                    return Err(anyhow!(
                        "sell fill without open position token={} qty={} notional_sol={}",
                        token,
                        qty,
                        notional_sol
                    ));
                };
                if current_qty <= EPS || current_cost <= EPS {
                    return Err(anyhow!(
                        "sell fill on non-positive open position token={} qty={} cost_sol={}",
                        token,
                        current_qty,
                        current_cost
                    ));
                }

                let qty_closed = qty.min(current_qty);
                let avg_cost = current_cost / current_qty;
                let realized_cost = avg_cost * qty_closed;
                let effective_notional = if qty_closed < qty {
                    notional_sol * (qty_closed / qty)
                } else {
                    notional_sol
                };
                let effective_fee = if qty_closed < qty {
                    fee_sol * (qty_closed / qty)
                } else {
                    fee_sol
                };
                let realized_pnl = effective_notional - realized_cost - effective_fee;
                let next_qty = (current_qty - qty_closed).max(0.0);
                let next_cost = (current_cost - realized_cost).max(0.0);
                let next_pnl = current_pnl.unwrap_or(0.0) + realized_pnl;

                if next_qty <= EPS {
                    conn.execute(
                        "UPDATE positions
                         SET qty = 0.0,
                             cost_sol = 0.0,
                             pnl_sol = ?1,
                             state = 'closed',
                             closed_ts = ?2
                         WHERE position_id = ?3",
                        params![next_pnl, ts.to_rfc3339(), position_id],
                    )
                    .context("failed closing live position after sell fill")?;
                } else {
                    conn.execute(
                        "UPDATE positions
                         SET qty = ?1,
                             cost_sol = ?2,
                             pnl_sol = ?3
                         WHERE position_id = ?4",
                        params![next_qty, next_cost, next_pnl, position_id],
                    )
                    .context("failed partially updating live position after sell fill")?;
                }
            }
            _ => {
                return Err(anyhow!("unsupported execution fill side: {}", side));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use copybot_core_types::SwapEvent;
    use tempfile::tempdir;

    fn copy_migrations_through(dest: &Path, max_version: &str) -> Result<()> {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        fs::create_dir_all(dest)
            .with_context(|| format!("failed to create temp migration dir {}", dest.display()))?;
        for entry in fs::read_dir(&source)
            .with_context(|| format!("failed to read migrations dir {}", source.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", source.display()))?;
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name <= max_version {
                fs::copy(&path, dest.join(file_name)).with_context(|| {
                    format!(
                        "failed to copy migration {} into {}",
                        path.display(),
                        dest.display()
                    )
                })?;
            }
        }
        Ok(())
    }

    #[test]
    fn close_shadow_lots_fifo_atomic_handles_parallel_sells_without_double_close() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-close-race.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        seed_store.insert_shadow_lot("wallet", "token", 100.0, 1.0, opened_ts)?;
        drop(seed_store);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let db_path_a = db_path.clone();
        let barrier_a = barrier.clone();
        let opened_ts_a = opened_ts;
        let worker_a = std::thread::spawn(move || -> Result<ShadowCloseOutcome> {
            let store = SqliteStore::open(Path::new(&db_path_a))?;
            barrier_a.wait();
            store.close_shadow_lots_fifo_atomic(
                "signal-a",
                "wallet",
                "token",
                80.0,
                0.02,
                opened_ts_a + Duration::minutes(1),
            )
        });

        let db_path_b = db_path.clone();
        let barrier_b = barrier.clone();
        let opened_ts_b = opened_ts;
        let worker_b = std::thread::spawn(move || -> Result<ShadowCloseOutcome> {
            let store = SqliteStore::open(Path::new(&db_path_b))?;
            barrier_b.wait();
            store.close_shadow_lots_fifo_atomic(
                "signal-b",
                "wallet",
                "token",
                80.0,
                0.02,
                opened_ts_b + Duration::minutes(2),
            )
        });

        barrier.wait();
        let close_a = worker_a
            .join()
            .expect("worker A thread panicked")
            .context("worker A close failed")?;
        let close_b = worker_b
            .join()
            .expect("worker B thread panicked")
            .context("worker B close failed")?;

        let total_closed = close_a.closed_qty + close_b.closed_qty;
        assert!(
            (total_closed - 100.0).abs() < 1e-9,
            "expected total closed qty to equal available inventory, got {total_closed}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert!(
            !verify_store.has_shadow_lots("wallet", "token")?,
            "all lots should be closed exactly once"
        );

        Ok(())
    }

    #[test]
    fn insert_shadow_lot_returns_inserted_row_id_after_retryable_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-insert-rowid-retry.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        drop(seed_store);

        let blocker_store = SqliteStore::open(Path::new(&db_path))?;
        blocker_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_store
            .conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_path = db_path.clone();
        let worker_barrier = barrier.clone();
        let worker = std::thread::spawn(move || -> Result<i64> {
            let store = SqliteStore::open(Path::new(&worker_path))?;
            store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten worker busy timeout")?;
            worker_barrier.wait();
            let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc);
            store.insert_shadow_lot("wallet", "token", 100.0, 1.0, opened_ts)
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;

        let lot_id = worker
            .join()
            .expect("worker thread panicked")
            .context("worker insert failed")?;
        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let lots = verify_store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1, "expected exactly one inserted shadow lot");
        assert_eq!(
            lots[0].id, lot_id,
            "returned row id must match persisted shadow lot"
        );
        Ok(())
    }

    #[test]
    fn has_shadow_lots_ignores_zero_and_dust_qty_rows() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-dust-open-check.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet", "token", 10.0, 1.0, opened_ts)?;
        assert!(store.has_shadow_lots("wallet", "token")?);

        store.update_shadow_lot(lot_id, 1e-13, 1e-15)?;

        assert!(
            !store.has_shadow_lots("wallet", "token")?,
            "dust lots should not count as open inventory"
        );
        assert_eq!(
            store.shadow_open_lots_count()?,
            0,
            "dust lots should not count toward open lot metrics"
        );
        assert!(
            store.shadow_open_notional_sol()?.abs() < 1e-12,
            "dust lots should not contribute to open notional metrics"
        );
        assert!(
            !store
                .list_shadow_open_pairs()?
                .contains(&("wallet".to_string(), "token".to_string())),
            "dust lots should not appear in open pair queries"
        );
        assert!(
            store
                .list_open_shadow_lots_older_than(opened_ts + chrono::Duration::minutes(1), 10)?
                .is_empty(),
            "dust lots should not be returned as stale open lots"
        );

        Ok(())
    }

    #[test]
    fn execution_lifecycle_updates_orders_signals_and_positions() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-lifecycle.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-1:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "shadow_recorded".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store
                .list_copy_signals_by_status("shadow_recorded", 10)?
                .len(),
            1
        );
        assert!(store.update_copy_signal_status(&signal.signal_id, "execution_pending")?);

        let order_id = "ord-test-1";
        let client_order_id = "cb_test_signal_a1";
        assert_eq!(
            store.insert_execution_order_pending(
                order_id,
                &signal.signal_id,
                client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-test-1-dup",
                &signal.signal_id,
                client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Duplicate
        );
        store.mark_order_simulated(order_id, "ok", Some("paper_simulation_ok"))?;
        store.mark_order_submitted(
            order_id,
            "paper",
            "paper:tx-1",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_confirmed(order_id, now + Duration::seconds(1))?;
        let order = store
            .execution_order_by_client_order_id(client_order_id)?
            .context("expected order row to exist")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(order.attempt, 1);
        assert_eq!(order.signal_id, signal.signal_id);

        store.insert_execution_fill(order_id, "token-a", 1.0, 0.25, 0.0, 50.0)?;
        store.apply_execution_fill_to_positions("token-a", "buy", 1.0, 0.25, now)?;
        assert!(store.live_has_open_position("token-a")?);
        assert_eq!(store.live_open_positions_count()?, 1);
        let exposure_after_buy = store.live_open_exposure_sol()?;
        assert!(
            (exposure_after_buy - 0.25).abs() < 1e-9,
            "unexpected exposure after buy: {exposure_after_buy}"
        );

        store.apply_execution_fill_to_positions(
            "token-a",
            "sell",
            1.0,
            0.30,
            now + Duration::seconds(2),
        )?;
        assert!(!store.live_has_open_position("token-a")?);
        assert_eq!(store.live_open_positions_count()?, 0);
        let exposure_after_sell = store.live_open_exposure_sol()?;
        assert!(exposure_after_sell <= 1e-9);

        Ok(())
    }

    #[test]
    fn finalize_execution_confirmed_order_is_atomic_and_idempotent() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-confirm-finalize.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-2:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);

        let order_id = "ord-finalize-1";
        let client_order_id = "cb_test_finalize_a1";
        assert_eq!(
            store.insert_execution_order_pending(
                order_id,
                &signal.signal_id,
                client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            order_id,
            "paper",
            "paper:tx-finalize",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

        let first = store.finalize_execution_confirmed_order(
            order_id,
            &signal.signal_id,
            "token-a",
            "buy",
            1.0,
            0.25,
            0.25,
            0.0,
            50.0,
            now + Duration::seconds(1),
        )?;
        let FinalizeExecutionConfirmOutcome::Applied(snapshot) = first else {
            panic!("expected applied outcome, got {:?}", first);
        };
        assert!((snapshot.total_exposure_sol - 0.25).abs() < 1e-9);
        assert!((snapshot.token_exposure_sol - 0.25).abs() < 1e-9);
        assert_eq!(snapshot.open_positions, 1);

        let order = store
            .execution_order_by_client_order_id(client_order_id)?
            .context("expected order row after finalize")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(
            store
                .list_copy_signals_by_status("execution_confirmed", 10)?
                .len(),
            1
        );
        assert!(store.live_has_open_position("token-a")?);

        let second = store.finalize_execution_confirmed_order(
            order_id,
            &signal.signal_id,
            "token-a",
            "buy",
            1.0,
            0.25,
            0.25,
            0.0,
            50.0,
            now + Duration::seconds(2),
        )?;
        assert_eq!(second, FinalizeExecutionConfirmOutcome::AlreadyConfirmed);

        let fills_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM fills WHERE order_id = ?1",
            params![order_id],
            |row| row.get(0),
        )?;
        assert_eq!(fills_count, 1);

        Ok(())
    }

    #[test]
    fn finalize_execution_confirmed_order_rejects_transactional_status_regression() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-confirm-transactional-guard.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-tx-confirm-guard:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-tx-confirm-guard-1",
                &signal.signal_id,
                "cb_tx_confirm_guard_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-tx-confirm-guard-1",
            "paper",
            "paper:tx-tx-confirm-guard",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.conn.execute_batch(
            "CREATE TRIGGER tx_confirm_guard_flip_status
             AFTER INSERT ON fills
             BEGIN
                 UPDATE orders
                 SET status = 'execution_failed',
                     err_code = 'trigger_flip'
                 WHERE order_id = NEW.order_id;
             END;",
        )?;

        let error = store
            .finalize_execution_confirmed_order(
                "ord-tx-confirm-guard-1",
                &signal.signal_id,
                "token-a",
                "buy",
                1.0,
                0.25,
                0.25,
                0.0,
                50.0,
                now + Duration::seconds(1),
            )
            .expect_err("transactional status regression must be rejected");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("unexpected status=execution_failed"),
            "unexpected error: {error_chain}"
        );

        let order = store
            .execution_order_by_client_order_id("cb_tx_confirm_guard_a1")?
            .context("expected order row after rejected transactional regression")?;
        assert_eq!(order.status, "execution_submitted");
        assert_eq!(order.err_code, None);
        assert_eq!(order.confirm_ts, None);
        assert_eq!(store.live_open_positions_count()?, 0);

        let fills_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM fills WHERE order_id = ?1",
            params!["ord-tx-confirm-guard-1"],
            |row| row.get(0),
        )?;
        assert_eq!(fills_count, 0);

        Ok(())
    }

    #[test]
    fn finalize_execution_confirmed_order_accepts_reconcile_pending_status() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-confirm-reconcile-pending.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-reconcile:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS.to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);

        let order_id = "ord-reconcile-pending-1";
        let client_order_id = "cb_test_reconcile_pending_a1";
        assert_eq!(
            store.insert_execution_order_pending(
                order_id,
                &signal.signal_id,
                client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            order_id,
            "paper",
            "paper:tx-reconcile-pending",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store
            .mark_order_reconcile_pending(order_id, "confirm_timeout_manual_reconcile_required")?;

        let outcome = store.finalize_execution_confirmed_order(
            order_id,
            &signal.signal_id,
            "token-a",
            "buy",
            1.0,
            0.25,
            0.25,
            0.0,
            50.0,
            now + Duration::seconds(1),
        )?;
        let FinalizeExecutionConfirmOutcome::Applied(snapshot) = outcome else {
            panic!("expected applied outcome, got {:?}", outcome);
        };
        assert!((snapshot.total_exposure_sol - 0.25).abs() < 1e-9);
        assert_eq!(snapshot.open_positions, 1);

        let order = store
            .execution_order_by_client_order_id(client_order_id)?
            .context("expected order row after late confirm finalize")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(order.err_code, None);
        assert_eq!(
            store
                .list_copy_signals_by_status("execution_confirmed", 10)?
                .len(),
            1
        );

        Ok(())
    }

    #[test]
    fn mark_order_confirmed_accepts_reconcile_pending_status() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("mark-order-confirmed-reconcile-pending.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-direct-confirm-reconcile:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS.to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-direct-confirm-reconcile-1",
                &signal.signal_id,
                "cb_direct_confirm_reconcile_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-direct-confirm-reconcile-1",
            "paper",
            "paper:tx-direct-confirm-reconcile",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_reconcile_pending(
            "ord-direct-confirm-reconcile-1",
            "confirm_timeout_manual_reconcile_required",
        )?;

        store.mark_order_confirmed("ord-direct-confirm-reconcile-1", now + Duration::seconds(1))?;

        let order = store
            .execution_order_by_client_order_id("cb_direct_confirm_reconcile_a1")?
            .context("expected order row after direct confirm helper")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(
            order.err_code.as_deref(),
            Some("confirm_timeout_manual_reconcile_required")
        );
        assert_eq!(order.confirm_ts, Some(now + Duration::seconds(1)));

        Ok(())
    }

    #[test]
    fn finalize_execution_confirmed_order_accounts_for_fee_in_cost_and_pnl() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-confirm-fee-accounting.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let buy_signal = CopySignalRow {
            signal_id: "shadow:sig-fee:wallet:buy:token-fee".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-fee".to_string(),
            notional_sol: 0.20,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&buy_signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-fee-buy-1",
                &buy_signal.signal_id,
                "cb_fee_buy_a1",
                "rpc",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-fee-buy-1",
            "rpc",
            "sig-fee-buy",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        let buy_outcome = store.finalize_execution_confirmed_order(
            "ord-fee-buy-1",
            &buy_signal.signal_id,
            "token-fee",
            "buy",
            1.0,
            0.20,
            0.20,
            0.01,
            50.0,
            now + Duration::seconds(1),
        )?;
        let FinalizeExecutionConfirmOutcome::Applied(buy_snapshot) = buy_outcome else {
            panic!("expected applied buy outcome, got {:?}", buy_outcome);
        };
        assert!((buy_snapshot.total_exposure_sol - 0.21).abs() < 1e-9);
        assert!((buy_snapshot.token_exposure_sol - 0.21).abs() < 1e-9);
        assert_eq!(buy_snapshot.open_positions, 1);

        let exposure_after_buy = store.live_open_exposure_sol()?;
        assert!(
            (exposure_after_buy - 0.21).abs() < 1e-9,
            "buy exposure should include fee in cost basis: {exposure_after_buy}"
        );

        let sell_signal = CopySignalRow {
            signal_id: "shadow:sig-fee:wallet:sell:token-fee".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "sell".to_string(),
            token: "token-fee".to_string(),
            notional_sol: 0.25,
            ts: now + Duration::seconds(2),
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&sell_signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-fee-sell-1",
                &sell_signal.signal_id,
                "cb_fee_sell_a1",
                "rpc",
                now + Duration::seconds(2),
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-fee-sell-1",
            "rpc",
            "sig-fee-sell",
            now + Duration::seconds(2),
            None,
            None,
            None,
            None,
            None,
        )?;
        let sell_outcome = store.finalize_execution_confirmed_order(
            "ord-fee-sell-1",
            &sell_signal.signal_id,
            "token-fee",
            "sell",
            1.0,
            0.25,
            0.25,
            0.02,
            50.0,
            now + Duration::seconds(3),
        )?;
        let FinalizeExecutionConfirmOutcome::Applied(sell_snapshot) = sell_outcome else {
            panic!("expected applied sell outcome, got {:?}", sell_outcome);
        };
        assert!(sell_snapshot.total_exposure_sol <= 1e-9);
        assert!(sell_snapshot.token_exposure_sol <= 1e-9);
        assert_eq!(sell_snapshot.open_positions, 0);

        let exposure_after_sell = store.live_open_exposure_sol()?;
        assert!(exposure_after_sell <= 1e-9);

        let pnl_sol: f64 = store.conn.query_row(
            "SELECT pnl_sol
             FROM positions
             WHERE token = ?1
               AND state = 'closed'
             LIMIT 1",
            params!["token-fee"],
            |row| row.get(0),
        )?;
        assert!(
            (pnl_sol - 0.02).abs() < 1e-9,
            "realized pnl should account for both buy/sell fees: {pnl_sol}"
        );

        Ok(())
    }

    #[test]
    fn mark_order_submitted_rejects_fee_breakdown_over_i64_max() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-fee-overflow.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let signal = CopySignalRow {
            signal_id: "shadow:sig-overflow:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-overflow-1",
                &signal.signal_id,
                "cb_overflow_a1",
                "rpc",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let error = store
            .mark_order_submitted(
                "ord-overflow-1",
                "rpc",
                "sig-overflow",
                now,
                None,
                Some((i64::MAX as u64).saturating_add(1)),
                None,
                None,
                None,
            )
            .expect_err("lamports above i64::MAX must be rejected");
        assert!(
            error
                .to_string()
                .contains("orders.ata_create_rent_lamports"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn mark_order_submitted_rejects_network_fee_hint_over_i64_max() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-fee-hint-overflow.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let signal = CopySignalRow {
            signal_id: "shadow:sig-hint-overflow:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-hint-overflow-1",
                &signal.signal_id,
                "cb_hint_overflow_a1",
                "rpc",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let error = store
            .mark_order_submitted(
                "ord-hint-overflow-1",
                "rpc",
                "sig-hint-overflow",
                now,
                None,
                None,
                Some((i64::MAX as u64).saturating_add(1)),
                None,
                None,
            )
            .expect_err("network fee hint above i64::MAX must be rejected");
        assert!(
            error
                .to_string()
                .contains("orders.network_fee_lamports_hint"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn mark_order_simulated_rejects_status_regression_from_submitted() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-simulated-regression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let signal = CopySignalRow {
            signal_id: "shadow:sig-sim-regress:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-sim-regress-1",
                &signal.signal_id,
                "cb_sim_regress_a1",
                "rpc",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-sim-regress-1",
            "rpc",
            "sig-sim-regress",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

        let error = store
            .mark_order_simulated("ord-sim-regress-1", "ok", Some("late simulation"))
            .expect_err("submitted order must not regress to execution_simulated");
        assert!(error
            .to_string()
            .contains("unexpected status=execution_submitted"));
        let order = store
            .execution_order_by_client_order_id("cb_sim_regress_a1")?
            .context("expected order row after rejected regression")?;
        assert_eq!(order.status, "execution_submitted");
        Ok(())
    }

    #[test]
    fn mark_order_submitted_rejects_status_regression_from_confirmed() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-submitted-regression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-submit-regress:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-submit-regress-1",
                &signal.signal_id,
                "cb_submit_regress_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-submit-regress-1",
            "paper",
            "paper:tx-submit-regress",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        let _ = store.finalize_execution_confirmed_order(
            "ord-submit-regress-1",
            &signal.signal_id,
            "token-a",
            "buy",
            1.0,
            0.25,
            0.25,
            0.0,
            50.0,
            now + Duration::seconds(1),
        )?;

        let error = store
            .mark_order_submitted(
                "ord-submit-regress-1",
                "paper",
                "paper:tx-submit-regress-2",
                now + Duration::seconds(2),
                None,
                None,
                None,
                None,
                None,
            )
            .expect_err("confirmed order must not regress to execution_submitted");
        assert!(error
            .to_string()
            .contains("unexpected status=execution_confirmed"));
        let order = store
            .execution_order_by_client_order_id("cb_submit_regress_a1")?
            .context("expected order row after rejected regression")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(
            order.tx_signature.as_deref(),
            Some("paper:tx-submit-regress")
        );
        Ok(())
    }

    #[test]
    fn try_mark_order_dropped_reports_unexpected_status_without_masking_as_error() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("execution-dropped-guard-unexpected-status.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-drop-guard:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-drop-guard-1",
                &signal.signal_id,
                "cb_drop_guard_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-drop-guard-1",
            "paper",
            "paper:tx-drop-guard",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

        let outcome = store.try_mark_order_dropped(
            "ord-drop-guard-1",
            "signal_stale",
            Some("late status sync"),
        )?;
        assert_eq!(
            outcome,
            MarkOrderDroppedOutcome::UnexpectedStatus("execution_submitted".to_string())
        );
        let order = store
            .execution_order_by_client_order_id("cb_drop_guard_a1")?
            .context("expected order row after guarded drop rejection")?;
        assert_eq!(order.status, "execution_submitted");
        assert_eq!(order.err_code, None);

        Ok(())
    }

    #[test]
    fn try_schedule_order_retry_reports_unexpected_status_without_mutating_attempt() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("execution-retry-guard-unexpected-status.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-retry-guard:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-retry-guard-1",
                &signal.signal_id,
                "cb_retry_guard_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-retry-guard-1",
            "paper",
            "paper:tx-retry-guard",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

        let outcome = store.try_schedule_order_retry(
            "ord-retry-guard-1",
            "execution_pending",
            2,
            Some("late retry scheduling"),
        )?;
        assert_eq!(
            outcome,
            ScheduleOrderRetryOutcome::UnexpectedStatus("execution_submitted".to_string())
        );
        let order = store
            .execution_order_by_client_order_id("cb_retry_guard_a1")?
            .context("expected order row after guarded retry rejection")?;
        assert_eq!(order.status, "execution_submitted");
        assert_eq!(order.attempt, 1);
        assert_eq!(order.simulation_error, None);

        Ok(())
    }

    #[test]
    fn mark_order_failed_rejects_status_regression_from_confirmed() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-failed-regression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-failed-regress:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-failed-regress-1",
                &signal.signal_id,
                "cb_failed_regress_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-failed-regress-1",
            "paper",
            "paper:tx-failed-regress",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        let _ = store.finalize_execution_confirmed_order(
            "ord-failed-regress-1",
            &signal.signal_id,
            "token-a",
            "buy",
            1.0,
            0.25,
            0.25,
            0.0,
            50.0,
            now + Duration::seconds(1),
        )?;

        let error = store
            .mark_order_failed(
                "ord-failed-regress-1",
                "late_failure",
                Some("should not overwrite confirmed"),
            )
            .expect_err("confirmed order must not regress to execution_failed");
        assert!(error
            .to_string()
            .contains("unexpected status=execution_confirmed"));
        let order = store
            .execution_order_by_client_order_id("cb_failed_regress_a1")?
            .context("expected order row after rejected failed regression")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(order.err_code, None);
        Ok(())
    }

    #[test]
    fn mark_order_confirmed_rejects_status_regression_from_failed() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-confirmed-regression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-confirm-regress:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-confirm-regress-1",
                &signal.signal_id,
                "cb_confirm_regress_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-confirm-regress-1",
            "paper",
            "paper:tx-confirm-regress",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_failed(
            "ord-confirm-regress-1",
            "submit_transport_failed",
            Some("simulated regression guard"),
        )?;

        let error = store
            .mark_order_confirmed("ord-confirm-regress-1", now + Duration::seconds(1))
            .expect_err("failed order must not regress to execution_confirmed");
        assert!(
            error
                .to_string()
                .contains("unexpected status=execution_failed"),
            "unexpected error: {error}"
        );
        let order = store
            .execution_order_by_client_order_id("cb_confirm_regress_a1")?
            .context("expected order row after rejected confirm regression")?;
        assert_eq!(order.status, "execution_failed");
        assert_eq!(order.confirm_ts, None);

        Ok(())
    }

    #[test]
    fn parse_non_negative_i64_rejects_negative_values() {
        let error = parse_non_negative_i64("orders.ata_create_rent_lamports", "ord-1", Some(-7))
            .expect_err("negative sqlite value must be rejected");
        assert!(
            error.to_string().contains("must be >= 0"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn persist_discovery_cycle_keeps_only_latest_wallet_metric_windows() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-wallet-metrics-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let wallet_id = "wallet-retention".to_string();
        let base = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for offset_minutes in 0..4 {
            let window_start = base + Duration::minutes(offset_minutes);
            let wallets = vec![WalletUpsertRow {
                wallet_id: wallet_id.clone(),
                first_seen: base,
                last_seen: window_start,
                status: "active".to_string(),
            }];
            let metrics = vec![WalletMetricRow {
                wallet_id: wallet_id.clone(),
                window_start,
                pnl: 0.0,
                win_rate: 0.0,
                trades: 1,
                closed_trades: 1,
                hold_median_seconds: 0,
                score: 1.0,
                buy_total: 1,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }];
            let desired = vec![wallet_id.clone()];
            store.persist_discovery_cycle(
                &wallets,
                &metrics,
                &desired,
                window_start,
                "retention-test",
            )?;
        }

        let mut stmt = store.conn.prepare(
            "SELECT DISTINCT window_start FROM wallet_metrics ORDER BY window_start ASC",
        )?;
        let windows: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;

        assert_eq!(
            windows.len(),
            3,
            "expected retention to keep 3 latest windows"
        );
        assert_eq!(windows[0], (base + Duration::minutes(1)).to_rfc3339());
        assert_eq!(windows[1], (base + Duration::minutes(2)).to_rfc3339());
        assert_eq!(windows[2], (base + Duration::minutes(3)).to_rfc3339());
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_retries_after_immediate_write_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-write-retry.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        drop(seed_store);

        let blocker_store = SqliteStore::open(Path::new(&db_path))?;
        blocker_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_store
            .conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_db_path = db_path.clone();
        let worker_barrier = barrier.clone();
        let worker = std::thread::spawn(move || -> Result<FollowlistUpdateResult> {
            let store = SqliteStore::open(Path::new(&worker_db_path))?;
            store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten worker busy timeout")?;
            let window_start = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc);
            let wallets = vec![WalletUpsertRow {
                wallet_id: "wallet-retry".to_string(),
                first_seen: window_start,
                last_seen: window_start,
                status: "candidate".to_string(),
            }];
            let metrics = vec![WalletMetricRow {
                wallet_id: "wallet-retry".to_string(),
                window_start,
                pnl: 0.0,
                win_rate: 0.0,
                trades: 1,
                closed_trades: 1,
                hold_median_seconds: 0,
                score: 1.0,
                buy_total: 1,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }];
            let desired_wallets = vec!["wallet-retry".to_string()];
            worker_barrier.wait();
            store.persist_discovery_cycle(
                &wallets,
                &metrics,
                &desired_wallets,
                window_start,
                "retry-test",
            )
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;

        let follow_delta = worker
            .join()
            .expect("worker thread panicked")
            .context("worker discovery cycle failed")?;
        assert_eq!(follow_delta.activated, 1);
        assert_eq!(follow_delta.deactivated, 0);

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert!(
            verify_store
                .list_active_follow_wallets()?
                .contains("wallet-retry"),
            "followlist activation should commit after retry"
        );
        let windows: i64 = verify_store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_metrics WHERE wallet_id = ?1",
            params!["wallet-retry"],
            |row| row.get(0),
        )?;
        assert_eq!(windows, 1, "wallet metric insert should commit after retry");
        Ok(())
    }

    #[test]
    fn followlist_single_active_guard_migration_dedupes_existing_duplicates() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("followlist-single-active-guard.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(&legacy_migrations, "0017_positions_closed_state_index.sql")?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        legacy_store.conn.execute(
            "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, NULL, 1)",
            params!["wallet-dup", "2026-02-20T00:00:00Z"],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, NULL, 1)",
            params!["wallet-dup", "2026-02-20T00:01:00Z"],
        )?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        let rows: Vec<(i64, String, i64, Option<String>, Option<String>)> = {
            let mut stmt = migrated_store.conn.prepare(
                "SELECT id, added_at, active, removed_at, reason
                 FROM followlist
                 WHERE wallet_id = ?1
                 ORDER BY id ASC",
            )?;
            let mapped_rows = stmt.query_map(params!["wallet-dup"], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?;
            mapped_rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(rows.len(), 2, "expected both historical rows to remain");
        assert_eq!(
            rows[0].2, 0,
            "older duplicate must be deactivated by migration"
        );
        assert_eq!(
            rows[0].3.as_deref(),
            Some("2026-02-20T00:01:00Z"),
            "migration should close duplicate row at the surviving row start time",
        );
        assert_eq!(
            rows[0].4.as_deref(),
            Some("migration_dedup_active_followlist"),
            "migration should annotate deduplicated row",
        );
        assert_eq!(rows[1].2, 1, "latest active row must stay active");
        assert!(
            migrated_store.was_wallet_followed_at(
                "wallet-dup",
                DateTime::parse_from_rfc3339("2026-02-20T00:00:30Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
            )?,
            "dedup must preserve historical follow membership before the surviving active row"
        );

        let duplicate_insert = migrated_store.conn.execute(
            "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, ?3, 1)",
            params!["wallet-dup", "2026-02-20T00:02:00Z", "duplicate-test"],
        );
        assert!(
            duplicate_insert.is_err(),
            "unique partial index must reject a second active followlist row"
        );
        Ok(())
    }

    #[test]
    fn positions_single_open_guard_migration_merges_duplicate_open_rows() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("positions-single-open-guard.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(
            &legacy_migrations,
            "0018_followlist_single_active_guard.sql",
        )?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        legacy_store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, 'open')",
            params![
                "pos-open-a",
                "token-dup",
                1.5_f64,
                0.30_f64,
                "2026-03-01T00:00:00+00:00",
                0.05_f64,
            ],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, 'open')",
            params![
                "pos-open-b",
                "token-dup",
                2.0_f64,
                0.45_f64,
                "2026-03-01T00:05:00+00:00",
                0.07_f64,
            ],
        )?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        let open_rows: Vec<(String, f64, f64, String, Option<f64>)> = {
            let mut stmt = migrated_store.conn.prepare(
                "SELECT position_id, qty, cost_sol, opened_ts, pnl_sol
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                 ORDER BY opened_ts ASC",
            )?;
            let mapped_rows = stmt.query_map(params!["token-dup"], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?;
            mapped_rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(
            open_rows.len(),
            1,
            "migration must leave exactly one open row"
        );
        assert!(
            (open_rows[0].1 - 3.5).abs() < 1e-9,
            "qty should be merged into surviving open row"
        );
        assert!(
            (open_rows[0].2 - 0.75).abs() < 1e-9,
            "cost_sol should be merged into surviving open row"
        );
        assert_eq!(
            open_rows[0].3, "2026-03-01T00:00:00+00:00",
            "merged row should preserve earliest opened_ts"
        );
        assert!(
            (open_rows[0].4.unwrap_or_default() - 0.12).abs() < 1e-9,
            "pnl_sol should be preserved across merged open rows"
        );
        assert_eq!(
            migrated_store.live_open_positions_count()?,
            1,
            "runtime open position count should observe deduplicated schema invariant"
        );
        assert_eq!(
            migrated_store.live_open_position_qty_cost("token-dup")?,
            Some((3.5, 0.75)),
            "runtime open position lookup should see merged aggregate row"
        );

        let duplicate_insert = migrated_store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, state, pnl_sol)
             VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0.0)",
            params![
                "pos-open-c",
                "token-dup",
                0.5_f64,
                0.10_f64,
                "2026-03-01T00:10:00+00:00",
            ],
        );
        assert!(
            duplicate_insert.is_err(),
            "unique partial index must reject a second open position row for the same token"
        );
        Ok(())
    }

    #[test]
    fn execution_foreign_keys_migration_cleans_orphans_and_enforces_chain() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-foreign-keys.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(&legacy_migrations, "0019_positions_single_open_guard.sql")?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        legacy_store.conn.execute(
            "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                "sig-valid",
                "wallet-1",
                "buy",
                "token-a",
                0.25_f64,
                now.to_rfc3339(),
                "execution_submitted",
            ],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO orders(
                order_id, signal_id, route, submit_ts, status, client_order_id, attempt
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                "ord-valid",
                "sig-valid",
                "paper",
                now.to_rfc3339(),
                "execution_submitted",
                "cb_valid_order_a1",
                1_i64,
            ],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO orders(
                order_id, signal_id, route, submit_ts, status, client_order_id, attempt
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                "ord-orphan",
                "sig-missing",
                "paper",
                now.to_rfc3339(),
                "execution_failed",
                "cb_orphan_order_a1",
                1_i64,
            ],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params!["ord-valid", "token-a", 1.0_f64, 0.25_f64, 0.0_f64, 0.0_f64],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "ord-missing",
                "token-b",
                2.0_f64,
                0.15_f64,
                0.0_f64,
                0.0_f64,
            ],
        )?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        let order_count: i64 =
            migrated_store
                .conn
                .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))?;
        assert_eq!(
            order_count, 1,
            "orphan orders should be removed by migration"
        );

        let fill_count: i64 =
            migrated_store
                .conn
                .query_row("SELECT COUNT(*) FROM fills", [], |row| row.get(0))?;
        assert_eq!(fill_count, 1, "orphan fills should be removed by migration");

        let preserved = migrated_store
            .execution_order_by_client_order_id("cb_valid_order_a1")?
            .context("valid order should survive foreign key migration")?;
        assert_eq!(preserved.order_id, "ord-valid");

        let orphan_order_insert = migrated_store.conn.execute(
            "INSERT INTO orders(
                order_id, signal_id, route, submit_ts, status, client_order_id, attempt
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                "ord-after-migration",
                "sig-missing",
                "paper",
                now.to_rfc3339(),
                "execution_pending",
                "cb_after_migration_a1",
                1_i64,
            ],
        );
        assert!(
            orphan_order_insert.is_err(),
            "orders.signal_id foreign key must reject missing copy signal"
        );

        let orphan_fill_insert = migrated_store.conn.execute(
            "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "ord-missing-after-migration",
                "token-c",
                1.0_f64,
                0.10_f64,
                0.0_f64,
                0.0_f64,
            ],
        );
        assert!(
            orphan_fill_insert.is_err(),
            "fills.order_id foreign key must reject missing order"
        );

        Ok(())
    }

    #[test]
    fn live_max_drawdown_since_respects_subsecond_closed_ts_order() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-max-drawdown-subsecond-order.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, 'closed')",
            params![
                "pos-loss-first",
                "token-drawdown",
                (base - Duration::minutes(5)).to_rfc3339(),
                (base + Duration::milliseconds(100)).to_rfc3339(),
                -0.4_f64
            ],
        )?;
        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, 'closed')",
            params![
                "pos-profit-second",
                "token-drawdown",
                (base - Duration::minutes(4)).to_rfc3339(),
                (base + Duration::milliseconds(900)).to_rfc3339(),
                0.5_f64
            ],
        )?;
        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, 'closed')",
            params![
                "pos-loss-third",
                "token-drawdown",
                (base - Duration::minutes(3)).to_rfc3339(),
                (base + Duration::seconds(1)).to_rfc3339(),
                -0.4_f64
            ],
        )?;

        let drawdown = store.live_max_drawdown_since(base - Duration::seconds(1))?;
        assert!(
            (drawdown - 0.4).abs() < 1e-9,
            "drawdown should follow subsecond close ordering, got {drawdown}"
        );
        Ok(())
    }

    #[test]
    fn live_max_drawdown_since_excludes_history_outside_window() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-max-drawdown-window.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::hours(24);

        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, 'closed')",
            params![
                "pos-old-loss",
                "token-old",
                (now - Duration::hours(50)).to_rfc3339(),
                (now - Duration::hours(48)).to_rfc3339(),
                -1.0_f64
            ],
        )?;
        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, 'closed')",
            params![
                "pos-recent-gain",
                "token-new",
                (now - Duration::hours(2)).to_rfc3339(),
                (now - Duration::hours(1)).to_rfc3339(),
                0.2_f64
            ],
        )?;

        let drawdown = store.live_max_drawdown_since(window_start)?;
        assert!(
            drawdown <= 1e-9,
            "drawdown should ignore old losses outside window, got {drawdown}"
        );
        Ok(())
    }

    #[test]
    fn live_unrealized_pnl_sol_uses_reliable_price_and_counts_missing_quotes() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-unrealized-pnl.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions("token-priced", "buy", 1.0, 0.20, now)?;
        store.apply_execution_fill_to_positions("token-missing", "buy", 1.0, 0.30, now)?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-priced".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-live-unrealized".to_string(),
            slot: 12345,
            ts_utc: now + Duration::seconds(1),
        })?;

        let (unrealized_pnl_sol, missing_price_count) =
            store.live_unrealized_pnl_sol(now + Duration::seconds(2))?;
        assert!(
            (unrealized_pnl_sol + 0.10).abs() < 1e-9,
            "unexpected unrealized pnl: {unrealized_pnl_sol}"
        );
        assert_eq!(missing_price_count, 1);
        Ok(())
    }

    #[test]
    fn live_unrealized_pnl_sol_ignores_micro_swap_outlier_price() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-unrealized-pnl-micro-outlier.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions("token-priced", "buy", 1.0, 0.20, now)?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-priced".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-live-unrealized-normal".to_string(),
            slot: 12346,
            ts_utc: now + Duration::seconds(1),
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-priced".to_string(),
            amount_in: 0.001,
            amount_out: 0.000001,
            signature: "sig-live-unrealized-micro-outlier".to_string(),
            slot: 12347,
            ts_utc: now + Duration::seconds(2),
        })?;

        let (unrealized_pnl_sol, missing_price_count) =
            store.live_unrealized_pnl_sol(now + Duration::seconds(3))?;
        assert!(
            (unrealized_pnl_sol + 0.10).abs() < 1e-9,
            "micro notional outlier should be ignored, got unrealized pnl={unrealized_pnl_sol}"
        );
        assert_eq!(missing_price_count, 0);
        Ok(())
    }

    #[test]
    fn live_unrealized_pnl_sol_counts_missing_when_only_micro_quotes_exist() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-unrealized-pnl-only-micro.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions("token-micro-only", "buy", 1.0, 0.20, now)?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-micro-only".to_string(),
            amount_in: 0.001,
            amount_out: 0.01,
            signature: "sig-live-unrealized-only-micro".to_string(),
            slot: 12348,
            ts_utc: now + Duration::seconds(1),
        })?;

        let (unrealized_pnl_sol, missing_price_count) =
            store.live_unrealized_pnl_sol(now + Duration::seconds(2))?;
        assert!(
            unrealized_pnl_sol.abs() < 1e-9,
            "expected zero unrealized pnl when reliable quote is unavailable, got {unrealized_pnl_sol}"
        );
        assert_eq!(missing_price_count, 1);
        Ok(())
    }

    #[test]
    fn live_max_drawdown_with_unrealized_since_includes_open_position_loss() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-drawdown-with-unrealized.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::hours(24);

        store.apply_execution_fill_to_positions(
            "token-closed",
            "buy",
            1.0,
            0.10,
            now - Duration::minutes(30),
        )?;
        store.apply_execution_fill_to_positions(
            "token-closed",
            "sell",
            1.0,
            0.30,
            now - Duration::minutes(29),
        )?;
        store.apply_execution_fill_to_positions("token-open", "buy", 1.0, 0.40, now)?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-open".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-drawdown-unrealized".to_string(),
            slot: 12346,
            ts_utc: now + Duration::seconds(1),
        })?;

        let (unrealized_pnl_sol, missing_price_count) =
            store.live_unrealized_pnl_sol(now + Duration::seconds(2))?;
        let drawdown_sol =
            store.live_max_drawdown_with_unrealized_since(window_start, unrealized_pnl_sol)?;
        assert!(
            (drawdown_sol - 0.30).abs() < 1e-9,
            "drawdown should include terminal open-position unrealized loss, got {drawdown_sol}"
        );
        assert_eq!(missing_price_count, 0);
        Ok(())
    }

    #[test]
    fn insert_observed_swap_retries_after_write_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-retry.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let blocker_store = SqliteStore::open(Path::new(&db_path))?;
        blocker_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_store
            .conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_db_path = db_path.clone();
        let worker_barrier = barrier.clone();
        let handle = std::thread::spawn(move || -> Result<()> {
            let worker_store = SqliteStore::open(Path::new(&worker_db_path))?;
            worker_store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten worker busy timeout")?;
            worker_barrier.wait();
            let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc);
            let inserted = worker_store.insert_observed_swap(&SwapEvent {
                wallet: "wallet-retry".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-retry".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-retry".to_string(),
                slot: 999,
                ts_utc: now,
            })?;
            assert!(
                inserted,
                "expected observed swap insert to succeed after retry"
            );
            Ok(())
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;
        handle
            .join()
            .expect("worker thread panicked")
            .context("worker insert should succeed after retry")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-retry");
        Ok(())
    }

    #[test]
    fn record_heartbeat_retries_after_write_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("heartbeat-retry.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let blocker_store = SqliteStore::open(Path::new(&db_path))?;
        blocker_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_store
            .conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_db_path = db_path.clone();
        let worker_barrier = barrier.clone();
        let handle = std::thread::spawn(move || -> Result<()> {
            let worker_store = SqliteStore::open(Path::new(&worker_db_path))?;
            worker_store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten worker busy timeout")?;
            worker_barrier.wait();
            worker_store.record_heartbeat("copybot-app", "alive")?;
            Ok(())
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;
        handle
            .join()
            .expect("worker thread panicked")
            .context("worker heartbeat should succeed after retry")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let count: i64 = verify_store.conn.query_row(
            "SELECT COUNT(*) FROM system_heartbeat WHERE component = ?1 AND status = ?2",
            params!["copybot-app", "alive"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);
        Ok(())
    }
}

fn u64_to_sql_i64(field: &str, value: u64) -> Result<i64> {
    i64::try_from(value)
        .with_context(|| format!("{}={} exceeds sqlite INTEGER max (i64::MAX)", field, value))
}

fn parse_non_negative_i64(field: &str, order_id: &str, value: Option<i64>) -> Result<Option<u64>> {
    match value {
        Some(value) if value < 0 => Err(anyhow!(
            "invalid {}={} for order_id={} (must be >= 0)",
            field,
            value,
            order_id
        )),
        Some(value) => Ok(Some(value as u64)),
        None => Ok(None),
    }
}
