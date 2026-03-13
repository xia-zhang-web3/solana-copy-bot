use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

pub use copybot_core_types::{
    CopySignalRow, ExactSwapAmounts, ExecutionConfirmStateSnapshot, ExecutionOrderRow,
    FinalizeExecutionConfirmOutcome, InsertExecutionOrderPendingOutcome, Lamports, SignedLamports,
    TokenQualityCacheRow, TokenQualityRpcRow, TokenQuantity, WalletMetricRow, WalletUpsertRow,
    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
    EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS, EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
};

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];
const DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS: i64 = 3;
pub const STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES: i64 = 30;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL: f64 = 0.05;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES: usize = 3;
pub const STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES: usize = 60;
pub const SHADOW_CLOSE_CONTEXT_MARKET: &str = "market";
pub const SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE: &str = "stale_terminal_zero_price";
const LIVE_UNREALIZED_RELIABLE_PRICE_WINDOW_MINUTES: i64 = 30;
const LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SOL_NOTIONAL: f64 = 0.05;
const LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SAMPLES: usize = 1;
const LIVE_UNREALIZED_RELIABLE_PRICE_MAX_SAMPLES: usize = 60;
// Until D-2 exact quantities land, sub-nano residual qty should not count as an open live position.
pub const LIVE_POSITION_OPEN_EPS: f64 = 1e-9;
pub(crate) const POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER: &str = "legacy_pre_cutover";
pub(crate) const POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER: &str = "exact_post_cutover";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
static SQLITE_WRITE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static SQLITE_BUSY_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

mod discovery;
mod discovery_scoring;
mod execution_orders;
mod history_retention;
mod market_data;
mod migrations;
mod pricing;
mod risk_metrics;
mod shadow;
mod sqlite_retry;
mod system_events;

pub use execution_orders::{MarkOrderDroppedOutcome, ScheduleOrderRetryOutcome};
pub use history_retention::{HistoryRetentionCutoffs, HistoryRetentionSummary};
pub use sqlite_retry::is_retryable_sqlite_anyhow_error;
pub use system_events::RiskEventRow;

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
pub struct WalletActivityDayRow {
    pub wallet_id: String,
    pub activity_day: NaiveDate,
    pub last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PersistedWalletMetricSnapshotRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub pnl: f64,
    pub win_rate: f64,
    pub trades: u32,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub score: f64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
}

#[derive(Debug, Clone, Default)]
pub struct DiscoveryAggregateWriteConfig {
    pub max_tx_per_minute: u32,
    pub rug_lookahead_seconds: u32,
    pub helius_http_url: Option<String>,
    pub min_token_age_hint_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct WalletScoringDayRow {
    pub wallet_id: String,
    pub activity_day: NaiveDate,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub trades: u32,
    pub spent_sol: f64,
    pub max_buy_notional_sol: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalletScoringQualitySource {
    Fresh,
    Stale,
    Deferred,
    Missing,
}

#[derive(Debug, Clone)]
pub struct WalletScoringBuyFactRow {
    pub wallet_id: String,
    pub token: String,
    pub ts: DateTime<Utc>,
    pub notional_sol: f64,
    pub market_volume_5m_sol: f64,
    pub market_unique_traders_5m: u32,
    pub market_liquidity_proxy_sol: f64,
    pub quality_source: WalletScoringQualitySource,
    pub quality_token_age_seconds: Option<u64>,
    pub quality_holders: Option<u64>,
    pub quality_liquidity_sol: Option<f64>,
    pub rug_check_after_ts: DateTime<Utc>,
    pub rug_volume_lookahead_sol: Option<f64>,
    pub rug_unique_traders_lookahead: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct WalletScoringCloseFactRow {
    pub wallet_id: String,
    pub token: String,
    pub closed_ts: DateTime<Utc>,
    pub pnl_sol: f64,
    pub hold_seconds: i64,
    pub win: bool,
}

#[derive(Debug, Clone)]
pub struct WalletScoringSnapshot {
    pub days: Vec<WalletScoringDayRow>,
    pub buy_facts: Vec<WalletScoringBuyFactRow>,
    pub close_facts: Vec<WalletScoringCloseFactRow>,
    pub max_tx_counts: std::collections::HashMap<String, u32>,
}

#[derive(Debug, Clone)]
pub struct ShadowLotRow {
    pub id: i64,
    pub wallet_id: String,
    pub token: String,
    pub accounting_bucket: String,
    pub qty: f64,
    pub qty_exact: Option<TokenQuantity>,
    pub cost_sol: f64,
    pub cost_lamports: Option<Lamports>,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowCloseOutcome {
    pub closed_qty: f64,
    pub realized_pnl_sol: f64,
    pub has_open_lots_after: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentActiveBuyOrderRow {
    pub signal_id: String,
    pub submit_ts: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone, Default)]
pub struct TokenMarketStats {
    pub first_seen: Option<DateTime<Utc>>,
    pub holders_proxy: u64,
    pub liquidity_sol_proxy: f64,
    pub volume_5m_sol: f64,
    pub unique_traders_5m: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryRuntimeCursor {
    pub ts_utc: DateTime<Utc>,
    pub slot: u64,
    pub signature: String,
}

#[derive(Debug, Clone)]
struct LiveOpenPositionRow {
    position_id: String,
    accounting_bucket: String,
    qty: f64,
    qty_raw: Option<String>,
    qty_decimals: Option<i64>,
    cost_sol: f64,
    cost_lamports_raw: Option<i64>,
    pnl_sol: Option<f64>,
    pnl_lamports_raw: Option<i64>,
}

impl SqliteStore {
    fn load_live_open_positions(
        conn: &Connection,
        token: &str,
        accounting_bucket: Option<&str>,
    ) -> Result<Vec<LiveOpenPositionRow>> {
        let (query, params): (&str, Vec<rusqlite::types::Value>) =
            if let Some(accounting_bucket) = accounting_bucket {
                (
                    "SELECT position_id, accounting_bucket, qty, qty_raw, qty_decimals,
                            cost_sol, cost_lamports, pnl_sol, pnl_lamports
                     FROM positions
                     WHERE token = ?1
                       AND accounting_bucket = ?2
                       AND state = 'open'
                     ORDER BY opened_ts ASC, rowid ASC",
                    vec![
                        rusqlite::types::Value::from(token.to_string()),
                        rusqlite::types::Value::from(accounting_bucket.to_string()),
                    ],
                )
            } else {
                (
                    "SELECT position_id, accounting_bucket, qty, qty_raw, qty_decimals,
                            cost_sol, cost_lamports, pnl_sol, pnl_lamports
                     FROM positions
                     WHERE token = ?1
                       AND state = 'open'
                     ORDER BY opened_ts ASC, rowid ASC",
                    vec![rusqlite::types::Value::from(token.to_string())],
                )
            };
        let mut stmt = conn
            .prepare(query)
            .context("failed to prepare live open positions query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying live open positions")?;
        let mut open_positions = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating live open positions rows")?
        {
            open_positions.push(LiveOpenPositionRow {
                position_id: row.get(0).context("failed reading positions.position_id")?,
                accounting_bucket: row
                    .get(1)
                    .context("failed reading positions.accounting_bucket")?,
                qty: row.get(2).context("failed reading positions.qty")?,
                qty_raw: row.get(3).context("failed reading positions.qty_raw")?,
                qty_decimals: row
                    .get(4)
                    .context("failed reading positions.qty_decimals")?,
                cost_sol: row.get(5).context("failed reading positions.cost_sol")?,
                cost_lamports_raw: row
                    .get(6)
                    .context("failed reading positions.cost_lamports")?,
                pnl_sol: row.get(7).context("failed reading positions.pnl_sol")?,
                pnl_lamports_raw: row
                    .get(8)
                    .context("failed reading positions.pnl_lamports")?,
            });
        }
        Ok(open_positions)
    }

    fn live_open_exposure_lamports_on_conn(
        conn: &Connection,
        token: Option<&str>,
    ) -> Result<Lamports> {
        let (query, params): (&str, Vec<rusqlite::types::Value>) = if let Some(token) = token {
            (
                "SELECT cost_sol, cost_lamports
                 FROM positions
                 WHERE state = 'open'
                   AND qty > ?2
                   AND token = ?1",
                vec![
                    rusqlite::types::Value::from(token.to_string()),
                    rusqlite::types::Value::from(LIVE_POSITION_OPEN_EPS),
                ],
            )
        } else {
            (
                "SELECT cost_sol, cost_lamports
                 FROM positions
                 WHERE state = 'open'
                   AND qty > ?1",
                vec![rusqlite::types::Value::from(LIVE_POSITION_OPEN_EPS)],
            )
        };
        let mut stmt = conn
            .prepare(query)
            .context("failed to prepare live open exposure lamports query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying live open exposure lamports")?;

        let mut total = Lamports::ZERO;
        while let Some(row) = rows
            .next()
            .context("failed iterating live open exposure lamports rows")?
        {
            let cost_sol: f64 = row.get(0).context("failed reading positions.cost_sol")?;
            let cost_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading positions.cost_lamports")?;
            let cost_lamports =
                position_cost_lamports(cost_sol, cost_lamports_raw, "live open exposure")?;
            total = total.checked_add(cost_lamports).ok_or_else(|| {
                anyhow!("live open exposure lamports overflow while summing positions")
            })?;
        }

        Ok(total)
    }

    fn live_execution_state_snapshot_on_conn(
        conn: &Connection,
        token: &str,
    ) -> Result<ExecutionConfirmStateSnapshot> {
        let total_exposure_lamports = Self::live_open_exposure_lamports_on_conn(conn, None)?;
        let token_exposure_lamports = Self::live_open_exposure_lamports_on_conn(conn, Some(token))?;
        let total_exposure_sol = lamports_to_sol(total_exposure_lamports);
        let token_exposure_sol = lamports_to_sol(token_exposure_lamports);
        let open_positions: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT token)
                 FROM positions
                 WHERE state = 'open'
                   AND qty > ?1",
                params![LIVE_POSITION_OPEN_EPS],
                |row| row.get(0),
            )
            .context("failed querying live open positions count in finalize confirm transaction")?;

        Ok(ExecutionConfirmStateSnapshot {
            total_exposure_lamports,
            total_exposure_sol: total_exposure_sol.max(0.0),
            token_exposure_lamports,
            token_exposure_sol: token_exposure_sol.max(0.0),
            open_positions: open_positions.max(0) as u64,
        })
    }

    fn exact_money_cutover_ts_on_conn(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
        let table_exists = conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table'
                   AND name = 'exact_money_cutover_state'
                 LIMIT 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed checking exact_money_cutover_state presence")?
            .is_some();
        if !table_exists {
            return Ok(None);
        }

        let cutover_ts_raw: Option<String> = conn
            .query_row(
                "SELECT cutover_ts
                 FROM exact_money_cutover_state
                 WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .context("failed loading exact money cutover timestamp")?;
        let Some(cutover_ts_raw) = cutover_ts_raw else {
            return Ok(None);
        };

        let cutover_ts = DateTime::parse_from_rfc3339(&cutover_ts_raw)
            .with_context(|| {
                format!("invalid exact_money_cutover_state.cutover_ts={cutover_ts_raw}")
            })?
            .with_timezone(&Utc);
        Ok(Some(cutover_ts))
    }

    pub fn exact_money_cutover_ts(&self) -> Result<Option<DateTime<Utc>>> {
        Self::exact_money_cutover_ts_on_conn(&self.conn)
    }

    pub fn exact_money_cutover_active_at(&self, ts: DateTime<Utc>) -> Result<bool> {
        Ok(self
            .exact_money_cutover_ts()?
            .map(|cutover_ts| ts >= cutover_ts)
            .unwrap_or(false))
    }

    pub fn upsert_exact_money_cutover_state(
        &self,
        cutover_ts: DateTime<Utc>,
        note: Option<&str>,
    ) -> Result<()> {
        let recorded_ts = Utc::now();
        self.conn
            .execute(
                "INSERT INTO exact_money_cutover_state(id, cutover_ts, recorded_ts, note)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                   cutover_ts = excluded.cutover_ts,
                   recorded_ts = excluded.recorded_ts,
                   note = excluded.note",
                params![cutover_ts.to_rfc3339(), recorded_ts.to_rfc3339(), note],
            )
            .context("failed upserting exact_money_cutover_state")?;
        Ok(())
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
        let notional_lamports =
            sol_to_lamports_ceil_storage(notional_sol, "execution fill notional_sol")?;
        let fee_lamports = sol_to_lamports_ceil_storage(fee, "execution fill fee_sol")?;
        self.finalize_execution_confirmed_order_exact(
            order_id,
            signal_id,
            token,
            side,
            qty,
            None,
            notional_sol,
            notional_lamports,
            avg_price,
            fee,
            fee_lamports,
            slippage_bps,
            confirmed_ts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finalize_execution_confirmed_order_exact(
        &self,
        order_id: &str,
        signal_id: &str,
        token: &str,
        side: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        notional_sol: f64,
        notional_lamports: Lamports,
        avg_price: f64,
        fee: f64,
        fee_lamports: Lamports,
        slippage_bps: f64,
        confirmed_ts: DateTime<Utc>,
    ) -> Result<FinalizeExecutionConfirmOutcome> {
        const CONFIRM_ACTION: &str = "marking order confirmed in finalize confirm transaction";
        const CONFIRM_EXPECTED: &[&str] = &[
            "execution_submitted",
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
            EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS,
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
                    "execution_submitted"
                        | EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS
                        | EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS
                ) {
                    return Err(anyhow!(
                        "failed finalizing confirmed order: order_id={} has unexpected status={}",
                        order_id,
                        status
                    ));
                }

                self.conn
                    .execute(
                        "INSERT INTO fills(
                            order_id,
                            token,
                            qty,
                            qty_raw,
                            qty_decimals,
                            avg_price,
                            fee,
                            slippage_bps,
                            notional_lamports,
                            fee_lamports
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            order_id,
                            token,
                            qty,
                            qty_exact.as_ref().map(|value| value.raw().to_string()),
                            qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                            avg_price,
                            fee,
                            slippage_bps,
                            u64_to_sql_i64("fills.notional_lamports", notional_lamports.as_u64())?,
                            u64_to_sql_i64("fills.fee_lamports", fee_lamports.as_u64())?,
                        ],
                    )
                    .context("failed inserting execution fill in finalize confirm transaction")?;

                Self::apply_execution_fill_to_positions_on_conn(
                    &self.conn,
                    token,
                    side,
                    qty,
                    qty_exact,
                    notional_sol,
                    notional_lamports,
                    fee,
                    fee_lamports,
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
                           AND status IN ('execution_submitted', ?3, ?4)",
                        params![
                            confirmed_ts.to_rfc3339(),
                            order_id,
                            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
                            EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS
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
                "INSERT INTO fills(
                    order_id,
                    token,
                    qty,
                    qty_raw,
                    qty_decimals,
                    avg_price,
                    fee,
                    slippage_bps,
                    notional_lamports,
                    fee_lamports
                 ) VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5, ?6, NULL, NULL)",
                params![order_id, token, qty, avg_price, fee, slippage_bps],
            )
        })
        .context("failed inserting execution fill")?;
        Ok(())
    }

    pub fn live_open_exposure_lamports(&self) -> Result<Lamports> {
        Self::live_open_exposure_lamports_on_conn(&self.conn, None)
    }

    pub fn live_open_exposure_lamports_for_token(&self, token: &str) -> Result<Lamports> {
        Self::live_open_exposure_lamports_on_conn(&self.conn, Some(token))
    }

    pub fn live_open_exposure_sol(&self) -> Result<f64> {
        Ok(lamports_to_sol(self.live_open_exposure_lamports()?))
    }

    pub fn live_open_exposure_sol_for_token(&self, token: &str) -> Result<f64> {
        Ok(lamports_to_sol(
            self.live_open_exposure_lamports_for_token(token)?,
        ))
    }

    pub fn live_open_positions_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(DISTINCT token)
                 FROM positions
                 WHERE state = 'open'
                   AND qty > ?1",
                params![LIVE_POSITION_OPEN_EPS],
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
                   AND qty > ?2
                 LIMIT 1",
                params![token, LIVE_POSITION_OPEN_EPS],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking live open position by token")?;
        Ok(exists.is_some())
    }

    pub fn live_open_position_qty_cost(&self, token: &str) -> Result<Option<(f64, f64)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT qty, cost_sol, cost_lamports
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                   AND qty > ?2
                 ORDER BY opened_ts ASC, rowid ASC",
            )
            .context("failed querying live open position qty/cost by token")?;
        let mut rows = stmt
            .query(params![token, LIVE_POSITION_OPEN_EPS])
            .context("failed querying live open position qty/cost rows")?;
        let mut total_qty = 0.0_f64;
        let mut total_cost_lamports = Lamports::ZERO;
        let mut saw_row = false;
        while let Some(row) = rows
            .next()
            .context("failed iterating live open position qty/cost rows")?
        {
            let qty: f64 = row.get(0).context("failed reading positions.qty")?;
            let cost_sol: f64 = row.get(1).context("failed reading positions.cost_sol")?;
            let cost_lamports_raw: Option<i64> = row
                .get(2)
                .context("failed reading positions.cost_lamports")?;
            if !qty.is_finite() || qty <= LIVE_POSITION_OPEN_EPS {
                continue;
            }
            if !cost_sol.is_finite() || cost_sol < 0.0 {
                return Err(anyhow!(
                    "invalid open position row for qty/cost token={} qty={} cost_sol={}",
                    token,
                    qty,
                    cost_sol
                ));
            }
            let cost_lamports =
                position_cost_lamports(cost_sol, cost_lamports_raw, "live open position")?;
            total_qty += qty;
            total_cost_lamports = total_cost_lamports
                .checked_add(cost_lamports)
                .ok_or_else(|| anyhow!("live open position cost overflow for token={token}"))?;
            saw_row = true;
        }
        if saw_row {
            Ok(Some((total_qty, lamports_to_sol(total_cost_lamports))))
        } else {
            Ok(None)
        }
    }

    pub fn apply_execution_fill_to_positions(
        &self,
        token: &str,
        side: &str,
        qty: f64,
        notional_sol: f64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        self.apply_execution_fill_to_positions_exact(token, side, qty, None, notional_sol, ts)
    }

    pub fn apply_execution_fill_to_positions_exact(
        &self,
        token: &str,
        side: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        notional_sol: f64,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        if qty <= LIVE_POSITION_OPEN_EPS
            || notional_sol <= 0.0
            || !qty.is_finite()
            || !notional_sol.is_finite()
        {
            return Ok(());
        }
        let notional_lamports =
            sol_to_lamports_ceil_storage(notional_sol, "execution fill notional_sol")?;
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
                qty_exact,
                notional_sol,
                notional_lamports,
                0.0,
                Lamports::ZERO,
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
        qty_exact: Option<TokenQuantity>,
        notional_sol: f64,
        notional_lamports: Lamports,
        fee_sol: f64,
        fee_lamports: Lamports,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        if !fee_sol.is_finite() || fee_sol < 0.0 {
            return Err(anyhow!(
                "invalid execution fill fee token={} side={} fee_sol={}",
                token,
                side,
                fee_sol
            ));
        }

        let side_norm = side.trim().to_ascii_lowercase();
        match side_norm.as_str() {
            "buy" => {
                let accounting_bucket = position_accounting_bucket_for_fill(qty_exact);
                let effective_cost = notional_sol + fee_sol;
                let effective_cost_lamports =
                    notional_lamports.checked_add(fee_lamports).ok_or_else(|| {
                        anyhow!("execution fill cost_lamports overflow for token={token}")
                    })?;
                if let Some(existing) =
                    Self::load_live_open_positions(conn, token, Some(accounting_bucket))?
                        .into_iter()
                        .next()
                {
                    let current_qty_exact = token_quantity_from_sql(
                        existing.qty_raw,
                        existing.qty_decimals,
                        "live open position buy update",
                    )?;
                    let next_qty_exact =
                        merge_position_qty_exact_on_buy(current_qty_exact, qty_exact)?;
                    let current_cost_lamports = position_cost_lamports(
                        existing.cost_sol,
                        existing.cost_lamports_raw,
                        "live open position buy update",
                    )?;
                    let current_pnl_lamports = position_pnl_lamports(
                        existing.pnl_sol.unwrap_or(0.0),
                        existing.pnl_lamports_raw,
                        "live open position buy update",
                    )?;
                    let next_cost_lamports = current_cost_lamports
                        .checked_add(effective_cost_lamports)
                        .ok_or_else(|| {
                            anyhow!("live position cost_lamports overflow for token={token}")
                        })?;
                    conn.execute(
                        "UPDATE positions
                         SET qty = ?1,
                             qty_raw = ?2,
                             qty_decimals = ?3,
                             cost_sol = ?4,
                             cost_lamports = ?5,
                             pnl_sol = ?6,
                             pnl_lamports = ?7,
                             state = 'open',
                             closed_ts = NULL
                         WHERE position_id = ?8",
                        params![
                            existing.qty + qty,
                            next_qty_exact.as_ref().map(|value| value.raw().to_string()),
                            next_qty_exact
                                .as_ref()
                                .map(|value| i64::from(value.decimals())),
                            existing.cost_sol + effective_cost,
                            u64_to_sql_i64("positions.cost_lamports", next_cost_lamports.as_u64())?,
                            existing.pnl_sol.unwrap_or(0.0),
                            signed_lamports_to_sql_i64(
                                "positions.pnl_lamports",
                                current_pnl_lamports
                            )?,
                            existing.position_id,
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
                            qty_raw,
                            qty_decimals,
                            cost_sol,
                            cost_lamports,
                            accounting_bucket,
                            opened_ts,
                            state,
                            pnl_sol,
                            pnl_lamports
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'open', 0.0, 0)",
                        params![
                            position_id,
                            token,
                            qty,
                            qty_exact.as_ref().map(|value| value.raw().to_string()),
                            qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                            effective_cost,
                            u64_to_sql_i64(
                                "positions.cost_lamports",
                                effective_cost_lamports.as_u64()
                            )?,
                            accounting_bucket,
                            ts.to_rfc3339(),
                        ],
                    )
                    .context("failed inserting new live position for buy fill")?;
                }
            }
            "sell" => {
                let open_positions = Self::load_live_open_positions(conn, token, None)?;
                if open_positions.is_empty() {
                    return Err(anyhow!(
                        "sell fill without open position token={} qty={} notional_sol={}",
                        token,
                        qty,
                        notional_sol
                    ));
                }
                let mut remaining_qty = qty;
                let mut remaining_qty_exact = qty_exact;
                let mut remaining_notional_sol = notional_sol;
                let mut remaining_notional_lamports = notional_lamports;
                let mut remaining_fee_sol = fee_sol;
                let mut remaining_fee_lamports = fee_lamports;

                for open_position in open_positions {
                    if remaining_qty <= LIVE_POSITION_OPEN_EPS {
                        break;
                    }
                    if open_position.qty <= LIVE_POSITION_OPEN_EPS {
                        continue;
                    }
                    if open_position.cost_sol <= 0.0 {
                        return Err(anyhow!(
                            "sell fill on non-positive open position token={} qty={} cost_sol={}",
                            token,
                            open_position.qty,
                            open_position.cost_sol
                        ));
                    }

                    let qty_closed = remaining_qty.min(open_position.qty);
                    let final_segment = (remaining_qty - qty_closed) <= LIVE_POSITION_OPEN_EPS;
                    let effective_notional = split_f64_pro_rata(
                        remaining_notional_sol,
                        qty_closed,
                        remaining_qty,
                        final_segment,
                        "live sell notional_sol",
                    )?;
                    let effective_fee = split_f64_pro_rata(
                        remaining_fee_sol,
                        qty_closed,
                        remaining_qty,
                        final_segment,
                        "live sell fee_sol",
                    )?;
                    let effective_notional_lamports = split_lamports_pro_rata(
                        remaining_notional_lamports,
                        qty_closed,
                        remaining_qty,
                        final_segment,
                        false,
                        "live sell notional_lamports",
                    )?;
                    let effective_fee_lamports = split_lamports_pro_rata(
                        remaining_fee_lamports,
                        qty_closed,
                        remaining_qty,
                        final_segment,
                        true,
                        "live sell fee_lamports",
                    )?;
                    let (segment_qty_exact, next_remaining_qty_exact) = match remaining_qty_exact {
                        Some(total_exact) => split_token_quantity_pro_rata(
                            total_exact,
                            qty_closed,
                            remaining_qty,
                            final_segment,
                            "live sell qty_exact",
                        )?,
                        None => (None, None),
                    };
                    let avg_cost = open_position.cost_sol / open_position.qty;
                    let realized_cost = avg_cost * qty_closed;
                    let realized_pnl = effective_notional - realized_cost - effective_fee;
                    let next_qty = (open_position.qty - qty_closed).max(0.0);
                    let next_cost = (open_position.cost_sol - realized_cost).max(0.0);
                    let next_pnl = open_position.pnl_sol.unwrap_or(0.0) + realized_pnl;
                    let current_cost_lamports = position_cost_lamports(
                        open_position.cost_sol,
                        open_position.cost_lamports_raw,
                        "live open position sell update",
                    )?;
                    let current_pnl_lamports = position_pnl_lamports(
                        open_position.pnl_sol.unwrap_or(0.0),
                        open_position.pnl_lamports_raw,
                        "live open position sell update",
                    )?;
                    let current_qty_exact = token_quantity_from_sql(
                        open_position.qty_raw,
                        open_position.qty_decimals,
                        "live open position sell update",
                    )?;
                    let next_qty_exact = merge_position_qty_exact_on_sell(
                        current_qty_exact,
                        segment_qty_exact,
                        next_qty <= LIVE_POSITION_OPEN_EPS,
                    )?;
                    let next_cost_lamports = if next_qty <= LIVE_POSITION_OPEN_EPS {
                        Lamports::ZERO
                    } else {
                        let estimated_remaining_cost_lamports = sol_to_lamports_ceil_storage(
                            next_cost,
                            "remaining live position cost_sol",
                        )?;
                        if estimated_remaining_cost_lamports > current_cost_lamports {
                            current_cost_lamports
                        } else {
                            estimated_remaining_cost_lamports
                        }
                    };
                    let realized_cost_lamports = current_cost_lamports
                        .checked_sub(next_cost_lamports)
                        .ok_or_else(|| {
                            anyhow!("live position realized cost underflow for token={token}")
                        })?;
                    let realized_pnl_lamports = SignedLamports::from(effective_notional_lamports)
                        .checked_sub(SignedLamports::from(realized_cost_lamports))
                        .and_then(|value| {
                            value.checked_sub(SignedLamports::from(effective_fee_lamports))
                        })
                        .ok_or_else(|| {
                            anyhow!("live position pnl_lamports overflow for token={token}")
                        })?;
                    let next_pnl_lamports = current_pnl_lamports
                        .checked_add(realized_pnl_lamports)
                        .ok_or_else(|| {
                            anyhow!("live cumulative pnl_lamports overflow for token={token}")
                        })?;

                    if next_qty <= LIVE_POSITION_OPEN_EPS {
                        conn.execute(
                            "UPDATE positions
                             SET qty = 0.0,
                                 qty_raw = ?1,
                                 qty_decimals = ?2,
                                 cost_sol = 0.0,
                                 cost_lamports = 0,
                                 pnl_sol = ?3,
                                 pnl_lamports = ?4,
                                 state = 'closed',
                                 closed_ts = ?5
                             WHERE position_id = ?6",
                            params![
                                next_qty_exact.as_ref().map(|value| value.raw().to_string()),
                                next_qty_exact
                                    .as_ref()
                                    .map(|value| i64::from(value.decimals())),
                                next_pnl,
                                signed_lamports_to_sql_i64(
                                    "positions.pnl_lamports",
                                    next_pnl_lamports
                                )?,
                                ts.to_rfc3339(),
                                open_position.position_id
                            ],
                        )
                        .with_context(|| {
                            format!(
                                "failed closing live position after sell fill bucket={}",
                                open_position.accounting_bucket
                            )
                        })?;
                    } else {
                        conn.execute(
                            "UPDATE positions
                             SET qty = ?1,
                                 qty_raw = ?2,
                                 qty_decimals = ?3,
                                 cost_sol = ?4,
                                 cost_lamports = ?5,
                                 pnl_sol = ?6,
                                 pnl_lamports = ?7
                             WHERE position_id = ?8",
                            params![
                                next_qty,
                                next_qty_exact.as_ref().map(|value| value.raw().to_string()),
                                next_qty_exact
                                    .as_ref()
                                    .map(|value| i64::from(value.decimals())),
                                next_cost,
                                u64_to_sql_i64(
                                    "positions.cost_lamports",
                                    next_cost_lamports.as_u64()
                                )?,
                                next_pnl,
                                signed_lamports_to_sql_i64(
                                    "positions.pnl_lamports",
                                    next_pnl_lamports
                                )?,
                                open_position.position_id
                            ],
                        )
                        .with_context(|| {
                            format!(
                                "failed partially updating live position after sell fill bucket={}",
                                open_position.accounting_bucket
                            )
                        })?;
                    }

                    remaining_qty = (remaining_qty - qty_closed).max(0.0);
                    remaining_notional_sol = (remaining_notional_sol - effective_notional).max(0.0);
                    remaining_fee_sol = (remaining_fee_sol - effective_fee).max(0.0);
                    remaining_notional_lamports = remaining_notional_lamports
                        .checked_sub(effective_notional_lamports)
                        .ok_or_else(|| anyhow!("live sell notional_lamports underflow"))?;
                    remaining_fee_lamports = remaining_fee_lamports
                        .checked_sub(effective_fee_lamports)
                        .ok_or_else(|| anyhow!("live sell fee_lamports underflow"))?;
                    remaining_qty_exact = next_remaining_qty_exact;
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
    fn shadow_open_notional_sol_prefers_cost_lamports_sidecar() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-cost-lamports-preference.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.conn.execute(
            "INSERT INTO shadow_lots(wallet_id, token, qty, cost_sol, cost_lamports, opened_ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "wallet",
                "token",
                10.0_f64,
                0.100000001_f64,
                100_000_123_i64,
                opened_ts.to_rfc3339()
            ],
        )?;

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].cost_lamports, Some(Lamports::new(100_000_123)));

        let open_notional_lamports = store.shadow_open_notional_lamports()?;
        assert_eq!(open_notional_lamports, Lamports::new(100_000_123));

        let open_notional = store.shadow_open_notional_sol()?;
        assert!(
            (open_notional - 0.100000123).abs() < 1e-12,
            "expected shadow open notional to prefer lamport sidecar, got {open_notional}"
        );
        Ok(())
    }

    #[test]
    fn shadow_lot_and_closed_trade_persist_exact_qty_sidecars() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-exact-qty-sidecars.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T11:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            2.0,
            Some(TokenQuantity::new(2_000_000, 6)),
            0.20,
            opened_ts,
        )?;

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(2_000_000, 6)));

        let close = store.close_shadow_lots_fifo_atomic_exact(
            "signal",
            "wallet",
            "token",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.12,
            closed_ts,
        )?;
        assert!((close.closed_qty - 0.5).abs() < 1e-12);

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(1_500_000, 6)));

        let closed_row: (String, Option<String>, Option<i64>) = store.conn.query_row(
            "SELECT accounting_bucket, qty_raw, qty_decimals
             FROM shadow_closed_trades
             WHERE signal_id = ?1",
            params!["signal"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(
            closed_row.0,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string()
        );
        assert_eq!(closed_row.1.as_deref(), Some("500000"));
        assert_eq!(closed_row.2, Some(6));
        Ok(())
    }

    #[test]
    fn shadow_fifo_close_preserves_bucket_provenance_across_legacy_and_exact_lots() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-bucket-fifo.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T11:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_lot("wallet", "token", 1.0, 0.10, opened_ts)?;
        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.20,
            opened_ts + Duration::seconds(1),
        )?;

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 2);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER
        );
        assert_eq!(
            lots[1].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );

        let close = store.close_shadow_lots_fifo_atomic_exact(
            "signal-mixed",
            "wallet",
            "token",
            1.5,
            Some(TokenQuantity::new(1_500_000, 6)),
            0.30,
            closed_ts,
        )?;
        assert!((close.closed_qty - 1.5).abs() < 1e-12);
        assert!(close.has_open_lots_after);

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );
        assert!((lots[0].qty - 0.5).abs() < 1e-12);
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(500_000, 6)));

        let closed_rows: Vec<(String, f64, Option<String>, Option<i64>)> = {
            let mut stmt = store.conn.prepare(
                "SELECT accounting_bucket, qty, qty_raw, qty_decimals
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                 ORDER BY opened_ts ASC, id ASC",
            )?;
            let mapped = stmt.query_map(params!["signal-mixed"], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
            mapped.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(closed_rows.len(), 2);
        assert_eq!(
            closed_rows[0],
            (
                POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER.to_string(),
                1.0_f64,
                None,
                None
            )
        );
        assert_eq!(
            closed_rows[1],
            (
                POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string(),
                0.5_f64,
                Some("500000".to_string()),
                Some(6_i64)
            )
        );
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_insert_shadow_lot_exact_rejects_zero_raw_exact_qty() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-zero-raw-lot-reject.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let error = store
            .insert_shadow_lot_exact(
                "wallet",
                "token",
                0.5,
                Some(TokenQuantity::new(0, 6)),
                0.20,
                opened_ts,
            )
            .expect_err("zero-raw exact shadow lot must fail closed");
        let error_chain = format!("{error:#}");
        assert!(error_chain.contains("zero-raw exact quantity"));
        assert!(store.list_shadow_lots("wallet", "token")?.is_empty());
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_insert_shadow_closed_trade_exact_rejects_zero_raw_exact_qty() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-zero-raw-closed-trade-reject.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = opened_ts + Duration::hours(1);
        let error = store
            .insert_shadow_closed_trade_exact(
                "signal",
                "wallet",
                "token",
                0.5,
                Some(TokenQuantity::new(0, 6)),
                0.10,
                0.12,
                0.02,
                opened_ts,
                closed_ts,
            )
            .expect_err("zero-raw exact shadow closed trade must fail closed");
        let error_chain = format!("{error:#}");
        assert!(error_chain.contains("zero-raw exact quantity"));
        let count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM shadow_closed_trades WHERE signal_id = ?1",
            params!["signal"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_fifo_close_rejects_zero_raw_exact_segment_and_rolls_back() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-zero-raw-fifo-close-reject.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = opened_ts + Duration::hours(1);

        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.10,
            opened_ts,
        )?;
        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.10,
            opened_ts + Duration::seconds(1),
        )?;

        let error = store
            .close_shadow_lots_fifo_atomic_exact(
                "signal-zero-raw-segment",
                "wallet",
                "token",
                1.0,
                Some(TokenQuantity::new(1, 6)),
                0.25,
                closed_ts,
            )
            .expect_err("zero-raw exact fifo segment must fail closed");
        let error_chain = format!("{error:#}");
        assert!(error_chain.contains("zero-raw exact quantity"));

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 2, "failed close must roll back lot mutation");
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(500_000, 6)));
        assert_eq!(lots[1].qty_exact, Some(TokenQuantity::new(500_000, 6)));
        let closed_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM shadow_closed_trades WHERE signal_id = ?1",
            params!["signal-zero-raw-segment"],
            |row| row.get(0),
        )?;
        assert_eq!(
            closed_count, 0,
            "failed close must not persist closed trades"
        );
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_fifo_close_allows_legitimate_exact_full_close() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-exact-full-close-ok.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = opened_ts + Duration::hours(1);

        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.10,
            opened_ts,
        )?;

        let close = store.close_shadow_lots_fifo_atomic_exact(
            "signal-full-close",
            "wallet",
            "token",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.25,
            closed_ts,
        )?;
        assert!((close.closed_qty - 1.0).abs() < 1e-12);
        assert!(!close.has_open_lots_after);

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert!(
            lots.is_empty(),
            "exact full close should delete the open lot instead of erroring"
        );
        let closed_row: (String, Option<String>, Option<i64>) = store.conn.query_row(
            "SELECT accounting_bucket, qty_raw, qty_decimals
             FROM shadow_closed_trades
             WHERE signal_id = ?1",
            params!["signal-full-close"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(
            closed_row.0,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string()
        );
        assert_eq!(closed_row.1.as_deref(), Some("1000000"));
        assert_eq!(closed_row.2, Some(6));
        Ok(())
    }

    #[test]
    fn shadow_risk_metrics_prefer_closed_trade_lamport_sidecars() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-closed-trade-lamports.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                "sig-shadow",
                "wallet",
                "token",
                10.0_f64,
                0.10_f64,
                200_000_000_i64,
                0.05_f64,
                50_000_000_i64,
                -0.05_f64,
                -150_000_000_i64,
                opened_ts.to_rfc3339(),
                closed_ts.to_rfc3339()
            ],
        )?;

        let (trades, pnl_lamports) =
            store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(trades, 1);
        assert_eq!(pnl_lamports, SignedLamports::new(-150_000_000));

        let (trades, pnl) = store.shadow_realized_pnl_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(trades, 1);
        assert!(
            (pnl + 0.15).abs() < 1e-12,
            "expected realized pnl to prefer lamport sidecar, got {pnl}"
        );

        let rug_count =
            store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?;
        assert_eq!(
            rug_count, 1,
            "expected rug-loss count to prefer exact lamport sidecars"
        );

        let (recent_rug_count, total_count, rug_rate) =
            store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
        assert_eq!(recent_rug_count, 1);
        assert_eq!(total_count, 1);
        assert!((rug_rate - 1.0).abs() < 1e-12);
        Ok(())
    }

    #[test]
    fn shadow_risk_metrics_ignore_stale_terminal_zero_close_context() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-risk-ignore-terminal-zero.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_closed_trade_exact_with_context(
            "sig-terminal-zero",
            "wallet",
            "token",
            10.0,
            None,
            0.10,
            0.0,
            -0.10,
            SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
            opened_ts,
            closed_ts,
        )?;

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(all_trades, 1);
        assert_eq!(all_pnl, SignedLamports::new(-100_000_000));

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        assert_eq!(
            store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?,
            0
        );
        let (recent_rug_count, total_count, rug_rate) =
            store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
        assert_eq!(recent_rug_count, 0);
        assert_eq!(total_count, 0);
        assert_eq!(rug_rate, 0.0);
        Ok(())
    }

    #[test]
    fn shadow_rug_loss_rate_recent_keeps_zero_entry_rows_in_sample_size() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-rug-rate-denominator.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                "sig-rug",
                "wallet",
                "token",
                10.0_f64,
                0.10_f64,
                200_000_000_i64,
                0.05_f64,
                50_000_000_i64,
                -0.05_f64,
                -150_000_000_i64,
                opened_ts.to_rfc3339(),
                closed_ts.to_rfc3339()
            ],
        )?;
        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                "sig-zero-entry",
                "wallet",
                "token",
                1.0_f64,
                0.0_f64,
                0_i64,
                0.01_f64,
                10_000_000_i64,
                0.01_f64,
                10_000_000_i64,
                opened_ts.to_rfc3339(),
                (closed_ts + Duration::minutes(1)).to_rfc3339()
            ],
        )?;

        let (rug_count, total_count, rug_rate) =
            store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
        assert_eq!(
            rug_count, 1,
            "only the positive-entry trade should count as rug"
        );
        assert_eq!(
            total_count, 2,
            "zero-entry trades should still remain in the recent sample size"
        );
        assert!(
            (rug_rate - 0.5).abs() < 1e-12,
            "expected denominator to include both sampled rows, got {rug_rate}"
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
    fn copy_signal_roundtrip_preserves_exact_notional_lamports() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("copy-signal-exact-notional.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-exact:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: Some(Lamports::new(250_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
            ts: now,
            status: "shadow_recorded".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);

        let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].notional_lamports,
            Some(Lamports::new(250_000_000))
        );
        let origin: String = store.conn.query_row(
            "SELECT notional_origin FROM copy_signals WHERE signal_id = ?1",
            params![signal.signal_id],
            |row| row.get(0),
        )?;
        assert_eq!(origin, "leader_exact_lamports");
        Ok(())
    }

    #[test]
    fn copy_signal_approximate_origin_preserves_approximate_notional_sidecar_on_read() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("copy-signal-approx-origin.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:30:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO copy_signals(
                signal_id, wallet_id, side, token, notional_sol, notional_lamports, notional_origin, ts, status
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "shadow:sig-approx:wallet:buy:token-a",
                "wallet-1",
                "buy",
                "token-a",
                0.25_f64,
                250_000_000_i64,
                "leader_approximate",
                now.to_rfc3339(),
                "shadow_recorded",
            ],
        )?;

        let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].notional_lamports,
            Some(Lamports::new(250_000_000))
        );
        assert_eq!(
            signals[0].notional_origin,
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
        );
        Ok(())
    }

    #[test]
    fn insert_copy_signal_rejects_exact_origin_without_notional_lamports() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("copy-signal-missing-exact-notional.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:45:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let err = store
            .insert_copy_signal(&CopySignalRow {
                signal_id: "shadow:sig-missing-exact:wallet:buy:token-a".to_string(),
                wallet_id: "wallet-1".to_string(),
                side: "buy".to_string(),
                token: "token-a".to_string(),
                notional_sol: 0.25,
                notional_lamports: None,
                notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
                ts: now,
                status: "shadow_recorded".to_string(),
            })
            .expect_err("exact origin without lamport mirror must fail closed");
        assert!(
            err.to_string().contains("missing notional_lamports"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn insert_copy_signal_rejects_zero_notional_lamports() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("copy-signal-zero-notional.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:50:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let err = store
            .insert_copy_signal(&CopySignalRow {
                signal_id: "shadow:sig-zero-notional:wallet:buy:token-a".to_string(),
                wallet_id: "wallet-1".to_string(),
                side: "buy".to_string(),
                token: "token-a".to_string(),
                notional_sol: 0.25,
                notional_lamports: Some(Lamports::ZERO),
                notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                ts: now,
                status: "shadow_recorded".to_string(),
            })
            .expect_err("zero lamport mirror must fail closed");
        assert!(
            err.to_string().contains("zero notional_lamports"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn latest_active_buy_order_prefers_buy_side_active_statuses_and_respects_exclusion(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("latest-active-buy-order.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T13:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let older_buy = CopySignalRow {
            signal_id: "shadow:cooldown:older:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.10,
            notional_lamports: Some(Lamports::new(100_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now,
            status: "execution_pending".to_string(),
        };
        assert!(store.insert_copy_signal(&older_buy)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-cooldown-older-buy",
                &older_buy.signal_id,
                "cb_cooldown_older_buy_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let failed_buy_ts = now + Duration::seconds(10);
        let failed_buy = CopySignalRow {
            signal_id: "shadow:cooldown:failed:wallet:buy:token-b".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-b".to_string(),
            notional_sol: 0.20,
            notional_lamports: Some(Lamports::new(200_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: failed_buy_ts,
            status: "execution_pending".to_string(),
        };
        assert!(store.insert_copy_signal(&failed_buy)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-cooldown-failed-buy",
                &failed_buy.signal_id,
                "cb_cooldown_failed_buy_a1",
                "paper",
                failed_buy_ts,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_failed(
            "ord-cooldown-failed-buy",
            "test_failed_order",
            Some("ignore in latest active buy query"),
        )?;
        assert!(store.update_copy_signal_status(&failed_buy.signal_id, "execution_failed")?);

        let sell_ts = now + Duration::seconds(20);
        let sell_signal = CopySignalRow {
            signal_id: "shadow:cooldown:sell:wallet:sell:token-c".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "sell".to_string(),
            token: "token-c".to_string(),
            notional_sol: 0.15,
            notional_lamports: Some(Lamports::new(150_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: sell_ts,
            status: "execution_pending".to_string(),
        };
        assert!(store.insert_copy_signal(&sell_signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-cooldown-sell",
                &sell_signal.signal_id,
                "cb_cooldown_sell_a1",
                "paper",
                sell_ts,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let latest_buy_ts = now + Duration::seconds(30);
        let latest_buy = CopySignalRow {
            signal_id: "shadow:cooldown:latest:wallet:buy:token-d".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-d".to_string(),
            notional_sol: 0.25,
            notional_lamports: Some(Lamports::new(250_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: latest_buy_ts,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&latest_buy)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-cooldown-latest-buy",
                &latest_buy.signal_id,
                "cb_cooldown_latest_buy_a1",
                "paper",
                latest_buy_ts,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-cooldown-latest-buy",
            "paper",
            "sig-cooldown-latest-buy",
            latest_buy_ts,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_confirmed(
            "ord-cooldown-latest-buy",
            latest_buy_ts + Duration::seconds(1),
        )?;
        assert!(store.update_copy_signal_status(&latest_buy.signal_id, "execution_confirmed")?);

        let latest = store
            .latest_active_buy_order(None)?
            .context("latest active buy order should exist")?;
        assert_eq!(latest.signal_id, latest_buy.signal_id);
        assert_eq!(latest.status, "execution_confirmed");

        let previous = store
            .latest_active_buy_order(Some(latest_buy.signal_id.as_str()))?
            .context("excluding latest active buy should return older active buy")?;
        assert_eq!(previous.signal_id, older_buy.signal_id);
        assert_eq!(previous.status, "execution_pending");

        Ok(())
    }

    #[test]
    fn finalize_execution_confirmed_order_exact_persists_execution_lamport_sidecars() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-exact-lamports.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-exact:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.10,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now,
            status: "execution_submitted".to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-exact-1",
                &signal.signal_id,
                "cb_exact_1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-exact-1",
            "paper",
            "paper:tx-exact",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

        let outcome = store.finalize_execution_confirmed_order_exact(
            "ord-exact-1",
            &signal.signal_id,
            "token-a",
            "buy",
            2.0,
            Some(TokenQuantity::new(2_000_000, 6)),
            0.10,
            Lamports::new(100_000_000),
            0.05,
            0.000005,
            Lamports::new(5_000),
            50.0,
            now + Duration::seconds(1),
        )?;
        assert!(matches!(
            outcome,
            FinalizeExecutionConfirmOutcome::Applied(_)
        ));

        let fill_row: (i64, i64, String, i64) = store.conn.query_row(
            "SELECT notional_lamports, fee_lamports, qty_raw, qty_decimals
             FROM fills
             WHERE order_id = ?1",
            params!["ord-exact-1"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(fill_row, (100_000_000, 5_000, "2000000".to_string(), 6));

        let position_row: (i64, f64, String, i64) = store.conn.query_row(
            "SELECT cost_lamports, cost_sol, qty_raw, qty_decimals
             FROM positions
             WHERE token = ?1
               AND state = 'open'",
            params!["token-a"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(position_row.0, 100_005_000);
        assert!((position_row.1 - 0.100005).abs() < 1e-9);
        assert_eq!(position_row.2, "2000000");
        assert_eq!(position_row.3, 6);
        assert_eq!(
            store.live_open_exposure_lamports()?,
            Lamports::new(100_005_000)
        );

        Ok(())
    }

    #[test]
    fn apply_execution_fill_to_positions_exact_preserves_and_drops_qty_sidecars_conservatively(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-exact-qty-sidecars.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions_exact(
            "token-exact-qty",
            "buy",
            2.0,
            Some(TokenQuantity::new(2_000_000, 6)),
            0.20,
            now,
        )?;
        store.apply_execution_fill_to_positions_exact(
            "token-exact-qty",
            "sell",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.06,
            now + Duration::seconds(1),
        )?;

        let row: (f64, String, i64) = store.conn.query_row(
            "SELECT qty, qty_raw, qty_decimals
             FROM positions
             WHERE token = ?1
               AND state = 'open'",
            params!["token-exact-qty"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert!((row.0 - 1.5).abs() < 1e-9);
        assert_eq!(row.1, "1500000");
        assert_eq!(row.2, 6);

        store.apply_execution_fill_to_positions("token-exact-qty", "buy", 1.0, 0.10, now)?;
        let rows: Vec<(String, f64, Option<String>, Option<i64>)> = {
            let mut stmt = store.conn.prepare(
                "SELECT accounting_bucket, qty, qty_raw, qty_decimals
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                 ORDER BY accounting_bucket ASC",
            )?;
            let mapped = stmt.query_map(params!["token-exact-qty"], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
            mapped.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(rows.len(), 2, "legacy and exact buckets must stay separate");
        assert_eq!(
            rows[0],
            (
                POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string(),
                1.5_f64,
                Some("1500000".to_string()),
                Some(6_i64)
            )
        );
        assert_eq!(
            rows[1],
            (
                POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER.to_string(),
                1.0_f64,
                None,
                None
            )
        );
        assert_eq!(store.live_open_positions_count()?, 1);
        let aggregate = store
            .live_open_position_qty_cost("token-exact-qty")?
            .expect("aggregated open position exists");
        assert!((aggregate.0 - 2.5).abs() < 1e-9);
        assert!(
            (aggregate.1 - 0.250000001).abs() < 1e-9,
            "aggregated cost should sum buckets, got {}",
            aggregate.1
        );

        Ok(())
    }

    #[test]
    fn apply_execution_fill_to_positions_sell_spans_buckets_fifo() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-position-bucket-fifo.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions("token-bucket-fifo", "buy", 1.0, 0.10, now)?;
        store.apply_execution_fill_to_positions_exact(
            "token-bucket-fifo",
            "buy",
            2.0,
            Some(TokenQuantity::new(2_000_000, 6)),
            0.20,
            now + Duration::seconds(1),
        )?;
        store.apply_execution_fill_to_positions_exact(
            "token-bucket-fifo",
            "sell",
            1.5,
            Some(TokenQuantity::new(1_500_000, 6)),
            0.18,
            now + Duration::seconds(2),
        )?;

        let rows: Vec<(String, String, f64, Option<String>, Option<i64>)> = {
            let mut stmt = store.conn.prepare(
                "SELECT accounting_bucket, state, qty, qty_raw, qty_decimals
                 FROM positions
                 WHERE token = ?1
                 ORDER BY opened_ts ASC, rowid ASC",
            )?;
            let mapped = stmt.query_map(params!["token-bucket-fifo"], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?;
            mapped.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            (
                POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER.to_string(),
                "closed".to_string(),
                0.0_f64,
                None,
                None
            )
        );
        assert_eq!(
            rows[1],
            (
                POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string(),
                "open".to_string(),
                1.5_f64,
                Some("1500000".to_string()),
                Some(6_i64)
            )
        );
        assert_eq!(store.live_open_positions_count()?, 1);
        let aggregate = store
            .live_open_position_qty_cost("token-bucket-fifo")?
            .expect("aggregated open position exists");
        assert!((aggregate.0 - 1.5).abs() < 1e-9);

        Ok(())
    }

    #[test]
    fn apply_execution_fill_to_positions_exact_drops_qty_sidecar_on_sell_underflow() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-exact-qty-underflow.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions_exact(
            "token-exact-qty-underflow",
            "buy",
            2.0,
            Some(TokenQuantity::new(2_000_000, 6)),
            0.20,
            now,
        )?;

        store.apply_execution_fill_to_positions_exact(
            "token-exact-qty-underflow",
            "sell",
            1.0,
            Some(TokenQuantity::new(3_000_000, 6)),
            0.12,
            now + Duration::seconds(1),
        )?;

        let row: (f64, Option<String>, Option<i64>) = store.conn.query_row(
            "SELECT qty, qty_raw, qty_decimals
             FROM positions
             WHERE token = ?1
               AND state = 'open'",
            params!["token-exact-qty-underflow"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert!((row.0 - 1.0).abs() < 1e-9);
        assert_eq!(row.1, None);
        assert_eq!(row.2, None);

        Ok(())
    }

    #[test]
    fn live_position_queries_ignore_dust_open_row() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-dust-open-row.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, state, pnl_sol)
             VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0.0)",
            params![
                "live-dust",
                "token-dust",
                LIVE_POSITION_OPEN_EPS / 10.0,
                0.15_f64,
                now.to_rfc3339(),
            ],
        )?;

        assert!(!store.live_has_open_position("token-dust")?);
        assert_eq!(store.live_open_positions_count()?, 0);
        assert_eq!(store.live_open_exposure_sol()?, 0.0);
        assert_eq!(store.live_open_exposure_sol_for_token("token-dust")?, 0.0);
        assert_eq!(store.live_open_position_qty_cost("token-dust")?, None);

        let snapshot =
            SqliteStore::live_execution_state_snapshot_on_conn(&store.conn, "token-dust")?;
        assert_eq!(snapshot.open_positions, 0);
        assert_eq!(snapshot.total_exposure_lamports, Lamports::ZERO);
        assert_eq!(snapshot.total_exposure_sol, 0.0);
        assert_eq!(snapshot.token_exposure_lamports, Lamports::ZERO);
        assert_eq!(snapshot.token_exposure_sol, 0.0);
        assert_eq!(snapshot.token_exposure_sol, 0.0);

        let (unrealized_pnl_sol, missing_price_count) = store.live_unrealized_pnl_sol(now)?;
        assert_eq!(unrealized_pnl_sol, 0.0);
        assert_eq!(missing_price_count, 0);
        Ok(())
    }

    #[test]
    fn live_exposure_queries_prefer_cost_lamports_sidecar() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-cost-lamports-sidecar.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, cost_lamports, opened_ts, state, pnl_sol)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', 0.0)",
            params![
                "live-sidecar",
                "token-sidecar",
                1.0_f64,
                0.1_f64,
                100_000_123_i64,
                now.to_rfc3339(),
            ],
        )?;

        assert_eq!(
            store.live_open_exposure_lamports_for_token("token-sidecar")?,
            Lamports::new(100_000_123)
        );
        let (_, cost_sol) = store
            .live_open_position_qty_cost("token-sidecar")?
            .expect("open position exists");
        assert!(
            (cost_sol - 0.100000123).abs() < 1e-12,
            "expected lamport-sidecar-derived cost, got {cost_sol}"
        );

        Ok(())
    }

    #[test]
    fn live_pnl_queries_prefer_pnl_lamports_sidecar() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-pnl-lamports-sidecar.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, pnl_lamports, state
             ) VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, ?6, 'closed')",
            params![
                "live-pnl-positive",
                "token-pnl-a",
                (now - Duration::minutes(2)).to_rfc3339(),
                (now - Duration::minutes(1)).to_rfc3339(),
                0.10_f64,
                200_000_000_i64,
            ],
        )?;
        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, pnl_lamports, state
             ) VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, ?6, 'closed')",
            params![
                "live-pnl-negative",
                "token-pnl-b",
                (now - Duration::minutes(1)).to_rfc3339(),
                now.to_rfc3339(),
                -0.05_f64,
                -300_000_000_i64,
            ],
        )?;

        let (trades, realized_pnl) = store.live_realized_pnl_since(now - Duration::hours(1))?;
        assert_eq!(trades, 2);
        assert!(
            (realized_pnl + 0.10).abs() < 1e-12,
            "expected realized pnl to prefer lamport sidecars, got {realized_pnl}"
        );

        let drawdown = store.live_max_drawdown_since(now - Duration::hours(1))?;
        assert!(
            (drawdown - 0.30).abs() < 1e-12,
            "expected drawdown to prefer lamport sidecars, got {drawdown}"
        );

        Ok(())
    }

    #[test]
    fn live_drawdown_with_unrealized_prefers_pnl_lamports_sidecar() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("live-drawdown-unrealized-lamports-sidecar.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::hours(1);

        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, pnl_lamports, state
             ) VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, ?6, 'closed')",
            params![
                "live-drawdown-pos",
                "token-drawdown-a",
                (now - Duration::minutes(10)).to_rfc3339(),
                (now - Duration::minutes(9)).to_rfc3339(),
                0.10_f64,
                200_000_000_i64,
            ],
        )?;
        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, pnl_lamports, state
             ) VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, ?6, 'closed')",
            params![
                "live-drawdown-neg",
                "token-drawdown-b",
                (now - Duration::minutes(8)).to_rfc3339(),
                (now - Duration::minutes(7)).to_rfc3339(),
                -0.05_f64,
                -300_000_000_i64,
            ],
        )?;

        let drawdown = store.live_max_drawdown_with_unrealized_since(window_start, -0.15_f64)?;
        assert!(
            (drawdown - 0.45).abs() < 1e-12,
            "expected drawdown with unrealized to prefer lamport sidecars, got {drawdown}"
        );

        Ok(())
    }

    #[test]
    fn live_pnl_queries_tolerate_legacy_null_pnl_sol() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-null-pnl-sol-legacy.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let window_start = now - Duration::hours(1);

        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, pnl_lamports, state
             ) VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, ?6, 'closed')",
            params![
                "live-null-sidecar",
                "token-null-sidecar",
                (now - Duration::minutes(10)).to_rfc3339(),
                (now - Duration::minutes(9)).to_rfc3339(),
                Option::<f64>::None,
                Some(50_000_000_i64),
            ],
        )?;
        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, pnl_lamports, state
             ) VALUES (?1, ?2, 0.0, 0.0, ?3, ?4, ?5, ?6, 'closed')",
            params![
                "live-null-legacy",
                "token-null-legacy",
                (now - Duration::minutes(8)).to_rfc3339(),
                (now - Duration::minutes(7)).to_rfc3339(),
                Option::<f64>::None,
                Option::<i64>::None,
            ],
        )?;

        let (trades, realized_pnl) = store.live_realized_pnl_since(window_start)?;
        assert_eq!(trades, 2);
        assert!(
            (realized_pnl - 0.05).abs() < 1e-12,
            "expected NULL pnl_sol rows to fall back cleanly, got {realized_pnl}"
        );

        let drawdown = store.live_max_drawdown_since(window_start)?;
        assert!(
            drawdown.abs() < 1e-12,
            "expected no drawdown from +0.05 then 0.0 legacy row, got {drawdown}"
        );

        let drawdown_with_unrealized =
            store.live_max_drawdown_with_unrealized_since(window_start, 0.0)?;
        assert!(
            drawdown_with_unrealized.abs() < 1e-12,
            "expected NULL pnl_sol rows to remain compatible in drawdown-with-unrealized, got {drawdown_with_unrealized}"
        );

        Ok(())
    }

    #[test]
    fn apply_execution_fill_closes_live_position_when_residual_qty_is_dust() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-dust-residual-close.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.apply_execution_fill_to_positions("token-dust-close", "buy", 1.0, 0.25, now)?;
        let residual_qty = LIVE_POSITION_OPEN_EPS / 2.0;
        let sell_qty = 1.0 - residual_qty;
        store.apply_execution_fill_to_positions(
            "token-dust-close",
            "sell",
            sell_qty,
            0.30 * sell_qty,
            now + Duration::seconds(1),
        )?;

        assert!(!store.live_has_open_position("token-dust-close")?);
        assert_eq!(store.live_open_positions_count()?, 0);
        assert_eq!(store.live_open_exposure_sol()?, 0.0);
        assert_eq!(store.live_open_position_qty_cost("token-dust-close")?, None);

        let row: (f64, f64, String) = store.conn.query_row(
            "SELECT qty, cost_sol, state
             FROM positions
             WHERE token = ?1
             LIMIT 1",
            params!["token-dust-close"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(row.2, "closed");
        assert_eq!(row.0, 0.0);
        assert_eq!(row.1, 0.0);

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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
        assert_eq!(snapshot.total_exposure_lamports, Lamports::new(250_000_000));
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
    fn finalize_execution_confirmed_order_accepts_confirmed_reconcile_pending_status() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("execution-confirm-confirmed-reconcile-pending.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-confirmed-reconcile:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now,
            status: EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS.to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);

        let order_id = "ord-confirmed-reconcile-pending-1";
        let client_order_id = "cb_confirmed_reconcile_pending_a1";
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
            "paper:tx-confirmed-reconcile-pending",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_confirmed_reconcile_pending(
            order_id,
            "confirm_observed_fill_unavailable_manual_reconcile_required",
            now + Duration::seconds(1),
        )?;

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
            now + Duration::seconds(2),
        )?;
        let FinalizeExecutionConfirmOutcome::Applied(snapshot) = outcome else {
            panic!("expected applied outcome, got {:?}", outcome);
        };
        assert!((snapshot.total_exposure_sol - 0.25).abs() < 1e-9);
        assert_eq!(snapshot.open_positions, 1);

        let order = store
            .execution_order_by_client_order_id(client_order_id)?
            .context("expected order row after confirmed reconcile finalize")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(order.err_code, None);
        assert_eq!(order.confirm_ts, Some(now + Duration::seconds(2)));

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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
    fn mark_order_confirmed_accepts_confirmed_reconcile_pending_status() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("mark-order-confirmed-confirmed-reconcile-pending.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-direct-confirmed-reconcile:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now,
            status: EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS.to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-direct-confirmed-reconcile-1",
                &signal.signal_id,
                "cb_direct_confirmed_reconcile_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-direct-confirmed-reconcile-1",
            "paper",
            "paper:tx-direct-confirmed-reconcile",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_confirmed_reconcile_pending(
            "ord-direct-confirmed-reconcile-1",
            "confirm_observed_fill_unavailable_manual_reconcile_required",
            now + Duration::seconds(1),
        )?;

        store.mark_order_confirmed(
            "ord-direct-confirmed-reconcile-1",
            now + Duration::seconds(2),
        )?;

        let order = store
            .execution_order_by_client_order_id("cb_direct_confirmed_reconcile_a1")?
            .context("expected order row after direct confirmed-reconcile helper")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(
            order.err_code.as_deref(),
            Some("confirm_observed_fill_unavailable_manual_reconcile_required")
        );
        assert_eq!(order.confirm_ts, Some(now + Duration::seconds(2)));

        Ok(())
    }

    #[test]
    fn mark_order_reconcile_pending_rejects_downgrade_from_confirmed_reconcile_pending(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("mark-order-reconcile-pending-downgrade-rejected.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-02-19T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let signal = CopySignalRow {
            signal_id: "shadow:sig-reconcile-downgrade-guard:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now,
            status: EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS.to_string(),
        };
        assert!(store.insert_copy_signal(&signal)?);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-reconcile-downgrade-guard-1",
                &signal.signal_id,
                "cb_reconcile_downgrade_guard_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-reconcile-downgrade-guard-1",
            "paper",
            "paper:tx-reconcile-downgrade-guard",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;
        store.mark_order_confirmed_reconcile_pending(
            "ord-reconcile-downgrade-guard-1",
            "confirm_observed_fill_unavailable_manual_reconcile_required",
            now + Duration::seconds(1),
        )?;

        let error = store
            .mark_order_reconcile_pending(
                "ord-reconcile-downgrade-guard-1",
                "confirm_timeout_manual_reconcile_required",
            )
            .expect_err("confirmed reconcile surface must not downgrade back to submitted");
        assert!(
            error
                .to_string()
                .contains("unexpected status=execution_confirmed_reconcile_pending"),
            "unexpected error: {error}"
        );

        let order = store
            .execution_order_by_client_order_id("cb_reconcile_downgrade_guard_a1")?
            .context("expected order row after rejected downgrade attempt")?;
        assert_eq!(order.status, EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS);
        assert_eq!(
            order.err_code.as_deref(),
            Some("confirm_observed_fill_unavailable_manual_reconcile_required")
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
        assert_eq!(
            buy_snapshot.total_exposure_lamports,
            Lamports::new(210_000_000)
        );
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
                true,
                true,
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
    fn persist_discovery_cycle_retention_keeps_cold_start_windows() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("discovery-wallet-metrics-cold-start-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let wallet_id = "wallet-cold-start".to_string();
        let base = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for offset_minutes in 0..2 {
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
                true,
                true,
                window_start,
                "cold-start-retention-test",
            )?;
        }

        let mut stmt = store.conn.prepare(
            "SELECT DISTINCT window_start FROM wallet_metrics ORDER BY window_start ASC",
        )?;
        let windows: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;

        assert_eq!(
            windows,
            vec![
                base.to_rfc3339(),
                (base + Duration::minutes(1)).to_rfc3339(),
            ],
            "retention must not delete cold-start metric windows before the threshold is reached"
        );
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_skips_metric_retention_when_metric_batch_is_empty() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-wallet-metrics-empty-batch.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let wallet_id = "wallet-empty-batch".to_string();
        let window_start = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: window_start,
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
            true,
            true,
            window_start,
            "seed-metrics",
        )?;
        let latest_before = store
            .latest_wallet_metrics_window_start()?
            .expect("expected wallet_metrics window after initial persist");

        let empty_follow_delta = store.persist_discovery_cycle(
            &wallets,
            &[],
            &desired,
            true,
            true,
            window_start + Duration::minutes(10),
            "skip-metrics",
        )?;
        assert_eq!(empty_follow_delta.activated, 0);
        assert_eq!(empty_follow_delta.deactivated, 0);

        let latest_after = store
            .latest_wallet_metrics_window_start()?
            .expect("wallet_metrics window should survive empty batch");
        assert_eq!(latest_after, latest_before);
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_can_suppress_followlist_deactivations() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("discovery-followlist-deactivation-suppression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet-keep-active".to_string();
        store.activate_follow_wallet(&wallet_id, now, "seed-follow")?;
        assert!(store.list_active_follow_wallets()?.contains(&wallet_id));

        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: now,
            last_seen: now,
            status: "observed".to_string(),
        }];

        let suppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            &[],
            true,
            false,
            now + Duration::minutes(1),
            "suppressed-demotions",
        )?;
        assert_eq!(suppressed.activated, 0);
        assert_eq!(suppressed.deactivated, 0);
        assert!(
            store.list_active_follow_wallets()?.contains(&wallet_id),
            "active wallet must remain followed when deactivations are suppressed"
        );

        let unsuppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            &[],
            true,
            true,
            now + Duration::minutes(2),
            "allow-demotions",
        )?;
        assert_eq!(unsuppressed.activated, 0);
        assert_eq!(unsuppressed.deactivated, 1);
        assert!(
            !store.list_active_follow_wallets()?.contains(&wallet_id),
            "active wallet should deactivate again once suppression is lifted"
        );
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_can_suppress_followlist_activations() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("discovery-followlist-activation-suppression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet-dont-activate".to_string();
        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: now,
            last_seen: now,
            status: "candidate".to_string(),
        }];

        let suppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            std::slice::from_ref(&wallet_id),
            false,
            true,
            now + Duration::minutes(1),
            "suppressed-promotions",
        )?;
        assert_eq!(suppressed.activated, 0);
        assert_eq!(suppressed.deactivated, 0);
        assert!(
            !store.list_active_follow_wallets()?.contains(&wallet_id),
            "candidate wallet must stay inactive when followlist activations are suppressed"
        );

        let unsuppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            std::slice::from_ref(&wallet_id),
            true,
            true,
            now + Duration::minutes(2),
            "allow-promotions",
        )?;
        assert_eq!(unsuppressed.activated, 1);
        assert_eq!(unsuppressed.deactivated, 0);
        assert!(
            store.list_active_follow_wallets()?.contains(&wallet_id),
            "candidate wallet should activate once suppression is lifted"
        );
        Ok(())
    }

    #[test]
    fn wallet_activity_day_counts_since_returns_day_level_counts() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-days.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let rows = vec![
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 5).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-b".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T18:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
        ];
        store.upsert_wallet_activity_days(&rows)?;

        let counts = store.wallet_active_day_counts_since(
            &["wallet-a".to_string(), "wallet-b".to_string()],
            DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert_eq!(counts.get("wallet-a"), Some(&1));
        assert!(
            !counts.contains_key("wallet-b"),
            "same-day activity before exact window_start must not be counted"
        );
        Ok(())
    }

    #[test]
    fn backfill_wallet_activity_days_since_uses_existing_observed_swaps() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-backfill.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let window_start = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&SwapEvent {
            signature: "backfill-pre-window".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBackfill111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        })?;
        store.insert_observed_swap(&SwapEvent {
            signature: "backfill-boundary-window".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBackfill111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 2,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        })?;
        store.insert_observed_swap(&SwapEvent {
            signature: "backfill-later-day".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBackfill111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 3,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        })?;

        store.backfill_wallet_activity_days_since(window_start)?;

        let counts =
            store.wallet_active_day_counts_since(&["wallet-a".to_string()], window_start)?;
        assert_eq!(
            counts.get("wallet-a"),
            Some(&2),
            "backfill should use existing observed_swaps at or after the exact window_start"
        );
        Ok(())
    }

    #[test]
    fn discovery_scoring_coverage_marker_gates_window_readiness() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-scoring-coverage.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let window_start = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let max_lag = Duration::minutes(10);
        assert!(!store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "covered_since alone must not activate aggregate reads without a near-head watermark"
        );
        store.conn.execute(
            "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
             VALUES ('covered_through_ts', ?1, ?2)
             ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at",
            params![
                (now - Duration::minutes(5)).to_rfc3339(),
                Utc::now().to_rfc3339()
            ],
        )?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "timestamp-only covered_through state must not enable aggregate reads without the exact cursor"
        );

        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(5),
            slot: 42,
            signature: "covered-through-ready".to_string(),
        })?;
        assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "gap-row".to_string(),
        })?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "latched materialization gaps must block aggregate readiness even with near-head watermarks"
        );
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(30),
                slot: 0,
                signature: String::new(),
            },
        )?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "observing an earlier cursor must not clear the exact continuity blocker"
        );
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(15),
                slot: 100,
                signature: "gap-row".to_string(),
            },
        )?;
        assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "gap-row-b".to_string(),
        })?;
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(15),
                slot: 100,
                signature: "zzz-after-gap".to_string(),
            },
        )?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "observing a different row at the same timestamp must not clear the exact continuity blocker"
        );
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(15),
                slot: 100,
                signature: "gap-row-b".to_string(),
            },
        )?;
        assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_covered_since(window_start + Duration::hours(1))?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "coverage marker later than window_start must not enable aggregate reads yet"
        );

        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        let later_now = now + Duration::hours(3);
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, later_now, max_lag)?,
            "stale covered_through watermark must keep aggregate reads disabled"
        );
        Ok(())
    }

    #[test]
    fn apply_discovery_scoring_batch_records_fifo_buy_and_close_facts() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-scoring-batch.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let buy_one = SwapEvent {
            signature: "scoring-buy-1".to_string(),
            wallet: "wallet-scoring".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenScoring11111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let buy_two = SwapEvent {
            signature: "scoring-buy-2".to_string(),
            wallet: "wallet-scoring".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenScoring11111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 50.0,
            exact_amounts: None,
            slot: 2,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let sell = SwapEvent {
            signature: "scoring-sell-1".to_string(),
            wallet: "wallet-scoring".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenScoring11111111111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 120.0,
            amount_out: 3.0,
            exact_amounts: None,
            slot: 3,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T11:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let swaps = vec![buy_one.clone(), buy_two.clone(), sell.clone()];
        store.insert_observed_swaps_batch(&swaps)?;

        store.apply_discovery_scoring_batch(
            &swaps,
            &DiscoveryAggregateWriteConfig {
                max_tx_per_minute: 50,
                rug_lookahead_seconds: 60,
                helius_http_url: None,
                min_token_age_hint_seconds: None,
            },
        )?;

        let days = store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].trades, 3);
        assert!((days[0].spent_sol - 2.0).abs() < 1e-9);
        assert!((days[0].max_buy_notional_sol - 1.0).abs() < 1e-9);

        let buy_facts = store.load_wallet_scoring_buy_facts_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert_eq!(buy_facts.len(), 2);

        let close_facts = store.load_wallet_scoring_close_facts_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert_eq!(close_facts.len(), 2, "sell should close two FIFO segments");
        let total_pnl: f64 = close_facts.iter().map(|row| row.pnl_sol).sum();
        assert!(
            (total_pnl - 1.6).abs() < 1e-9,
            "expected FIFO pnl split across close facts"
        );

        let (remaining_qty, remaining_cost): (f64, f64) = store.conn.query_row(
            "SELECT qty, cost_sol
             FROM wallet_scoring_open_lots
             WHERE wallet_id = 'wallet-scoring'
               AND token = 'TokenScoring11111111111111111111111111111'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((remaining_qty - 30.0).abs() < 1e-9);
        assert!((remaining_cost - 0.6).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn prune_discovery_scoring_keeps_old_open_lots_for_late_sell_accounting() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-scoring-open-lot-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let buy = SwapEvent {
            signature: "carryover-buy".to_string(),
            wallet: "wallet-carryover".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenCarryover111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[buy.clone()])?;
        store.apply_discovery_scoring_batch(&[buy], &DiscoveryAggregateWriteConfig::default())?;

        let cutoff = DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.prune_discovery_scoring_before(cutoff)?;

        let open_lot_count: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM wallet_scoring_open_lots", [], |row| {
                    row.get(0)
                })?;
        assert_eq!(
            open_lot_count, 1,
            "still-open scoring inventory must survive retention prune until consumed"
        );

        let carryover_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(carryover_count, 0);

        let sell = SwapEvent {
            signature: "carryover-sell".to_string(),
            wallet: "wallet-carryover".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenCarryover111111111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 100.0,
            amount_out: 2.0,
            exact_amounts: None,
            slot: 2,
            ts_utc: DateTime::parse_from_rfc3339("2026-02-15T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[sell.clone()])?;
        store.apply_discovery_scoring_batch(&[sell], &DiscoveryAggregateWriteConfig::default())?;

        let close_fact_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_close_facts
             WHERE sell_signature = 'carryover-sell'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(close_fact_count, 1);
        let (pnl_sol, hold_seconds): (f64, i64) = store.conn.query_row(
            "SELECT pnl_sol, hold_seconds
             FROM wallet_scoring_close_facts
             WHERE sell_signature = 'carryover-sell'
               AND segment_index = 0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((pnl_sol - 1.0).abs() < 1e-9);
        assert!(hold_seconds > 0);

        let remaining_open_lots: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_open_lots
             WHERE wallet_id = 'wallet-carryover'
               AND token = 'TokenCarryover111111111111111111111111111'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(remaining_open_lots, 0);
        Ok(())
    }

    #[test]
    fn observed_swap_batch_with_activity_days_is_atomic_on_activity_upsert_failure() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-activity-atomic.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        store.conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced wallet activity day failure');
             END;",
        )?;

        let swap = SwapEvent {
            signature: "atomic-activity-fail".to_string(),
            wallet: "wallet-atomic".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenAtomic11111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };

        let error = store
            .insert_observed_swaps_batch_with_activity_days(&[swap.clone()])
            .expect_err("wallet_activity_days failure should abort the whole batch");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("forced wallet activity day failure"),
            "unexpected atomic batch error: {error_chain}"
        );

        let swaps = store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-08T11:59:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert!(
            swaps.is_empty(),
            "observed_swaps insert must roll back when wallet_activity_days upsert fails"
        );
        let counts = store.wallet_active_day_counts_since(
            &["wallet-atomic".to_string()],
            DateTime::parse_from_rfc3339("2026-03-08T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert!(
            counts.is_empty(),
            "wallet_activity_days must also remain empty"
        );
        Ok(())
    }

    #[test]
    fn wallet_metrics_window_start_index_migration_is_present() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-metrics-window-start-index.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(&legacy_migrations, "0020_execution_foreign_keys.sql")?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        let index_sql: Option<String> = migrated_store
            .conn
            .query_row(
                "SELECT sql
                 FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_wallet_metrics_window_start'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        assert!(
            index_sql.is_some(),
            "wallet_metrics(window_start) hotfix index must exist after migration"
        );
        Ok(())
    }

    #[test]
    fn observed_swap_cursor_is_strictly_lexicographic() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-cursor-lexicographic.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let base = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let swaps = [
            SwapEvent {
                signature: "sig-a".to_string(),
                wallet: "wallet-1".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-a".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                slot: 100,
                ts_utc: base,
                exact_amounts: None,
            },
            SwapEvent {
                signature: "sig-b".to_string(),
                wallet: "wallet-1".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-b".to_string(),
                amount_in: 1.1,
                amount_out: 11.0,
                slot: 100,
                ts_utc: base,
                exact_amounts: None,
            },
            SwapEvent {
                signature: "sig-c".to_string(),
                wallet: "wallet-1".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-c".to_string(),
                amount_in: 1.2,
                amount_out: 12.0,
                slot: 101,
                ts_utc: base,
                exact_amounts: None,
            },
            SwapEvent {
                signature: "sig-d".to_string(),
                wallet: "wallet-1".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-d".to_string(),
                amount_in: 1.3,
                amount_out: 13.0,
                slot: 1,
                ts_utc: base + Duration::seconds(1),
                exact_amounts: None,
            },
        ];
        for swap in &swaps {
            assert!(store.insert_observed_swap(swap)?);
        }

        let mut seen = Vec::new();
        let count = store.for_each_observed_swap_after_cursor(base, 100, "sig-a", 10, |swap| {
            seen.push((swap.signature, swap.slot, swap.ts_utc));
            Ok(())
        })?;

        assert_eq!(count, 3);
        assert_eq!(
            seen,
            vec![
                ("sig-b".to_string(), 100, base),
                ("sig-c".to_string(), 101, base),
                ("sig-d".to_string(), 1, base + Duration::seconds(1)),
            ]
        );
        Ok(())
    }

    #[test]
    fn observed_swap_cursor_query_respects_expired_deadline() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-expired-deadline.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        assert!(store.insert_observed_swap(&SwapEvent {
            signature: "sig-deadline".to_string(),
            wallet: "wallet-deadline".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-deadline".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base,
            exact_amounts: None,
        })?);

        let page = store.for_each_observed_swap_after_cursor_with_budget(
            base - Duration::seconds(1),
            0,
            "",
            10,
            std::time::Instant::now(),
            |_swap| Ok(()),
        )?;
        assert_eq!(page.rows_seen, 0);
        assert!(page.time_budget_exhausted);
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
                true,
                true,
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
    fn positions_accounting_bucket_migration_allows_exact_bucket_beside_legacy_row() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("positions-accounting-bucket.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(&legacy_migrations, "0032_copy_signals_notional_origin.sql")?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        legacy_store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, state, pnl_sol)
             VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0.0)",
            params![
                "pos-legacy",
                "token-bucket",
                1.0_f64,
                0.10_f64,
                "2026-03-08T12:00:00+00:00",
            ],
        )?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        migrated_store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports,
                accounting_bucket, opened_ts, state, pnl_sol, pnl_lamports
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'open', 0.0, 0)",
            params![
                "pos-exact",
                "token-bucket",
                2.0_f64,
                "2000000",
                6_i64,
                0.20_f64,
                200_000_000_i64,
                POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER,
                "2026-03-08T12:01:00+00:00",
            ],
        )?;

        let duplicate_legacy_insert = migrated_store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, cost_sol, accounting_bucket, opened_ts, state, pnl_sol
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', 0.0)",
            params![
                "pos-legacy-dup",
                "token-bucket",
                0.5_f64,
                0.05_f64,
                POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER,
                "2026-03-08T12:02:00+00:00",
            ],
        );
        assert!(
            duplicate_legacy_insert.is_err(),
            "unique partial index must still reject a second legacy open row"
        );
        assert_eq!(
            migrated_store.live_open_positions_count()?,
            1,
            "distinct-token open count should ignore bucket multiplicity"
        );
        let aggregate = migrated_store
            .live_open_position_qty_cost("token-bucket")?
            .expect("aggregated open position exists");
        assert!((aggregate.0 - 3.0).abs() < 1e-9);
        assert!((aggregate.1 - 0.30).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn shadow_accounting_bucket_migration_backfills_legacy_rows() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-accounting-bucket.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(&legacy_migrations, "0033_positions_accounting_bucket.sql")?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        legacy_store.conn.execute(
            "INSERT INTO shadow_lots(
                wallet_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "wallet",
                "token",
                1.0_f64,
                "1000000",
                6_i64,
                0.10_f64,
                100_000_000_i64,
                "2026-03-08T00:00:00+00:00",
            ],
        )?;
        legacy_store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, qty_raw, qty_decimals,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                "signal",
                "wallet",
                "token",
                0.5_f64,
                "500000",
                6_i64,
                0.05_f64,
                50_000_000_i64,
                0.08_f64,
                80_000_000_i64,
                0.03_f64,
                30_000_000_i64,
                "2026-03-08T00:00:00+00:00",
                "2026-03-08T00:05:00+00:00",
            ],
        )?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        let lot_bucket: String = migrated_store.conn.query_row(
            "SELECT accounting_bucket FROM shadow_lots LIMIT 1",
            [],
            |row| row.get(0),
        )?;
        let trade_bucket: String = migrated_store.conn.query_row(
            "SELECT accounting_bucket FROM shadow_closed_trades LIMIT 1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(lot_bucket, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER);
        assert_eq!(trade_bucket, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER);
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
                "ord-orphan",
                "token-orphan-chain",
                0.5_f64,
                0.20_f64,
                0.0_f64,
                0.0_f64,
            ],
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
            exact_amounts: None,
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
    fn live_unrealized_pnl_prefers_exact_qty_sidecar() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("live-unrealized-pnl-exact-qty.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts, state, pnl_sol, pnl_lamports
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'open', 0.0, 0)",
            params![
                "token-exact-qty",
                "token-exact-qty",
                1.0_f64,
                "2000000",
                6_i64,
                0.20_f64,
                200_000_000_i64,
                now.to_rfc3339(),
            ],
        )?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-exact-qty".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-live-unrealized-exact-qty".to_string(),
            slot: 12349,
            ts_utc: now + Duration::seconds(1),
            exact_amounts: None,
        })?;

        let (unrealized_pnl_sol, missing_price_count) =
            store.live_unrealized_pnl_sol(now + Duration::seconds(2))?;
        assert!(
            (unrealized_pnl_sol - 0.0).abs() < 1e-9,
            "expected exact qty sidecar to win over float qty drift, got {unrealized_pnl_sol}"
        );
        assert_eq!(missing_price_count, 0);

        let drawdown = store.live_max_drawdown_with_unrealized_since(
            now - Duration::hours(1),
            unrealized_pnl_sol,
        )?;
        assert!(
            drawdown.abs() < 1e-9,
            "expected unrealized drawdown to use exact qty sidecar, got {drawdown}"
        );

        Ok(())
    }

    #[test]
    fn live_unrealized_pnl_lamports_requires_exact_quote_boundary() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("live-unrealized-pnl-lamports-exact-quote.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts, state, pnl_sol, pnl_lamports
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'open', 0.0, 0)",
            params![
                "token-exact-only",
                "token-exact-only",
                2.0_f64,
                "2000000",
                6_i64,
                0.20_f64,
                200_000_000_i64,
                now.to_rfc3339(),
            ],
        )?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-exact-only".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-live-unrealized-float-only".to_string(),
            slot: 12350,
            ts_utc: now + Duration::seconds(1),
            exact_amounts: None,
        })?;

        let (unrealized_pnl_lamports, missing_price_count) =
            store.live_unrealized_pnl_lamports(now + Duration::seconds(2))?;
        assert_eq!(unrealized_pnl_lamports, SignedLamports::ZERO);
        assert_eq!(missing_price_count, 1);
        Ok(())
    }

    #[test]
    fn live_unrealized_pnl_lamports_uses_lower_median_exact_quote() -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("live-unrealized-pnl-lamports-lower-median.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO positions(
                position_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts, state, pnl_sol, pnl_lamports
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'open', 0.0, 0)",
            params![
                "token-lower-median",
                "token-lower-median",
                1.0_f64,
                "1000000",
                6_i64,
                0.15_f64,
                150_000_000_i64,
                now.to_rfc3339(),
            ],
        )?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-lower-median".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-live-unrealized-exact-low".to_string(),
            slot: 12351,
            ts_utc: now + Duration::seconds(1),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "10000000".to_string(),
                amount_out_decimals: 6,
            }),
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "token-lower-median".to_string(),
            amount_in: 3.0,
            amount_out: 10.0,
            signature: "sig-live-unrealized-exact-high".to_string(),
            slot: 12352,
            ts_utc: now + Duration::seconds(2),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "3000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "10000000".to_string(),
                amount_out_decimals: 6,
            }),
        })?;

        let (unrealized_pnl_lamports, missing_price_count) =
            store.live_unrealized_pnl_lamports(now + Duration::seconds(3))?;
        assert_eq!(unrealized_pnl_lamports, SignedLamports::new(-50_000_000));
        assert_eq!(missing_price_count, 0);

        let drawdown = store.live_max_drawdown_with_unrealized_lamports_since(
            now - Duration::hours(1),
            unrealized_pnl_lamports,
        )?;
        assert_eq!(drawdown, Lamports::new(50_000_000));
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
            exact_amounts: None,
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
            exact_amounts: None,
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
            exact_amounts: None,
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
            exact_amounts: None,
        })?;

        let (unrealized_pnl_sol, missing_price_count) =
            store.live_unrealized_pnl_sol(now + Duration::seconds(2))?;
        let drawdown_sol =
            store.live_max_drawdown_with_unrealized_since(window_start, unrealized_pnl_sol)?;
        assert!(
            (drawdown_sol - 0.300000001).abs() < 1e-12,
            "drawdown should include terminal open-position unrealized loss conservatively, got {drawdown_sol}"
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
                exact_amounts: None,
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
    fn insert_observed_swaps_batch_returns_insert_flags_in_order() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-batch-flags.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let swap_a = SwapEvent {
            wallet: "wallet-batch".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-batch-a".to_string(),
            slot: 100,
            ts_utc: now,
            exact_amounts: None,
        };
        let swap_b = SwapEvent {
            wallet: "wallet-batch".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-batch-b".to_string(),
            slot: 101,
            ts_utc: now + Duration::seconds(1),
            exact_amounts: None,
        };

        let inserted =
            store.insert_observed_swaps_batch(&[swap_a.clone(), swap_a.clone(), swap_b.clone()])?;
        assert_eq!(inserted, vec![true, false, true]);

        let swaps = store.load_observed_swaps_since(now - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 2);
        assert_eq!(swaps[0].signature, "sig-batch-a");
        assert_eq!(swaps[1].signature, "sig-batch-b");
        Ok(())
    }

    #[test]
    fn observed_swap_roundtrip_preserves_exact_amounts() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-exact-roundtrip.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let swap = SwapEvent {
            wallet: "wallet-exact".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-exact".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            signature: "sig-exact".to_string(),
            slot: 100,
            ts_utc: now,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "100000000".to_string(),
                amount_out_decimals: 6,
            }),
        };

        assert!(store.insert_observed_swap(&swap)?);
        let swaps = store.load_observed_swaps_since(now - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].exact_amounts, swap.exact_amounts);
        Ok(())
    }

    #[test]
    fn insert_observed_swaps_batch_retries_after_write_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-batch-retry.db");
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
            let inserted = worker_store.insert_observed_swaps_batch(&[
                SwapEvent {
                    wallet: "wallet-retry".to_string(),
                    dex: "raydium".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-retry-a".to_string(),
                    amount_in: 1.0,
                    amount_out: 10.0,
                    signature: "sig-observed-swap-batch-retry-a".to_string(),
                    slot: 999,
                    ts_utc: now,
                    exact_amounts: None,
                },
                SwapEvent {
                    wallet: "wallet-retry".to_string(),
                    dex: "raydium".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-retry-b".to_string(),
                    amount_in: 2.0,
                    amount_out: 20.0,
                    signature: "sig-observed-swap-batch-retry-b".to_string(),
                    slot: 1000,
                    ts_utc: now + Duration::seconds(1),
                    exact_amounts: None,
                },
            ])?;
            assert_eq!(inserted, vec![true, true]);
            Ok(())
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;
        handle
            .join()
            .expect("worker thread panicked")
            .context("worker batch insert should succeed after retry")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 2);
        Ok(())
    }

    #[test]
    fn delete_observed_swaps_before_applies_time_retention_cutoff() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let recent_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.insert_observed_swap(&SwapEvent {
            wallet: "wallet-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-stale".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-stale".to_string(),
            slot: 1,
            ts_utc: stale_ts,
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "wallet-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-recent".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-observed-swap-recent".to_string(),
            slot: 2,
            ts_utc: recent_ts,
            exact_amounts: None,
        })?;

        let deleted = store.delete_observed_swaps_before(recent_ts - Duration::days(1))?;
        assert_eq!(deleted, 1);

        let swaps = store.load_observed_swaps_since(stale_ts - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-recent");
        Ok(())
    }

    #[test]
    fn apply_history_retention_preserves_undelivered_warn_events_after_cursor() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for (event_id, event_type, severity, ts) in [
            ("info-old", "info_event", "info", stale_ts),
            ("warn-delivered", "warn_event", "warn", stale_ts),
            ("warn-pending", "warn_event", "warn", stale_ts),
            ("warn-fresh", "warn_event", "warn", fresh_ts),
        ] {
            store.conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, ?2, ?3, ?4, NULL)",
                params![event_id, event_type, severity, ts.to_rfc3339()],
            )?;
        }

        let delivered_rowid: i64 = store.conn.query_row(
            "SELECT rowid FROM risk_events WHERE event_id = 'warn-delivered'",
            [],
            |row| row.get(0),
        )?;
        store.upsert_alert_delivery_cursor("webhook", delivered_rowid)?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            true,
        )?;
        assert_eq!(summary.risk_events_deleted, 2);

        let mut stmt = store.conn.prepare(
            "SELECT event_id
             FROM risk_events
             ORDER BY rowid ASC",
        )?;
        let remaining = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(remaining, vec!["warn-pending", "warn-fresh"]);
        Ok(())
    }

    #[test]
    fn exact_money_cutover_state_round_trips_and_applies_activation_boundary() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("exact-money-cutover-state.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        assert_eq!(store.exact_money_cutover_ts()?, None);

        let cutover_ts = DateTime::parse_from_rfc3339("2026-03-10T08:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.upsert_exact_money_cutover_state(cutover_ts, Some("test-cutover"))?;

        assert_eq!(store.exact_money_cutover_ts()?, Some(cutover_ts));
        assert!(!store.exact_money_cutover_active_at(cutover_ts - Duration::seconds(1))?);
        assert!(store.exact_money_cutover_active_at(cutover_ts)?);
        assert!(store.exact_money_cutover_active_at(cutover_ts + Duration::seconds(1))?);
        Ok(())
    }

    #[test]
    fn apply_history_retention_preserves_warn_events_before_first_alert_delivery() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-retention-no-cursor.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for (event_id, severity, ts) in [
            ("info-old", "info", stale_ts),
            ("warn-old", "warn", stale_ts),
            ("error-old", "error", stale_ts),
            ("warn-fresh", "warn", fresh_ts),
        ] {
            store.conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, 'risk_event', ?2, ?3, NULL)",
                params![event_id, severity, ts.to_rfc3339()],
            )?;
        }

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            true,
        )?;
        assert_eq!(summary.risk_events_deleted, 1);

        let mut stmt = store.conn.prepare(
            "SELECT event_id
             FROM risk_events
             ORDER BY rowid ASC",
        )?;
        let remaining = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(remaining, vec!["warn-old", "error-old", "warn-fresh"]);
        Ok(())
    }

    #[test]
    fn latest_risk_event_by_type_returns_latest_row() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-latest.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let first_ts = DateTime::parse_from_rfc3339("2026-03-10T08:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let second_ts = DateTime::parse_from_rfc3339("2026-03-10T08:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let third_ts = DateTime::parse_from_rfc3339("2026-03-10T08:06:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_risk_event("shadow_risk_pause", "warn", first_ts, Some("{\"seq\":1}"))?;
        store.insert_risk_event("other_event", "warn", second_ts, Some("{\"seq\":2}"))?;
        store.insert_risk_event("shadow_risk_pause", "warn", third_ts, Some("{\"seq\":3}"))?;

        let latest = store
            .latest_risk_event_by_type("shadow_risk_pause")?
            .expect("latest event");
        assert_eq!(latest.event_type, "shadow_risk_pause");
        assert_eq!(latest.ts, third_ts.to_rfc3339());
        assert_eq!(latest.details_json.as_deref(), Some("{\"seq\":3}"));
        assert!(latest.rowid > 0);
        Ok(())
    }

    #[test]
    fn apply_history_retention_deletes_terminal_execution_history_child_first() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-history-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for signal in [
            ("sig-old-confirmed", "execution_confirmed", stale_ts),
            ("sig-old-pending", "execution_submitted", stale_ts),
            (
                "sig-old-submit-recent-confirmed",
                "execution_confirmed",
                stale_ts,
            ),
            ("sig-fresh-confirmed", "execution_confirmed", fresh_ts),
        ] {
            store.conn.execute(
                "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
                 VALUES (?1, 'wallet-1', 'buy', 'token-1', 0.5, ?2, ?3)",
                params![signal.0, signal.2.to_rfc3339(), signal.1],
            )?;
        }

        for order in [
            (
                "ord-old-confirmed",
                "sig-old-confirmed",
                stale_ts,
                Some(stale_ts + Duration::minutes(1)),
                "execution_confirmed",
                "cli-old-confirmed",
            ),
            (
                "ord-old-pending",
                "sig-old-pending",
                stale_ts,
                None,
                "execution_submitted",
                "cli-old-pending",
            ),
            (
                "ord-old-submit-recent-confirmed",
                "sig-old-submit-recent-confirmed",
                stale_ts,
                Some(fresh_ts),
                "execution_confirmed",
                "cli-old-submit-recent-confirmed",
            ),
            (
                "ord-fresh-confirmed",
                "sig-fresh-confirmed",
                fresh_ts,
                Some(fresh_ts + Duration::minutes(1)),
                "execution_confirmed",
                "cli-fresh-confirmed",
            ),
        ] {
            store.conn.execute(
                "INSERT INTO orders(
                    order_id, signal_id, route, submit_ts, confirm_ts, status, err_code,
                    client_order_id, tx_signature, simulation_status, simulation_error, attempt
                 ) VALUES (?1, ?2, 'rpc', ?3, ?4, ?5, NULL, ?6, 'sig', NULL, NULL, 1)",
                params![
                    order.0,
                    order.1,
                    order.2.to_rfc3339(),
                    order.3.map(|ts| ts.to_rfc3339()),
                    order.4,
                    order.5,
                ],
            )?;
        }

        for fill in [
            ("ord-old-confirmed", "token-1", 10.0, 0.05, 0.001, 10.0),
            (
                "ord-old-submit-recent-confirmed",
                "token-1",
                10.5,
                0.05,
                0.001,
                10.0,
            ),
            ("ord-fresh-confirmed", "token-1", 11.0, 0.05, 0.001, 10.0),
        ] {
            store.conn.execute(
                "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![fill.0, fill.1, fill.2, fill.3, fill.4, fill.5],
            )?;
        }

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(summary.fills_deleted, 1);
        assert_eq!(summary.orders_deleted, 1);
        assert_eq!(summary.copy_signals_deleted, 1);

        let remaining_orders: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))?;
        let remaining_fills: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM fills", [], |row| row.get(0))?;
        let remaining_signals: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM copy_signals", [], |row| row.get(0))?;
        assert_eq!(remaining_orders, 3);
        assert_eq!(remaining_fills, 2);
        assert_eq!(remaining_signals, 3);

        let old_pending_status: String = store.conn.query_row(
            "SELECT status FROM orders WHERE order_id = 'ord-old-pending'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(old_pending_status, "execution_submitted");

        let recent_confirm_status: String = store.conn.query_row(
            "SELECT status FROM orders WHERE order_id = 'ord-old-submit-recent-confirmed'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(recent_confirm_status, "execution_confirmed");
        Ok(())
    }

    #[test]
    fn apply_history_retention_deletes_old_shadow_closed_trades() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-closed-trades-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_opened = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stale_closed = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_opened = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_closed = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol, opened_ts, closed_ts
             ) VALUES ('sig-old', 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?1, ?2)",
            params![stale_opened.to_rfc3339(), stale_closed.to_rfc3339()],
        )?;
        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol, opened_ts, closed_ts
             ) VALUES ('sig-fresh', 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?1, ?2)",
            params![fresh_opened.to_rfc3339(), fresh_closed.to_rfc3339()],
        )?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_closed - Duration::days(1),
                copy_signals_before: fresh_closed - Duration::days(1),
                orders_before: fresh_closed - Duration::days(1),
                shadow_closed_trades_before: fresh_closed - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(summary.shadow_closed_trades_deleted, 1);
        let remaining: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM shadow_closed_trades", [], |row| {
                    row.get(0)
                })?;
        assert_eq!(remaining, 1);
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

pub(crate) fn lamports_to_sol(lamports: Lamports) -> f64 {
    lamports.as_u64() as f64 / LAMPORTS_PER_SOL
}

pub(crate) fn signed_lamports_to_sol(lamports: SignedLamports) -> f64 {
    lamports.as_i128() as f64 / LAMPORTS_PER_SOL
}

pub(crate) fn sol_to_lamports_ceil_storage(sol: f64, label: &str) -> Result<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!(
            "invalid {}={} (must be finite and >= 0)",
            label,
            sol
        ));
    }
    let scaled = sol * LAMPORTS_PER_SOL;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return Err(anyhow!(
            "invalid {}={} (exceeds representable lamports)",
            label,
            sol
        ));
    }
    Ok(Lamports::new(scaled.ceil() as u64))
}

pub(crate) fn sol_to_lamports_floor_storage(sol: f64, label: &str) -> Result<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!(
            "invalid {}={} (must be finite and >= 0)",
            label,
            sol
        ));
    }
    let scaled = sol * LAMPORTS_PER_SOL;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return Err(anyhow!(
            "invalid {}={} (exceeds representable lamports)",
            label,
            sol
        ));
    }
    Ok(Lamports::new(scaled.floor() as u64))
}

pub(crate) fn sol_to_signed_lamports_conservative_storage(
    sol: f64,
    label: &str,
) -> Result<SignedLamports> {
    if !sol.is_finite() {
        return Err(anyhow!("invalid {}={} (must be finite)", label, sol));
    }
    let magnitude = sol.abs() * LAMPORTS_PER_SOL;
    if !magnitude.is_finite() || magnitude > i64::MAX as f64 {
        return Err(anyhow!(
            "invalid {}={} (exceeds representable signed lamports)",
            label,
            sol
        ));
    }
    let signed = if sol >= 0.0 {
        magnitude.floor() as i128
    } else {
        -(magnitude.ceil() as i128)
    };
    Ok(SignedLamports::new(signed))
}

pub(crate) fn position_cost_lamports(
    cost_sol: f64,
    cost_lamports_raw: Option<i64>,
    context: &str,
) -> Result<Lamports> {
    if let Some(raw) = cost_lamports_raw {
        if raw < 0 {
            return Err(anyhow!(
                "invalid negative positions.cost_lamports={} in {}",
                raw,
                context
            ));
        }
        return Ok(Lamports::new(raw as u64));
    }
    sol_to_lamports_ceil_storage(cost_sol, "positions.cost_sol")
        .with_context(|| format!("failed deriving cost_lamports in {context}"))
}

pub(crate) fn position_pnl_lamports(
    pnl_sol: f64,
    pnl_lamports_raw: Option<i64>,
    context: &str,
) -> Result<SignedLamports> {
    if let Some(raw) = pnl_lamports_raw {
        return Ok(SignedLamports::new(i128::from(raw)));
    }
    sol_to_signed_lamports_conservative_storage(pnl_sol, "positions.pnl_sol")
        .with_context(|| format!("failed deriving pnl_lamports in {context}"))
}

pub(crate) fn token_quantity_from_sql(
    raw: Option<String>,
    decimals: Option<i64>,
    context: &str,
) -> Result<Option<TokenQuantity>> {
    match (raw, decimals) {
        (None, None) => Ok(None),
        (Some(raw), Some(decimals)) => {
            let decimals = u8::try_from(decimals).with_context(|| {
                format!(
                    "invalid qty_decimals={} in {} (must fit into u8)",
                    decimals, context
                )
            })?;
            let raw_value = raw.parse::<u64>().with_context(|| {
                format!("invalid qty_raw={:?} in {} (must parse as u64)", raw, context)
            })?;
            Ok(Some(TokenQuantity::new(raw_value, decimals)))
        }
        _ => Err(anyhow!(
            "partial exact quantity sidecar in {} (qty_raw and qty_decimals must both be NULL or both be populated)",
            context
        )),
    }
}

pub(crate) fn position_qty_sol(
    qty: f64,
    qty_raw: Option<String>,
    qty_decimals: Option<i64>,
    context: &str,
) -> Result<f64> {
    if let Some(exact) = token_quantity_from_sql(qty_raw, qty_decimals, context)? {
        return Ok(exact.as_f64());
    }
    if !qty.is_finite() || qty < 0.0 {
        return Err(anyhow!(
            "invalid positions.qty={} in {} (must be finite and >= 0)",
            qty,
            context
        ));
    }
    Ok(qty)
}

fn position_accounting_bucket_for_fill(qty_exact: Option<TokenQuantity>) -> &'static str {
    if qty_exact.is_some() {
        POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
    } else {
        POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER
    }
}

fn split_f64_pro_rata(
    total: f64,
    consumed_qty: f64,
    remaining_qty: f64,
    final_segment: bool,
    context: &str,
) -> Result<f64> {
    if final_segment {
        return Ok(total);
    }
    if !total.is_finite()
        || !consumed_qty.is_finite()
        || !remaining_qty.is_finite()
        || consumed_qty < 0.0
        || remaining_qty <= 0.0
    {
        return Err(anyhow!(
            "invalid pro-rata {} inputs total={} consumed_qty={} remaining_qty={}",
            context,
            total,
            consumed_qty,
            remaining_qty
        ));
    }
    let share = consumed_qty / remaining_qty;
    if !share.is_finite() || !(0.0..=1.0).contains(&share) {
        return Err(anyhow!(
            "invalid pro-rata {} share consumed_qty={} remaining_qty={}",
            context,
            consumed_qty,
            remaining_qty
        ));
    }
    Ok(total * share)
}

fn split_lamports_pro_rata(
    total: Lamports,
    consumed_qty: f64,
    remaining_qty: f64,
    final_segment: bool,
    round_up: bool,
    context: &str,
) -> Result<Lamports> {
    if final_segment {
        return Ok(total);
    }
    if !consumed_qty.is_finite()
        || !remaining_qty.is_finite()
        || consumed_qty < 0.0
        || remaining_qty <= 0.0
    {
        return Err(anyhow!(
            "invalid pro-rata {} inputs consumed_qty={} remaining_qty={}",
            context,
            consumed_qty,
            remaining_qty
        ));
    }
    let share = consumed_qty / remaining_qty;
    if !share.is_finite() || !(0.0..=1.0).contains(&share) {
        return Err(anyhow!(
            "invalid pro-rata {} share consumed_qty={} remaining_qty={}",
            context,
            consumed_qty,
            remaining_qty
        ));
    }
    let scaled = total.as_u64() as f64 * share;
    if !scaled.is_finite() || scaled < 0.0 || scaled > u64::MAX as f64 {
        return Err(anyhow!(
            "invalid pro-rata {} lamports scaling total={} share={}",
            context,
            total.as_u64(),
            share
        ));
    }
    let raw = if round_up {
        scaled.ceil() as u64
    } else {
        scaled.floor() as u64
    }
    .min(total.as_u64());
    Ok(Lamports::new(raw))
}

fn split_token_quantity_pro_rata(
    total: TokenQuantity,
    consumed_qty: f64,
    remaining_qty: f64,
    final_segment: bool,
    context: &str,
) -> Result<(Option<TokenQuantity>, Option<TokenQuantity>)> {
    if final_segment {
        return Ok((Some(total), None));
    }
    if !consumed_qty.is_finite()
        || !remaining_qty.is_finite()
        || consumed_qty < 0.0
        || remaining_qty <= 0.0
    {
        return Err(anyhow!(
            "invalid pro-rata {} token split inputs consumed_qty={} remaining_qty={}",
            context,
            consumed_qty,
            remaining_qty
        ));
    }
    let share = consumed_qty / remaining_qty;
    if !share.is_finite() || !(0.0..=1.0).contains(&share) {
        return Err(anyhow!(
            "invalid pro-rata {} token split share consumed_qty={} remaining_qty={}",
            context,
            consumed_qty,
            remaining_qty
        ));
    }
    let raw = ((total.raw() as f64) * share).floor() as u64;
    let raw = raw.min(total.raw());
    let segment = Some(TokenQuantity::new(raw, total.decimals()));
    let remainder_raw = total.raw().saturating_sub(raw);
    let remainder = if remainder_raw == 0 {
        None
    } else {
        Some(TokenQuantity::new(remainder_raw, total.decimals()))
    };
    Ok((segment, remainder))
}

fn merge_position_qty_exact_on_buy(
    current: Option<TokenQuantity>,
    added: Option<TokenQuantity>,
) -> Result<Option<TokenQuantity>> {
    match (current, added) {
        (Some(current), Some(added)) if current.decimals() == added.decimals() => {
            let raw = current.raw().checked_add(added.raw()).ok_or_else(|| {
                anyhow!(
                    "position qty_raw overflow while adding {} + {}",
                    current.raw(),
                    added.raw()
                )
            })?;
            Ok(Some(TokenQuantity::new(raw, current.decimals())))
        }
        (Some(_), Some(_)) => Ok(None),
        (None, Some(_)) | (Some(_), None) | (None, None) => Ok(None),
    }
}

pub(crate) fn merge_position_qty_exact_on_sell(
    current: Option<TokenQuantity>,
    closed: Option<TokenQuantity>,
    closing: bool,
) -> Result<Option<TokenQuantity>> {
    match (current, closed) {
        (Some(current), Some(closed)) if current.decimals() == closed.decimals() => {
            let Some(raw) = current.raw().checked_sub(closed.raw()) else {
                return Ok(None);
            };
            if closing {
                if raw == 0 {
                    Ok(Some(TokenQuantity::new(0, current.decimals())))
                } else {
                    Ok(None)
                }
            } else {
                Ok(Some(TokenQuantity::new(raw, current.decimals())))
            }
        }
        (Some(_), Some(_)) => Ok(None),
        (Some(_), None) | (None, Some(_)) | (None, None) => Ok(None),
    }
}

pub(crate) fn shadow_lot_cost_lamports(
    cost_sol: f64,
    cost_lamports_raw: Option<i64>,
    context: &str,
) -> Result<Lamports> {
    if let Some(raw) = cost_lamports_raw {
        if raw < 0 {
            return Err(anyhow!(
                "invalid negative shadow_lots.cost_lamports={} in {}",
                raw,
                context
            ));
        }
        return Ok(Lamports::new(raw as u64));
    }
    sol_to_lamports_ceil_storage(cost_sol, "shadow_lots.cost_sol")
        .with_context(|| format!("failed deriving shadow_lot cost_lamports in {context}"))
}

pub(crate) fn shadow_closed_trade_entry_cost_lamports(
    entry_cost_sol: f64,
    entry_cost_lamports_raw: Option<i64>,
    context: &str,
) -> Result<Lamports> {
    if let Some(raw) = entry_cost_lamports_raw {
        if raw < 0 {
            return Err(anyhow!(
                "invalid negative shadow_closed_trades.entry_cost_lamports={} in {}",
                raw,
                context
            ));
        }
        return Ok(Lamports::new(raw as u64));
    }
    sol_to_lamports_ceil_storage(entry_cost_sol, "shadow_closed_trades.entry_cost_sol")
        .with_context(|| {
            format!("failed deriving shadow closed trade entry_cost_lamports in {context}")
        })
}

pub(crate) fn shadow_closed_trade_pnl_lamports(
    pnl_sol: f64,
    pnl_lamports_raw: Option<i64>,
    context: &str,
) -> Result<SignedLamports> {
    if let Some(raw) = pnl_lamports_raw {
        return Ok(SignedLamports::new(i128::from(raw)));
    }
    sol_to_signed_lamports_conservative_storage(pnl_sol, "shadow_closed_trades.pnl_sol")
        .with_context(|| format!("failed deriving shadow closed trade pnl_lamports in {context}"))
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

fn signed_lamports_to_sql_i64(field: &str, value: SignedLamports) -> Result<i64> {
    i64::try_from(value.as_i128()).with_context(|| {
        format!(
            "{}={} exceeds sqlite INTEGER range (i64)",
            field,
            value.as_i128()
        )
    })
}
