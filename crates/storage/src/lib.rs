use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use reqwest::blocking::Client;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];
static SQLITE_WRITE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static SQLITE_BUSY_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

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

#[derive(Debug, Clone)]
pub struct WalletMetricRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
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

#[derive(Debug, Clone)]
pub struct WalletUpsertRow {
    pub wallet_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FollowlistUpdateResult {
    pub activated: usize,
    pub deactivated: usize,
}

#[derive(Debug, Clone)]
pub struct CopySignalRow {
    pub signal_id: String,
    pub wallet_id: String,
    pub side: String,
    pub token: String,
    pub notional_sol: f64,
    pub ts: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionOrderRow {
    pub order_id: String,
    pub signal_id: String,
    pub client_order_id: String,
    pub route: String,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub status: String,
    pub err_code: Option<String>,
    pub tx_signature: Option<String>,
    pub simulation_status: Option<String>,
    pub simulation_error: Option<String>,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertExecutionOrderPendingOutcome {
    Inserted,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeExecutionConfirmOutcome {
    Applied,
    AlreadyConfirmed,
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
pub struct TokenQualityCacheRow {
    pub mint: String,
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct TokenQualityRpcRow {
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
}

impl SqliteStore {
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

    pub fn run_migrations(&mut self, migrations_dir: &Path) -> Result<usize> {
        if !migrations_dir.exists() {
            return Err(anyhow!(
                "migrations directory not found: {}",
                migrations_dir.display()
            ));
        }

        let mut files = self.read_migration_files(migrations_dir)?;
        files.sort();

        let tx = self
            .conn
            .transaction()
            .context("failed to open sqlite migration transaction")?;
        let mut applied = 0usize;

        for path in files {
            let version = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("invalid migration filename: {}", path.display()))?;

            let already_applied: Option<String> = tx
                .query_row(
                    "SELECT version FROM schema_migrations WHERE version = ?1",
                    params![version],
                    |row| row.get(0),
                )
                .optional()
                .with_context(|| format!("failed checking migration {}", version))?;

            if already_applied.is_some() {
                continue;
            }

            let sql = fs::read_to_string(&path)
                .with_context(|| format!("failed reading migration file {}", path.display()))?;
            tx.execute_batch(&sql)
                .with_context(|| format!("failed applying migration {}", version))?;
            tx.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, datetime('now'))",
                params![version],
            )
            .with_context(|| format!("failed recording migration {}", version))?;

            applied += 1;
            tracing::info!(version = version, "migration applied");
        }

        tx.commit().context("failed to commit migrations")?;
        Ok(applied)
    }

    pub fn record_heartbeat(&self, component: &str, status: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO system_heartbeat(component, ts, status) VALUES (?1, datetime('now'), ?2)",
                params![component, status],
            )
            .context("failed to record heartbeat")?;
        Ok(())
    }

    pub fn insert_risk_event(
        &self,
        event_type: &str,
        severity: &str,
        ts: DateTime<Utc>,
        details_json: Option<&str>,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    uuid::Uuid::new_v4().to_string(),
                    event_type,
                    severity,
                    ts.to_rfc3339(),
                    details_json,
                ],
            )
        })
        .context("failed to insert risk event")?;
        Ok(())
    }

    pub fn insert_observed_swap(&self, swap: &SwapEvent) -> Result<bool> {
        let written = self
            .conn
            .execute(
                "INSERT OR IGNORE INTO observed_swaps(
                    signature,
                    wallet_id,
                    dex,
                    token_in,
                    token_out,
                    qty_in,
                    qty_out,
                    slot,
                    ts
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    &swap.signature,
                    &swap.wallet,
                    &swap.dex,
                    &swap.token_in,
                    &swap.token_out,
                    swap.amount_in,
                    swap.amount_out,
                    swap.slot as i64,
                    swap.ts_utc.to_rfc3339(),
                ],
            )
            .context("failed to insert observed swap")?;
        Ok(written > 0)
    }

    fn execute_with_retry<F>(&self, mut operation: F) -> rusqlite::Result<usize>
    where
        F: FnMut(&Connection) -> rusqlite::Result<usize>,
    {
        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            match operation(&self.conn) {
                Ok(changed) => return Ok(changed),
                Err(error) => {
                    let retryable = is_retryable_sqlite_error(&error);
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
                    return Err(error);
                }
            }
        }
        unreachable!("retry loop must return on success or terminal error");
    }

    pub fn load_observed_swaps_since(&self, since: DateTime<Utc>) -> Result<Vec<SwapEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts ASC, slot ASC",
            )
            .context("failed to prepare observed_swaps load query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed to query observed_swaps")?;

        let mut swaps = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps rows")?
        {
            swaps.push(Self::row_to_swap_event(row)?);
        }

        Ok(swaps)
    }

    pub fn for_each_observed_swap_since<F>(
        &self,
        since: DateTime<Utc>,
        mut on_swap: F,
    ) -> Result<usize>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts ASC, slot ASC",
            )
            .context("failed to prepare observed_swaps streaming query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed to stream observed_swaps rows")?;

        let mut seen = 0usize;
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps stream")?
        {
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(seen)
    }

    pub fn list_unique_sol_buy_mints_since(&self, since: DateTime<Utc>) -> Result<HashSet<String>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT token_out
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND token_in = ?2
                   AND token_out <> ?2",
            )
            .context("failed to prepare unique sol-buy mints query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), SOL_MINT])
            .context("failed to query unique sol-buy mints")?;

        let mut out = HashSet::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating unique sol-buy mints rows")?
        {
            let mint: String = row
                .get(0)
                .context("failed reading observed_swaps.token_out")?;
            out.insert(mint);
        }
        Ok(out)
    }

    fn row_to_swap_event(row: &rusqlite::Row<'_>) -> Result<SwapEvent> {
        let ts_raw: String = row.get(8).context("failed reading observed_swaps.ts")?;
        let ts_utc = DateTime::parse_from_rfc3339(&ts_raw)
            .map(|dt| dt.with_timezone(&Utc))
            .with_context(|| format!("invalid observed_swaps.ts rfc3339 value: {ts_raw}"))?;
        let slot_raw: i64 = row.get(7).context("failed reading observed_swaps.slot")?;
        let slot = if slot_raw < 0 { 0 } else { slot_raw as u64 };

        Ok(SwapEvent {
            signature: row
                .get(0)
                .context("failed reading observed_swaps.signature")?,
            wallet: row
                .get(1)
                .context("failed reading observed_swaps.wallet_id")?,
            dex: row.get(2).context("failed reading observed_swaps.dex")?,
            token_in: row
                .get(3)
                .context("failed reading observed_swaps.token_in")?,
            token_out: row
                .get(4)
                .context("failed reading observed_swaps.token_out")?,
            amount_in: row.get(5).context("failed reading observed_swaps.qty_in")?,
            amount_out: row
                .get(6)
                .context("failed reading observed_swaps.qty_out")?,
            slot,
            ts_utc,
        })
    }

    pub fn upsert_wallet(
        &self,
        wallet_id: &str,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        status: &str,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(wallet_id) DO UPDATE SET
                    first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
                    last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
                    status = excluded.status",
                params![
                    wallet_id,
                    first_seen.to_rfc3339(),
                    last_seen.to_rfc3339(),
                    status,
                ],
            )
            .context("failed to upsert wallet")?;
        Ok(())
    }

    pub fn insert_wallet_metric(&self, metric: &WalletMetricRow) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO wallet_metrics(
                    wallet_id,
                    window_start,
                    pnl,
                    win_rate,
                    trades,
                    closed_trades,
                    hold_median_seconds,
                    score,
                    buy_total,
                    tradable_ratio,
                    rug_ratio
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    &metric.wallet_id,
                    metric.window_start.to_rfc3339(),
                    metric.pnl,
                    metric.win_rate,
                    metric.trades as i64,
                    metric.closed_trades as i64,
                    metric.hold_median_seconds,
                    metric.score,
                    metric.buy_total as i64,
                    metric.tradable_ratio,
                    metric.rug_ratio,
                ],
            )
            .context("failed to insert wallet metric")?;
        Ok(())
    }

    pub fn persist_discovery_cycle(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("failed to open discovery write transaction")?;

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(wallet_id) DO UPDATE SET
                        first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
                        last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
                        status = excluded.status",
                )
                .context("failed to prepare discovery wallet upsert statement")?;
            for wallet in wallets {
                stmt.execute(params![
                    &wallet.wallet_id,
                    wallet.first_seen.to_rfc3339(),
                    wallet.last_seen.to_rfc3339(),
                    &wallet.status,
                ])
                .context("failed to upsert wallet in discovery transaction")?;
            }
        }

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO wallet_metrics(
                        wallet_id,
                        window_start,
                        pnl,
                        win_rate,
                        trades,
                        closed_trades,
                        hold_median_seconds,
                        score,
                        buy_total,
                        tradable_ratio,
                        rug_ratio
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )
                .context("failed to prepare discovery wallet metric insert statement")?;
            for metric in metrics {
                stmt.execute(params![
                    &metric.wallet_id,
                    metric.window_start.to_rfc3339(),
                    metric.pnl,
                    metric.win_rate,
                    metric.trades as i64,
                    metric.closed_trades as i64,
                    metric.hold_median_seconds,
                    metric.score,
                    metric.buy_total as i64,
                    metric.tradable_ratio,
                    metric.rug_ratio,
                ])
                .context("failed to insert wallet metric in discovery transaction")?;
            }
        }

        let desired: HashSet<&str> = desired_wallets.iter().map(String::as_str).collect();
        let active_wallets: Vec<String> = {
            let mut stmt = tx
                .prepare("SELECT wallet_id FROM followlist WHERE active = 1")
                .context("failed to prepare active followlist query in discovery transaction")?;
            let mut rows = stmt
                .query([])
                .context("failed querying active followlist in discovery transaction")?;
            let mut wallets = Vec::new();
            while let Some(row) = rows
                .next()
                .context("failed iterating active followlist in discovery transaction")?
            {
                wallets.push(
                    row.get(0)
                        .context("failed reading followlist.wallet_id in discovery transaction")?,
                );
            }
            wallets
        };

        let now_raw = now.to_rfc3339();
        let mut result = FollowlistUpdateResult::default();
        {
            let mut deactivate_stmt = tx
                .prepare_cached(
                    "UPDATE followlist
                     SET active = 0, removed_at = ?1, reason = ?2
                     WHERE wallet_id = ?3 AND active = 1",
                )
                .context("failed to prepare followlist deactivate statement")?;
            for wallet_id in active_wallets.iter() {
                if !desired.contains(wallet_id.as_str()) {
                    let changed = deactivate_stmt
                        .execute(params![&now_raw, reason, wallet_id])
                        .context("failed to deactivate follow wallet in discovery transaction")?;
                    if changed > 0 {
                        result.deactivated += 1;
                    }
                }
            }
        }

        {
            let mut exists_stmt = tx
                .prepare_cached(
                    "SELECT id FROM followlist WHERE wallet_id = ?1 AND active = 1 LIMIT 1",
                )
                .context("failed to prepare followlist active check statement")?;
            let mut activate_stmt = tx
                .prepare_cached(
                    "INSERT INTO followlist(wallet_id, added_at, reason, active)
                     VALUES (?1, ?2, ?3, 1)",
                )
                .context("failed to prepare followlist activate statement")?;
            for wallet_id in desired_wallets {
                let already_active: Option<i64> = exists_stmt
                    .query_row(params![wallet_id], |row| row.get(0))
                    .optional()
                    .context("failed checking existing active follow wallet in transaction")?;
                if already_active.is_none() {
                    activate_stmt
                        .execute(params![wallet_id, &now_raw, reason])
                        .context("failed to activate follow wallet in discovery transaction")?;
                    result.activated += 1;
                }
            }
        }

        tx.commit()
            .context("failed to commit discovery write transaction")?;
        Ok(result)
    }

    pub fn list_active_follow_wallets(&self) -> Result<HashSet<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_id FROM followlist WHERE active = 1")
            .context("failed to prepare active followlist query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying active followlist entries")?;

        let mut wallets = HashSet::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating active followlist entries")?
        {
            let wallet_id: String = row.get(0).context("failed reading followlist.wallet_id")?;
            wallets.insert(wallet_id);
        }
        Ok(wallets)
    }

    pub fn was_wallet_followed_at(&self, wallet_id: &str, ts: DateTime<Utc>) -> Result<bool> {
        let ts_raw = ts.to_rfc3339();
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1
                 FROM followlist
                 WHERE wallet_id = ?1
                   AND added_at <= ?2
                   AND (removed_at IS NULL OR ?2 < removed_at)
                 LIMIT 1",
                params![wallet_id, ts_raw],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking temporal followlist membership")?;
        Ok(exists.is_some())
    }

    pub fn deactivate_follow_wallet(
        &self,
        wallet_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<bool> {
        let changed = self
            .conn
            .execute(
                "UPDATE followlist
                 SET active = 0, removed_at = ?1, reason = ?2
                 WHERE wallet_id = ?3 AND active = 1",
                params![now.to_rfc3339(), reason, wallet_id],
            )
            .context("failed to deactivate follow wallet")?;
        Ok(changed > 0)
    }

    pub fn activate_follow_wallet(
        &self,
        wallet_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<bool> {
        let already_active: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM followlist WHERE wallet_id = ?1 AND active = 1 LIMIT 1",
                params![wallet_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking existing active follow entry")?;
        if already_active.is_some() {
            return Ok(false);
        }

        self.conn
            .execute(
                "INSERT INTO followlist(wallet_id, added_at, reason, active)
                 VALUES (?1, ?2, ?3, 1)",
                params![wallet_id, now.to_rfc3339(), reason],
            )
            .context("failed to activate follow wallet")?;
        Ok(true)
    }

    pub fn reconcile_followlist(
        &self,
        desired_wallets: &[String],
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        let desired: HashSet<&str> = desired_wallets.iter().map(String::as_str).collect();
        let active = self.list_active_follow_wallets()?;
        let mut result = FollowlistUpdateResult::default();

        for wallet_id in active.iter() {
            if !desired.contains(wallet_id.as_str())
                && self.deactivate_follow_wallet(wallet_id, now, reason)?
            {
                result.deactivated += 1;
            }
        }

        for wallet_id in desired_wallets {
            if self.activate_follow_wallet(wallet_id, now, reason)? {
                result.activated += 1;
            }
        }

        Ok(result)
    }

    pub fn insert_copy_signal(&self, signal: &CopySignalRow) -> Result<bool> {
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO copy_signals(
                    signal_id,
                    wallet_id,
                    side,
                    token,
                    notional_sol,
                    ts,
                    status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        &signal.signal_id,
                        &signal.wallet_id,
                        &signal.side,
                        &signal.token,
                        signal.notional_sol,
                        signal.ts.to_rfc3339(),
                        &signal.status,
                    ],
                )
            })
            .context("failed to insert copy signal")?;
        Ok(written > 0)
    }

    pub fn list_copy_signals_by_status(
        &self,
        status: &str,
        limit: u32,
    ) -> Result<Vec<CopySignalRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signal_id, wallet_id, side, token, notional_sol, ts, status
                 FROM copy_signals
                 WHERE status = ?1
                 ORDER BY ts ASC
                 LIMIT ?2",
            )
            .context("failed to prepare copy_signals by status query")?;
        let mut rows = stmt
            .query(params![status, limit.max(1) as i64])
            .context("failed querying copy_signals by status")?;

        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating copy_signals by status rows")?
        {
            let ts_raw: String = row.get(5).context("failed reading copy_signals.ts")?;
            let ts = DateTime::parse_from_rfc3339(&ts_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid copy_signals.ts rfc3339 value: {ts_raw}"))?;
            out.push(CopySignalRow {
                signal_id: row
                    .get(0)
                    .context("failed reading copy_signals.signal_id")?,
                wallet_id: row
                    .get(1)
                    .context("failed reading copy_signals.wallet_id")?,
                side: row.get(2).context("failed reading copy_signals.side")?,
                token: row.get(3).context("failed reading copy_signals.token")?,
                notional_sol: row
                    .get(4)
                    .context("failed reading copy_signals.notional_sol")?,
                ts,
                status: row.get(6).context("failed reading copy_signals.status")?,
            });
        }
        Ok(out)
    }

    pub fn update_copy_signal_status(&self, signal_id: &str, status: &str) -> Result<bool> {
        let changed = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "UPDATE copy_signals SET status = ?1 WHERE signal_id = ?2",
                    params![status, signal_id],
                )
            })
            .with_context(|| format!("failed updating copy_signals status for {}", signal_id))?;
        Ok(changed > 0)
    }

    pub fn execution_order_by_client_order_id(
        &self,
        client_order_id: &str,
    ) -> Result<Option<ExecutionOrderRow>> {
        let row = self
            .conn
            .query_row(
                "SELECT
                    order_id,
                    signal_id,
                    client_order_id,
                    route,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt
                 FROM orders
                 WHERE client_order_id = ?1
                 ORDER BY submit_ts DESC
                 LIMIT 1",
                params![client_order_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, i64>(11)?,
                    ))
                },
            )
            .optional()
            .context("failed querying order by client_order_id")?;

        row.map(
            |(
                order_id,
                signal_id,
                client_id,
                route,
                submit_ts_raw,
                confirm_ts_raw,
                status,
                err_code,
                tx_signature,
                simulation_status,
                simulation_error,
                attempt_raw,
            )| {
                let submit_ts = DateTime::parse_from_rfc3339(&submit_ts_raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| {
                        format!("invalid orders.submit_ts rfc3339 value: {submit_ts_raw}")
                    })?;
                let confirm_ts = confirm_ts_raw
                    .as_deref()
                    .map(|raw| {
                        DateTime::parse_from_rfc3339(raw)
                            .map(|dt| dt.with_timezone(&Utc))
                            .with_context(|| {
                                format!("invalid orders.confirm_ts rfc3339 value: {raw}")
                            })
                    })
                    .transpose()?;
                Ok(ExecutionOrderRow {
                    order_id,
                    signal_id,
                    client_order_id: client_id,
                    route,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt: attempt_raw.max(0) as u32,
                })
            },
        )
        .transpose()
    }

    pub fn insert_execution_order_pending(
        &self,
        order_id: &str,
        signal_id: &str,
        client_order_id: &str,
        route: &str,
        submit_ts: DateTime<Utc>,
        attempt: u32,
    ) -> Result<InsertExecutionOrderPendingOutcome> {
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO orders(
                        order_id,
                        signal_id,
                        client_order_id,
                        route,
                        submit_ts,
                        status,
                        attempt
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        order_id,
                        signal_id,
                        client_order_id,
                        route,
                        submit_ts.to_rfc3339(),
                        "execution_pending",
                        attempt.max(1) as i64,
                    ],
                )
            })
            .context("failed inserting pending execution order")?;
        if written > 0 {
            return Ok(InsertExecutionOrderPendingOutcome::Inserted);
        }

        let duplicate = self
            .conn
            .query_row(
                "SELECT 1
                 FROM orders
                 WHERE client_order_id = ?1
                    OR order_id = ?2
                 LIMIT 1",
                params![client_order_id, order_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed verifying duplicate execution order insert")?;
        if duplicate.is_some() {
            return Ok(InsertExecutionOrderPendingOutcome::Duplicate);
        }

        Err(anyhow!(
            "execution order insert ignored without duplicate detection order_id={} client_order_id={}",
            order_id,
            client_order_id
        ))
    }

    pub fn mark_order_simulated(
        &self,
        order_id: &str,
        simulation_status: &str,
        simulation_detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_simulated',
                     simulation_status = ?1,
                     simulation_error = ?2
                 WHERE order_id = ?3",
                params![simulation_status, simulation_detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order simulated: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_submitted(
        &self,
        order_id: &str,
        route: &str,
        tx_signature: &str,
        submit_ts: DateTime<Utc>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_submitted',
                     route = ?1,
                     tx_signature = ?2,
                     submit_ts = ?3
                 WHERE order_id = ?4",
                params![route, tx_signature, submit_ts.to_rfc3339(), order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order submitted: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn set_order_attempt(
        &self,
        order_id: &str,
        attempt: u32,
        detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET attempt = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3",
                params![attempt.max(1) as i64, detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed setting order attempt: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_confirmed(&self, order_id: &str, confirm_ts: DateTime<Utc>) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_confirmed',
                     confirm_ts = ?1
                 WHERE order_id = ?2",
                params![confirm_ts.to_rfc3339(), order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order confirmed: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_dropped(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_dropped',
                     err_code = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3",
                params![err_code, detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order dropped: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_failed(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_failed',
                     err_code = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3",
                params![err_code, detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order failed: order_id={} not found",
                order_id
            ));
        }
        Ok(())
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
                if status != "execution_submitted" {
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
                    confirmed_ts,
                )?;

                let changed_order = self
                    .conn
                    .execute(
                        "UPDATE orders
                         SET status = 'execution_confirmed',
                             confirm_ts = ?1
                         WHERE order_id = ?2",
                        params![confirmed_ts.to_rfc3339(), order_id],
                    )
                    .context("failed marking order confirmed in finalize confirm transaction")?;
                if changed_order == 0 {
                    return Err(anyhow!(
                        "failed marking order confirmed in finalize confirm transaction: order_id={} not found",
                        order_id
                    ));
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

                Ok(FinalizeExecutionConfirmOutcome::Applied)
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
        ts: DateTime<Utc>,
    ) -> Result<()> {
        const EPS: f64 = 1e-12;

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
                            current_cost + notional_sol,
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
                        params![position_id, token, qty, notional_sol, ts.to_rfc3339()],
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
                let realized_pnl = effective_notional - realized_cost;
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

    pub fn insert_shadow_lot(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO shadow_lots(wallet_id, token, qty, cost_sol, opened_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![wallet_id, token, qty, cost_sol, opened_ts.to_rfc3339()],
            )
        })
        .context("failed to insert shadow lot")?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn list_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, qty, cost_sol, opened_ts
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )
            .context("failed to prepare shadow lot query")?;
        let mut rows = stmt
            .query(params![wallet_id, token])
            .context("failed querying shadow lots")?;

        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating shadow lots")? {
            let opened_raw: String = row.get(5).context("failed reading shadow_lots.opened_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid shadow_lots.opened_ts rfc3339 value: {opened_raw}")
                })?;
            lots.push(ShadowLotRow {
                id: row.get(0).context("failed reading shadow_lots.id")?,
                wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
                token: row.get(2).context("failed reading shadow_lots.token")?,
                qty: row.get(3).context("failed reading shadow_lots.qty")?,
                cost_sol: row.get(4).context("failed reading shadow_lots.cost_sol")?,
                opened_ts,
            });
        }
        Ok(lots)
    }

    pub fn list_open_shadow_lots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, qty, cost_sol, opened_ts
                 FROM shadow_lots
                 WHERE qty > 0
                   AND opened_ts <= ?1
                 ORDER BY opened_ts ASC, id ASC
                 LIMIT ?2",
            )
            .context("failed to prepare stale shadow lot query")?;
        let mut rows = stmt
            .query(params![cutoff.to_rfc3339(), limit.max(1) as i64])
            .context("failed querying stale shadow lots")?;

        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating stale shadow lots")? {
            let opened_raw: String = row.get(5).context("failed reading shadow_lots.opened_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid shadow_lots.opened_ts rfc3339 value: {opened_raw}")
                })?;
            lots.push(ShadowLotRow {
                id: row.get(0).context("failed reading shadow_lots.id")?,
                wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
                token: row.get(2).context("failed reading shadow_lots.token")?,
                qty: row.get(3).context("failed reading shadow_lots.qty")?,
                cost_sol: row.get(4).context("failed reading shadow_lots.cost_sol")?,
                opened_ts,
            });
        }

        Ok(lots)
    }

    pub fn has_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<bool> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM shadow_lots WHERE wallet_id = ?1 AND token = ?2",
                params![wallet_id, token],
                |row| row.get(0),
            )
            .context("failed querying shadow lots existence")?;
        Ok(count > 0)
    }

    pub fn list_shadow_open_pairs(&self) -> Result<HashSet<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT wallet_id, token
                 FROM shadow_lots
                 WHERE qty > 0",
            )
            .context("failed to prepare shadow open lots query")?;
        let mut rows = stmt.query([]).context("failed querying shadow open lots")?;

        let mut pairs = HashSet::new();
        while let Some(row) = rows.next().context("failed iterating shadow open lots")? {
            let wallet_id: String = row
                .get(0)
                .context("failed reading shadow_lots.wallet_id in open lots query")?;
            let token: String = row
                .get(1)
                .context("failed reading shadow_lots.token in open lots query")?;
            pairs.insert((wallet_id, token));
        }
        Ok(pairs)
    }

    pub fn latest_token_sol_price(&self, token: &str, as_of: DateTime<Utc>) -> Result<Option<f64>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let as_of_raw = as_of.to_rfc3339();
        let price: Option<f64> = self
            .conn
            .query_row(
                "SELECT price
                 FROM (
                    SELECT qty_in / qty_out AS price, ts
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND qty_in > 0
                      AND qty_out > 0
                      AND ts <= ?3
                    UNION ALL
                    SELECT qty_out / qty_in AS price, ts
                    FROM observed_swaps
                    WHERE token_in = ?2
                      AND token_out = ?1
                      AND qty_in > 0
                      AND qty_out > 0
                      AND ts <= ?3
                 )
                 ORDER BY ts DESC
                 LIMIT 1",
                params![SOL_MINT, token, as_of_raw],
                |row| row.get(0),
            )
            .optional()
            .context("failed querying latest token/sol price")?;

        Ok(price.filter(|value| value.is_finite() && *value > 0.0))
    }

    pub fn close_shadow_lots_fifo_atomic(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        const EPS: f64 = 1e-12;

        if target_qty <= EPS {
            return Ok(ShadowCloseOutcome {
                has_open_lots_after: self.has_shadow_lots(wallet_id, token)?,
                ..ShadowCloseOutcome::default()
            });
        }

        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            match self.close_shadow_lots_fifo_atomic_once(
                signal_id,
                wallet_id,
                token,
                target_qty,
                exit_price_sol,
                closed_ts,
            ) {
                Ok(outcome) => return Ok(outcome),
                Err(error) => {
                    let retryable = is_retryable_sqlite_error(&error);
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
                    return Err(error).context("failed to close shadow fifo lots atomically");
                }
            }
        }

        unreachable!("retry loop must return on success or terminal error");
    }

    fn close_shadow_lots_fifo_atomic_once(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> rusqlite::Result<ShadowCloseOutcome> {
        const EPS: f64 = 1e-12;

        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let close_result = (|| -> rusqlite::Result<ShadowCloseOutcome> {
            let mut qty_remaining = target_qty;
            let mut closed_qty = 0.0;
            let mut realized_pnl_sol = 0.0;

            let mut stmt = self.conn.prepare(
                "SELECT id, qty, cost_sol, opened_ts
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )?;
            let mut rows = stmt.query(params![wallet_id, token])?;
            let mut lots: Vec<(i64, f64, f64, String)> = Vec::new();
            while let Some(row) = rows.next()? {
                lots.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
            }
            drop(rows);
            drop(stmt);

            for (lot_id, lot_qty, lot_cost_sol, lot_opened_ts) in lots {
                if qty_remaining <= EPS {
                    break;
                }

                if lot_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                    continue;
                }

                let take_qty = qty_remaining.min(lot_qty);
                let entry_cost_sol = lot_cost_sol * (take_qty / lot_qty);
                let remaining_qty = (lot_qty - take_qty).max(0.0);
                let remaining_cost = (lot_cost_sol - entry_cost_sol).max(0.0);

                if remaining_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                } else {
                    self.conn.execute(
                        "UPDATE shadow_lots SET qty = ?1, cost_sol = ?2 WHERE id = ?3",
                        params![remaining_qty, remaining_cost, lot_id],
                    )?;
                }

                let exit_value_sol = take_qty * exit_price_sol;
                let pnl_sol = exit_value_sol - entry_cost_sol;
                self.conn.execute(
                    "INSERT INTO shadow_closed_trades(
                        signal_id,
                        wallet_id,
                        token,
                        qty,
                        entry_cost_sol,
                        exit_value_sol,
                        pnl_sol,
                        opened_ts,
                        closed_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        signal_id,
                        wallet_id,
                        token,
                        take_qty,
                        entry_cost_sol,
                        exit_value_sol,
                        pnl_sol,
                        lot_opened_ts,
                        closed_ts.to_rfc3339(),
                    ],
                )?;

                qty_remaining -= take_qty;
                closed_qty += take_qty;
                realized_pnl_sol += pnl_sol;
            }

            let remaining_lots: i64 = self.conn.query_row(
                "SELECT COUNT(*)
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2 AND qty > 0",
                params![wallet_id, token],
                |row| row.get(0),
            )?;

            Ok(ShadowCloseOutcome {
                closed_qty,
                realized_pnl_sol,
                has_open_lots_after: remaining_lots > 0,
            })
        })();

        match close_result {
            Ok(outcome) => match self.conn.execute_batch("COMMIT") {
                Ok(()) => Ok(outcome),
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    Err(error)
                }
            },
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn token_market_stats(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<TokenMarketStats> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let as_of_raw = as_of.to_rfc3339();

        let first_seen_raw: Option<String> = self
            .conn
            .query_row(
                "SELECT MIN(ts)
                 FROM (
                    SELECT ts FROM observed_swaps WHERE token_in = ?1 AND ts <= ?2
                    UNION ALL
                    SELECT ts FROM observed_swaps WHERE token_out = ?1 AND ts <= ?2
                 )",
                params![token, &as_of_raw],
                |row| row.get(0),
            )
            .context("failed querying token first_seen")?;

        let first_seen = first_seen_raw
            .as_deref()
            .map(|raw| {
                DateTime::parse_from_rfc3339(raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| format!("invalid observed_swaps.ts rfc3339 value: {raw}"))
            })
            .transpose()?;

        let holders_proxy_raw: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM (
                    SELECT DISTINCT wallet_id
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND ts <= ?2
                    UNION
                    SELECT DISTINCT wallet_id
                    FROM observed_swaps
                    WHERE token_out = ?1
                      AND ts <= ?2
                 )",
                params![token, &as_of_raw],
                |row| row.get(0),
            )
            .context("failed querying token holders proxy")?;

        let window_start = (as_of - Duration::minutes(5)).to_rfc3339();
        let window_end = as_of.to_rfc3339();
        let (volume_5m_sol, liquidity_sol_proxy, unique_traders_5m_raw): (f64, f64, i64) = self
            .conn
            .query_row(
                "SELECT
                    COALESCE(SUM(sol_notional), 0.0) AS volume_5m_sol,
                    COALESCE(MAX(sol_notional), 0.0) AS liquidity_sol_proxy,
                    COUNT(DISTINCT wallet_id) AS unique_traders_5m
                 FROM (
                    SELECT wallet_id, qty_out AS sol_notional
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND ts >= ?3
                      AND ts <= ?4
                    UNION ALL
                    SELECT wallet_id, qty_in AS sol_notional
                    FROM observed_swaps
                    WHERE token_out = ?1
                      AND token_in = ?2
                      AND ts >= ?3
                      AND ts <= ?4
                 )",
                params![token, SOL_MINT, window_start, window_end],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("failed querying token 5m market stats")?;

        Ok(TokenMarketStats {
            first_seen,
            holders_proxy: holders_proxy_raw.max(0) as u64,
            liquidity_sol_proxy,
            volume_5m_sol,
            unique_traders_5m: unique_traders_5m_raw.max(0) as u64,
        })
    }

    pub fn get_token_quality_cache(&self, mint: &str) -> Result<Option<TokenQualityCacheRow>> {
        let row: Option<(String, Option<i64>, Option<f64>, Option<i64>, String)> = self
            .conn
            .query_row(
                "SELECT mint, holders, liquidity_sol, token_age_seconds, fetched_at
                 FROM token_quality_cache
                 WHERE mint = ?1",
                params![mint],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .context("failed querying token_quality_cache row")?;

        row.map(|(mint, holders, liquidity_sol, token_age_seconds, fetched_at_raw)| {
            let fetched_at = DateTime::parse_from_rfc3339(&fetched_at_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid token_quality_cache.fetched_at rfc3339 value: {fetched_at_raw}")
                })?;
            Ok(TokenQualityCacheRow {
                mint,
                holders: holders.map(|value| value.max(0) as u64),
                liquidity_sol,
                token_age_seconds: token_age_seconds.map(|value| value.max(0) as u64),
                fetched_at,
            })
        })
        .transpose()
    }

    pub fn upsert_token_quality_cache(
        &self,
        mint: &str,
        holders: Option<u64>,
        liquidity_sol: Option<f64>,
        token_age_seconds: Option<u64>,
        fetched_at: DateTime<Utc>,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO token_quality_cache(
                    mint,
                    holders,
                    liquidity_sol,
                    token_age_seconds,
                    fetched_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(mint) DO UPDATE SET
                    holders = excluded.holders,
                    liquidity_sol = excluded.liquidity_sol,
                    token_age_seconds = excluded.token_age_seconds,
                    fetched_at = excluded.fetched_at",
                params![
                    mint,
                    holders.map(|value| value as i64),
                    liquidity_sol,
                    token_age_seconds.map(|value| value as i64),
                    fetched_at.to_rfc3339(),
                ],
            )
            .context("failed upserting token_quality_cache row")?;
        Ok(())
    }

    pub fn fetch_token_quality_from_helius(
        helius_http_url: &str,
        mint: &str,
        timeout_ms: u64,
        max_signature_pages: u32,
        min_age_hint_seconds: Option<u64>,
    ) -> Result<TokenQualityRpcRow> {
        let client = Client::builder()
            .timeout(StdDuration::from_millis(timeout_ms.max(100)))
            .build()
            .context("failed building reqwest blocking client for token quality fetch")?;

        let holders = fetch_token_holders(&client, helius_http_url, mint).ok();
        let token_age_seconds = fetch_token_age_seconds(
            &client,
            helius_http_url,
            mint,
            max_signature_pages.max(1),
            min_age_hint_seconds,
        )
        .ok()
        .flatten();
        if holders.is_none() && token_age_seconds.is_none() {
            return Err(anyhow!(
                "failed to fetch token quality fields for mint {} via helius",
                mint
            ));
        }

        Ok(TokenQualityRpcRow {
            holders,
            liquidity_sol: None,
            token_age_seconds,
        })
    }

    pub fn update_shadow_lot(&self, id: i64, qty: f64, cost_sol: f64) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE shadow_lots SET qty = ?1, cost_sol = ?2 WHERE id = ?3",
                params![qty, cost_sol, id],
            )
        })
        .context("failed to update shadow lot")?;
        Ok(())
    }

    pub fn delete_shadow_lot(&self, id: i64) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute("DELETE FROM shadow_lots WHERE id = ?1", params![id])
        })
        .context("failed to delete shadow lot")?;
        Ok(())
    }

    pub fn insert_shadow_closed_trade(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO shadow_closed_trades(
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    entry_cost_sol,
                    exit_value_sol,
                    pnl_sol,
                    opened_ts,
                    closed_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    entry_cost_sol,
                    exit_value_sol,
                    pnl_sol,
                    opened_ts.to_rfc3339(),
                    closed_ts.to_rfc3339(),
                ],
            )
        })
        .context("failed to insert shadow closed trade")?;
        Ok(())
    }

    pub fn shadow_open_lots_count(&self) -> Result<u64> {
        let value: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM shadow_lots", [], |row| row.get(0))
            .context("failed querying shadow open lots count")?;
        Ok(value.max(0) as u64)
    }

    pub fn shadow_open_notional_sol(&self) -> Result<f64> {
        let notional: f64 = self
            .conn
            .query_row(
                "SELECT COALESCE(SUM(cost_sol), 0.0) FROM shadow_lots",
                [],
                |row| row.get(0),
            )
            .context("failed querying shadow open notional")?;
        Ok(notional.max(0.0))
    }

    pub fn shadow_realized_pnl_since(&self, since: DateTime<Utc>) -> Result<(u64, f64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT COUNT(*) as trades, COALESCE(SUM(pnl_sol), 0.0) as pnl
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1",
            )
            .context("failed to prepare shadow pnl query")?;
        let (trades, pnl): (i64, f64) = stmt
            .query_row(params![since.to_rfc3339()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .context("failed querying shadow pnl summary")?;
        Ok((trades.max(0) as u64, pnl))
    }

    pub fn shadow_rug_loss_count_since(
        &self,
        since: DateTime<Utc>,
        return_threshold: f64,
    ) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND entry_cost_sol > 0
                   AND pnl_sol <= entry_cost_sol * ?2",
                params![since.to_rfc3339(), return_threshold],
                |row| row.get(0),
            )
            .context("failed querying shadow rug-loss count since window")?;
        Ok(count.max(0) as u64)
    }

    pub fn shadow_rug_loss_rate_recent(
        &self,
        sample_size: u64,
        return_threshold: f64,
    ) -> Result<(u64, u64, f64)> {
        let limit = sample_size.max(1).min(i64::MAX as u64) as i64;
        let (rug_count, total_count): (i64, i64) = self
            .conn
            .query_row(
                "SELECT
                    COALESCE(
                        SUM(
                            CASE
                                WHEN entry_cost_sol > 0
                                     AND pnl_sol <= entry_cost_sol * ?1
                                THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS rug_count,
                    COUNT(*) AS total_count
                 FROM (
                    SELECT entry_cost_sol, pnl_sol
                    FROM shadow_closed_trades
                    ORDER BY closed_ts DESC, id DESC
                    LIMIT ?2
                 )",
                params![return_threshold, limit],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("failed querying shadow rug-loss recent sample")?;
        let rug_count = rug_count.max(0) as u64;
        let total_count = total_count.max(0) as u64;
        let rug_rate = if total_count > 0 {
            rug_count as f64 / total_count as f64
        } else {
            0.0
        };
        Ok((rug_count, total_count, rug_rate))
    }

    fn read_migration_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(dir)
            .with_context(|| format!("failed to read migrations dir {}", dir.display()))?;
        let mut files = Vec::new();

        for entry in entries {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
                files.push(path);
            }
        }

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
        store.mark_order_submitted(order_id, "paper", "paper:tx-1", now)?;
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
        store.mark_order_submitted(order_id, "paper", "paper:tx-finalize", now)?;

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
        assert_eq!(first, FinalizeExecutionConfirmOutcome::Applied);

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
}

fn rpc_result<'a>(payload: &'a Value) -> &'a Value {
    payload.get("result").unwrap_or(payload)
}

fn is_retryable_sqlite_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("database is locked")
        || lowered.contains("database is busy")
        || lowered.contains("database table is locked")
}

fn is_retryable_sqlite_error(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteFailure(code, message) => {
            matches!(
                code.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) || message
                .as_deref()
                .map(is_retryable_sqlite_message)
                .unwrap_or(false)
        }
        _ => is_retryable_sqlite_message(&error.to_string()),
    }
}

pub fn is_retryable_sqlite_anyhow_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(sqlite_error) = cause.downcast_ref::<rusqlite::Error>() {
            return is_retryable_sqlite_error(sqlite_error);
        }
        is_retryable_sqlite_message(&cause.to_string())
    })
}

fn post_helius_json(client: &Client, helius_http_url: &str, payload: &Value) -> Result<Value> {
    let response = client
        .post(helius_http_url)
        .json(payload)
        .send()
        .with_context(|| format!("failed posting JSON-RPC request to {}", helius_http_url))?
        .error_for_status()
        .with_context(|| format!("non-success JSON-RPC response from {}", helius_http_url))?
        .json::<Value>()
        .context("failed parsing JSON-RPC response body")?;
    if let Some(error) = response.get("error") {
        return Err(anyhow!("JSON-RPC error: {}", error));
    }
    Ok(response)
}

fn fetch_token_holders(client: &Client, helius_http_url: &str, mint: &str) -> Result<u64> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": format!("holders-{}", mint),
        "method": "getTokenAccounts",
        "params": {
            "mint": mint,
            "page": 1,
            "limit": 1
        }
    });
    let response = post_helius_json(client, helius_http_url, &payload)?;
    rpc_result(&response)
        .get("total")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing getTokenAccounts.result.total for mint {}", mint))
}

fn fetch_token_age_seconds(
    client: &Client,
    helius_http_url: &str,
    mint: &str,
    max_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<Option<u64>> {
    let now_ts = Utc::now().timestamp();
    let mut before: Option<String> = None;
    let mut max_age_seconds: Option<u64> = None;

    for page in 0..max_pages {
        let mut options = json!({
            "limit": 1000,
            "commitment": "finalized"
        });
        if let Some(before_sig) = before.as_deref() {
            options["before"] = Value::String(before_sig.to_string());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": format!("token-age-{}-{}", mint, page),
            "method": "getSignaturesForAddress",
            "params": [mint, options]
        });
        let response = post_helius_json(client, helius_http_url, &payload)?;
        let entries = rpc_result(&response)
            .as_array()
            .ok_or_else(|| anyhow!("invalid getSignaturesForAddress result for mint {}", mint))?;
        if entries.is_empty() {
            break;
        }

        let mut oldest_block_time: Option<i64> = None;
        let mut last_signature: Option<String> = None;
        for entry in entries {
            if let Some(value) = entry.get("blockTime").and_then(Value::as_i64) {
                oldest_block_time =
                    Some(oldest_block_time.map_or(value, |current| current.min(value)));
            }
            if let Some(signature) = entry.get("signature").and_then(Value::as_str) {
                last_signature = Some(signature.to_string());
            }
        }

        if let Some(block_time) = oldest_block_time {
            if block_time > 0 && now_ts > block_time {
                let age_seconds = (now_ts - block_time) as u64;
                max_age_seconds =
                    Some(max_age_seconds.map_or(age_seconds, |current| current.max(age_seconds)));
                if min_age_hint_seconds
                    .map(|hint| age_seconds >= hint)
                    .unwrap_or(false)
                {
                    break;
                }
            }
        }

        if entries.len() < 1000 {
            break;
        }
        let Some(signature) = last_signature else {
            break;
        };
        before = Some(signature);
    }

    Ok(max_age_seconds)
}
