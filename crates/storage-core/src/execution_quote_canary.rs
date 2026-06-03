use crate::{
    observed_row::parse_sqlite_slot, observed_timestamp::parse_rfc3339_utc,
    ExecutionCanaryCloseCandidate, ExecutionCanaryObservedLeg, ExecutionQuoteCanaryEventInsert,
    ExecutionQuoteCanaryRecordOutcome, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports};
use rusqlite::{params, OptionalExtension};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

pub(crate) fn ensure_execution_quote_canary_tables(store: &SqliteDiscoveryStore) -> Result<()> {
    store
        .conn
        .execute_batch(EXECUTION_QUOTE_CANARY_SCHEMA)
        .context("failed ensuring execution quote canary schema")
}

impl SqliteDiscoveryStore {
    pub fn load_copy_signal_by_signal_id(&self, signal_id: &str) -> Result<Option<CopySignalRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    signal_id,
                    wallet_id,
                    side,
                    token,
                    notional_sol,
                    notional_lamports,
                    notional_origin,
                    ts,
                    status
                 FROM copy_signals
                 WHERE signal_id = ?1
                 LIMIT 1",
            )
            .context("failed to prepare copy signal lookup by signal_id")?;
        let mut rows = stmt
            .query(params![signal_id])
            .context("failed querying copy signal by signal_id")?;
        let Some(row) = rows
            .next()
            .context("failed iterating copy signal by signal_id")?
        else {
            return Ok(None);
        };
        Ok(Some(copy_signal_from_row(row)?))
    }

    pub fn list_execution_quote_canary_entry_candidates(
        &self,
        copy_signal_status: &str,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CopySignalRow>> {
        ensure_execution_quote_canary_tables(self)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    signal_id,
                    wallet_id,
                    side,
                    token,
                    notional_sol,
                    notional_lamports,
                    notional_origin,
                    ts,
                    status
                 FROM copy_signals
                 WHERE status = ?1
                   AND ts >= ?2
                   AND lower(side) = 'buy'
                   AND NOT EXISTS (
                        SELECT 1 FROM execution_quote_canary_events
                        WHERE execution_quote_canary_events.signal_id = copy_signals.signal_id
                          AND lower(execution_quote_canary_events.side) = 'buy'
                   )
                 ORDER BY ts ASC
                 LIMIT ?3",
            )
            .context("failed to prepare execution quote canary entry candidate query")?;
        let mut rows = stmt
            .query(params![
                copy_signal_status,
                since.to_rfc3339(),
                limit.max(1) as i64
            ])
            .context("failed querying execution quote canary entry candidates")?;

        let mut signals = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating execution quote canary entry candidates")?
        {
            signals.push(copy_signal_from_row(row)?);
        }
        Ok(signals)
    }

    pub fn list_execution_quote_canary_close_candidates_for_signal(
        &self,
        signal_id: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryCloseCandidate>> {
        ensure_execution_quote_canary_tables(self)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    id,
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    qty_raw,
                    qty_decimals,
                    exit_value_sol,
                    closed_ts
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                   AND COALESCE(close_context, 'market') = 'market'
                   AND signal_id NOT LIKE 'stale-close-%'
                   AND NOT EXISTS (
                        SELECT 1 FROM execution_quote_canary_events
                        WHERE execution_quote_canary_events.shadow_closed_trade_id = shadow_closed_trades.id
                          AND lower(execution_quote_canary_events.side) = 'sell'
                   )
                 ORDER BY closed_ts ASC, id ASC
                 LIMIT ?2",
            )
            .context("failed to prepare execution quote canary close candidate by signal query")?;
        let mut rows = stmt
            .query(params![signal_id, limit.max(1) as i64])
            .context("failed querying execution quote canary close candidates by signal")?;

        let mut candidates = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating execution quote canary close candidates by signal")?
        {
            candidates.push(close_candidate_from_row(row)?);
        }
        Ok(candidates)
    }

    pub fn list_execution_quote_canary_close_candidates(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryCloseCandidate>> {
        ensure_execution_quote_canary_tables(self)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    id,
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    qty_raw,
                    qty_decimals,
                    exit_value_sol,
                    closed_ts
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND COALESCE(close_context, 'market') = 'market'
                   AND signal_id NOT LIKE 'stale-close-%'
                   AND NOT EXISTS (
                        SELECT 1 FROM execution_quote_canary_events
                        WHERE execution_quote_canary_events.shadow_closed_trade_id = shadow_closed_trades.id
                          AND lower(execution_quote_canary_events.side) = 'sell'
                   )
                 ORDER BY closed_ts ASC, id ASC
                 LIMIT ?2",
            )
            .context("failed to prepare execution quote canary close candidate query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), limit.max(1) as i64])
            .context("failed querying execution quote canary close candidates")?;

        let mut candidates = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating execution quote canary close candidates")?
        {
            candidates.push(close_candidate_from_row(row)?);
        }
        Ok(candidates)
    }

    pub fn load_execution_canary_observed_leg_by_signature(
        &self,
        signature: &str,
    ) -> Result<Option<ExecutionCanaryObservedLeg>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    signature,
                    wallet_id,
                    CASE WHEN token_in = ?1 THEN 1 ELSE 0 END AS is_buy,
                    CASE WHEN token_in = ?1 THEN token_out ELSE token_in END AS token_mint,
                    CASE WHEN token_in = ?1 THEN qty_out ELSE qty_in END AS token_qty,
                    CASE WHEN token_in = ?1 THEN qty_in ELSE qty_out END AS sol_notional,
                    CASE WHEN token_in = ?1 THEN qty_out_raw ELSE qty_in_raw END AS token_raw_amount,
                    CASE WHEN token_in = ?1 THEN qty_out_decimals ELSE qty_in_decimals END AS token_decimals,
                    slot,
                    ts
                 FROM observed_swaps
                 WHERE signature = ?2
                   AND (token_in = ?1 OR token_out = ?1)
                 LIMIT 1",
            )
            .context("failed to prepare execution canary observed leg lookup")?;
        stmt.query_row(params![SOL_MINT, signature], |row| {
            let is_buy_raw: i64 = row.get(2)?;
            let token_decimals_raw: Option<i64> = row.get(7)?;
            let slot_raw: i64 = row.get(8)?;
            let ts_raw: String = row.get(9)?;
            let token_decimals = token_decimals_raw
                .map(|value| {
                    u8::try_from(value).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })
                })
                .transpose()?;
            let slot = parse_sqlite_slot(slot_raw, "observed_swaps.slot").map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    8,
                    rusqlite::types::Type::Integer,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error.to_string(),
                    )),
                )
            })?;
            let ts_utc = parse_rfc3339_utc(&ts_raw, "observed_swaps.ts").map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error.to_string(),
                    )),
                )
            })?;
            Ok(ExecutionCanaryObservedLeg {
                signature: row.get(0)?,
                wallet_id: row.get(1)?,
                is_buy: is_buy_raw == 1,
                token_mint: row.get(3)?,
                token_qty: row.get(4)?,
                sol_notional: row.get(5)?,
                token_raw_amount: row.get(6)?,
                token_decimals,
                slot,
                ts_utc,
            })
        })
        .optional()
        .context("failed loading execution canary observed leg by signature")
    }

    pub fn record_execution_quote_canary_event(
        &self,
        event: &ExecutionQuoteCanaryEventInsert,
    ) -> Result<ExecutionQuoteCanaryRecordOutcome> {
        ensure_execution_quote_canary_tables(self)?;
        let quote_latency_ms = optional_u64_to_i64(
            "execution_quote_canary_events.quote_latency_ms",
            event.quote_latency_ms,
        )?;
        let decision_delay_ms = optional_u64_to_i64(
            "execution_quote_canary_events.decision_delay_ms",
            event.decision_delay_ms,
        )?;
        let priority_fee_lamports = optional_u64_to_i64(
            "execution_quote_canary_events.priority_fee_lamports",
            event.priority_fee_lamports,
        )?;
        let inserted = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO execution_quote_canary_events(
                        event_id,
                        signal_id,
                        shadow_closed_trade_id,
                        wallet_id,
                        token,
                        side,
                        quote_status,
                        request_ts,
                        signal_ts,
                        decision_delay_ms,
                        quote_latency_ms,
                        leader_notional_sol,
                        quote_in_amount_raw,
                        quote_out_amount_raw,
                        quote_price_sol,
                        shadow_price_sol,
                        slippage_bps,
                        price_impact_pct,
                        route_plan_json,
                        priority_fee_status,
                        priority_fee_lamports,
                        priority_fee_json,
                        decision_status,
                        decision_reason,
                        error
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                        ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23,
                        ?24, ?25
                    )",
                    params![
                        &event.event_id,
                        event.signal_id.as_deref(),
                        event.shadow_closed_trade_id,
                        &event.wallet_id,
                        &event.token,
                        &event.side,
                        &event.quote_status,
                        event.request_ts.to_rfc3339(),
                        event.signal_ts.as_ref().map(DateTime::to_rfc3339),
                        decision_delay_ms,
                        quote_latency_ms,
                        event.leader_notional_sol,
                        event.quote_in_amount_raw.as_deref(),
                        event.quote_out_amount_raw.as_deref(),
                        event.quote_price_sol,
                        event.shadow_price_sol,
                        event.slippage_bps,
                        event.price_impact_pct,
                        event.route_plan_json.as_deref(),
                        event.priority_fee_status.as_deref(),
                        priority_fee_lamports,
                        event.priority_fee_json.as_deref(),
                        event.decision_status.as_deref(),
                        event.decision_reason.as_deref(),
                        event.error.as_deref(),
                    ],
                )
            })
            .context("failed recording execution quote canary event")?;
        if inserted > 0 {
            Ok(ExecutionQuoteCanaryRecordOutcome::Inserted)
        } else {
            Ok(ExecutionQuoteCanaryRecordOutcome::Existing)
        }
    }
}

fn copy_signal_from_row(row: &rusqlite::Row<'_>) -> Result<CopySignalRow> {
    let ts_raw: String = row.get(7).context("failed reading copy_signals.ts")?;
    let notional_lamports_raw: Option<i64> = row
        .get(5)
        .context("failed reading copy_signals.notional_lamports")?;
    let notional_lamports = notional_lamports_raw
        .map(|value| {
            u64::try_from(value)
                .map(Lamports::new)
                .with_context(|| format!("invalid copy_signals.notional_lamports: {value}"))
        })
        .transpose()?;
    Ok(CopySignalRow {
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
        notional_lamports,
        notional_origin: row
            .get(6)
            .context("failed reading copy_signals.notional_origin")?,
        ts: parse_rfc3339_utc(&ts_raw, "copy_signals.ts")?,
        status: row.get(8).context("failed reading copy_signals.status")?,
    })
}

fn close_candidate_from_row(row: &rusqlite::Row<'_>) -> Result<ExecutionCanaryCloseCandidate> {
    let qty_decimals_raw: Option<i64> = row
        .get(6)
        .context("failed reading shadow_closed_trades.qty_decimals")?;
    let qty_decimals = qty_decimals_raw
        .map(|value| {
            u8::try_from(value)
                .with_context(|| format!("invalid shadow_closed_trades.qty_decimals: {value}"))
        })
        .transpose()?;
    let closed_ts_raw: String = row
        .get(8)
        .context("failed reading shadow_closed_trades.closed_ts")?;
    Ok(ExecutionCanaryCloseCandidate {
        id: row
            .get(0)
            .context("failed reading shadow_closed_trades.id")?,
        signal_id: row
            .get(1)
            .context("failed reading shadow_closed_trades.signal_id")?,
        wallet_id: row
            .get(2)
            .context("failed reading shadow_closed_trades.wallet_id")?,
        token: row
            .get(3)
            .context("failed reading shadow_closed_trades.token")?,
        qty: row
            .get(4)
            .context("failed reading shadow_closed_trades.qty")?,
        qty_raw: row
            .get(5)
            .context("failed reading shadow_closed_trades.qty_raw")?,
        qty_decimals,
        exit_value_sol: row
            .get(7)
            .context("failed reading shadow_closed_trades.exit_value_sol")?,
        closed_ts: parse_rfc3339_utc(&closed_ts_raw, "shadow_closed_trades.closed_ts")?,
    })
}

fn optional_u64_to_i64(field: &str, value: Option<u64>) -> Result<Option<i64>> {
    value
        .map(|raw| i64::try_from(raw).with_context(|| format!("{field} exceeds i64: {raw}")))
        .transpose()
}

const EXECUTION_QUOTE_CANARY_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS execution_quote_canary_events (
    event_id TEXT PRIMARY KEY,
    signal_id TEXT,
    shadow_closed_trade_id INTEGER,
    wallet_id TEXT NOT NULL,
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK(lower(side) IN ('buy', 'sell')),
    quote_status TEXT NOT NULL,
    request_ts TEXT NOT NULL,
    signal_ts TEXT,
    decision_delay_ms INTEGER,
    quote_latency_ms INTEGER,
    leader_notional_sol REAL,
    quote_in_amount_raw TEXT,
    quote_out_amount_raw TEXT,
    quote_price_sol REAL,
    shadow_price_sol REAL,
    slippage_bps REAL,
    price_impact_pct REAL,
    route_plan_json TEXT,
    priority_fee_status TEXT,
    priority_fee_lamports INTEGER,
    priority_fee_json TEXT,
    decision_status TEXT,
    decision_reason TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_signal_side
    ON execution_quote_canary_events(signal_id, side);
CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_closed_side
    ON execution_quote_canary_events(shadow_closed_trade_id, side);
CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_side_request_ts
    ON execution_quote_canary_events(side, request_ts);
CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_decision_request_ts
    ON execution_quote_canary_events(decision_status, request_ts);
";
