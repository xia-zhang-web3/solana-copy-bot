use crate::first_sell_full_exit_report_db_support::{
    bounded_vec, collect_bounded, collect_optional, optional_entry_metadata_expr,
    optional_i64_to_u64, parse_ts,
};
use crate::first_sell_full_exit_report_types::AuditCoverage;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection};
use std::collections::HashSet;

const MAIN_ENTRY_PREFIX: &str = "quote:entry:%";
const ENTRY_DIAG_PREFIX: &str = "quote:entry-shadow-diag:%";

#[derive(Debug, Clone)]
pub(crate) struct EntryEvidence {
    pub(crate) signal_id: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) signal_ts: DateTime<Utc>,
    pub(crate) http_request_started_ts: Option<DateTime<Utc>>,
    pub(crate) actual_entry_ready_ts: Option<DateTime<Utc>>,
    pub(crate) modeled_entry_ready_ts: Option<DateTime<Utc>>,
    pub(crate) copy_signal_status: Option<String>,
    pub(crate) source_cohort: Option<String>,
    pub(crate) discovery_rank: Option<u64>,
    pub(crate) quote_status: String,
    pub(crate) decision_status: Option<String>,
    pub(crate) shadow_gate_status: Option<String>,
    pub(crate) quote_in_amount_raw: Option<String>,
    pub(crate) quote_out_amount_raw: Option<String>,
    pub(crate) quote_response_json: Option<String>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) slippage_bps: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) priority_fee_status: Option<String>,
    pub(crate) priority_fee_lamports: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct SellSignalEvidence {
    pub(crate) signal_id: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct SellQuoteEvidence {
    pub(crate) signal_id: String,
    pub(crate) event_id: String,
    pub(crate) request_ts: DateTime<Utc>,
    pub(crate) quote_status: String,
    pub(crate) quote_in_amount_raw: Option<String>,
    pub(crate) quote_out_amount_raw: Option<String>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) priority_fee_status: Option<String>,
    pub(crate) priority_fee_lamports: Option<u64>,
    pub(crate) decision_status: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CloseEvidence {
    pub(crate) signal_id: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) opened_ts: DateTime<Utc>,
    pub(crate) closed_ts: DateTime<Utc>,
    pub(crate) close_context: String,
}

#[derive(Debug)]
pub(crate) struct AuditEvidence {
    pub(crate) entries: Vec<EntryEvidence>,
    pub(crate) sell_signals: Vec<SellSignalEvidence>,
    pub(crate) sell_quotes: Vec<SellQuoteEvidence>,
    pub(crate) closes: Vec<CloseEvidence>,
    pub(crate) coverage: AuditCoverage,
}

pub(crate) fn load_audit_evidence(
    conn: &Connection,
    since: DateTime<Utc>,
    entry_until: DateTime<Utc>,
    outcome_until: DateTime<Utc>,
    entry_limit: u32,
    related_limit: u32,
) -> Result<AuditEvidence> {
    let (entries, entry_limit_hit) =
        load_entries(conn, since, entry_until, outcome_until, entry_limit.max(1))?;
    let related_since = entries
        .iter()
        .map(|entry| entry.signal_ts)
        .min()
        .map(|entry_since| entry_since.min(since))
        .unwrap_or(since);
    let (sell_signals, sell_limit_hit) =
        load_sell_signals(conn, related_since, outcome_until, related_limit.max(1))?;
    let (sell_quotes, quote_limit_hit) =
        load_sell_quotes(conn, related_since, outcome_until, related_limit.max(1))?;
    let (closes, close_limit_hit) =
        load_closes(conn, related_since, outcome_until, related_limit.max(1))?;
    let coverage = AuditCoverage {
        loaded_entry_events: entries.len() as u64,
        actual_http_entry_events: entries
            .iter()
            .filter(|e| e.http_request_started_ts.is_some())
            .count() as u64,
        unknown_http_entry_events: entries
            .iter()
            .filter(|e| e.http_request_started_ts.is_none())
            .count() as u64,
        actual_entry_ready_events: entries
            .iter()
            .filter(|e| e.actual_entry_ready_ts.is_some())
            .count() as u64,
        loaded_sell_signals: sell_signals.len() as u64,
        loaded_sell_quote_events: sell_quotes.len() as u64,
        loaded_close_outcomes: closes.len() as u64,
        entry_limit_hit,
        sell_limit_hit,
        quote_limit_hit,
        close_limit_hit,
        ..AuditCoverage::default()
    };
    Ok(AuditEvidence {
        entries,
        sell_signals,
        sell_quotes,
        closes,
        coverage,
    })
}

fn load_entries(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    outcome_until: DateTime<Utc>,
    limit: u32,
) -> Result<(Vec<EntryEvidence>, bool)> {
    let actual = copybot_storage_core::quote_http_started_expr(
        conn,
        "execution_quote_canary_events",
        "entry",
    )?;
    let source_expr = optional_entry_metadata_expr(conn, "source_cohort")?;
    let rank_expr = optional_entry_metadata_expr(conn, "discovery_rank")?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT entry.signal_id, entry.wallet_id, entry.token, entry.request_ts,
                    entry.signal_ts, entry.quote_latency_ms, gate.recorded_ts,
                    buy_signal.status, {source_expr}, {rank_expr}, entry.quote_status,
                    entry.decision_status, gate.status, entry.quote_in_amount_raw,
                    entry.quote_out_amount_raw, entry.quote_response_json,
                    entry.quote_price_sol, entry.slippage_bps, entry.route_plan_json,
                    entry.priority_fee_status, entry.priority_fee_lamports, {actual}
             FROM execution_quote_canary_events AS entry
             INDEXED BY idx_execution_quote_canary_events_side_request_ts
             LEFT JOIN execution_quote_canary_events AS diag
               ON diag.event_id = 'quote:entry-shadow-diag:' || entry.signal_id
             LEFT JOIN execution_quote_canary_shadow_gate_events AS gate
               ON gate.signal_id = entry.signal_id AND gate.recorded_ts < ?5
             LEFT JOIN copy_signals AS buy_signal
               ON buy_signal.signal_id = entry.signal_id AND buy_signal.side = 'buy'
             WHERE entry.side = 'buy'
               AND entry.request_ts >= ?1
               AND entry.request_ts < ?2
               AND entry.event_id LIKE ?3
               AND entry.event_id NOT LIKE ?4
             ORDER BY entry.request_ts ASC, entry.event_id ASC
             LIMIT ?6"
        ))
        .context("failed preparing first-sell entry evidence query")?;
    let rows = stmt.query_map(
        params![
            since.to_rfc3339(),
            until.to_rfc3339(),
            MAIN_ENTRY_PREFIX,
            ENTRY_DIAG_PREFIX,
            outcome_until.to_rfc3339(),
            i64::from(limit) + 1,
        ],
        |row| {
            let signal_id: Option<String> = row.get(0)?;
            let Some(signal_id) = signal_id else {
                return Ok(None);
            };
            let request_raw: String = row.get(3)?;
            let request_ts = parse_ts(&request_raw, "entry.request_ts")?;
            let actual = crate::quote_timing::read_start(row, 21)?;
            let signal_raw: Option<String> = row.get(4)?;
            let quote_latency_ms = optional_i64_to_u64(row.get(5)?, 5)?;
            let gate_recorded_raw: Option<String> = row.get(6)?;
            let gate_recorded_ts = gate_recorded_raw
                .as_deref()
                .map(|raw| parse_ts(raw, "entry.gate_recorded_ts"))
                .transpose()?;
            Ok(Some(EntryEvidence {
                signal_id,
                wallet_id: row.get(1)?,
                token: row.get(2)?,
                signal_ts: parse_ts(
                    signal_raw.as_deref().unwrap_or(&request_raw),
                    "entry.signal_ts",
                )?,
                http_request_started_ts: actual,
                actual_entry_ready_ts: actual.and_then(|started| {
                    modeled_entry_ready_ts(started, quote_latency_ms, gate_recorded_ts)
                }),
                modeled_entry_ready_ts: modeled_entry_ready_ts(
                    request_ts,
                    quote_latency_ms,
                    gate_recorded_ts,
                ),
                copy_signal_status: row.get(7)?,
                source_cohort: row.get(8)?,
                discovery_rank: optional_i64_to_u64(row.get(9)?, 9)?,
                quote_status: row.get(10)?,
                decision_status: row.get(11)?,
                shadow_gate_status: row.get(12)?,
                quote_in_amount_raw: row.get(13)?,
                quote_out_amount_raw: row.get(14)?,
                quote_response_json: row.get(15)?,
                quote_price_sol: row.get(16)?,
                slippage_bps: row.get(17)?,
                route_plan_json: row.get(18)?,
                priority_fee_status: row.get(19)?,
                priority_fee_lamports: optional_i64_to_u64(row.get(20)?, 20)?,
            }))
        },
    )?;
    let rows = collect_optional(rows)?;
    let hit = rows.len() > limit as usize;
    let mut seen = HashSet::new();
    let mut entries = rows
        .into_iter()
        .filter(|entry| seen.insert(entry.signal_id.clone()))
        .collect::<Vec<_>>();
    entries.truncate(limit as usize);
    Ok((entries, hit))
}

fn modeled_entry_ready_ts(
    request_ts: DateTime<Utc>,
    quote_latency_ms: Option<u64>,
    gate_recorded_ts: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    let latency = i64::try_from(quote_latency_ms?).ok()?;
    let quote_completed = request_ts.checked_add_signed(Duration::milliseconds(latency))?;
    Some(quote_completed.max(gate_recorded_ts?))
}

fn load_sell_signals(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<(Vec<SellSignalEvidence>, bool)> {
    let mut stmt = conn
        .prepare(
            "SELECT signal_id, wallet_id, token, ts
             FROM copy_signals INDEXED BY idx_copy_signals_status_ts
             WHERE status = 'shadow_recorded'
               AND side = 'sell'
               AND ts >= ?1
               AND ts < ?2
             ORDER BY ts ASC, signal_id ASC
             LIMIT ?3",
        )
        .context("failed preparing first-sell signal query")?;
    let rows = stmt.query_map(
        params![since.to_rfc3339(), until.to_rfc3339(), i64::from(limit) + 1],
        |row| {
            Ok(SellSignalEvidence {
                signal_id: row.get(0)?,
                wallet_id: row.get(1)?,
                token: row.get(2)?,
                ts: parse_ts(&row.get::<_, String>(3)?, "copy_signals.ts")?,
            })
        },
    )?;
    collect_bounded(rows, limit, "first-sell signals")
}

fn load_sell_quotes(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<(Vec<SellQuoteEvidence>, bool)> {
    let mut stmt = conn
        .prepare(
            "SELECT signal_id, event_id, request_ts, quote_status,
                    quote_in_amount_raw, quote_out_amount_raw,
                    quote_price_sol, route_plan_json, priority_fee_status,
                    priority_fee_lamports, decision_status, error
             FROM execution_quote_canary_events
             INDEXED BY idx_execution_quote_canary_events_side_request_ts
             WHERE side = 'sell'
               AND request_ts >= ?1
               AND request_ts < ?2
               AND (event_id LIKE 'quote:close:%'
                    OR event_id LIKE 'quote:owned-close:%'
                    OR event_id LIKE 'quote:owned-stale-close:%')
             ORDER BY request_ts ASC, event_id ASC
             LIMIT ?3",
        )
        .context("failed preparing first-sell quote evidence query")?;
    let rows = stmt.query_map(
        params![since.to_rfc3339(), until.to_rfc3339(), i64::from(limit) + 1],
        |row| {
            let signal_id: Option<String> = row.get(0)?;
            let Some(signal_id) = signal_id else {
                return Ok(None);
            };
            Ok(Some(SellQuoteEvidence {
                signal_id,
                event_id: row.get(1)?,
                request_ts: parse_ts(&row.get::<_, String>(2)?, "sell_quote.request_ts")?,
                quote_status: row.get(3)?,
                quote_in_amount_raw: row.get(4)?,
                quote_out_amount_raw: row.get(5)?,
                quote_price_sol: row.get(6)?,
                route_plan_json: row.get(7)?,
                priority_fee_status: row.get(8)?,
                priority_fee_lamports: optional_i64_to_u64(row.get(9)?, 9)?,
                decision_status: row.get(10)?,
                error: row.get(11)?,
            }))
        },
    )?;
    let rows = collect_optional(rows)?;
    Ok(bounded_vec(rows, limit))
}

fn load_closes(
    conn: &Connection,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<(Vec<CloseEvidence>, bool)> {
    let mut stmt = conn
        .prepare(
            "SELECT signal_id, wallet_id, token, opened_ts, closed_ts,
                    COALESCE(close_context, 'market')
             FROM shadow_closed_trades INDEXED BY idx_shadow_closed_trades_closed_ts
             WHERE closed_ts >= ?1
               AND closed_ts < ?2
             ORDER BY closed_ts ASC, id ASC
             LIMIT ?3",
        )
        .context("failed preparing first-sell close evidence query")?;
    let rows = stmt.query_map(
        params![since.to_rfc3339(), until.to_rfc3339(), i64::from(limit) + 1],
        |row| {
            Ok(CloseEvidence {
                signal_id: row.get(0)?,
                wallet_id: row.get(1)?,
                token: row.get(2)?,
                opened_ts: parse_ts(&row.get::<_, String>(3)?, "close.opened_ts")?,
                closed_ts: parse_ts(&row.get::<_, String>(4)?, "close.closed_ts")?,
                close_context: row.get(5)?,
            })
        },
    )?;
    collect_bounded(rows, limit, "first-sell closes")
}
