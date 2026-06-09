use crate::{
    ExecutionTinyEntryFunnel, ExecutionTinyEntryFunnelBucket,
    ExecutionTinyEntryFunnelDropReasonCount, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_FAILED,
    EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::BTreeMap;

const QUOTE_STATUS_OK: &str = "ok";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_SKIP: &str = "would_skip";
const SHADOW_RECORDED: &str = "shadow_recorded";
const SHADOW_DROPPED: &str = "shadow_dropped";
const SHADOW_PENDING: &str = "shadow_pending";
const MISSING: &str = "missing";

impl SqliteDiscoveryStore {
    pub fn execution_tiny_entry_funnel(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionTinyEntryFunnel> {
        let mut stmt = self
            .conn
            .prepare(ENTRY_FUNNEL_SQL)
            .context("failed to prepare execution tiny entry funnel query")?;
        let rows = stmt
            .query_map(
                params![since.to_rfc3339(), i64::from(limit.max(1))],
                read_entry_funnel_row,
            )
            .context("failed querying execution tiny entry funnel rows")?;
        let rows = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution tiny entry funnel rows")?;
        Ok(summarize_entry_funnel(rows))
    }
}

#[derive(Debug)]
struct EntryFunnelRow {
    quote_status: String,
    decision_status: String,
    shadow_gate_status: String,
    shadow_gate_reason: String,
    order_status: String,
    err_code: String,
    simulation_status: String,
    quote_source: String,
}

fn read_entry_funnel_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EntryFunnelRow> {
    Ok(EntryFunnelRow {
        quote_status: string_or_missing(row.get(0)?),
        decision_status: string_or_missing(row.get(1)?),
        shadow_gate_status: string_or_default(row.get(2)?, SHADOW_PENDING),
        shadow_gate_reason: string_or_default(row.get(3)?, "unknown"),
        order_status: string_or_missing(row.get(4)?),
        err_code: string_or_missing(row.get(5)?),
        simulation_status: string_or_missing(row.get(6)?),
        quote_source: string_or_default(row.get(7)?, "not_ordered"),
    })
}

fn summarize_entry_funnel(rows: Vec<EntryFunnelRow>) -> ExecutionTinyEntryFunnel {
    let mut summary = ExecutionTinyEntryFunnel::default();
    let mut drop_reasons = BTreeMap::<String, u64>::new();
    let mut execute_drop_reasons = BTreeMap::<String, u64>::new();
    let mut buckets = BTreeMap::<
        (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
        u64,
    >::new();
    for row in rows {
        summary.total_buy_quote_events += 1;
        if row.quote_status == QUOTE_STATUS_OK {
            summary.quote_ok_events += 1;
        }
        let would_execute = row.decision_status == DECISION_WOULD_EXECUTE;
        let actionable = row.shadow_gate_status == SHADOW_RECORDED && would_execute;
        match row.decision_status.as_str() {
            DECISION_WOULD_EXECUTE => summary.quote_would_execute_events += 1,
            DECISION_WOULD_SKIP => summary.quote_would_skip_events += 1,
            _ => {}
        }
        match row.shadow_gate_status.as_str() {
            SHADOW_RECORDED => {
                summary.shadow_recorded_events += 1;
                if would_execute {
                    summary.quote_would_execute_shadow_recorded_events += 1;
                    summary.actionable_quote_would_execute_events += 1;
                }
            }
            SHADOW_DROPPED => {
                summary.shadow_dropped_events += 1;
                if would_execute {
                    summary.quote_would_execute_shadow_dropped_events += 1;
                }
                *drop_reasons
                    .entry(row.shadow_gate_reason.clone())
                    .or_insert(0) += 1;
                if would_execute {
                    *execute_drop_reasons
                        .entry(row.shadow_gate_reason.clone())
                        .or_insert(0) += 1;
                }
            }
            _ => {
                summary.shadow_pending_events += 1;
                if would_execute {
                    summary.quote_would_execute_shadow_pending_events += 1;
                }
            }
        }
        match row.order_status.as_str() {
            MISSING => {
                summary.tiny_missing_order_events += 1;
                if actionable {
                    summary.actionable_tiny_missing_order_events += 1;
                }
                match row.shadow_gate_status.as_str() {
                    SHADOW_RECORDED => summary.tiny_missing_order_shadow_recorded_events += 1,
                    SHADOW_DROPPED => summary.tiny_missing_order_shadow_dropped_events += 1,
                    _ => summary.tiny_missing_order_shadow_pending_events += 1,
                }
            }
            EXECUTION_STATUS_CANARY_CONFIRMED => {
                summary.tiny_ordered_events += 1;
                summary.tiny_confirmed_events += 1;
                if actionable {
                    summary.actionable_tiny_ordered_events += 1;
                    summary.actionable_tiny_confirmed_events += 1;
                }
            }
            EXECUTION_STATUS_CANARY_FAILED => {
                summary.tiny_ordered_events += 1;
                summary.tiny_failed_events += 1;
                if actionable {
                    summary.actionable_tiny_ordered_events += 1;
                }
            }
            EXECUTION_STATUS_CANARY_SUBMIT_DISABLED => {
                summary.tiny_ordered_events += 1;
                summary.tiny_submit_disabled_events += 1;
                if actionable {
                    summary.actionable_tiny_ordered_events += 1;
                }
            }
            _ => {
                summary.tiny_ordered_events += 1;
                if actionable {
                    summary.actionable_tiny_ordered_events += 1;
                }
            }
        }
        *buckets
            .entry((
                row.quote_source,
                row.quote_status,
                row.decision_status,
                row.shadow_gate_status,
                row.shadow_gate_reason,
                row.order_status,
                row.err_code,
                row.simulation_status,
            ))
            .or_insert(0) += 1;
    }
    summary.buckets = buckets
        .into_iter()
        .map(
            |(
                (
                    quote_source,
                    quote_status,
                    decision_status,
                    shadow_gate_status,
                    shadow_gate_reason,
                    order_status,
                    err_code,
                    simulation_status,
                ),
                events,
            )| ExecutionTinyEntryFunnelBucket {
                quote_source,
                quote_status,
                decision_status,
                shadow_gate_status,
                shadow_gate_reason,
                order_status,
                err_code,
                simulation_status,
                events,
            },
        )
        .collect();
    summary.shadow_drop_reason_counts = drop_reasons
        .into_iter()
        .map(|(reason, events)| ExecutionTinyEntryFunnelDropReasonCount { reason, events })
        .collect();
    summary.quote_would_execute_shadow_drop_reason_counts = execute_drop_reasons
        .into_iter()
        .map(|(reason, events)| ExecutionTinyEntryFunnelDropReasonCount { reason, events })
        .collect();
    summary
}

fn string_or_missing(value: Option<String>) -> String {
    string_or_default(value, MISSING)
}

fn string_or_default(value: Option<String>, default: &str) -> String {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

const ENTRY_FUNNEL_SQL: &str = "
SELECT
    quote.quote_status,
    quote.decision_status,
    gate.status,
    gate.reason,
    orders.status,
    orders.err_code,
    orders.simulation_status,
    metadata.quote_source
FROM (
    SELECT event_id, signal_id, quote_status, decision_status, request_ts
    FROM execution_quote_canary_events
    WHERE lower(side) = 'buy'
      AND request_ts >= ?1
    ORDER BY request_ts DESC, event_id DESC
    LIMIT ?2
) AS quote
LEFT JOIN execution_quote_canary_shadow_gate_events AS gate
    ON gate.signal_id = quote.signal_id
LEFT JOIN orders
    ON orders.order_id = (
        SELECT candidate.order_id
        FROM orders AS candidate
        WHERE candidate.signal_id = quote.signal_id
          AND candidate.order_id LIKE 'exec-canary:%'
        ORDER BY candidate.submit_ts DESC, candidate.order_id DESC
        LIMIT 1
    )
LEFT JOIN execution_canary_build_plan_metadata AS metadata
    ON metadata.order_id = orders.order_id
ORDER BY quote.request_ts DESC";
