use crate::{
    ExecutionCanaryQuoteShadowGateReasonCount, ExecutionCanaryQuoteShadowGateSummary,
    SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

pub const EXECUTION_QUOTE_CANARY_SHADOW_GATE_RECORDED: &str = "shadow_recorded";
pub const EXECUTION_QUOTE_CANARY_SHADOW_GATE_DROPPED: &str = "shadow_dropped";

const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_SKIP: &str = "would_skip";

pub(crate) fn ensure_execution_quote_canary_shadow_gate_table(
    store: &SqliteDiscoveryStore,
) -> Result<()> {
    store
        .conn
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS execution_quote_canary_shadow_gate_events (
                signal_id TEXT PRIMARY KEY,
                wallet_id TEXT NOT NULL,
                token TEXT NOT NULL,
                side TEXT NOT NULL CHECK(lower(side) IN ('buy', 'sell')),
                status TEXT NOT NULL CHECK(status IN ('shadow_recorded', 'shadow_dropped')),
                reason TEXT,
                recorded_ts TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_shadow_gate_request
                ON execution_quote_canary_shadow_gate_events(side, recorded_ts);",
        )
        .context("failed ensuring execution quote canary shadow gate schema")
        .map(|_| ())
}

impl SqliteDiscoveryStore {
    pub fn record_execution_quote_canary_shadow_gate_event(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        side: &str,
        status: &str,
        reason: Option<&str>,
        recorded_ts: DateTime<Utc>,
    ) -> Result<()> {
        ensure_execution_quote_canary_shadow_gate_table(self)?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO execution_quote_canary_shadow_gate_events(
                    signal_id,
                    wallet_id,
                    token,
                    side,
                    status,
                    reason,
                    recorded_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(signal_id) DO UPDATE SET
                    wallet_id = excluded.wallet_id,
                    token = excluded.token,
                    side = excluded.side,
                    status = excluded.status,
                    reason = excluded.reason,
                    recorded_ts = excluded.recorded_ts",
                params![
                    signal_id,
                    wallet_id,
                    token,
                    side,
                    status,
                    reason,
                    recorded_ts.to_rfc3339(),
                ],
            )
        })
        .with_context(|| {
            format!("failed recording execution quote canary shadow gate event {signal_id}")
        })?;
        Ok(())
    }

    pub fn execution_quote_canary_shadow_gate_summary(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionCanaryQuoteShadowGateSummary> {
        ensure_execution_quote_canary_shadow_gate_table(self)?;
        let rows = self.execution_quote_canary_shadow_gate_rows(since, limit)?;
        Ok(summarize_shadow_gate_rows(rows))
    }

    fn execution_quote_canary_shadow_gate_rows(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ShadowGateRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    event.decision_status,
                    gate.status,
                    gate.reason
                 FROM (
                    SELECT signal_id, decision_status, request_ts, event_id
                    FROM execution_quote_canary_events
                    WHERE lower(side) = 'buy'
                      AND request_ts >= ?1
                    ORDER BY request_ts DESC, event_id DESC
                    LIMIT ?2
                 ) AS event
                 LEFT JOIN execution_quote_canary_shadow_gate_events AS gate
                    ON gate.signal_id = event.signal_id
                 ORDER BY event.request_ts DESC, event.event_id DESC",
            )
            .context("failed to prepare execution quote canary shadow gate summary query")?;
        let rows = stmt
            .query_map(
                params![since.to_rfc3339(), i64::from(limit.max(1))],
                |row| {
                    Ok(ShadowGateRow {
                        decision_status: row.get(0)?,
                        gate_status: row.get(1)?,
                        gate_reason: row.get(2)?,
                    })
                },
            )
            .context("failed querying execution quote canary shadow gate summary")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution quote canary shadow gate summary")
    }
}

struct ShadowGateRow {
    decision_status: Option<String>,
    gate_status: Option<String>,
    gate_reason: Option<String>,
}

fn summarize_shadow_gate_rows(rows: Vec<ShadowGateRow>) -> ExecutionCanaryQuoteShadowGateSummary {
    let mut summary = ExecutionCanaryQuoteShadowGateSummary::default();
    let mut reason_counts = std::collections::BTreeMap::<String, u64>::new();
    let mut execute_reason_counts = std::collections::BTreeMap::<String, u64>::new();

    for row in rows {
        record_quote_decision(&mut summary, row.decision_status.as_deref());
        record_gate_status(
            &mut summary,
            &mut reason_counts,
            &mut execute_reason_counts,
            &row,
        );
    }

    summary.drop_reason_counts = reason_counts
        .into_iter()
        .map(|(reason, events)| ExecutionCanaryQuoteShadowGateReasonCount { reason, events })
        .collect();
    summary.quote_would_execute_drop_reason_counts = execute_reason_counts
        .into_iter()
        .map(|(reason, events)| ExecutionCanaryQuoteShadowGateReasonCount { reason, events })
        .collect();
    summary
}

fn record_quote_decision(
    summary: &mut ExecutionCanaryQuoteShadowGateSummary,
    decision_status: Option<&str>,
) {
    summary.total_buy_quote_events += 1;
    match decision_status {
        Some(DECISION_WOULD_EXECUTE) => summary.quote_would_execute_events += 1,
        Some(DECISION_WOULD_SKIP) => summary.quote_would_skip_events += 1,
        _ => summary.quote_unknown_events += 1,
    }
}

fn record_gate_status(
    summary: &mut ExecutionCanaryQuoteShadowGateSummary,
    reason_counts: &mut std::collections::BTreeMap<String, u64>,
    execute_reason_counts: &mut std::collections::BTreeMap<String, u64>,
    row: &ShadowGateRow,
) {
    match row.gate_status.as_deref() {
        Some(EXECUTION_QUOTE_CANARY_SHADOW_GATE_RECORDED) => {
            summary.shadow_gate_sampled_events += 1;
            summary.shadow_recorded_events += 1;
            if row.decision_status.as_deref() == Some(DECISION_WOULD_EXECUTE) {
                summary.quote_would_execute_shadow_recorded_events += 1;
            }
        }
        Some(EXECUTION_QUOTE_CANARY_SHADOW_GATE_DROPPED) => {
            summary.shadow_gate_sampled_events += 1;
            summary.shadow_dropped_events += 1;
            let would_execute = row.decision_status.as_deref() == Some(DECISION_WOULD_EXECUTE);
            if would_execute {
                summary.quote_would_execute_shadow_dropped_events += 1;
            }
            let reason = row
                .gate_reason
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            *reason_counts.entry(reason).or_insert(0) += 1;
            if would_execute {
                let reason = row
                    .gate_reason
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                *execute_reason_counts.entry(reason).or_insert(0) += 1;
            }
        }
        _ => {
            summary.shadow_pending_events += 1;
            if row.decision_status.as_deref() == Some(DECISION_WOULD_EXECUTE) {
                summary.quote_would_execute_shadow_pending_events += 1;
            }
        }
    }
}
