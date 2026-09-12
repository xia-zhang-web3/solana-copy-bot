use super::{storage, NativeAccountObservations, ObservationCoverage as Cov};
use crate::{receipt_facts_identity, receipt_facts_rows, SqliteDiscoveryStore};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NativeObservationReport {
    pub since: String,
    pub as_of: String,
    pub source_basis: String,
    pub window_basis: String,
    pub coverage: String,
    pub decomposition: String,
    pub ordering: String,
    pub total_orders: String,
    pub covered_orders: String,
    pub partial_orders: String,
    pub uncovered_orders: String,
    pub conflict_orders: String,
    pub account_rows: String,
    pub instruction_rows: String,
    pub rows_truncated: bool,
    pub rows: Vec<NativeObservationReportRow>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeObservationReportRow {
    pub order_id: String,
    pub operation_at: String,
    pub side: String,
    pub coverage: String,
    pub reason: Option<String>,
    pub observations: Option<NativeAccountObservations>,
}
impl SqliteDiscoveryStore {
    pub fn receipt_native_observations_report(
        &self,
        since: DateTime<Utc>,
        as_of: DateTime<Utc>,
        limit: u32,
    ) -> Result<NativeObservationReport> {
        let tx = self.conn.unchecked_transaction()?;
        let report = on_conn(&tx, since, as_of, limit)?;
        tx.commit()?;
        Ok(report)
    }
}
pub(crate) fn on_conn(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
    limit: u32,
) -> Result<NativeObservationReport> {
    ensure!(since <= as_of, "native observation report window invalid");
    let schema:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='execution_receipt_native_observations')",[],|r|r.get(0))?;
    let mut report = NativeObservationReport {
        since: since.to_rfc3339(),
        as_of: as_of.to_rfc3339(),
        source_basis: "successful_getTransaction_jsonParsed_confirmed".into(),
        window_basis: "original_order_submit_time".into(),
        decomposition: "unresolved".into(),
        ordering: "rpc_outer_inner_positions_only_no_total_cpi_order".into(),
        ..Default::default()
    };
    let (
        mut total,
        mut covered,
        mut partial,
        mut uncovered,
        mut conflicts,
        mut accounts,
        mut instructions,
    ) = (0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64);
    let mut stmt=conn.prepare("SELECT o.order_id,o.submit_ts,lower(s.side) FROM orders o JOIN copy_signals s ON s.signal_id=o.signal_id WHERE o.order_id LIKE 'exec-canary:%' AND (o.status IN ('execution_canary_confirmed','execution_canary_confirmed_unreconciled') OR EXISTS(SELECT 1 FROM fills f WHERE f.order_id=o.order_id)) AND julianday(o.submit_ts)>=julianday(?1) AND julianday(o.submit_ts)<julianday(?2) ORDER BY o.submit_ts,o.order_id")?;
    let rows = stmt.query_map(params![report.since, report.as_of], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (id, at, side) = row?;
        total += 1;
        let data = if schema {
            storage::load(conn, &id)?
        } else {
            None
        };
        let (observation, mut reason) = data
            .map(|(o, r)| (Some(o), r))
            .unwrap_or((None, Some("legacy_or_unavailable_no_observations".into())));
        if let Some(o) = &observation {
            let facts = receipt_facts_rows::load(conn, &id)?;
            if facts.as_ref().is_none_or(|f| {
                f.tx_signature != o.tx_signature
                    || f.wallet_pubkey != o.wallet_pubkey
                    || f.token != o.token
                    || f.side != o.side
                    || f.slot.to_string() != o.slot
            }) {
                reason = Some("native_observation_binding_conflict".into());
            }
            if let Some(f) = &facts {
                if let Err(e) = receipt_facts_identity::validate_identity(conn, f) {
                    if e.is::<crate::receipt_facts_identity::ReceiptFactsIdentityRejection>() {
                        reason = Some("native_observation_binding_conflict".into());
                    } else {
                        return Err(e);
                    }
                }
            }
            accounts += o.accounts.len() as u64;
            instructions += o.instructions.len() as u64;
        }
        let coverage = if observation.is_none() {
            uncovered += 1;
            "uncovered"
        } else if reason.is_some() {
            conflicts += 1;
            "conflict"
        } else if observation.as_ref().is_some_and(|o| {
            o.accounts_coverage == Cov::Known && o.instructions_coverage == Cov::Known
        }) {
            covered += 1;
            "covered_observations"
        } else {
            partial += 1;
            "partial"
        };
        if report.rows.len() < (limit.min(100) as usize) {
            report.rows.push(NativeObservationReportRow {
                order_id: id,
                operation_at: at,
                side,
                coverage: coverage.into(),
                reason,
                observations: observation,
            });
        }
    }
    report.total_orders = total.to_string();
    report.covered_orders = covered.to_string();
    report.partial_orders = partial.to_string();
    report.uncovered_orders = uncovered.to_string();
    report.conflict_orders = conflicts.to_string();
    report.account_rows = accounts.to_string();
    report.instruction_rows = instructions.to_string();
    report.rows_truncated = report.rows.len() < (total as usize);
    report.coverage = if !schema {
        "schema_unavailable"
    } else if total == 0 {
        "empty_unknown"
    } else if uncovered + partial + conflicts > 0 {
        "partial_unresolved"
    } else {
        "covered_observations"
    }
    .into();
    Ok(report)
}
