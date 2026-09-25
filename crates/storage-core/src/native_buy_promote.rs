use super::{cohort, verify, NativeBuyCandidate, NativeBuyPending, NativeBuySourceAmounts, SPL_TOKEN_PROGRAM, STATUS};
use crate::{ExecutionCanaryOrder, SqliteDiscoveryStore};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS;
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    /// Bounded discovery of original captured BUY admissions awaiting external
    /// finalized proof. Invalidated sessions/cohorts are skipped, never repaired.
    pub fn list_native_buy_pending(&self, limit: u32) -> Result<Vec<NativeBuyPending>> {
        let limit = limit.min(64).max(1);
        let ids = scan_native_buy_ids(&self.conn, "pending", limit)?;
        let now = Utc::now();
        let mut out = Vec::new();
        for id in ids {
            let Some(d) = verify::load(&self.conn, &id)? else { continue; };
            if verify::valid(&self.conn, &d, now, None, false)? {
                if let Some(value) = verify::pending(&d) { out.push(value); }
            }
        }
        Ok(out)
    }

    /// Resume the crash gap after a finalized source proof. Existing financial
    /// dispatch obligations are deliberately handled by receipt reconciliation.
    pub fn list_native_buy_finalized(
        &self,
        limit: u32,
        now: DateTime<Utc>,
        max_age_seconds: u64,
    ) -> Result<Vec<NativeBuyPending>> {
        let limit = limit.min(64).max(1);
        let ids = scan_native_buy_ids(&self.conn, "finalized", limit)?;
        let mut out = Vec::new();
        for id in ids {
            let Some(d) = verify::load(&self.conn, &id)? else { continue; };
            if verify::valid(&self.conn, &d, now, Some(max_age_seconds), true)? {
                if let Some(value) = verify::pending(&d) { out.push(value); }
            }
        }
        Ok(out)
    }

    /// Read-only signed BUY obligations for strict-mode restart. A durable
    /// dispatch is mandatory; this never reselects ordinary retry candidates.
    pub fn list_native_buy_receipt_obligations(
        &self,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        let limit = limit.min(64).max(1);
        let mut q = self.conn.prepare("SELECT o.order_id FROM orders o
            JOIN native_buy_decisions n ON n.signal_id=o.signal_id
            JOIN execution_canary_dispatch d ON d.order_id=o.order_id AND d.signal_id=o.signal_id AND d.side='buy'
            WHERE o.signal_id LIKE 'native-buy-v1:%'
              AND o.status IN ('execution_canary_submitted',
                'execution_canary_confirmed_unreconciled','execution_canary_confirmed',
                'execution_canary_failed','execution_canary_expired')
              AND NOT EXISTS(SELECT 1 FROM fills f WHERE f.order_id=o.order_id)
            ORDER BY o.submit_ts,o.order_id LIMIT ?1")?;
        let ids = q.query_map([limit], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter().map(|id| self.load_execution_canary_order(id)?
            .ok_or_else(|| anyhow::anyhow!("native_buy_receipt_order_missing"))).collect()
    }

    /// The caller must validate a request-bound finalized getTransaction and
    /// mint owner before calling this. Storage independently binds its identity
    /// to the first admission and provider terminal. This is not a receipt fill.
    pub fn native_buy_record_finalized(
        &self,
        signature: &str,
        slot: u64,
        token_program: &str,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        if token_program != SPL_TOKEN_PROGRAM || slot > i64::MAX as u64 { return Ok(false); }
        self.with_immediate_transaction_retry("native BUY finalized proof", |conn| {
            let Some(d) = verify::load(conn, signature)? else { return Ok(false); };
            if d.slot != slot as i64 || !verify::valid(conn, &d, now, None, false)? {
                return Ok(false);
            }
            if let Some(old) = d.finalized_at {
                return Ok(d.finalized_slot == Some(slot as i64)
                    && DateTime::parse_from_rfc3339(&old).is_ok());
            }
            let changed = conn.execute("UPDATE native_buy_decisions SET finalized_at=?2,finalized_slot=?3 WHERE signature=?1 AND finalized_at IS NULL",
                params![signature,now.to_rfc3339(),slot as i64])?;
            Ok(changed == 1)
        })
    }

    /// Promotes only a checked finalized admission. The source signal timestamp
    /// is app availability; legacy source-age consumers must not select STATUS.
    pub fn native_buy_ready(
        &self,
        signature: &str,
        now: DateTime<Utc>,
        max_age_seconds: u64,
    ) -> Result<Option<NativeBuyCandidate>> {
        self.with_immediate_transaction_retry("native BUY signal promotion", |conn| {
            let Some(d) = verify::load(conn, signature)? else { return Ok(None); };
            if !verify::valid(conn, &d, now, Some(max_age_seconds), true)? { return Ok(None); }
            let Some(value) = verify::candidate(&d) else { return Ok(None); };
            conn.execute("INSERT OR IGNORE INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,notional_lamports,notional_origin,ts,status) VALUES(?1,?2,'buy',?3,?4,?5,?6,?7,?8)",
                params![value.signal_id,value.wallet,value.mint,value.amount_lamports as f64 / 1e9,i64::try_from(value.amount_lamports)?,COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,value.admitted_at.to_rfc3339(),STATUS])?;
            ensure!(signal_matches(conn, &value)?, "native_buy_signal_conflict");
            Ok(Some(value))
        })
    }

    /// Plain reads only: safe to call inside the dispatch write transaction.
    pub fn native_buy_recheck(
        &self,
        signal_id: &str,
        decision_id: &str,
        now: DateTime<Utc>,
        max_age_seconds: u64,
    ) -> Result<bool> {
        recheck(&self.conn, signal_id, decision_id, now, max_age_seconds)
    }

    /// Immutable policy identity pinned before the first admission. Callers
    /// compare this to the current execution config after every await.
    pub fn native_buy_policy_identity(&self, signal_id: &str) -> Result<Option<String>> {
        let Some(signature) = signal_id.strip_prefix("native-buy-v1:") else { return Ok(None); };
        if let Some(binding) = cohort::binding(&self.conn, signature)? {
            return Ok(Some(binding.policy_identity));
        }
        Ok(self.conn.query_row(
            "SELECT f.policy_identity FROM native_buy_decisions d JOIN native_buy_fences f ON f.session=d.first_session WHERE d.signature=?1 AND d.signal_id=?2",
            params![signature,signal_id], |r| r.get(0)).optional()?)
    }

    /// Read the first admission only after the complete durable decision is
    /// revalidated. Caller binds the same signal and decision again after waits.
    pub fn native_buy_source_amounts(
        &self, signal_id: &str, decision_id: &str,
        now: DateTime<Utc>, max_age_seconds: u64,
    ) -> Result<Option<NativeBuySourceAmounts>> {
        if !self.native_buy_recheck(signal_id, decision_id, now, max_age_seconds)? {
            return Ok(None);
        }
        let Some(signature) = signal_id.strip_prefix("native-buy-v1:") else { return Ok(None); };
        let Some(d) = verify::load(&self.conn, signature)? else { return Ok(None); };
        let admission: copybot_core_types::association_delivery::AdmissionFacts =
            serde_json::from_str(&d.admission)?;
        let Some(exact) = admission.facts.exact_amounts else { return Ok(None); };
        let (Ok(amount_in_lamports), Ok(amount_out_raw)) =
            (exact.amount_in_raw.parse::<u64>(), exact.amount_out_raw.parse::<u64>())
        else { return Ok(None); };
        if amount_in_lamports == 0 || amount_out_raw == 0
            || amount_in_lamports != u64::try_from(d.amount)?
            || exact.amount_in_decimals != 9 || exact.amount_out_decimals > 18
        { return Ok(None); }
        Ok(Some(NativeBuySourceAmounts {
            amount_in_lamports, amount_out_raw,
            amount_out_decimals: exact.amount_out_decimals,
        }))
    }
}

fn recheck(c: &rusqlite::Connection, signal_id: &str, decision_id: &str,
    now: DateTime<Utc>, max_age_seconds: u64) -> Result<bool> {
    let Some(signature) = signal_id.strip_prefix("native-buy-v1:") else { return Ok(false); };
    let Some(d) = verify::load(c, signature)? else { return Ok(false); };
    if d.signal_id != signal_id || d.decision_id != decision_id
        || !verify::valid(c, &d, now, Some(max_age_seconds), true)?
    { return Ok(false); }
    let Some(value) = verify::candidate(&d) else { return Ok(false); };
    signal_matches(c, &value)
}

/// Called inside protected-anchor BEGIN IMMEDIATE, so a changed fence, signal,
/// policy or reserved order cannot be accepted between recheck and insertion.
pub(crate) fn activation_current(c: &rusqlite::Connection,
    b: &super::NativeBuyActivationBinding, now: DateTime<Utc>) -> Result<bool> {
    if !recheck(c, &b.signal_id, &b.decision_id, now, b.max_age_seconds)? {
        return Ok(false);
    }
    let Some(signature) = b.signal_id.strip_prefix("native-buy-v1:") else { return Ok(false); };
    let policy: Option<String> = if let Some(binding) = cohort::binding(c, signature)? {
        Some(binding.policy_identity)
    } else {
        c.query_row(
            "SELECT f.policy_identity FROM native_buy_decisions d JOIN native_buy_fences f ON f.session=d.first_session WHERE d.signature=?1 AND d.signal_id=?2",
            params![signature, b.signal_id], |r| r.get(0)).optional()?
    };
    if policy.as_deref() != Some(b.policy_identity.as_str()) { return Ok(false); }
    let order: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id=?1 AND signal_id=?2 AND client_order_id=?3
            AND attempt=?4 AND route=?5 AND status=?6 AND tx_signature IS NULL)",
        params![b.order_id, b.signal_id, b.client_order_id, b.attempt, b.route,
            crate::EXECUTION_STATUS_CANARY_CANDIDATE], |r| r.get(0))?;
    Ok(order)
}

/// At most `limit` rows are verified on each tick. Persisting the keyset cursor
/// means a stale or unresolved old row cannot permanently hide later admissions.
fn scan_native_buy_ids(c: &rusqlite::Connection, kind: &str, limit: u32) -> Result<Vec<String>> {
    let cursor: Option<(String, String)> = c.query_row(
        "SELECT admitted_at,signature FROM native_buy_scan_cursor WHERE kind=?1",
        [kind], |r| Ok((r.get(0)?, r.get(1)?)),
    ).optional()?;
    let (at, signature) = cursor.unwrap_or_default();
    let predicate = if kind == "pending" {
        "d.finalized_at IS NULL AND d.late=0"
    } else {
        "d.finalized_at IS NOT NULL AND d.late=0
         AND NOT EXISTS(SELECT 1 FROM execution_canary_dispatch x WHERE x.signal_id=d.signal_id)
         AND NOT EXISTS(SELECT 1 FROM fills f JOIN orders o ON o.order_id=f.order_id WHERE o.signal_id=d.signal_id)"
    };
    let query = format!("SELECT d.admitted_at,d.signature FROM native_buy_decisions d
        WHERE {predicate} AND (d.admitted_at>?1 OR (d.admitted_at=?1 AND d.signature>?2))
        ORDER BY d.admitted_at,d.signature LIMIT ?3");
    let read = |at: &str, signature: &str| -> Result<Vec<(String, String)>> {
        let mut q = c.prepare(&query)?;
        let rows = q.query_map(params![at,signature,limit], |r| Ok((r.get(0)?,r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    };
    let mut rows = read(&at, &signature)?;
    if rows.is_empty() && (!at.is_empty() || !signature.is_empty()) {
        rows = read("", "")?;
    }
    if let Some((last_at,last_sig)) = rows.last() {
        c.execute("INSERT INTO native_buy_scan_cursor(kind,admitted_at,signature) VALUES(?1,?2,?3)
            ON CONFLICT(kind) DO UPDATE SET admitted_at=excluded.admitted_at,signature=excluded.signature",
            params![kind,last_at,last_sig])?;
    }
    Ok(rows.into_iter().map(|(_,id)| id).collect())
}

fn signal_matches(c: &rusqlite::Connection, d: &NativeBuyCandidate) -> Result<bool> {
    let row: Option<(String,String,String,String,Option<i64>,String,String,String,f64)> = c.query_row(
        "SELECT wallet_id,side,token,status,notional_lamports,notional_origin,ts,signal_id,notional_sol FROM copy_signals WHERE signal_id=?1", [&d.signal_id],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
    Ok(row.is_some_and(|(wallet,side,mint,status,amount,origin,ts,id,sol)|
        wallet == d.wallet && side == "buy" && mint == d.mint && status == STATUS
            && amount == i64::try_from(d.amount_lamports).ok()
            && origin == COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
            && ts == d.admitted_at.to_rfc3339() && id == d.signal_id
            && sol.to_bits() == (d.amount_lamports as f64 / 1e9).to_bits()))
}
