//! Durable dispatch permission. Only a newly committed claim permits network I/O.
use crate::{ExecutionCanaryOrder, SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_SIMULATED};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use rusqlite::{params, OptionalExtension};

pub const EXECUTION_UNRESOLVED_BUY_REASON: &str = "unresolved_buy_dispatch";

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionCanaryDispatch {
    pub order_id: String,
    pub signal_id: String,
    pub client_order_id: String,
    pub route: String,
    pub attempt: u32,
    pub wallet: String,
    pub token: String,
    pub side: String,
    pub tx_signature: String,
    pub transaction_sha256: String,
    pub message_sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionDispatchClaim {
    New,
    Existing,
}

impl SqliteDiscoveryStore {
    pub fn load_execution_canary_dispatch(
        &self,
        id: &str,
    ) -> Result<Option<ExecutionCanaryDispatch>> {
        Ok(self.conn.query_row("SELECT order_id,signal_id,client_order_id,route,attempt,wallet,token,side,
            tx_signature,transaction_sha256,message_sha256 FROM execution_canary_dispatch WHERE order_id=?1", [id],
            |r| Ok(ExecutionCanaryDispatch { order_id:r.get(0)?, signal_id:r.get(1)?, client_order_id:r.get(2)?,
                route:r.get(3)?, attempt:r.get(4)?, wallet:r.get(5)?, token:r.get(6)?, side:r.get(7)?,
                tx_signature:r.get(8)?, transaction_sha256:r.get(9)?, message_sha256:r.get(10)? })).optional()?)
    }

    /// No except-order escape hatch. An existing claim never grants another send.
    pub fn claim_execution_canary_dispatch(
        &self,
        expected: &ExecutionCanaryOrder,
        signal: &CopySignalRow,
        identity: &ExecutionCanaryDispatch,
        now: DateTime<Utc>,
    ) -> Result<ExecutionDispatchClaim> {
        self.claim_dispatch(expected, signal, identity, None, &|| Ok(now))
    }

    pub fn claim_tiny_experiment_dispatch(
        &self,
        expected: &ExecutionCanaryOrder,
        signal: &CopySignalRow,
        identity: &ExecutionCanaryDispatch,
        budget: &crate::TinyBudgetClaim,
        now: DateTime<Utc>,
    ) -> Result<ExecutionDispatchClaim> {
        self.claim_tiny_experiment_dispatch_with_clock(expected, signal, identity, budget, || {
            Ok(now)
        })
    }

    /// Sample the final decision clock only after the SQLite write lock is held.
    pub fn claim_tiny_experiment_dispatch_with_clock(
        &self,
        expected: &ExecutionCanaryOrder,
        signal: &CopySignalRow,
        identity: &ExecutionCanaryDispatch,
        budget: &crate::TinyBudgetClaim,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionDispatchClaim> {
        self.claim_dispatch(expected, signal, identity, Some(budget), &clock)
    }

    fn claim_dispatch(
        &self,
        expected: &ExecutionCanaryOrder,
        signal: &CopySignalRow,
        identity: &ExecutionCanaryDispatch,
        budget: Option<&crate::TinyBudgetClaim>,
        clock: &impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionDispatchClaim> {
        ensure!(
            identity.order_id == expected.order_id
                && identity.signal_id == expected.signal_id
                && identity.client_order_id == expected.client_order_id
                && identity.route == expected.route
                && identity.attempt == expected.attempt
                && identity.token == signal.token
                && identity.side == signal.side.to_ascii_lowercase()
                && signal.signal_id == expected.signal_id,
            "dispatch_identity_mismatch"
        );
        ensure!(
            !identity.wallet.trim().is_empty()
                && !identity.tx_signature.trim().is_empty()
                && identity.message_sha256.len() == 64
                && identity.transaction_sha256.len() == 64,
            "dispatch_binding_missing"
        );
        self.with_immediate_transaction_retry("claim canary dispatch", |conn| {
            if let Some(old) = self.load_execution_canary_dispatch(&identity.order_id)? {
                ensure!(&old == identity, "dispatch_identity_conflict");
                return Ok(ExecutionDispatchClaim::Existing);
            }
            let now = clock()?;
            let current = self
                .load_execution_canary_order(&expected.order_id)?
                .context("dispatch_order_missing")?;
            ensure!(
                &current == expected
                    && current.status == EXECUTION_STATUS_CANARY_SIMULATED
                    && current
                        .tx_signature
                        .as_deref()
                        .is_none_or(|s| s.trim().is_empty()),
                "dispatch_order_changed"
            );
            let saved = self
                .load_copy_signal_by_signal_id(&signal.signal_id)?
                .context("dispatch_signal_missing")?;
            ensure!(same_signal(signal, &saved), "dispatch_signal_changed");
            if identity.side == "buy" {
                ensure!(
                    !self.execution_canary_unresolved_buy()?,
                    "unresolved_buy_dispatch"
                );
                ensure!(
                    !self.execution_canary_accounting_pending()?,
                    "confirmed_accounting_pending"
                );
            }
            ensure!(
                self.execution_canary_receipt_submit_block_reason(
                    &identity.order_id,
                    &identity.token,
                    &identity.side
                )?
                .is_none(),
                "dispatch_receipt_blocked"
            );
            ensure!(
                self.execution_sell_intent_block_in_snapshot(conn, &saved)?
                    .is_none(),
                "dispatch_sell_source_changed"
            );
            insert(conn, identity, now)?;
            crate::tiny_experiment::reserve(conn, identity, budget, now)?;
            mark_submitted(conn, identity, now)?;
            Ok(ExecutionDispatchClaim::New)
        })
    }

    pub fn execution_canary_unresolved_buy(&self) -> Result<bool> {
        Ok(self.execution_canary_unresolved_buy_order_id()?.is_some())
    }

    /// One deterministic witness of the unchanged global BUY predicate.
    pub fn execution_canary_unresolved_buy_order_id(&self) -> Result<Option<String>> {
        // ALTER TABLE may rewrite view references to a backup: explicitly require
        // the current tables too, rather than treating a readable old view as proof.
        self.conn.prepare("SELECT order_id,signal_id,client_order_id,route,attempt,wallet,token,side,
            tx_signature,transaction_sha256,message_sha256,claimed_at,transport_note FROM execution_canary_dispatch LIMIT 0")?.exists([])?;
        self.conn.prepare(
            "SELECT order_id,last_attempt_at FROM execution_canary_reconcile_attempts LIMIT 0",
        )?;
        // Missing/corrupt modern schema is an error, never an old-schema exemption.
        Ok(self.conn.query_row(
            "SELECT order_id FROM execution_canary_unresolved_dispatch WHERE side!='sell' ORDER BY order_id LIMIT 1",
            [],
            |r| r.get(0),
        ).optional()?)
    }

    /// Informational only: even a failed outcome write leaves the committed claim.
    pub fn note_execution_canary_dispatch(
        &self,
        identity: &ExecutionCanaryDispatch,
        note: &str,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("note canary dispatch", |conn| {
            ensure!(
                self.load_execution_canary_dispatch(&identity.order_id)?
                    .as_ref()
                    == Some(identity),
                "dispatch_identity_conflict"
            );
            conn.execute(
                "UPDATE execution_canary_dispatch SET transport_note=?2 WHERE order_id=?1",
                params![
                    identity.order_id,
                    note.chars().take(240).collect::<String>()
                ],
            )?;
            Ok(())
        })
    }

    /// Persist a visit before I/O, including pending/error responses and process restart.
    pub fn visit_execution_canary_reconciliation(
        &self,
        id: &str,
        wallet: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("visit canary reconciliation", |conn| {
            if let Some(d)=self.load_execution_canary_dispatch(id)? {
                let o=self.load_execution_canary_order(id)?.context("dispatch_order_missing")?;
                ensure!(d.wallet==wallet && o.tx_signature.as_deref()==Some(&d.tx_signature)
                    && d.attempt==o.attempt && d.signal_id==o.signal_id && d.route==o.route
                    && d.client_order_id==o.client_order_id,"dispatch_reconciliation_identity_conflict");
            }
            conn.execute("INSERT INTO execution_canary_reconcile_attempts(order_id,last_attempt_at) VALUES (?1,?2)
                ON CONFLICT(order_id) DO UPDATE SET last_attempt_at=MAX(last_attempt_at,excluded.last_attempt_at)",params![id,now.to_rfc3339()])?;
            // Old signed expired/failed obligations resume status polling; preserve
            // their original reason and never touch fills or monetary rows.
            conn.execute("UPDATE orders SET status='execution_canary_submitted' WHERE order_id=?1
                AND ((status IN ('execution_canary_expired','execution_canary_failed') AND length(trim(COALESCE(tx_signature,'')))>0)
                    OR (status='execution_canary_simulated' AND simulation_error LIKE 'retry_after_unknown_submit_timeout%'))
                AND EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch d WHERE d.order_id=orders.order_id)",[id])?;
            Ok(())
        })
    }
}

fn same_signal(a: &CopySignalRow, b: &CopySignalRow) -> bool {
    a.signal_id == b.signal_id
        && a.wallet_id == b.wallet_id
        && a.side == b.side
        && a.token == b.token
        && a.status == b.status
        && a.ts == b.ts
        && a.notional_sol.to_bits() == b.notional_sol.to_bits()
        && a.notional_lamports == b.notional_lamports
        && a.notional_origin == b.notional_origin
}

pub(crate) fn insert(
    conn: &rusqlite::Connection,
    identity: &ExecutionCanaryDispatch,
    now: DateTime<Utc>,
) -> Result<()> {
    conn.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,
                tx_signature,transaction_sha256,message_sha256,claimed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                params![identity.order_id,identity.signal_id,identity.client_order_id,identity.route,identity.attempt,
                    identity.wallet,identity.token,identity.side,identity.tx_signature,identity.transaction_sha256,identity.message_sha256,now.to_rfc3339()])?;

    Ok(())
}

pub(crate) fn mark_submitted(
    conn: &rusqlite::Connection,
    identity: &ExecutionCanaryDispatch,
    now: DateTime<Utc>,
) -> Result<()> {
    let changed=conn.execute("UPDATE orders SET status='execution_canary_submitted',tx_signature=?2,submit_ts=?3,
                confirm_ts=NULL,err_code=NULL,simulation_error='dispatch_outcome_unknown' WHERE order_id=?1",
                params![identity.order_id,identity.tx_signature,now.to_rfc3339()])?;
    ensure!(changed == 1, "dispatch_order_missing");

    Ok(())
}
