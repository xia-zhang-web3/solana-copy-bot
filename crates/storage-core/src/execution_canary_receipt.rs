use crate::{
    execution_canary_fill_marker::fill_exists, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{anyhow, ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

pub const EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED: &str =
    "execution_canary_confirmed_unreconciled";
pub const EXECUTION_ACCOUNTING_PENDING_REASON: &str = "confirmed_accounting_pending";

/// Durable network proof and immutable receipt identity. Completion lives in orders + fills.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCanaryReceiptProof {
    pub tx_signature: String,
    pub wallet_pubkey: String,
    pub token: String,
    pub side: String,
    pub confirmation_status: String,
    pub slot: Option<u64>,
    pub confirmed_at: DateTime<Utc>,
    pub reason: String,
}

impl SqliteDiscoveryStore {
    pub fn execution_canary_fill_exists(&self, order_id: &str) -> Result<bool> {
        fill_exists(&self.conn, order_id)
    }

    pub fn load_execution_canary_receipt_proof(
        &self,
        order_id: &str,
    ) -> Result<Option<ExecutionCanaryReceiptProof>> {
        let row: Option<(String, String, String, String, String, Option<String>, String, String)> = self.conn.query_row(
            "SELECT tx_signature, wallet_pubkey, token, side, confirmation_status, slot, confirmed_at, reason
             FROM execution_canary_receipt_proofs WHERE order_id = ?1", [order_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?))
        ).optional().context("load canary receipt proof")?;
        row.map(
            |(
                tx_signature,
                wallet_pubkey,
                token,
                side,
                confirmation_status,
                slot,
                confirmed_at,
                reason,
            )| {
                Ok(ExecutionCanaryReceiptProof {
                    tx_signature,
                    wallet_pubkey,
                    token,
                    side,
                    confirmation_status,
                    slot: slot.map(|v| v.parse()).transpose()?,
                    confirmed_at: DateTime::parse_from_rfc3339(&confirmed_at)?.with_timezone(&Utc),
                    reason,
                })
            },
        )
        .transpose()
    }

    pub fn mark_execution_canary_confirmed_unreconciled(
        &self,
        order_id: &str,
        proof: &ExecutionCanaryReceiptProof,
        attempted_at: DateTime<Utc>,
    ) -> Result<()> {
        ensure!(
            !proof.wallet_pubkey.trim().is_empty() && !proof.tx_signature.trim().is_empty(),
            "receipt identity missing"
        );
        ensure!(
            matches!(
                proof.confirmation_status.as_str(),
                "confirmed" | "finalized" | "legacy_confirmed"
            ),
            "invalid receipt confirmation proof"
        );
        ensure!(
            !proof.reason.trim().is_empty(),
            "receipt pending reason missing"
        );
        self.with_immediate_transaction_retry("record canary network confirmation", |conn| {
            if fill_exists(conn, order_id)? { return Ok(()); }
            let (token,side)=crate::rpc_owned_sell_handoff::dispatch::identity::token_side(conn,order_id)?;
            let (status, signature): (String, Option<String>) = conn.query_row(
                "SELECT o.status, o.tx_signature FROM orders o WHERE o.order_id = ?1", [order_id],
                |r| Ok((r.get(0)?, r.get(1)?)))?;
            ensure!(matches!(status.as_str(), EXECUTION_STATUS_CANARY_SUBMITTED | EXECUTION_STATUS_CANARY_CONFIRMED | EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED), "order cannot enter receipt reconciliation");
            ensure!(signature.as_deref() == Some(&proof.tx_signature) && token == proof.token && side.eq_ignore_ascii_case(&proof.side), "receipt proof identity mismatch");
            // Repeated attempts may change only the reason; the original proof remains authoritative.
            let existing: Option<(String, String, String, String)> = conn.query_row(
                "SELECT tx_signature, wallet_pubkey, token, side FROM execution_canary_receipt_proofs WHERE order_id = ?1", [order_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).optional()?;
            if let Some(identity) = existing {
                ensure!(identity == (proof.tx_signature.clone(), proof.wallet_pubkey.clone(), proof.token.clone(), proof.side.clone()), "stored receipt identity mismatch");
            }
            conn.execute("INSERT INTO execution_canary_receipt_proofs
                (order_id, tx_signature, wallet_pubkey, token, side, confirmation_status, slot, confirmed_at, reason, last_attempt_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(order_id) DO UPDATE SET reason = excluded.reason, last_attempt_at = excluded.last_attempt_at",
                params![order_id, proof.tx_signature, proof.wallet_pubkey, proof.token, proof.side,
                    proof.confirmation_status, proof.slot.map(|s| s.to_string()), proof.confirmed_at.to_rfc3339(), proof.reason, attempted_at.to_rfc3339()])?;
            conn.execute("UPDATE orders SET status = ?2, confirm_ts = COALESCE(confirm_ts, ?3),
                err_code = ?4, simulation_error = ?5 WHERE order_id = ?1",
                params![order_id, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED, proof.confirmed_at.to_rfc3339(), EXECUTION_ACCOUNTING_PENDING_REASON, proof.reason])?;
            Ok(())
        })
    }

    pub fn mark_execution_canary_receipt_failed(&self, order_id: &str, reason: &str) -> Result<()> {
        self.with_immediate_transaction_retry("record canary receipt execution failure", |conn| {
            ensure!(!fill_exists(conn, order_id)?, "cannot fail accounted order");
            let changed = conn.execute(
                "UPDATE orders SET status = ?2, err_code = ?3, simulation_error = ?4
                WHERE order_id = ?1 AND status = ?5",
                params![
                    order_id,
                    crate::EXECUTION_STATUS_CANARY_FAILED,
                    crate::EXECUTION_ERROR_CONFIRMATION_FAILED,
                    reason,
                    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
                ],
            )?;
            ensure!(changed == 1, "receipt failure requires pending accounting");
            conn.execute(
                "UPDATE execution_canary_receipt_proofs SET reason = ?2 WHERE order_id = ?1",
                params![order_id, reason],
            )?;
            Ok(())
        })
    }

    pub fn execution_canary_accounting_pending(&self) -> Result<bool> {
        if crate::failed_expenses::guards::conflicting_order(&self.conn, None, None)?.is_some() {
            return Ok(true);
        }
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM orders o
            WHERE o.order_id LIKE 'exec-canary:%' AND (o.status = ?1 OR
                (o.status = ?2 AND NOT EXISTS(SELECT 1 FROM fills f WHERE f.order_id = o.order_id))))",
            params![EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED, EXECUTION_STATUS_CANARY_CONFIRMED], |r| r.get(0))?)
    }

    /// Wallet-balance recovery must not preempt receipt accounting for this mint.
    pub fn execution_canary_token_accounting_pending(&self, token: &str) -> Result<bool> {
        Ok(pending_token_order(&self.conn, token, None)?.is_some())
    }

    /// Final tiny pre-submit check, also used for retry candidates that bypass reservation.
    pub fn execution_canary_receipt_submit_block_reason(
        &self,
        order_id: &str,
        token: &str,
        side: &str,
    ) -> Result<Option<&'static str>> {
        if side.eq_ignore_ascii_case("buy") && self.execution_canary_accounting_pending()? {
            return Ok(Some(EXECUTION_ACCOUNTING_PENDING_REASON));
        }
        if side.eq_ignore_ascii_case("sell")
            && pending_token_order(&self.conn, token, Some(order_id))?.is_some()
        {
            return Ok(Some("sell_token_accounting_pending"));
        }
        Ok(None)
    }
}

pub(crate) fn pending_token_order(
    conn: &Connection,
    token: &str,
    except: Option<&str>,
) -> Result<Option<String>> {
    if let Some(id) = crate::failed_expenses::guards::conflicting_order(conn, Some(token), except)?
    {
        return Ok(Some(id));
    }
    conn.query_row("SELECT o.order_id FROM orders o LEFT JOIN copy_signals s ON s.signal_id = o.signal_id LEFT JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        WHERE o.order_id LIKE 'exec-canary:%' AND COALESCE(s.token,p.token) = ?1 AND (?2 IS NULL OR o.order_id != ?2)
          AND (o.status = ?3 OR (o.status = ?4 AND NOT EXISTS(SELECT 1 FROM fills f WHERE f.order_id = o.order_id)))
        ORDER BY o.submit_ts, o.order_id LIMIT 1",
        params![token, except, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED, EXECUTION_STATUS_CANARY_CONFIRMED], |r| r.get(0))
        .optional().context("load token pending accounting blocker")
}

pub(crate) fn complete_receipt_accounting(conn: &Connection, order_id: &str) -> Result<()> {
    ensure!(
        fill_exists(conn, order_id)?,
        "receipt accounting completion requires fill"
    );
    let rows = conn.execute(
        "UPDATE orders SET status = ?2, err_code = NULL, simulation_error = NULL
        WHERE order_id = ?1 AND status = ?3",
        params![
            order_id,
            EXECUTION_STATUS_CANARY_CONFIRMED,
            EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
        ],
    )?;
    if rows != 1 {
        return Err(anyhow!(
            "receipt accounting completion requires pending order"
        ));
    }
    let proof_rows = conn.execute("UPDATE execution_canary_receipt_proofs SET reason = 'accounting_complete' WHERE order_id = ?1", [order_id])?;
    ensure!(proof_rows == 1, "receipt completion requires durable proof");
    Ok(())
}
