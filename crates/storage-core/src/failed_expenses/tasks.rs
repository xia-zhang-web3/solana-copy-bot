use super::FailedExpenseTask;
use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

const COLS: &str = "order_id,tx_signature,attempt,route,wallet,token,side,operation_at,detected_at,failure_source,commitment,slot,status,reason,failure_error_json,last_attempt_at";
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<FailedExpenseTask> {
    let slot: Option<String> = r.get(11)?;
    Ok(FailedExpenseTask {
        order_id: r.get(0)?,
        tx_signature: r.get(1)?,
        attempt: r.get(2)?,
        route: r.get(3)?,
        wallet: r.get(4)?,
        token: r.get(5)?,
        side: r.get(6)?,
        operation_at: r.get(7)?,
        detected_at: r.get(8)?,
        failure_source: r.get(9)?,
        commitment: r.get(10)?,
        slot: slot
            .map(|s| {
                s.parse().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        11,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })
            })
            .transpose()?,
        status: r.get(12)?,
        reason: r.get(13)?,
        failure_error_json: r.get(14)?,
        last_attempt_at: r.get(15)?,
    })
}
pub(super) fn load(conn: &Connection, id: &str) -> Result<Option<FailedExpenseTask>> {
    Ok(conn
        .query_row(
            &format!("SELECT {COLS} FROM execution_failed_expense_tasks WHERE order_id=?1"),
            [id],
            row,
        )
        .optional()?)
}
pub(super) fn conflict(conn: &Connection, id: &str, reason: &str) -> Result<()> {
    ensure!(conn.execute("UPDATE execution_failed_expense_tasks SET status='conflict', reason=?2 WHERE order_id=?1",params![id,reason])?==1,"failed expense conflict task missing");
    ensure!(
        load(conn, id)?.is_some_and(|t| t.status == "conflict" && t.reason == reason),
        "failed expense conflict readback mismatch"
    );
    Ok(())
}
pub(super) fn success_exists(conn: &Connection, signature: &str) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM orders o JOIN fills f ON f.order_id=o.order_id WHERE o.tx_signature=?1) OR EXISTS(SELECT 1 FROM execution_canary_receipt_facts WHERE tx_signature=?1)",[signature],|r|r.get(0))?)
}
pub(super) fn binding_matches(conn: &Connection, t: &FailedExpenseTask) -> Result<bool> {
    let proof:Option<(String,String,String,Option<String>)>=conn.query_row("SELECT wallet_pubkey,token,side,slot FROM execution_canary_receipt_proofs WHERE order_id=?1",[&t.order_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    if proof.is_some_and(|(wallet, token, side, slot)| {
        wallet != t.wallet
            || token != t.token
            || side != t.side
            || slot.zip(t.slot).is_some_and(|(a, b)| a != b.to_string())
    }) {
        return Ok(false);
    }
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM orders o JOIN copy_signals s ON s.signal_id=o.signal_id
        WHERE o.order_id=?1 AND o.tx_signature=?2 AND o.attempt=?3 AND o.route=?4 AND s.token=?5 AND lower(s.side)=?6 AND o.submit_ts=?7 AND EXISTS(SELECT 1 FROM execution_failed_expense_tasks t WHERE t.order_id=o.order_id AND t.wallet=?8))",
        params![t.order_id,t.tx_signature,t.attempt,t.route,t.token,t.side,t.operation_at,t.wallet],|r|r.get(0))?)
}
impl SqliteDiscoveryStore {
    pub fn load_failed_expense_task(&self, id: &str) -> Result<Option<FailedExpenseTask>> {
        load(&self.conn, id)
    }
    /// Create the task, mark terminal failure and reserve pending receipt work atomically.
    /// Complete/conflict tasks reserve no work. No expense/fill is implied.
    pub fn detect_failed_expense(
        &self,
        id: &str,
        wallet: &str,
        source: &str,
        commitment: &str,
        slot: Option<u64>,
        error: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> Result<FailedExpenseTask> {
        ensure!(
            super::evidence::proven_failure(error),
            "invalid failed expense structured error"
        );
        ensure!(
            !wallet.is_empty()
                && matches!(source, "signature_status" | "receipt_meta")
                && matches!(commitment, "confirmed" | "finalized"),
            "invalid failed expense proof"
        );
        self.with_immediate_transaction_retry("failed expense detection",|conn| {
            let (signature,attempt,route,token,side,operation,status):(String,u32,String,String,String,String,String)=conn.query_row(
                "SELECT o.tx_signature,o.attempt,o.route,s.token,lower(s.side),o.submit_ts,o.status FROM orders o JOIN copy_signals s ON s.signal_id=o.signal_id WHERE o.order_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
            ensure!(id.starts_with("exec-canary:") && !signature.trim().is_empty() && matches!(status.as_str(),crate::EXECUTION_STATUS_CANARY_SUBMITTED|crate::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED|crate::EXECUTION_STATUS_CANARY_CONFIRMED|crate::EXECUTION_STATUS_CANARY_FAILED),"failed expense requires sent order");
            if let Some(old)=load(conn,id)? {
                if !binding_matches(conn,&old)? || old.wallet!=wallet || old.slot.zip(slot).is_some_and(|(a,b)|a!=b) || serde_json::from_str::<serde_json::Value>(&old.failure_error_json)? != *error {
                    conflict(conn,id,"failed_expense_identity_conflict")?;
                    return Ok(load(conn,id)?.unwrap());
                }
            } else {
                conn.execute("INSERT INTO execution_failed_expense_tasks (order_id,tx_signature,attempt,route,wallet,token,side,operation_at,detected_at,failure_source,commitment,slot,status,reason,failure_error_json,last_attempt_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'pending','failed_receipt_pending',?13,?9)",params![id,signature,attempt,route,wallet,token,side,operation,now.to_rfc3339(),source,commitment,slot.map(|v|v.to_string()),serde_json::to_string(error)?])?;
            }
            ensure!(load(conn,id)?.is_some(),"failed expense detection task insert missing");
            let duplicate:bool=conn.query_row("SELECT COUNT(*)>1 FROM orders WHERE tx_signature=?1",[&signature],|r|r.get(0))?;
            if duplicate {
                conn.execute("UPDATE execution_failed_expense_tasks SET status='conflict',reason='failed_expense_signature_binding_conflict' WHERE tx_signature=?1",[&signature])?;
            } else if !binding_matches(conn,&load(conn,id)?.unwrap())? {
                conflict(conn,id,"failed_expense_identity_conflict")?;
            } else if success_exists(conn,&signature)? {
                conflict(conn,id,"failed_expense_success_conflict")?;
            } else if load(conn,id)?.unwrap().status!="conflict" {
                conn.execute("UPDATE execution_failed_expense_tasks SET slot=COALESCE(slot,?2) WHERE order_id=?1",params![id,slot.map(|v|v.to_string())])?;
                conn.execute("UPDATE orders SET status=?2,err_code=?3,simulation_error='on_chain_transaction_failed' WHERE order_id=?1",params![id,crate::EXECUTION_STATUS_CANARY_FAILED,crate::EXECUTION_ERROR_CONFIRMATION_FAILED])?;
                conn.execute("UPDATE execution_canary_receipt_proofs SET reason='receipt_transaction_failed' WHERE order_id=?1",[id])?;
            }
            let task=load(conn,id)?.ok_or_else(||anyhow::anyhow!("failed expense detection missing task"))?;
            if task.status!="conflict" {
                let terminal:bool=conn.query_row("SELECT status=?2 FROM orders WHERE order_id=?1",params![id,crate::EXECUTION_STATUS_CANARY_FAILED],|r|r.get(0))?;
                ensure!(terminal && binding_matches(conn,&task)?,"failed expense detection readback mismatch");
            }
            if task.status=="pending" {
                super::reservation::reserve(conn,&task.route,std::slice::from_ref(&task),now)?;
                return Ok(load(conn,id)?.ok_or_else(||anyhow::anyhow!("failed expense reservation missing task"))?);
            }
            Ok(task)
        })
    }
    /// Reserve bounded work BEFORE receipt I/O. Monotone sequence is fair even if
    /// several ticks use the same wall clock or the process reopens the DB.
    pub fn take_failed_expense_tasks(
        &self,
        route: &str,
        limit: u32,
        now: DateTime<Utc>,
    ) -> Result<Vec<FailedExpenseTask>> {
        self.with_immediate_transaction_retry("failed expense cursor",|conn| {
            let mut stmt=conn.prepare(&format!("SELECT {COLS} FROM execution_failed_expense_tasks WHERE route=?1 AND status='pending' ORDER BY attempt_seq,operation_at,order_id LIMIT ?2"))?;
            let tasks=stmt.query_map(params![route,i64::from(limit)],row)?.collect::<rusqlite::Result<Vec<_>>>()?;
            super::reservation::reserve(conn,route,&tasks,now)?;
            Ok(tasks)
        })
    }
    pub fn defer_failed_expense(&self, id: &str, reason: &str) -> Result<()> {
        ensure!(
            reason.len() <= 120 && !reason.is_empty(),
            "unbounded failed expense reason"
        );
        self.with_immediate_transaction_retry("failed expense pending",|conn| {
            ensure!(conn.execute("UPDATE execution_failed_expense_tasks SET reason=?2 WHERE order_id=?1 AND status='pending'",params![id,reason])?==1,"failed expense pending task missing");
            Ok(())
        })?;
        let t = load(&self.conn, id)?
            .ok_or_else(|| anyhow::anyhow!("failed expense pending task missing"))?;
        ensure!(
            t.status == "pending" && t.reason == reason && binding_matches(&self.conn, &t)?,
            "failed expense pending readback mismatch"
        );
        Ok(())
    }
    pub fn reject_failed_expense(&self, id: &str, reason: &str) -> Result<()> {
        ensure!(reason.len() <= 120, "unbounded failed expense conflict");
        self.with_immediate_transaction_retry("failed expense conflict", |conn| {
            conflict(conn, id, reason)
        })
    }
}
