use super::{tasks, FailedExpenseCoverage as Coverage, FailedTransactionFacts};
use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

pub(super) fn load_facts(conn: &Connection, id: &str) -> Result<Option<FailedTransactionFacts>> {
    let json: Option<String> = conn
        .query_row(
            "SELECT facts_json FROM execution_failed_expense_facts WHERE order_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    json.map(|json| {
        let facts: FailedTransactionFacts = serde_json::from_str(&json)?;
        facts.validate()?;
        Ok(facts)
    })
    .transpose()
}
fn merge(
    old: &FailedTransactionFacts,
    fresh: &FailedTransactionFacts,
) -> Result<FailedTransactionFacts> {
    ensure!(
        old.tx_signature == fresh.tx_signature
            && old.wallet == fresh.wallet
            && old.slot == fresh.slot
            && old.transaction_error == fresh.transaction_error
            && old.commitment == fresh.commitment,
        "failed receipt identity conflict"
    );
    let mut merged = old.clone();
    for (left, right) in [
        (
            &mut merged.transaction_fee_lamports,
            &fresh.transaction_fee_lamports,
        ),
        (&mut merged.payer, &fresh.payer),
        (
            &mut merged.wallet_native_pre_lamports,
            &fresh.wallet_native_pre_lamports,
        ),
        (
            &mut merged.wallet_native_post_lamports,
            &fresh.wallet_native_post_lamports,
        ),
    ] {
        ensure!(
            left.as_ref()
                .zip(right.as_ref())
                .is_none_or(|(a, b)| a == b),
            "failed receipt known fact conflict"
        );
        if left.is_none() {
            *left = right.clone();
        }
    }
    if merged.transaction_fee_lamports.is_some() {
        merged.fee_coverage = Coverage::Known;
    } else {
        merged.fee_coverage = fresh.fee_coverage;
    }
    if merged.payer.is_some() {
        merged.payer_coverage = Coverage::Known;
    } else {
        merged.payer_coverage = fresh.payer_coverage;
    }
    if merged.wallet_native_pre_lamports.is_some() {
        merged.native_coverage = Coverage::Known;
    } else {
        merged.native_coverage = fresh.native_coverage;
    }
    merged.validate()?;
    Ok(merged)
}
impl SqliteDiscoveryStore {
    pub fn load_failed_transaction_facts(
        &self,
        id: &str,
    ) -> Result<Option<FailedTransactionFacts>> {
        load_facts(&self.conn, id)
    }
    /// Facts enrichment, the signature-unique expense marker and task completion
    /// commit together. No position, fill or wallet balance is ever debited here.
    pub fn apply_failed_expense(
        &self,
        id: &str,
        fresh: &FailedTransactionFacts,
        now: DateTime<Utc>,
    ) -> Result<()> {
        fresh.validate()?;
        self.with_immediate_transaction_retry("failed expense accounting",|conn| {
            let task=tasks::load(conn,id)?.ok_or_else(||anyhow::anyhow!("failed expense task missing"))?;
            if task.status=="conflict" { return Ok(()); }
            let duplicate:bool=conn.query_row("SELECT COUNT(*)>1 FROM orders WHERE tx_signature=?1",[&task.tx_signature],|r|r.get(0))?;
            if duplicate || !tasks::binding_matches(conn,&task)? || fresh.wallet!=task.wallet || fresh.tx_signature!=task.tx_signature || task.slot.is_some_and(|s|s!=fresh.slot) || serde_json::from_str::<serde_json::Value>(&task.failure_error_json)? != fresh.transaction_error {
                return tasks::conflict(conn,id,"failed_expense_identity_conflict");
            }
            if tasks::success_exists(conn,&task.tx_signature)? { return tasks::conflict(conn,id,"failed_expense_success_conflict"); }
            let old=load_facts(conn,id)?;
            let facts=if let Some(old)=&old {
                match merge(old,fresh) { Ok(f)=>f, Err(_)=>return tasks::conflict(conn,id,"failed_expense_known_fact_conflict") }
            } else { fresh.clone() };
            if old.as_ref()!=Some(&facts) {
                conn.execute("INSERT INTO execution_failed_expense_facts(order_id,facts_json) VALUES(?1,?2) ON CONFLICT(order_id) DO UPDATE SET facts_json=excluded.facts_json",params![id,serde_json::to_string(&facts)?])?;
            }
            ensure!(load_facts(conn,id)?.as_ref()==Some(&facts),"failed expense facts readback mismatch");
            let fee=facts.wallet_fee()?;
            if let Some(fee)=fee {
                let expected=(id.to_owned(),fee.as_u64().to_string(),facts.transaction_fee_lamports.clone().unwrap(),facts.payer.clone().unwrap());
                let old:Option<(String,String,String,String)>=conn.query_row("SELECT order_id,wallet_fee_lamports,transaction_fee_lamports,payer FROM execution_failed_expense_ledger WHERE tx_signature=?1",[&task.tx_signature],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
                if old.as_ref().is_some_and(|old|old!=&expected) { return tasks::conflict(conn,id,"failed_expense_ledger_binding_conflict"); }
                if old.is_none() {
                    conn.execute("INSERT INTO execution_failed_expense_ledger(tx_signature,order_id,wallet_fee_lamports,transaction_fee_lamports,payer,recorded_at) VALUES(?1,?2,?3,?4,?5,?6)",params![task.tx_signature,expected.0,expected.1,expected.2,expected.3,now.to_rfc3339()])?;
                }
                let actual:(String,String,String,String)=conn.query_row("SELECT order_id,wallet_fee_lamports,transaction_fee_lamports,payer FROM execution_failed_expense_ledger WHERE tx_signature=?1",[&task.tx_signature],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
                ensure!(actual==expected,"failed expense ledger readback mismatch");
            }
            crate::tiny_experiment::settle(conn,id,&task.tx_signature,&facts.wallet,facts.payer.as_deref(),
                facts.transaction_fee_lamports.as_deref().map(str::parse::<u64>).transpose()?,"failed",now)?;
            crate::owner_exit_fee::settle(conn,id,&task.tx_signature,&facts.wallet,facts.payer.as_deref(),
                facts.transaction_fee_lamports.as_deref().map(str::parse::<u64>).transpose()?,"failed",now)?;
            let complete=fee.is_some() && facts.native_delta()?.is_some();
            let reason=if complete {"failed_expense_recorded"} else if facts.fee_coverage==Coverage::Invalid {"failed_receipt_fee_invalid"} else if fee.is_none() {"failed_receipt_fee_or_payer_unknown"} else {"failed_receipt_native_unknown"};
            conn.execute("UPDATE execution_failed_expense_tasks SET slot=COALESCE(slot,?2),status=?3,reason=?4 WHERE order_id=?1",params![id,facts.slot.to_string(),if complete {"complete"} else {"pending"},reason])?;
            let stored=tasks::load(conn,id)?.unwrap();
            ensure!(stored.status==if complete {"complete"} else {"pending"} && stored.reason==reason && stored.slot==Some(facts.slot),"failed expense completion readback mismatch");
            Ok(())
        })
    }
    /// Contradictory success must not book a fill on a transaction already bound
    /// to failure evidence (including another order with the same signature).
    pub fn success_conflicts_with_failed_expense(&self, signature: &str) -> Result<bool> {
        self.with_immediate_transaction_retry("failed expense success conflict",|conn| {
            let found:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE tx_signature=?1)",[signature],|r|r.get(0))?;
            if found { conn.execute("UPDATE execution_failed_expense_tasks SET status='conflict',reason='failed_expense_success_conflict' WHERE tx_signature=?1",[signature])?; }
            Ok(found)
        })
    }
}
