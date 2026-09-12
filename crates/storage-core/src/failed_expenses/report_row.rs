use super::{accounting, tasks, FailedExpenseReportRow};
use anyhow::{ensure, Result};
use rusqlite::{Connection, OptionalExtension};

pub(super) fn validated_row(
    conn: &Connection,
    id: &str,
    operation_at: String,
    has_schema: bool,
) -> Result<FailedExpenseReportRow> {
    let id = id.to_owned();
    let task = if has_schema {
        tasks::load(conn, &id)?
    } else {
        None
    };
    let facts = if has_schema {
        accounting::load_facts(conn, &id)?
    } else {
        None
    };
    let recorded: Option<String> = if has_schema {
        conn.query_row(
            "SELECT wallet_fee_lamports FROM execution_failed_expense_ledger WHERE order_id=?1",
            [&id],
            |r| r.get(0),
        )
        .optional()?
    } else {
        None
    };
    let mut sample = FailedExpenseReportRow {
        order_id: id.clone(),
        operation_at,
        reason: task
            .as_ref()
            .map(|t| t.reason.clone())
            .unwrap_or("legacy_failed_expense_uncovered".into()),
        task: task.clone(),
        facts: facts.clone(),
        recorded_wallet_fee_lamports: recorded.clone(),
        wallet_fee_lamports: None,
        native_delta_lamports: None,
        unexplained_delta_lamports: None,
    };
    let valid = if let Some(task) = &task {
        task.status != "conflict"
            && !conn.query_row(
                "SELECT COUNT(*)>1 FROM orders WHERE tx_signature=?1",
                [&task.tx_signature],
                |r| r.get::<_, bool>(0),
            )?
            && tasks::binding_matches(conn, task)?
            && proof_signature_matches(conn, task)?
            && !tasks::success_exists(conn, &task.tx_signature)?
    } else {
        false
    };
    if task.is_some() && !valid && task.as_ref().unwrap().status != "conflict" {
        sample.reason = "failed_expense_binding_unresolved".into();
    }
    if valid {
        if let Some(facts) = &facts {
            let task = task.as_ref().unwrap();
            ensure!(
                facts.tx_signature == task.tx_signature
                    && facts.wallet == task.wallet
                    && Some(facts.slot) == task.slot
                    && facts.transaction_error
                        == serde_json::from_str::<serde_json::Value>(&task.failure_error_json)?,
                "failed expense report identity mismatch"
            );
            if let Some(fee) = facts.wallet_fee()? {
                let value = fee.as_u64().to_string();
                let binding:(String,String,String)=conn.query_row("SELECT tx_signature,transaction_fee_lamports,payer FROM execution_failed_expense_ledger WHERE order_id=?1",[&id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                ensure!(
                    binding
                        == (
                            task.tx_signature.clone(),
                            facts.transaction_fee_lamports.clone().unwrap(),
                            facts.payer.clone().unwrap()
                        ),
                    "failed expense report ledger binding mismatch"
                );
                ensure!(
                    recorded.as_ref() == Some(&value),
                    "failed expense report missing or conflicting ledger"
                );
                sample.wallet_fee_lamports = Some(value);
            } else {
                ensure!(recorded.is_none(), "failed expense ledger lacks facts");
            }
            if let Some(delta) = facts.native_delta()? {
                sample.native_delta_lamports = Some(delta.as_i128().to_string());
            }
            if let Some(delta) = facts.unexplained_delta()? {
                sample.unexplained_delta_lamports = Some(delta.as_i128().to_string());
            }
        }
    }
    Ok(sample)
}

fn proof_signature_matches(conn: &Connection, task: &super::FailedExpenseTask) -> Result<bool> {
    let signature: Option<String> = conn
        .query_row(
            "SELECT tx_signature FROM execution_canary_receipt_proofs WHERE order_id=?1",
            [&task.order_id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(signature.is_none_or(|s| s == task.tx_signature))
}
