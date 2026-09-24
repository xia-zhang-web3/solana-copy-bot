use crate::{
    execution_canary_fill_marker::fill_exists, receipt_facts_identity::validate_identity,
    receipt_facts_rows, ExecutionCanaryReceiptFacts, ReceiptFactsRecordOutcome, ReceiptFeeCoverage,
    ReceiptWsolCoverage, SqliteDiscoveryStore,
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

impl SqliteDiscoveryStore {
    pub fn load_execution_canary_receipt_facts(
        &self,
        order_id: &str,
    ) -> Result<Option<ExecutionCanaryReceiptFacts>> {
        receipt_facts_rows::load(&self.conn, order_id)
    }

    /// Validate durable order/proof identity and all known facts before any write.
    /// Unknown -> known is monotonic; missing data never erases a prior observation.
    pub fn record_execution_canary_receipt_facts(
        &self,
        facts: &ExecutionCanaryReceiptFacts,
        recorded_at: DateTime<Utc>,
    ) -> Result<ReceiptFactsRecordOutcome> {
        facts.validate()?;
        self.with_immediate_transaction_retry("record canary receipt facts", |conn| {
            record_on_conn(conn, facts, recorded_at)
        })
    }
}

pub(crate) fn record_on_conn(
    conn: &Connection,
    facts: &ExecutionCanaryReceiptFacts,
    recorded_at: DateTime<Utc>,
) -> Result<ReceiptFactsRecordOutcome> {
    validate_identity(conn, facts)?;
    let existing = receipt_facts_rows::load(conn, &facts.order_id)?;
    let (merged, outcome) = if let Some(existing) = existing {
        let merged = merge(&existing, facts)?;
        if merged == existing {
            settle_budget(conn, &merged, recorded_at)?;
            return Ok(ReceiptFactsRecordOutcome::Existing);
        }
        // Completed evidence is immutable; no historical enrichment/backfill.
        ensure!(
            !fill_exists(conn, &facts.order_id)?,
            "receipt facts already accounted"
        );
        (merged, ReceiptFactsRecordOutcome::Enriched)
    } else {
        ensure!(
            !fill_exists(conn, &facts.order_id)?,
            "receipt facts historical fill immutable"
        );
        (facts.clone(), ReceiptFactsRecordOutcome::Inserted)
    };
    write(conn, &merged, recorded_at)?;
    settle_budget(conn, &merged, recorded_at)?;
    Ok(outcome)
}

pub(crate) fn merge(
    old: &ExecutionCanaryReceiptFacts,
    new: &ExecutionCanaryReceiptFacts,
) -> Result<ExecutionCanaryReceiptFacts> {
    let mut merged = old.clone();
    ensure!(
        old.order_id == new.order_id
            && old.tx_signature == new.tx_signature
            && old.wallet_pubkey == new.wallet_pubkey
            && old.token == new.token
            && old.side == new.side
            && old.slot == new.slot
            && old.wallet_native_pre == new.wallet_native_pre
            && old.wallet_native_post == new.wallet_native_post
            && old.wallet_native_delta == new.wallet_native_delta,
        "receipt facts known identity/native conflict"
    );
    merge_known(&mut merged.transaction_fee, &new.transaction_fee)?;
    if merged.transaction_fee.is_some() {
        merged.fee_coverage = ReceiptFeeCoverage::Known;
    }
    merge_known(&mut merged.fee_payer, &new.fee_payer)?;
    merge_known(&mut merged.block_time, &new.block_time)?;
    merge_known(&mut merged.token_delta, &new.token_delta)?;
    if old.token_delta.is_some() && new.token_delta.is_some() {
        ensure!(
            old.token_coverage == new.token_coverage,
            "receipt facts token coverage conflict"
        );
    } else if new.token_delta.is_some() {
        merged.token_coverage = new.token_coverage;
        merged.token_coverage_reason = None;
    }
    if new.wsol_coverage == ReceiptWsolCoverage::Observed {
        merged.wsol_coverage = ReceiptWsolCoverage::Observed;
    }
    merged.validate()?;
    Ok(merged)
}

fn merge_known<T: Clone + PartialEq>(old: &mut Option<T>, new: &Option<T>) -> Result<()> {
    match (old.as_ref(), new.as_ref()) {
        (Some(a), Some(b)) => ensure!(a == b, "receipt facts known value conflict"),
        (None, Some(b)) => *old = Some(b.clone()),
        _ => {}
    }
    Ok(())
}

fn write(
    conn: &Connection,
    f: &ExecutionCanaryReceiptFacts,
    recorded_at: DateTime<Utc>,
) -> Result<()> {
    ensure!(conn.execute("INSERT INTO execution_canary_receipt_facts
        (order_id, tx_signature, wallet_pubkey, token, side, slot, wallet_native_pre,
        wallet_native_post, wallet_native_delta, transaction_fee, fee_coverage, fee_payer,
        token_delta_raw, token_decimals, token_coverage, token_coverage_reason, wsol_coverage,
        block_time, decomposition, recorded_at)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)
        ON CONFLICT(order_id) DO UPDATE SET transaction_fee = excluded.transaction_fee,
        fee_coverage = excluded.fee_coverage, fee_payer = excluded.fee_payer,
        token_delta_raw = excluded.token_delta_raw, token_decimals = excluded.token_decimals,
        token_coverage = excluded.token_coverage, token_coverage_reason = excluded.token_coverage_reason,
        wsol_coverage = excluded.wsol_coverage, block_time = excluded.block_time",
        params![f.order_id, f.tx_signature, f.wallet_pubkey, f.token, f.side, f.slot.to_string(),
            f.wallet_native_pre.as_u64().to_string(), f.wallet_native_post.as_u64().to_string(),
            f.wallet_native_delta.as_i128().to_string(), f.transaction_fee.map(|v| v.as_u64().to_string()),
            f.fee_coverage.as_str(), f.fee_payer, f.token_delta.map(|d| d.raw.to_string()),
            f.token_delta.map(|d| d.decimals), f.token_coverage.as_str(), f.token_coverage_reason,
            f.wsol_coverage.as_str(), f.block_time.map(|t| t.to_string()), f.decomposition.as_str(),
            recorded_at.to_rfc3339()])? == 1, "receipt facts write missing");
    ensure!(
        receipt_facts_rows::load(conn, &f.order_id)?.as_ref() == Some(f),
        "receipt facts write readback mismatch"
    );
    Ok(())
}

fn settle_budget(
    conn: &Connection,
    f: &ExecutionCanaryReceiptFacts,
    now: DateTime<Utc>,
) -> Result<()> {
    crate::owner_exit_fee::settle(
        conn, &f.order_id, &f.tx_signature, &f.wallet_pubkey,
        f.fee_payer.as_deref(), f.transaction_fee.map(|v| v.as_u64()),
        "successful", now,
    )?;
    crate::tiny_experiment::settle(
        conn,
        &f.order_id,
        &f.tx_signature,
        &f.wallet_pubkey,
        f.fee_payer.as_deref(),
        f.transaction_fee.map(|v| v.as_u64()),
        "successful",
        now,
    )
}
