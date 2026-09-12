use super::{RecognizedSellCashEvents, UndatedSellCashObligations};
use crate::{
    execution_cash_settlement::validate_existing, receipt_facts_identity::validate_identity,
    receipt_facts_rows, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

pub(super) fn events(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> Result<RecognizedSellCashEvents> {
    let mut value = RecognizedSellCashEvents {
        signed_net_cash_result_lamports: "0".into(),
        gross_negative_cash_result_lamports: "0".into(),
        events: 0,
        negative_events: 0,
        zero_events: 0,
        positive_events: 0,
        partial_events: 0,
        full_events: 0,
    };
    let (mut net, mut gross) = (0_i128, 0_u128);
    visit_events(conn, since, as_of, |cash, selected| {
        if !selected {
            return Ok(());
        }
        let delta = cash.cash_result_delta.as_i128();
        net = net
            .checked_add(delta)
            .context("cash day signed net overflow")?;
        if delta < 0 {
            gross = gross
                .checked_add(delta.unsigned_abs())
                .context("cash day gross negative overflow")?;
            increment(&mut value.negative_events)?;
        } else if delta == 0 {
            increment(&mut value.zero_events)?;
        } else {
            increment(&mut value.positive_events)?;
        }
        increment(&mut value.events)?;
        increment(if cash.remaining_quantity.raw() == 0 {
            &mut value.full_events
        } else {
            &mut value.partial_events
        })?;
        Ok(())
    })?;
    value.signed_net_cash_result_lamports = net.to_string();
    value.gross_negative_cash_result_lamports = gross.to_string();
    Ok(value)
}

// All consumers share receipt, completion, ownership and date validation, including
// claims outside the requested day. Group by persisted position ID for bounded callers.
pub(crate) fn visit_events(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
    mut consume: impl FnMut(&crate::ExecutionCanaryCashSettlement, bool) -> Result<()>,
) -> Result<()> {
    validate_schema(conn)?;
    validate_receipt_ownership(conn)?;
    // Position ordering cannot be used to prove order uniqueness after corruption.
    let mut duplicates = conn.prepare(
        "SELECT 1 FROM fills WHERE order_id LIKE 'exec-canary:%'
         GROUP BY order_id HAVING COUNT(*)>1 LIMIT 1",
    )?;
    ensure!(
        duplicates.query([])?.next()?.is_none(),
        "duplicate cash day fill order"
    );
    let mut statement = conn.prepare(
        "SELECT order_id,settlement_ts,accounting_basis FROM fills
         WHERE order_id LIKE 'exec-canary:%' ORDER BY position_id,order_id",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let basis: String = row.get(2)?;
        if basis == "legacy_unclassified" {
            continue;
        }
        ensure!(
            basis == "receipt_native_cash",
            "unsupported cash day fill basis for {id}"
        );
        let timestamp: String = row.get(1)?;
        let at = DateTime::parse_from_rfc3339(&timestamp)
            .with_context(|| format!("invalid cash settlement timestamp for {id}"))?
            .with_timezone(&Utc);
        let facts =
            receipt_facts_rows::load(conn, &id)?.context("cash day event missing receipt facts")?;
        facts.validate()?;
        ensure!(facts.side == "sell", "cash day settlement is not SELL");
        validate_identity(conn, &facts)?;
        let cash = validate_existing(conn, &id)?;
        consume(&cash, at >= since && at < as_of)?;
    }
    Ok(())
}

fn validate_receipt_ownership(conn: &Connection) -> Result<()> {
    // A wallet's native balance change belongs to one order, regardless of mint,
    // slot or accounting date. Validate the whole ledger in the caller's snapshot.
    // Facts are keyed by order_id: one indexed join, one grouping pass, no growing
    // Rust identity set or per-event history scan. SQLite may use a temporary sort.
    let mut statement = conn.prepare(
        "SELECT 1 FROM fills f
         JOIN execution_canary_receipt_facts r ON r.order_id=f.order_id
         WHERE f.order_id LIKE 'exec-canary:%' AND f.accounting_basis='receipt_native_cash'
         GROUP BY r.wallet_pubkey,r.tx_signature
         HAVING COUNT(DISTINCT f.order_id)>1 LIMIT 1",
    )?;
    ensure!(
        statement.query([])?.next()?.is_none(),
        "duplicate wallet cash receipt claimed by multiple canary orders"
    );
    Ok(())
}

pub(crate) fn obligations(conn: &Connection) -> Result<UndatedSellCashObligations> {
    // No time predicate: these rows have no proven accounting timestamp. SELL may
    // be proven by the durable proof even if the original signal is missing.
    // A fill wins over order status; legacy history is never decomposed into events.
    let mut statement = conn.prepare(
        "SELECT o.status,o.tx_signature,f.accounting_basis
         FROM orders o LEFT JOIN copy_signals s ON s.signal_id=o.signal_id
         LEFT JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
         LEFT JOIN fills f ON f.order_id=o.order_id
         WHERE o.order_id LIKE 'exec-canary:%'
           AND (lower(s.side)='sell' OR lower(p.side)='sell')
           AND (f.accounting_basis='legacy_unclassified'
                OR (f.order_id IS NULL AND o.status IN (?1,?2,?3)))",
    )?;
    let mut rows = statement.query(params![
        EXECUTION_STATUS_CANARY_SUBMITTED,
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
        EXECUTION_STATUS_CANARY_CONFIRMED,
    ])?;
    let mut value = UndatedSellCashObligations {
        scope: "current_DB_read_snapshot_not_historical_as_of_no_day_attribution".into(),
        legacy_fill_orders: 0,
        submitted_without_fill: 0,
        confirmed_unreconciled_without_fill: 0,
        confirmed_without_fill: 0,
        without_signature: 0,
    };
    while let Some(row) = rows.next()? {
        let status: String = row.get(0)?;
        let signature: Option<String> = row.get(1)?;
        let basis: Option<String> = row.get(2)?;
        if basis.as_deref() == Some("legacy_unclassified") {
            increment(&mut value.legacy_fill_orders)?;
            continue;
        }
        increment(match status.as_str() {
            EXECUTION_STATUS_CANARY_SUBMITTED => &mut value.submitted_without_fill,
            EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED => {
                &mut value.confirmed_unreconciled_without_fill
            }
            EXECUTION_STATUS_CANARY_CONFIRMED => &mut value.confirmed_without_fill,
            _ => anyhow::bail!("cash obligation status outside selected domain"),
        })?;
        if signature.as_ref().is_none_or(|s| s.trim().is_empty()) {
            increment(&mut value.without_signature)?;
        }
    }
    Ok(value)
}

fn increment(value: &mut u64) -> Result<()> {
    *value = value.checked_add(1).context("cash day count overflow")?;
    Ok(())
}

// Prepare all columns used by the shared exact validators, even for an empty
// selected cohort. No schema creation or validation of invented facts occurs.
fn validate_schema(conn: &Connection) -> Result<()> {
    conn.prepare(
        "SELECT f.order_id,f.position_id,f.token,f.qty_raw,f.qty_decimals,
         f.remaining_qty_raw,f.wallet_native_delta_lamports,f.entry_basis_lamports,
         f.remaining_cost_lamports,f.cash_result_delta_lamports,
         f.accumulated_cash_result_lamports,f.accounting_basis,f.settlement_ts,
         r.tx_signature,r.wallet_pubkey,r.token,r.side,r.slot,
         r.wallet_native_pre,r.wallet_native_post,r.wallet_native_delta,r.transaction_fee,
         r.fee_coverage,r.fee_payer,r.token_delta_raw,r.token_decimals,r.token_coverage,
         r.token_coverage_reason,r.wsol_coverage,r.block_time,r.decomposition,
         o.status,o.tx_signature,s.token,s.side,p.reason,p.tx_signature,p.wallet_pubkey,
         p.token,p.side,p.slot,p.confirmation_status
         FROM fills f LEFT JOIN execution_canary_receipt_facts r ON r.order_id=f.order_id
         LEFT JOIN orders o ON o.order_id=f.order_id
         LEFT JOIN copy_signals s ON s.signal_id=o.signal_id
         LEFT JOIN execution_canary_receipt_proofs p ON p.order_id=f.order_id WHERE 0",
    )
    .context("required SELL cash day schema unavailable")?;
    Ok(())
}
