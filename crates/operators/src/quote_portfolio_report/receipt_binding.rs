//! Explicit selected receipt-derived expense; virtual association stays caller-supplied.
use super::{convert, input::*, time_binding::instant};
use crate::quote_portfolio as k;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Whole-input structural validation before any replay or reference resolution.
pub fn preflight(input: &Input) -> Result<&'static str> {
    let (mut scalar, mut bound) = (false, false);
    let mut wallet_payer = None;
    let mut payments = BTreeMap::new();
    for e in &input.events {
        let ActionInput::FailedAttemptExpense {
            amount,
            receipt_ref,
        } = &e.action
        else {
            continue;
        };
        let Some(r) = receipt_ref else {
            scalar = true;
            continue;
        };
        bound = true;
        r.source_provenance.validate()?;
        ensure!(
            [&r.order_id, &r.tx_signature, &r.wallet, &r.payer]
                .iter()
                .all(|s| !s.trim().is_empty()),
            "empty failed expense receipt identity"
        );
        ensure!(
            r.wallet == r.payer,
            "receipt-bound third-party payer unsupported"
        );
        let pair = (&r.wallet, &r.payer);
        ensure!(
            wallet_payer.is_none_or(|old| old == pair),
            "receipt-bound expenses require one wallet/payer"
        );
        wallet_payer = Some(pair);
        if let Some(a) = amount {
            a.provenance.validate()?;
            if let Presence::Known(n) = &a.value {
                raw(n)?;
            }
        }
        let caller = json!(e);
        if let Some(old) = payments.insert(&r.tx_signature, caller.clone()) {
            ensure!(
                old == caller,
                "receipt payment relabelled or conflicting; coverage unresolved"
            );
        }
    }
    ensure!(
        !(scalar && bound),
        "mixed scalar and receipt-bound failed expenses"
    );
    Ok(if bound {
        "receipt_bound"
    } else {
        "scalar_caller_assertion"
    })
}

pub fn expense(
    store: &SqliteStore,
    r: &ReceiptRef,
    assertion: Option<&Amount>,
    e: &EventInput,
    window: &Window,
) -> (k::Lamports, Value) {
    // The evidence's origin drives this operand's basis, including Unknown/zero.
    let provenance = convert::origin(&r.source_provenance);
    match resolve(store, r, assertion, e, window) {
        Ok((fee, verified)) => (
            k::Lamports {
                amount: k::Knowledge::Known(fee),
                provenance,
            },
            verified,
        ),
        Err(err) => {
            let reason = format!("{err:#}");
            (
                k::Lamports {
                    amount: k::Knowledge::Unknown(reason.clone()),
                    provenance,
                },
                json!({"mode":"receipt_bound","state":"unavailable","reason":reason,
                    "source_provenance":r.source_provenance,
                    "caller_assertions":{"receipt_ref":r,"amount":assertion},
                    "wallet_reconciliation_complete":false}),
            )
        }
    }
}

fn utc(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(s)
        .context("invalid receipt evidence timestamp")?
        .with_timezone(&Utc))
}
fn resolve(
    store: &SqliteStore,
    r: &ReceiptRef,
    assertion: Option<&Amount>,
    e: &EventInput,
    window: &Window,
) -> Result<(u64, Value)> {
    let a = assertion.context("receipt-bound amount assertion absent")?;
    let Presence::Known(amount) = &a.value else {
        anyhow::bail!("receipt-bound amount assertion unknown");
    };
    let asserted = raw(amount)?;
    let (row, recorded_at) = store.failed_expense_receipt_operand(&r.order_id)?;
    let task = row
        .task
        .as_ref()
        .context("failed expense task unavailable")?;
    let facts = row
        .facts
        .as_ref()
        .context("failed receipt facts unavailable")?;
    let fee = raw(row
        .wallet_fee_lamports
        .as_deref()
        .with_context(|| format!("validated failed fee unavailable: {}", row.reason))?)?;
    let recorded = utc(recorded_at
        .as_deref()
        .context("failed expense ledger time unavailable")?)?;
    let operation = utc(&row.operation_at)?;
    ensure!(
        row.order_id == r.order_id
            && task.order_id == r.order_id
            && task.tx_signature == r.tx_signature
            && facts.tx_signature == r.tx_signature
            && task.wallet == r.wallet
            && facts.wallet == r.wallet
            && facts.payer.as_deref() == Some(r.payer.as_str()),
        "failed receipt reference identity mismatch"
    );
    ensure!(asserted == fee, "failed receipt amount assertion mismatch");
    ensure!(
        utc(&r.operation_at)? == operation
            && utc(&task.operation_at)? == operation
            && utc(&r.recorded_at)? == recorded,
        "failed receipt reference timestamp mismatch"
    );
    let transition = instant(raw(&e.unix_ms)?)?;
    ensure!(
        instant(raw(&window.start_unix_ms)?)? <= operation
            && operation <= recorded
            && recorded <= transition
            && transition <= instant(raw(&window.end_unix_ms)?)?,
        "failed receipt evidence chronology outside scenario"
    );
    Ok((
        fee,
        json!({"mode":"receipt_bound","state":"validated_db_operand",
        "validated_payment":{"order_id":row.order_id,"tx_signature":facts.tx_signature,
            "wallet":facts.wallet,"payer":facts.payer,"wallet_fee_lamports":fee.to_string(),
            "operation_at":operation.to_rfc3339_opts(SecondsFormat::Nanos, true),
            "recorded_at":recorded.to_rfc3339_opts(SecondsFormat::Nanos, true)},
        "source_provenance":r.source_provenance,
        "caller_assertions":{"amount":a,"event_id":e.id,"position_id":e.position_id,
            "association":"receipt-derived operand selected for this virtual scenario; causation unproved"},
        "native_delta_lamports":row.native_delta_lamports,
        "unexplained_delta_lamports":row.unexplained_delta_lamports,
        "wallet_reconciliation_complete":false,
        "scope":"validated local ledger; no current-network truth or full cash-history proof"}),
    ))
}
