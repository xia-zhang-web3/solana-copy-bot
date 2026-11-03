//! A separately authorized SELL may take one expired, provably unsigned hold.
use crate::{OwnerExitIntent, TinyBudgetClaim};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
#[path = "owner_bot_unsigned_archive.rs"]
mod archive;

fn migrated(c: &Connection) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version='0100_unsigned_owner_exit_transfer.sql')", [], |r| r.get(0))?)
}
pub(crate) fn pending(c: &Connection) -> Result<&'static str> {
    Ok(if migrated(c)? {
        "rpc_owned_sell_pending_obligations"
    } else {
        "rpc_owned_sell_handoffs"
    })
}
pub(crate) fn transferred(c: &Connection, id: &str, owner: &str) -> Result<bool> {
    Ok(migrated(c)? && c.query_row("SELECT EXISTS(SELECT 1 FROM unsigned_owner_exit_transfers WHERE intent_id=?1 AND old_owner=?2)",params![id,owner],|r|r.get(0))?)
}
pub(crate) fn fee_bound(c: &Connection, i: &OwnerExitIntent, b: &TinyBudgetClaim) -> Result<u64> {
    if migrated(c)?
        && c.query_row(
            "SELECT EXISTS(SELECT 1 FROM unsigned_owner_exit_transfers WHERE exit_intent_id=?1)",
            [&i.intent_id],
            |r| r.get(0),
        )?
    {
        return Ok(crate::tiny_experiment::TINY_TRANSACTION_FEE);
    }
    Ok(b.total_fee)
}
/// Only a parent of this exact expired normal cycle, never a failed-parent child.
pub(crate) fn eligible(c: &Connection, i: &OwnerExitIntent) -> Result<Option<(String, String)>> {
    if !migrated(c)?
        || !i.intent_id.starts_with("bot-receipt-exit:")
        || !crate::real_session::active(c)?
    {
        return Ok(None);
    }
    let raw: Option<(String,String,String,String)> = c.query_row(
        "SELECT h.intent_id,h.owner,h.snapshot,h.deadline FROM rpc_owned_sell_pending_obligations h
        WHERE h.position_id=?1 AND h.wallet=?2 AND h.experiment_id=?3
          AND h.state='unsigned_prepared' AND h.fee_reserve=100000
          AND length(h.unsigned_payload) BETWEEN 1 AND 1644 AND length(h.message_sha256)=64
          AND h.message_sha256 NOT GLOB '*[^0-9a-fA-F]*'
          AND h.total_fee BETWEEN 0 AND 100000 AND h.priority_fee BETWEEN 0 AND 50000
          AND h.priority_fee<=h.total_fee
          AND NOT EXISTS(SELECT 1 FROM unsigned_owner_exit_transfers t WHERE t.intent_id=h.intent_id)",
        params![i.position_id,i.wallet,i.run_id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((id, owner, snapshot, deadline)) = raw else {
        return Ok(None);
    };
    let s: serde_json::Value = serde_json::from_str(&snapshot)?;
    let receipts = s["receipts"].as_array().context("unsigned_exit_receipts")?;
    if deadline.parse::<DateTime<Utc>>()? > i.activated_at
        || s["quote"]["position_id"] != i.position_id
        || s["quote"]["mint"] != i.mint
        || s["quote"]["raw"].as_u64() != Some(i.amount_raw)
        || s["quote"]["decimals"].as_u64() != Some(6)
        || receipts.len() != 1
        || receipts[0]["contributor"]["order_id"] != i.buy_order_id
        || receipts[0]["contributor"]["tx_signature"] != i.buy_receipt_signature
    {
        return Ok(None);
    }
    let order: String = c.query_row(
        "SELECT 'exec-canary:'||order_id FROM rpc_owned_sell_handoffs WHERE intent_id=?1",
        [&id],
        |r| r.get(0),
    )?;
    let touched: bool = c.query_row("SELECT
        EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches WHERE intent_id=?1 OR handoff_owner=?2 OR order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_canary_dispatch WHERE order_id=?3 OR signal_id=?1 OR client_order_id=?2)
        OR EXISTS(SELECT 1 FROM orders WHERE order_id=?3 OR signal_id=?1 OR client_order_id=?2)
        OR EXISTS(SELECT 1 FROM execution_order_sources WHERE identity_id=?1 OR owned_sell_intent_id=?1)
        OR EXISTS(SELECT 1 FROM execution_tiny_reservations WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_canary_receipt_facts WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM fills WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE order_id=?3)",params![id,owner,order],|r|r.get(0))?;
    Ok((!touched).then_some((id, owner)))
}
/// Called inside the same IMMEDIATE transaction as fee reservation and dispatch.
pub(crate) fn transfer(c: &Connection, i: &OwnerExitIntent, now: DateTime<Utc>) -> Result<()> {
    let Some((id, owner)) = eligible(c, i)? else {
        return Ok(());
    };
    let handoff = archive::row(c, "rpc_owned_sell_handoffs", "intent_id", &id)?
        .context("unsigned_exit_handoff_missing")?;
    let order: String = c.query_row(
        "SELECT 'exec-canary:'||order_id FROM rpc_owned_sell_handoffs WHERE intent_id=?1",
        [&id],
        |r| r.get(0),
    )?;
    let metadata = archive::row(
        c,
        "execution_canary_build_plan_metadata",
        "order_id",
        &order,
    )?;
    ensure!(
        handoff.len() + metadata.as_ref().map_or(0, String::len) <= 1 << 20,
        "unsigned_exit_history_capacity"
    );
    c.execute("INSERT INTO unsigned_owner_exit_transfers(intent_id,old_owner,exit_intent_id,order_id,held_fee,handoff_json,build_plan_json,transferred_at) VALUES(?1,?2,?3,?4,100000,?5,?6,?7)",params![id,owner,i.intent_id,crate::owner_exit_order_id(&i.intent_id),handoff,metadata,now.to_rfc3339()])?;
    Ok(())
}
