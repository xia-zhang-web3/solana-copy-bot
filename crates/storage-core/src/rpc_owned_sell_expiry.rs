//! One normal-session replacement, only before the durable dispatch/send boundary.
use super::super::{dispatch::Prepared, Handoff};
use crate::{association_inbox::InboxLimits, tiny_experiment};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

fn enabled(c: &Connection) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='owned_sell_quote_expiry_recoveries')",[],|r|r.get(0))?
        && c.query_row("SELECT EXISTS(SELECT 1 FROM real_session_sessions WHERE singleton=1)",[],|r|r.get(0))?)
}
pub(super) fn exhausted(c: &Connection, id: &str) -> Result<bool> {
    if !enabled(c)? {
        return Ok(false);
    }
    // Simulation recovery and deadline recovery share one normal-session refresh.
    Ok(c.query_row("SELECT (SELECT count(*) FROM owned_sell_quote_expiry_recoveries WHERE intent_id=?1)
        +(SELECT count(*) FROM owned_sell_preparation_recoveries WHERE json_extract(handoff,'$.intent_id')=?1)>=1",
        [id],|r|r.get(0))?)
}
fn no_dispatch(c: &Connection, h: &Handoff) -> Result<bool> {
    let order = format!("exec-canary:{}", h.order_id);
    Ok(!c.query_row("SELECT
        EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches WHERE intent_id=?1 OR handoff_owner=?2 OR order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_canary_dispatch WHERE order_id=?3 OR signal_id=?1 OR client_order_id=?2)
        OR EXISTS(SELECT 1 FROM orders WHERE order_id=?3 OR signal_id=?1 OR client_order_id=?2)
        OR EXISTS(SELECT 1 FROM execution_order_sources WHERE identity_id=?1 OR owned_sell_intent_id=?1)
        OR EXISTS(SELECT 1 FROM execution_tiny_reservations WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_canary_receipt_facts WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM fills WHERE order_id=?3)
        OR EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE order_id=?3)",
        params![h.intent_id,h.owner,order],|r|r.get::<_,bool>(0))?)
}
pub(super) fn candidate(c: &Connection, id: &str, now: DateTime<Utc>) -> Result<Option<Handoff>> {
    if !enabled(c)? || exhausted(c, id)? {
        return Ok(None);
    }
    let raw:Option<(String,String,String,String,String,String,String,String,String)>=c.query_row(
        "SELECT order_id,owner,snapshot,config_sha256,authority,quote,experiment_id,wallet,deadline
        FROM rpc_owned_sell_handoffs WHERE intent_id=?1 AND state='unsigned_prepared'
        AND unsigned_payload IS NOT NULL AND length(message_sha256)=64
        AND total_fee BETWEEN 0 AND 100000 AND priority_fee BETWEEN 0 AND 50000
        AND priority_fee<=total_fee AND fee_reserve=100000 AND deadline<=?2",
        params![id,now.to_rfc3339()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
    let Some((
        order_id,
        owner,
        snapshot,
        config_sha256,
        authority,
        quote,
        experiment_id,
        wallet,
        deadline,
    )) = raw
    else {
        return Ok(None);
    };
    let h = Handoff {
        intent_id: id.into(),
        order_id,
        owner,
        snapshot: serde_json::from_str(&snapshot)?,
        config_sha256,
        authority,
        quote: serde_json::from_str(&quote)?,
        experiment_id,
        wallet,
        deadline: deadline.parse()?,
    };
    if now < h.deadline || !no_dispatch(c, &h)? {
        return Ok(None);
    }
    let e = tiny_experiment::check_owned_sell_owner(
        c,
        &h.experiment_id,
        &h.wallet,
        &h.snapshot.quote.mint,
        &h.snapshot.quote.position_id,
        now,
    )?;
    super::super::cohort_sell_deadline(
        c,
        e.buy_order_id.as_deref().unwrap_or(""),
        &h.experiment_id,
        &h.snapshot.quote.mint,
        now,
    )?;
    Ok(Some(h))
}
fn build_plan(c: &Connection, order: &str) -> Result<Option<String>> {
    let mut statement =
        c.prepare("SELECT * FROM execution_canary_build_plan_metadata WHERE order_id=?1")?;
    let names = statement
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut rows = statement.query([order])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let mut data = serde_json::Map::new();
    for (index, name) in names.into_iter().enumerate() {
        use rusqlite::types::ValueRef;
        let value = match row.get_ref(index)? {
            ValueRef::Null => serde_json::Value::Null,
            ValueRef::Integer(v) => serde_json::json!(v),
            ValueRef::Real(v) => serde_json::Value::Number(
                serde_json::Number::from_f64(v).context("owned_sell_expiry_metadata_number")?,
            ),
            ValueRef::Text(v) => serde_json::Value::String(std::str::from_utf8(v)?.into()),
            ValueRef::Blob(_) => anyhow::bail!("owned_sell_expiry_metadata_blob"),
        };
        data.insert(name, value);
    }
    Ok(Some(serde_json::to_string(&data)?))
}
pub(super) fn archive(
    c: &Connection,
    old: &Handoff,
    new: &Handoff,
    l: InboxLimits,
    now: DateTime<Utc>,
) -> Result<()> {
    ensure!(
        candidate(c, &old.intent_id, now)?.as_ref() == Some(old),
        "owned_sell_expiry_changed"
    );
    let (payload,message_sha256,total_fee,priority_fee):(String,String,u64,u64)=c.query_row(
        "SELECT unsigned_payload,message_sha256,total_fee,priority_fee FROM rpc_owned_sell_handoffs WHERE intent_id=?1",
        [&old.intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    // Old durable operands plus experiment/limits observed at recovery. This
    // history is audit evidence, never a source for restoring financial authority.
    let prepared = Prepared {
        handoff: old.clone(),
        experiment: tiny_experiment::owned_snapshot(c)?,
        limits: (l.count, l.bytes, l.busy_ms),
        payload,
        message_sha256,
        total_fee,
        priority_fee,
    };
    ensure!(
        prepared.matches_saved(c)? && no_dispatch(c, old)?,
        "owned_sell_expiry_no_dispatch_unproven"
    );
    let serialized = serde_json::to_string(&prepared)?;
    let metadata = build_plan(c, &prepared.order_id())?;
    ensure!(
        serialized.len() + metadata.as_ref().map_or(0, String::len) <= l.bytes.min(1 << 20),
        "owned_sell_expiry_history_capacity"
    );
    c.execute("INSERT INTO owned_sell_quote_expiry_recoveries(intent_id,old_owner,new_owner,prepared_json,build_plan_json,recovered_at) VALUES(?1,?2,?3,?4,?5,?6)",
        params![old.intent_id,old.owner,new.owner,serialized,metadata,now.to_rfc3339()])?;
    Ok(())
}
