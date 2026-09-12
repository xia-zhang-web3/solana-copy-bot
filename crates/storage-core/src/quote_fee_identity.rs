use crate::{
    execution_canary_quote_pnl_rows::{QuoteEvent, QuotePnlRow},
    SqliteDiscoveryStore,
};
use anyhow::Result;

pub(crate) const EVENT_COLUMNS: &str = "event_id, quote_status, request_ts, signal_ts,
 decision_delay_ms, quote_latency_ms, quote_in_amount_raw, quote_out_amount_raw,
 quote_price_sol, shadow_price_sol, slippage_bps, price_impact_pct, route_plan_json,
 priority_fee_status, priority_fee_lamports, decision_status, decision_reason,
 leader_notional_sol, signal_id, shadow_closed_trade_id, wallet_id, token, side";

pub(crate) fn events(
    store: &SqliteDiscoveryStore,
    predicate: &str,
    args: impl rusqlite::Params,
) -> Result<Vec<QuoteEvent>> {
    let sql = format!(
        "SELECT {EVENT_COLUMNS}, {} FROM execution_quote_canary_events WHERE {predicate}",
        crate::quote_http_started_expr(&store.conn, "execution_quote_canary_events", "")?
    );
    let mut stmt = store.conn.prepare(&sql)?;
    let mut rows = stmt.query(args)?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(QuoteEvent::read(row, 0)?);
    }
    Ok(result)
}

pub(crate) fn bind_entry(
    store: &SqliteDiscoveryStore,
    row: &mut QuotePnlRow,
    candidates: &[QuoteEvent],
) -> Result<()> {
    let candidates: Vec<_> = candidates
        .iter()
        .filter(|q| q.signal_ts == Some(row.opened_ts))
        .collect();
    if candidates.len() != 1 {
        row.binding_error = Some(if candidates.is_empty() {
            "missing_entry_quote"
        } else {
            "ambiguous_entry_quote"
        });
        return Ok(());
    }
    row.buy = candidates[0].clone();
    let q = &row.buy;
    let canonical = q
        .event_id
        .as_deref()
        .and_then(|s| s.strip_prefix("quote:entry:"));
    if q.closed_id.is_some()
        || canonical.is_some_and(|id| q.signal_id.as_deref() != Some(id))
        || !valid_common(store, q, row, true)?
    {
        row.binding_error = Some("entry_binding_conflict");
    } else {
        row.entry_attributed = true;
    }
    Ok(())
}

pub(crate) fn bind_exit(store: &SqliteDiscoveryStore, row: &mut QuotePnlRow) -> Result<()> {
    // Both indexed bindings are inspected; a canonical ID with a conflicting persisted
    // close ID cannot be silently hidden by the closed_side join.
    let quotes = events(
        store,
        "shadow_closed_trade_id = ?1 OR event_id = ?2",
        rusqlite::params![row.id, format!("quote:close:{}", row.id)],
    )?;
    if quotes.len() != 1 {
        row.binding_error.get_or_insert(if quotes.is_empty() {
            "missing_exit_quote"
        } else {
            "ambiguous_exit_quote"
        });
        return Ok(());
    }
    row.sell = quotes[0].clone();
    let q = &row.sell;
    let canonical = q
        .event_id
        .as_deref()
        .and_then(|s| s.strip_prefix("quote:close:"));
    if q.closed_id != Some(row.id)
        || canonical.is_some_and(|id| id != row.id.to_string())
        || q.signal_id.as_ref().is_some_and(|id| id != &row.signal_id)
        || !valid_common(store, q, row, false)?
    {
        row.binding_error.get_or_insert("exit_binding_conflict");
    }
    Ok(())
}

fn valid_common(
    store: &SqliteDiscoveryStore,
    q: &QuoteEvent,
    row: &QuotePnlRow,
    buy: bool,
) -> Result<bool> {
    let side = if buy { "buy" } else { "sell" };
    let time = if buy { row.opened_ts } else { row.closed_ts };
    if q.wallet != row.wallet_id
        || q.token != row.token
        || !q.side.eq_ignore_ascii_case(side)
        || q.signal_ts != Some(time)
        || !q.request_ts.is_some_and(|t| t >= time)
        || row.closed_ts < row.opened_ts
    {
        return Ok(false);
    }
    if let Some(signal) = &q.signal_id {
        if let Some(s) = store.load_copy_signal_by_signal_id(signal)? {
            if s.wallet_id != row.wallet_id
                || s.token != row.token
                || !s.side.eq_ignore_ascii_case(side)
                || s.ts != time
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
