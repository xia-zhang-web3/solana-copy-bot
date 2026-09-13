//! Shares the signed tiny experiment guard; unsigned obligations survive restart.
use super::*;
pub(crate) fn unsigned_totals(c: &Connection) -> Result<(u64, u64)> {
    let exists:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='rpc_owned_sell_handoffs')",[],|r|r.get(0))?;
    if !exists {
        let applied:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version='0079_rpc_owned_sell_handoff.sql')",[],|r|r.get(0))?;
        ensure!(!applied, "owned_sell_schema_missing");
        return Ok((0, 0));
    }
    crate::rpc_owned_sell_handoff::required(c)?;
    let transfer: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='rpc_owned_sell_dispatches')",
        [],
        |r| r.get(0),
    )?;
    let query = if transfer {
        crate::rpc_owned_sell_handoff::dispatch::required(c)?;
        "SELECT COUNT(*),COALESCE(SUM(fee_reserve),0) FROM rpc_owned_sell_handoffs h WHERE NOT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id AND d.handoff_owner=h.owner)"
    } else {
        "SELECT COUNT(*),COALESCE(SUM(fee_reserve),0) FROM rpc_owned_sell_handoffs"
    };
    Ok(c.query_row(query, [], |r| Ok((r.get(0)?, r.get(1)?)))?)
}
pub(crate) fn check_owned_sell_owner(
    c: &Connection,
    id: &str,
    wallet: &str,
    token: &str,
    position: &str,
    now: DateTime<Utc>,
) -> Result<TinyExperiment> {
    let e = load(c)?.context("tiny_budget_inactive")?;
    ensure!(
        e.id == id
            && e.wallet == wallet
            && e.token.as_deref() == Some(token)
            && e.position_id.as_deref() == Some(position),
        "tiny_budget_position_identity"
    );
    ensure!(
        now >= e.activated_at && now >= e.last_decision_at && now < e.deadline,
        "tiny_budget_deadline"
    );
    // Refresh marks the second reserved slot stopped. Its existing owner may
    // finish preparation, but cannot create another reservation or spend it.
    ensure!(
        e.state == "active"
            || (e.state == "stopped"
                && matches!(
                    e.stop_reason.as_deref(),
                    Some("tiny_budget_sell_slots" | "tiny_budget_fee_exhausted")
                )),
        "tiny_budget_stopped"
    );
    ensure!(
        claim::confirmed_buy_position(c, &e, wallet, token)?,
        "tiny_budget_position_unconfirmed"
    );
    ensure!(
        crate::execution_canary_receipt::pending_token_order(c, token, None)?.is_none(),
        "sell_token_accounting_pending"
    );
    let other:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM positions WHERE state='open' AND accounting_bucket='execution_canary' AND position_id!=?1)",[position],|r|r.get(0))?;
    ensure!(!other, "tiny_budget_position_ambiguous");
    if protected::mode(c)? == "protected_native_capital" {
        protected::load_policy(c, &e)?;
    }
    Ok(e)
}
pub(crate) fn prepare_owned_sell(
    c: &Connection,
    id: &str,
    wallet: &str,
    token: &str,
    position: &str,
    now: DateTime<Utc>,
) -> Result<TinyExperiment> {
    refresh(c, now)?;
    let e = check_owned_sell_owner(c, id, wallet, token, position, now)?;
    ensure!(e.state == "active", "tiny_budget_stopped");
    let (buys, sells, fees) = totals(c)?;
    ensure!(buys == 1 && sells < 2, "tiny_budget_sell_slots");
    ensure!(
        fees.checked_add(TINY_TRANSACTION_FEE)
            .is_some_and(|v| v <= TINY_TOTAL_FEE),
        "tiny_budget_fee_exhausted"
    );
    Ok(e)
}
