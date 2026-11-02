//! Atomic canonical dispatch budget, shared by every cycle of one bounded grant.
use super::*;
use crate::{ExecutionCanaryDispatch, TinyBudgetClaim};
pub(crate) fn totals(c: &Connection) -> Result<(u64, u64)> {
    let a = load(c)?.context("local_session_missing")?;
    let (buy,fee):(u64,u64)=c.query_row("SELECT coalesce(sum(coalesce(e.requested_lamports,r.buy_lamports)),0),
        coalesce(sum(coalesce(r.actual_fee,r.fee_bound)),0) FROM execution_tiny_reservations r
        LEFT JOIN execution_tiny_capital_evidence e ON e.order_id=r.order_id WHERE r.experiment_id=?1",
        [&a.model.session_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let held:u64=c.query_row("SELECT coalesce(sum(h.fee_reserve),0) FROM rpc_owned_sell_handoffs h WHERE h.experiment_id=?1
        AND NOT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id AND d.handoff_owner=h.owner)",
        [&a.model.session_id],|r|r.get(0))?;
    Ok((
        buy,
        fee.checked_add(held).context("local_session_overflow")?,
    ))
}
pub(crate) fn cycle_counts(c: &Connection) -> Result<(u64, u64, u64)> {
    let p = state::current(c)?;
    let (buys, sells): (u64, u64) = c.query_row(
        &super::sql(c,"SELECT count(CASE WHEN side='buy' THEN 1 END),count(CASE WHEN side='sell' THEN 1 END)
        FROM local_bounded_orders WHERE cycle_id=?1")?,
        [&p.id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let unsigned:u64=c.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs h WHERE h.position_id=?1
        AND NOT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id AND d.handoff_owner=h.owner)",
        [p.position.as_deref().unwrap_or("")],|r|r.get(0))?;
    Ok((buys, sells + unsigned, totals(c)?.1))
}
pub(crate) fn can_prepare_sell(c: &Connection, now: DateTime<Utc>) -> Result<()> {
    let a = load(c)?.context("local_session_missing")?;
    let (transaction_cap,_,_)=super::bounds(c)?;
    let e = state::tiny(c)?.context("local_session_anchor_missing")?;
    ensure!(
        now >= e.activated_at && now < e.deadline && e.state == "active",
        "local_session_deadline_or_terminal"
    );
    let (buys, sells, _) = cycle_counts(c)?;
    ensure!(
        buys == 1
            && sells == 0
            && totals(c)?
                .1
                .checked_add(transaction_cap)
                .is_some_and(|v| v <= a.model.fee_cap),
        "local_session_sell_budget"
    );
    Ok(())
}
pub(crate) fn reserve(
    c: &Connection,
    d: &ExecutionCanaryDispatch,
    p: Option<&TinyBudgetClaim>,
    now: DateTime<Utc>,
    transferred: bool,
) -> Result<()> {
    let a = load(c)?.context("local_session_missing")?;
    state::refresh(c, now)?;
    let e = state::tiny(c)?.context("local_session_anchor_missing")?;
    let cycle = state::current(c)?;
    let p = p.context("local_session_budget_proof_required")?;
    let (transaction_cap,priority_cap,loss_cap)=super::bounds(c)?;
    crate::real_session_source::loss_within(c,loss_cap)?;
    ensure!(
        p.experiment_id == a.model.session_id
            && p.wallet == a.scope.bot_wallet
            && d.wallet == p.wallet
            && p.tx_signature == d.tx_signature
            && p.message_sha256 == d.message_sha256
            && p.transaction_sha256 == d.transaction_sha256,
        "local_session_claim_identity"
    );
    ensure!(
        now >= e.activated_at
            && now >= e.last_decision_at
            && now < e.deadline
            && e.state == "active"
            && cycle.state != "closed",
        "local_session_deadline_or_terminal"
    );
    ensure!(
        p.total_fee <= transaction_cap && p.priority_fee <= priority_cap && p.priority_fee <= p.total_fee,
        "local_session_transaction_fee"
    );
    let bound = if transferred { transaction_cap } else { p.total_fee };
    let (spent, fees) = totals(c)?;
    ensure!(
        fees.checked_add(bound)
            .is_some_and(|v| v <= a.model.fee_cap),
        "local_session_fee_cap"
    );
    match d.side.as_str() {
        "buy" => {
            let pending:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch WHERE order_id<>?1)",[&d.order_id],|r|r.get(0))?;
            let open:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM positions WHERE state='open' AND accounting_bucket='execution_canary')",[],|r|r.get(0))?;
            ensure!(
                !pending
                    && !open
                    && cycle.buy.is_none()
                    && d.signal_id == format!("native-buy-v1:{}", cycle.source_buy),
                "local_session_previous_obligation_or_event"
            );
            let amount = p
                .protected_capital
                .as_ref()
                .map(|v| v.requested_lamports)
                .or(p.buy_lamports);
            ensure!(
                amount == Some(10000000)
                    && spent
                        .checked_add(10000000)
                        .is_some_and(|v| v <= a.model.gross_buy_cap)
                    && fees
                        .checked_add(bound + transaction_cap)
                        .is_some_and(|v| v <= a.model.fee_cap),
                "local_session_buy_cap_or_exit_reserve"
            );
            ensure!(
                crate::tiny_experiment::protected::validate_claim(c, &e, p)?,
                "local_session_capital_proof"
            );
            c.execute(&super::sql(c,"UPDATE local_bounded_cycles SET state='active',buy_order_id=?2,position_id=?3,token=?4 WHERE cycle_id=?1 AND buy_order_id IS NULL")?,
                params![cycle.id,d.order_id,crate::execution_canary_position_open::execution_canary_position_id(&d.order_id),d.token])?;
        }
        "sell" => {
            ensure!(
                transferred
                    && e.token.as_deref() == Some(&d.token)
                    && p.buy_lamports == Some(0)
                    && p.protected_capital.is_none()
                    && crate::tiny_experiment::claim::confirmed_buy_position(
                        c, &e, &d.wallet, &d.token
                    )?,
                "local_session_sell_identity"
            );
        }
        _ => anyhow::bail!("local_session_side"),
    }
    c.execute(
        &super::sql(c,"INSERT INTO local_bounded_orders(order_id,cycle_id,side) VALUES(?1,?2,?3)")?,
        params![d.order_id, cycle.id, d.side],
    )?;
    c.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,
        fee_bound,priority_fee,fee_slot,reserved_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![d.order_id,p.experiment_id,d.tx_signature,
        d.wallet,d.side,d.message_sha256,d.transaction_sha256,p.buy_lamports,bound,p.priority_fee,p.fee_slot.to_string(),now.to_rfc3339()])?;
    crate::tiny_experiment::protected::record_claim(c, d, p)?;
    state::touch(c, now)?;
    Ok(())
}
