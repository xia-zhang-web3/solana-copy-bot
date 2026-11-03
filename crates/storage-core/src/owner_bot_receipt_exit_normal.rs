//! One expired normal cycle's receipt exit. The legacy singleton stays immutable.
use crate::{
    bounded_session, controlled_receipt_lot::Scope, native_buy::TechnicalCohortAuthority,
    ExecutionCanaryDispatch, OwnerExitIntent, TinyBudgetClaim,
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection};

pub(super) fn binding_sql(
    c: &Connection,
    i: &OwnerExitIntent,
    scope: &Scope,
    cohort: &TechnicalCohortAuthority,
    legacy: &str,
) -> Result<String> {
    let Some(a) = crate::real_session::load(c)? else {
        return Ok(legacy.into());
    };
    // The existing receipt-exit parent contract permits exactly one BUY. Do not
    // turn this compatibility path into a fresh or multi-cycle grant.
    let p = bounded_session::state::current(c)?;
    let mut expected = a.scope.clone();
    expected.source_buy_intent_id = p.id.clone();
    ensure!(
        a.max_cycles == 1
            && a.cohort == *cohort
            && a.cohort.run_id == i.run_id
            && p.scope == expected
            && *scope == p.scope
            && p.id == format!("session-cycle:{}", p.source_buy)
            && super::source(&i.intent_id) == Some(p.source_buy.as_str())
            && p.buy.as_deref() == Some(&i.buy_order_id)
            && p.position.as_deref() == Some(&i.position_id)
            && p.token.as_deref() == Some(&i.mint),
        "normal_receipt_exit_cycle_identity"
    );
    let tiny = bounded_session::state::tiny(c)?.context("normal_receipt_exit_anchor_missing")?;
    ensure!(
        tiny.id == i.run_id
            && tiny.wallet == i.wallet
            && tiny.buy_order_id.as_deref() == Some(&i.buy_order_id)
            && tiny.position_id.as_deref() == Some(&i.position_id),
        "normal_receipt_exit_anchor_identity"
    );
    let (cycles, buys, other): (u64, u64, u64) = c.query_row(
        "SELECT (SELECT count(*) FROM real_session_cycles),
        (SELECT count(*) FROM real_session_orders WHERE cycle_id=?1 AND order_id=?2 AND side='buy'),
        (SELECT count(*) FROM real_session_orders WHERE cycle_id<>?1
          OR NOT ((order_id=?2 AND side='buy') OR (order_id=?3 AND side='sell')))",
        params![
            p.id,
            i.buy_order_id,
            crate::owner_exit_order_id(&i.intent_id)
        ],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    ensure!(
        cycles == 1 && buys == 1 && other == 0,
        "normal_receipt_exit_cycle_orders"
    );
    // Every original native/fence/receipt/reservation predicate below remains.
    // Only their canonical session relations replace the legacy relations.
    Ok(legacy.replace("JOIN native_buy_cohort_decisions b", "JOIN real_session_native_decisions b")
        .replace("JOIN execution_tiny_experiment t ON t.buy_order_id=o.order_id", "JOIN (
        SELECT t.experiment_id,t.wallet,t.deadline,t.policy_mode,p.buy_order_id,p.token,p.position_id
        FROM execution_tiny_experiment t JOIN real_session_cycles p ON p.session_id=t.experiment_id
        JOIN real_session_native_decisions n ON n.cycle_id=p.cycle_id AND n.signature=p.source_buy_signature
        WHERE t.singleton=1 AND t.state='active' AND t.buy_order_id IS NULL
          AND t.token IS NULL AND t.position_id IS NULL) t ON t.buy_order_id=o.order_id"))
}

pub(crate) fn fees(c: &Connection, run: &str) -> Result<u64> {
    if !crate::real_session::active(c)? {
        return Ok(0);
    }
    Ok(c.query_row(
        "SELECT coalesce(sum(coalesce(f.actual_fee,f.fee_bound)),0)
        FROM owner_exit_fee_reservations f JOIN real_session_orders b ON b.order_id=f.order_id
        WHERE f.run_id=?1 AND b.side='sell'",
        [run],
        |r| r.get(0),
    )?)
}

pub(crate) fn link(
    c: &Connection,
    i: &OwnerExitIntent,
    d: &ExecutionCanaryDispatch,
    budget: &TinyBudgetClaim,
) -> Result<()> {
    let Some(a) = crate::real_session::load(c)? else {
        return Ok(());
    };
    ensure!(
        crate::bot_receipt_exit_id(&i.intent_id),
        "normal_receipt_exit_policy"
    );
    super::validate(c, i)?;
    let p = bounded_session::state::current(c)?;
    ensure!(
        p.state == "active"
            && d.side == "sell"
            && d.order_id == crate::owner_exit_order_id(&i.intent_id)
            && d.wallet == a.scope.bot_wallet
            && budget.total_fee <= a.total_fee_cap
            && bounded_session::budget::totals(c)?
                .1
                .checked_add(super::unsigned::fee_bound(c, i, budget)?)
                .is_some_and(|v| v <= a.fee_cap),
        "normal_receipt_exit_fee_or_cycle"
    );
    c.execute(
        "INSERT INTO real_session_orders(order_id,cycle_id,side) VALUES(?1,?2,'sell')",
        params![d.order_id, p.id],
    )?;
    Ok(())
}

pub(crate) fn complete(c: &Connection, cycle: &str, position: &str) -> Result<bool> {
    if !crate::real_session::active(c)? {
        return Ok(false);
    }
    let id: Option<String> = c.query_row("SELECT COALESCE(s.owner_exit_child_id,s.owner_exit_intent_id) FROM real_session_orders b
        JOIN orders o ON o.order_id=b.order_id JOIN execution_order_sources s ON s.identity_id=o.signal_id
        WHERE b.cycle_id=?1 AND b.side='sell' AND COALESCE(s.owner_exit_child_id,s.owner_exit_intent_id) IS NOT NULL", [cycle], |r| r.get(0))
        .optional()?;
    let Some(id) = id else { return Ok(false) };
    let i = crate::owner_exit_load::load(c, &id)?.context("normal_receipt_exit_intent_missing")?;
    ensure!(
        i.position_id == position,
        "normal_receipt_exit_position_identity"
    );
    super::historical(c, &i)?;
    let done: bool = c.query_row(&format!("SELECT EXISTS(SELECT 1 FROM real_session_orders b
        JOIN orders o ON o.order_id=b.order_id
        JOIN execution_canary_dispatch d ON d.order_id=o.order_id
        JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        JOIN execution_canary_receipt_facts r ON r.order_id=o.order_id
        JOIN fills f ON f.order_id=o.order_id JOIN positions x ON x.position_id=f.position_id
        JOIN owner_exit_fee_reservations v ON v.order_id=o.order_id
        WHERE b.cycle_id=?1 AND b.side='sell' AND o.order_id=?2
          AND o.status='execution_canary_confirmed' AND o.attempt=1
          AND d.signal_id=o.signal_id AND d.client_order_id=o.client_order_id
          AND d.attempt=1 AND d.side='sell' AND d.wallet=?3 AND d.token=?4 AND d.route=o.route
          AND o.tx_signature=d.tx_signature AND p.tx_signature=d.tx_signature AND r.tx_signature=d.tx_signature
          AND p.reason='accounting_complete' AND p.confirmation_status IN ('confirmed','finalized')
          AND p.side='sell' AND r.side='sell' AND p.slot=r.slot AND p.wallet_pubkey=?3 AND r.wallet_pubkey=?3
          AND p.token=?4 AND r.token=?4 AND r.token_delta_raw=?5 AND r.token_decimals=6
          AND f.token=?4 AND f.position_id=?6 AND f.qty_raw=?7 AND f.qty_decimals=6
          AND f.accounting_basis='receipt_native_cash' AND f.remaining_qty_raw='0' AND f.remaining_cost_lamports=0
          AND x.position_id=?6 AND x.state='closed' AND x.qty_raw='0' AND x.cost_lamports=0
          AND v.tx_signature=d.tx_signature AND v.wallet=?3 AND v.state='successful'
          AND v.actual_fee=CAST(r.transaction_fee AS INTEGER) AND r.fee_payer=?3
          AND NOT EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch)
          AND NOT EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE status!='complete')
          AND NOT EXISTS(SELECT 1 FROM owner_exit_fee_reservations WHERE state='pending')
          AND NOT EXISTS(SELECT 1 FROM {} h WHERE h.position_id=?6
            AND NOT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id AND d.handoff_owner=h.owner)))", super::unsigned::pending(c)?),
        params![cycle,crate::owner_exit_order_id(&id),i.wallet,i.mint,format!("-{}",i.amount_raw),position,i.amount_raw.to_string()],
        |r|r.get(0))?;
    Ok(done)
}
use rusqlite::OptionalExtension;
