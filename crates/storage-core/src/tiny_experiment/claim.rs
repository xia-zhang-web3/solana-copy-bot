use super::*;
use crate::ExecutionCanaryDispatch;

pub(crate) fn reserve(
    conn: &Connection,
    d: &ExecutionCanaryDispatch,
    p: Option<&TinyBudgetClaim>,
    now: DateTime<Utc>,
) -> Result<()> {
    // The legacy storage claim API cannot bypass a pinned experiment. Only app tiny
    // dispatch calls the proof-required API, including before any activation exists.
    let Some(p) = p else {
        ensure!(load(conn)?.is_none(), "tiny_budget_proof_required");
        return Ok(());
    };
    let e = refresh(conn, now)?.context("tiny_budget_inactive")?;
    ensure!(
        e.id == p.experiment_id && e.wallet == p.wallet && d.wallet == p.wallet,
        "tiny_budget_identity"
    );
    ensure!(
        p.tx_signature == d.tx_signature
            && p.message_sha256 == d.message_sha256
            && p.transaction_sha256 == d.transaction_sha256,
        "tiny_budget_fee_binding"
    );
    ensure!(
        now >= e.activated_at && now >= e.last_decision_at && now < e.deadline,
        "tiny_budget_deadline"
    );
    ensure!(e.state == "active", "tiny_budget_stopped");
    ensure!(
        p.total_fee <= TINY_TRANSACTION_FEE,
        "tiny_budget_transaction_fee"
    );
    ensure!(
        p.priority_fee <= TINY_PRIORITY_FEE && p.priority_fee <= p.total_fee,
        "tiny_budget_priority_fee"
    );
    let (buys, sells, committed): (u64, u64, u64) = conn.query_row(
        "SELECT COUNT(CASE WHEN side='buy' THEN 1 END),COUNT(CASE WHEN side='sell' THEN 1 END),
        COALESCE(SUM(COALESCE(actual_fee,fee_bound)),0) FROM execution_tiny_reservations",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    ensure!(
        committed
            .checked_add(p.total_fee)
            .is_some_and(|v| v <= TINY_TOTAL_FEE),
        "tiny_budget_fee_exhausted"
    );
    match d.side.as_str() {
        "buy" => {
            ensure!(
                buys == 0 && e.buy_order_id.is_none(),
                "tiny_budget_buy_slot"
            );
            ensure!(
                protected::validate_claim(conn, &e, p)?,
                "tiny_budget_buy_amount"
            );
            ensure!(
                p.total_fee <= TINY_TOTAL_FEE - TINY_EXIT_RESERVE,
                "tiny_budget_exit_reserve"
            );
            let open:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM positions WHERE state='open' AND position_id LIKE 'exec-canary-pos:%')",[],|r|r.get(0))?;
            ensure!(!open, "tiny_budget_existing_position");
            conn.execute("UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3 WHERE singleton=1",
                params![d.order_id,d.token,crate::execution_canary_position_open::execution_canary_position_id(&d.order_id)])?;
        }
        "sell" => {
            ensure!(buys == 1 && sells < 2, "tiny_budget_sell_slots");
            ensure!(
                p.buy_lamports == Some(0)
                    && p.protected_capital.is_none()
                    && e.token.as_deref() == Some(&d.token),
                "tiny_budget_position_identity"
            );
            let position = e
                .position_id
                .as_deref()
                .context("tiny_budget_position_missing")?;
            ensure!(
                confirmed_buy_position(conn, &e, d)?,
                "tiny_budget_position_unconfirmed"
            );
            let other:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM positions WHERE token=?1 AND state='open' AND position_id!=?2 AND position_id LIKE 'exec-canary-pos:%')",
                params![d.token,position],|r|r.get(0))?;
            ensure!(!other, "tiny_budget_position_ambiguous");
        }
        _ => anyhow::bail!("tiny_budget_side"),
    }
    conn.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,
        buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![d.order_id,e.id,d.tx_signature,d.wallet,d.side,d.message_sha256,d.transaction_sha256,p.buy_lamports,p.total_fee,p.priority_fee,p.fee_slot.to_string(),now.to_rfc3339()])?;
    protected::record_claim(conn, d, p)?;
    conn.execute(
        "UPDATE execution_tiny_experiment SET last_decision_at=?1 WHERE singleton=1",
        [now.to_rfc3339()],
    )?;
    refresh(conn, now)?;
    Ok(())
}

/// Execution ownership is canonical receipt + confirmed fill, not knowledge of fee.
/// This read-only guard never releases a reservation or enriches historical facts.
fn confirmed_buy_position(
    conn: &Connection,
    e: &TinyExperiment,
    sell: &ExecutionCanaryDispatch,
) -> Result<bool> {
    let order = e
        .buy_order_id
        .as_deref()
        .context("tiny_budget_buy_missing")?;
    if !crate::execution_canary_fill_marker::fill_exists(conn, order)? {
        return Ok(false);
    }
    let Some(facts) = crate::receipt_facts_rows::load(conn, order)? else {
        return Ok(false);
    };
    crate::receipt_facts_identity::validate_identity(conn, &facts)?;
    if facts.wallet_pubkey != e.wallet || facts.token != sell.token || facts.side != "buy" {
        return Ok(false);
    }
    let bound: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM execution_tiny_reservations r
         JOIN execution_canary_dispatch d ON d.order_id=r.order_id JOIN orders o ON o.order_id=d.order_id
         WHERE r.order_id=?1 AND r.experiment_id=?2 AND r.wallet=?3 AND r.side='buy'
         AND r.tx_signature=?4 AND d.tx_signature=r.tx_signature AND o.tx_signature=d.tx_signature
         AND d.wallet=r.wallet AND d.side=r.side AND d.token=?5
         AND d.message_sha256=r.message_sha256 AND d.transaction_sha256=r.transaction_sha256
         AND d.signal_id=o.signal_id AND d.client_order_id=o.client_order_id
         AND d.route=o.route AND d.attempt=o.attempt AND o.status='execution_canary_confirmed')",
        params![order,e.id,e.wallet,facts.tx_signature,sell.token], |r| r.get(0))?;
    if !bound {
        return Ok(false);
    }
    let position = crate::buy_fill_replay::load(conn, order, &sell.token)?;
    Ok(e.position_id.as_deref() == Some(position.position_id.as_str()) && position.state == "open")
}
