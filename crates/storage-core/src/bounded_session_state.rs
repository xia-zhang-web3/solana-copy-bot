//! Session progress derives from canonical accounting, never position.closed alone.
use super::*;
use crate::tiny_experiment::TinyExperiment;
#[derive(Debug)]
pub(crate) struct Cycle {
    pub id: String,
    pub state: String,
    pub buy: Option<String>,
    pub position: Option<String>,
    pub token: Option<String>,
    pub last: String,
    pub scope: Scope,
    pub source_buy: String,
    pub source_sell: String,
}
pub(crate) fn current(c: &Connection) -> Result<Cycle> {
    Ok(c.query_row(&super::sql(c,"SELECT cycle_id,state,buy_order_id,position_id,token,last_decision_at,scope_json,
        source_buy_signature,coalesce(source_sell_signature,'') FROM local_bounded_cycles
        ORDER BY CASE WHEN state='closed' THEN 1 ELSE 0 END,CASE WHEN state='closed' THEN -cycle_no ELSE cycle_no END LIMIT 1")?,[],
        |r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get::<_,String>(6)?,r.get(7)?,r.get(8)?)))
        .map(|(id,state,buy,position,token,last,scope,source_buy,source_sell)| ->Result<Cycle> {
            Ok(Cycle{id,state,buy,position,token,last,scope:serde_json::from_str(&scope)?,source_buy,source_sell})
        })??)
}
pub(crate) fn scope(c: &Connection) -> Result<Option<Scope>> {
    if !active(c)? {
        return Ok(None);
    }
    if crate::real_session::active(c)? && !c.query_row("SELECT EXISTS(SELECT 1 FROM real_session_cycles)",[],|r|r.get::<_,bool>(0))? {
        return Ok(Some(load(c)?.context("real_session_missing")?.scope));
    }
    Ok(Some(current(c)?.scope))
}
pub(crate) fn complete(c: &Connection, p: &Cycle) -> Result<bool> {
    complete_position(c,&p.id,p.position.as_deref())
}
pub(crate) fn complete_position(c: &Connection, cycle: &str, position: Option<&str>) -> Result<bool> {
    let Some(position) = position else {
        return Ok(false);
    };
    let closed:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM positions WHERE position_id=?1 AND state='closed' AND qty=0 AND cost_sol=0)",[position],|r|r.get(0))?;
    if !closed {
        return Ok(false);
    }
    if crate::owner_bot_receipt_exit::normal::complete(c, cycle, position)? {
        return Ok(true);
    }
    let (all,done):(u64,u64)=c.query_row(&super::sql(c,"SELECT count(*),count(CASE WHEN o.status='execution_canary_confirmed'
        AND f.order_id IS NOT NULL AND p.reason='accounting_complete' AND a.order_id IS NOT NULL
        AND r.actual_fee IS NOT NULL AND r.outcome='successful' AND u.order_id IS NULL THEN 1 END)
        FROM local_bounded_orders b JOIN orders o ON o.order_id=b.order_id
        LEFT JOIN fills f ON f.order_id=o.order_id LEFT JOIN execution_canary_receipt_facts a ON a.order_id=o.order_id
        LEFT JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        LEFT JOIN execution_tiny_reservations r ON r.order_id=o.order_id
        LEFT JOIN execution_canary_unresolved_dispatch u ON u.order_id=o.order_id WHERE b.cycle_id=?1")?,
        [cycle],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let unsigned:bool=c.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {} h WHERE h.position_id=?1
        AND NOT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id AND d.handoff_owner=h.owner))", crate::owner_bot_receipt_exit::unsigned::pending(c)?),[position],|r|r.get(0))?;
    Ok(all == 2 && done == 2 && !unsigned)
}
pub(crate) fn settled_sell(c: &Connection, order: &str) -> Result<bool> {
    if !active(c)? {return Ok(false)}
    let row:Option<(String,String)>=c.query_row(&super::sql(c,"SELECT b.cycle_id,p.position_id FROM local_bounded_orders b
        JOIN local_bounded_cycles p ON p.cycle_id=b.cycle_id AND p.state='closed'
        JOIN execution_canary_dispatch d ON d.order_id=b.order_id AND d.side='sell'
        JOIN rpc_owned_sell_dispatches x ON x.order_id=b.order_id
        JOIN rpc_owned_sell_handoffs h ON h.intent_id=x.intent_id AND h.owner=x.handoff_owner AND h.position_id=p.position_id
        WHERE b.order_id=?1 AND b.side='sell'")?,[order],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    match row {Some((id,position))=>complete_position(c,&id,Some(&position)),None=>Ok(false)}
}
pub(crate) fn refresh(c: &Connection, now: DateTime<Utc>) -> Result<()> {
    if !active(c)? {
        return Ok(());
    }
    if crate::real_session::active(c)? && !c.query_row("SELECT EXISTS(SELECT 1 FROM real_session_cycles)",[],|r|r.get::<_,bool>(0))? {return Ok(())}
    let p = current(c)?;
    if p.state != "closed" && complete(c, &p)? {
        c.execute(&super::sql(c,"UPDATE local_bounded_cycles SET state='closed',closed_at=?2 WHERE cycle_id=?1 AND state!='closed'")?,
            params![p.id,now.to_rfc3339()])?;
    }
    Ok(())
}
pub(crate) fn tiny(c: &Connection) -> Result<Option<TinyExperiment>> {
    let Some(a) = load(c)? else { return Ok(None) };
    let anchor:Option<(String,String,String,String,String,Option<String>)>=c.query_row(
        "SELECT experiment_id,wallet,activated_at,deadline,state,buy_order_id FROM execution_tiny_experiment WHERE singleton=1",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    let Some((id, wallet, at, end, state, buy)) = anchor else {
        return Ok(None);
    };
    ensure!(
        id == a.model.session_id
            && wallet == a.scope.bot_wallet
            && end == a.cohort.deadline.to_rfc3339()
            && state == "active"
            && buy.is_none(),
        "local_session_anchor_changed"
    );
    let p = current(c)?;
    let stopped: bool = c.query_row(
        &super::sql(c,"SELECT EXISTS(SELECT 1 FROM local_bounded_stops)")?,
        [],
        |r| r.get(0),
    )?;
    Ok(Some(TinyExperiment {
        id,
        wallet,
        activated_at: at.parse()?,
        deadline: end.parse()?,
        last_decision_at: p.last.parse()?,
        state: if stopped {
            "stopped"
        } else if p.state == "closed" {
            "completed"
        } else {
            "active"
        }
        .into(),
        stop_reason: None,
        buy_order_id: p.buy,
        token: p.token,
        position_id: p.position,
    }))
}
pub(crate) fn touch(c: &Connection, now: DateTime<Utc>) -> Result<()> {
    let p = current(c)?;
    c.execute(
        &super::sql(c,"UPDATE local_bounded_cycles SET last_decision_at=?2 WHERE cycle_id=?1")?,
        params![p.id, now.to_rfc3339()],
    )?;
    Ok(())
}
