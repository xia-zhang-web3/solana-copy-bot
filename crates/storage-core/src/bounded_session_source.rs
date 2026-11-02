//! Dynamic normal-session event bindings; LOCAL retains its fixed synthetic plan.
use super::*;
use copybot_core_types::association_delivery::{CheckedFacts,MessageTime};
const SOL:&str="So11111111111111111111111111111111111111112";
pub(crate) fn event_allowed(c:&Connection,f:&CheckedFacts,buy:bool)->Result<bool>{
    let Some(a)=crate::real_session::load(c)? else {return crate::local_bounded_session::source::event_allowed(c,f,buy)};
    state::refresh(c,chrono::Utc::now())?;
    if f.wallet!=a.scope.source_wallet || f.slot==0 || f.program_fallback
        || f.token_in!=if buy {SOL}else{&a.mint} || f.token_out!=if buy {&a.mint}else{SOL} {return Ok(false)}
    let Some(x)=&f.exact_amounts else {return Ok(false)};
    let valid=if buy {x.amount_in_decimals==9 && x.amount_out_decimals==6 && x.amount_in_raw.parse::<u64>().is_ok_and(|v|v>0)
        && x.amount_out_raw.parse::<u64>().is_ok_and(|v|v>0)} else {x.amount_in_decimals==6 && x.amount_out_decimals==9
        && x.amount_in_raw.parse::<u64>().is_ok_and(|v|v>0)};
    if !valid {return Ok(false)}
    let present:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM real_session_cycles)",[],|r|r.get(0))?;
    if !present {return Ok(buy)}
    let p=state::current(c)?;
    if buy {
        if p.state!="closed" {return Ok(p.source_buy==f.signature)}
        let n:u32=c.query_row("SELECT count(*) FROM real_session_cycles",[],|r|r.get(0))?;
        Ok(n<a.max_cycles && !c.query_row("SELECT EXISTS(SELECT 1 FROM real_session_cycles WHERE source_buy_signature=?1)",[&f.signature],|r|r.get::<_,bool>(0))?)
    } else {
        if p.state=="closed" || !p.source_sell.is_empty() && p.source_sell!=f.signature {return Ok(false)}
        let raw:String=c.query_row("SELECT admission FROM native_buy_decisions WHERE signature=?1",[&p.source_buy],|r|r.get(0))?;
        let buy:copybot_core_types::association_delivery::AdmissionFacts=serde_json::from_str(&raw)?;
        let acquired:u64=buy.facts.exact_amounts.context("session_source_buy_exact")?.amount_out_raw.parse()?;
        Ok(f.slot>buy.facts.slot && x.amount_in_raw.parse::<u64>()?<=acquired)
    }
}
pub(crate) fn admit(c:&Connection,signature:&str,cohort:&TechnicalCohortAuthority,epoch:i64,at:DateTime<Utc>)->Result<()>{
    let a=crate::real_session::load(c)?.context("real_session_missing")?;
    ensure!(a.cohort==*cohort && at>=a.cohort.activated_at && at<a.cohort.deadline,"real_session_admission_authority");
    let genesis:String=c.query_row("SELECT genesis_hash FROM native_buy_fence_epochs WHERE epoch_id=?1",[epoch],|r|r.get(0))?;
    ensure!(genesis==a.genesis,"real_session_fence_genesis");
    state::refresh(c,at)?;
    let raw:String=c.query_row("SELECT admission FROM native_buy_decisions WHERE signature=?1",[signature],|r|r.get(0))?;
    let event:copybot_core_types::association_delivery::AdmissionFacts=serde_json::from_str(&raw)?;
    ensure!(!matches!(event.message_time,MessageTime::RecoveredBlock{..}) && event_allowed(c,&event.facts,true)?,"real_session_fresh_native_admission");
    let prior:u32=c.query_row("SELECT count(*) FROM real_session_cycles",[],|r|r.get(0))?;
    if prior>0 {ensure!(state::current(c)?.state=="closed","real_session_previous_cycle_pending")}
    ensure!(prior<a.max_cycles,"real_session_cycle_cap");
    let pending:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch)
        OR EXISTS(SELECT 1 FROM positions WHERE state='open' AND accounting_bucket='execution_canary')",[],|r|r.get(0))?;
    ensure!(!pending,"real_session_previous_obligation");
    let id=format!("session-cycle:{signature}");let mut scope=a.scope.clone();scope.source_buy_intent_id=id.clone();
    c.execute("INSERT INTO real_session_cycles(session_id,cycle_no,cycle_id,scope_json,source_buy_signature,state,last_decision_at)
        VALUES(?1,?2,?3,?4,?5,'planned',?6)",params![a.cohort.run_id,prior+1,id,serde_json::to_string(&scope)?,signature,at.to_rfc3339()])?;
    Ok(())
}
pub(crate) fn buy(s:&Scope)->Result<Option<Option<crate::controlled_receipt_lot::LotReceipt>>>{
    if let Some(result)=crate::real_session_source::buy(s)? {return Ok(Some(result))}
    crate::local_bounded_session::source::buy(s)
}
pub(crate) fn pair(s:&Scope,sig:&str,slot:u64)->Result<Option<Option<crate::controlled_receipt_lot::SourceReceipts>>>{
    if let Some(result)=crate::real_session_source::pair(s,sig,slot)? {return Ok(Some(result))}
    crate::local_bounded_session::source::pair(s,sig,slot)
}
