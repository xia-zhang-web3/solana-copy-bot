//! Finalized receipt operands for dynamic native cycles. No owner SOURCE DB lookup.
use crate::{bounded_session::{self,state},controlled_receipt_lot::{Scope,LotReceipt,SourceReceipts},SqliteStore};
use anyhow::{ensure,Context,Result};
use chrono::{DateTime,Utc};
use rusqlite::{params,Connection,OptionalExtension};
fn database(s:&Scope)->Result<Option<SqliteStore>>{
    let db=SqliteStore::open_read_only(&s.source_state_db_path)?;
    if !crate::real_session::active(&db.conn)? {return Ok(None)}
    ensure!(state::scope(&db.conn)?.as_ref()==Some(s),"real_session_source_scope");Ok(Some(db))
}
fn receipt(c:&Connection,signature:&str,buy:bool)->Result<Option<LotReceipt>>{
    let p=state::current(c)?;
    let expected=if buy {&p.source_buy}else{&p.source_sell};
    if expected!=signature || expected.is_empty(){return Ok(None)}
    let Some(i)=crate::association_inbox::identity(c,signature)? else {return Ok(None)};
    if i.conflict || i.recovery {return Ok(None)}
    let Some(copybot_core_types::association_delivery::Terminal::ProviderAsserted(t))=i.terminal else{return Ok(None)};
    if t.signature!=signature || t.slot!=i.admission.facts.slot {return Ok(None)}
    let row:Option<(u64,String)>=c.query_row("SELECT slot,amount_raw FROM real_session_source_receipts WHERE signature=?1 AND cycle_id=?2 AND side=?3",params![signature,p.id,if buy {"buy"}else{"sell"}],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((slot,raw))=row else{return Ok(None)};
    ensure!(slot==t.slot,"real_session_receipt_slot");
    if buy && !c.query_row("SELECT EXISTS(SELECT 1 FROM native_buy_decisions WHERE signature=?1 AND finalized_at IS NOT NULL AND finalized_slot=?2 AND late=0)",params![signature,slot],|r|r.get::<_,bool>(0))? {return Ok(None)}
    Ok(Some(LotReceipt{order_id:format!("external-source:{signature}"),signature:signature.into(),slot,amount_raw:raw.parse()?}))
}
pub(crate) fn buy(s:&Scope)->Result<Option<Option<LotReceipt>>>{
    let Some(db)=database(s)? else{return Ok(None)};let p=state::current(&db.conn)?;
    Ok(Some(receipt(&db.conn,&p.source_buy,true)?))
}
pub(crate) fn pair(s:&Scope,sig:&str,slot:u64)->Result<Option<Option<SourceReceipts>>>{
    let Some(db)=database(s)? else{return Ok(None)};let p=state::current(&db.conn)?;
    let (Some(buy),Some(sell))=(receipt(&db.conn,&p.source_buy,true)?,receipt(&db.conn,sig,false)?) else{return Ok(Some(None))};
    ensure!(sell.slot==slot && sell.slot>buy.slot && sell.amount_raw<=buy.amount_raw,"real_session_source_pair");
    Ok(Some(Some(SourceReceipts{remaining_before_raw:buy.amount_raw,buy,sell})))
}
pub(crate) fn loss_within(c:&Connection,cap:u64)->Result<()>{
    if !crate::real_session::active(c)? {return Ok(())}
    let authority=crate::real_session::load(c)?.context("real_session_missing")?;
    let mut cycles=c.prepare("SELECT cycle_id,position_id FROM real_session_cycles WHERE state='closed'")?;
    let rows=cycles.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
    let mut loss=0u64;
    for row in rows {
        let (cycle,position)=row?;
        ensure!(state::complete_position(c,&cycle,Some(&position))?,"real_session_cash_accounting_pending");
        let mut facts=c.prepare("SELECT r.wallet_native_delta,r.wallet_pubkey,r.side FROM real_session_orders b
            JOIN orders o ON o.order_id=b.order_id JOIN fills f ON f.order_id=o.order_id
            JOIN execution_canary_receipt_facts r ON r.order_id=o.order_id
            WHERE b.cycle_id=?1 AND o.status='execution_canary_confirmed' AND f.position_id=?2 AND b.side=r.side")?;
        let amounts=facts.query_map(params![cycle,position],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?)))?;
        let mut cash=0i128;let mut sides=std::collections::HashSet::new();
        for amount in amounts {
            let (amount,wallet,side)=amount?;
            ensure!(wallet==authority.scope.bot_wallet && sides.insert(side),"real_session_cash_identity");
            cash=cash.checked_add(amount.parse::<i128>()?).context("real_session_cash_overflow")?;
        }
        ensure!(sides.len()==2 && sides.contains("buy") && sides.contains("sell"),"real_session_cash_accounting_unknown");
        if cash<0 {loss=loss.checked_add(u64::try_from(-cash)?).context("real_session_loss_overflow")?}
    }
    ensure!(loss<cap,"real_session_loss_cap");Ok(())
}
impl SqliteStore {
    /// Runtime calls after exact admission wire/signature/program/account checks.
    pub fn record_real_source_receipt(&self,signature:&str,side:&str,slot:u64,message:&str,transaction:&str,receipt:&str,amount:u64,now:DateTime<Utc>)->Result<()>{
        self.with_immediate_transaction_retry("record real source receipt",|c|{
            let a=crate::real_session::load(c)?.context("real_session_missing")?;let p=state::current(c)?;
            ensure!(now>=a.cohort.activated_at && now<a.cohort.deadline && amount>0,"real_session_source_deadline");
            let i=crate::association_inbox::identity(c,signature)?.context("real_session_source_identity")?;
            ensure!(!i.conflict && !i.recovery && i.admission.facts.slot==slot && bounded_session::source::event_allowed(c,&i.admission.facts,side=="buy")?,"real_session_source_event");
            let Some(copybot_core_types::association_delivery::Terminal::ProviderAsserted(t))=i.terminal else{anyhow::bail!("real_session_source_not_asserted")};
            ensure!(t.signature==signature && t.slot==slot && (side=="buy" || side=="sell"),"real_session_source_terminal");
            let exact=i.admission.facts.exact_amounts.context("real_session_source_exact")?;
            ensure!(amount.to_string()==if side=="buy" {exact.amount_out_raw}else{exact.amount_in_raw}
                && [message,transaction,receipt].iter().all(|v|v.len()==64 && v.bytes().all(|b|b.is_ascii_hexdigit())),"real_session_source_receipt_binding");
            if side=="sell" {
                ensure!(self::receipt(c,&p.source_buy,true)?.is_some(),"real_session_buy_receipt_pending");
                c.execute("UPDATE real_session_cycles SET source_sell_signature=?2 WHERE cycle_id=?1 AND (source_sell_signature IS NULL OR source_sell_signature=?2)",params![p.id,signature])?;
            }
            let expected=(p.id.clone(),side.to_string(),slot,message.to_string(),transaction.to_string(),receipt.to_string(),amount.to_string());
            let old:Option<(String,String,u64,String,String,String,String)>=c.query_row("SELECT cycle_id,side,slot,message_sha256,transaction_sha256,receipt_sha256,amount_raw FROM real_session_source_receipts WHERE signature=?1",[signature],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
            if let Some(old)=old {ensure!(old==expected,"real_session_source_receipt_conflict");return Ok(())}
            c.execute("INSERT INTO real_session_source_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![signature,p.id,side,slot,message,transaction,receipt,amount.to_string(),now.to_rfc3339()])?;Ok(())
        })
    }
}
#[cfg(test)]
#[path="real_session_loss_tests.rs"]
mod tests;
