//! Immutable opt-in normal session grant; never imports or rearms legacy authority.
use crate::{association_inbox::AssociationInbox,controlled_receipt_lot::Scope,
    native_buy::TechnicalCohortAuthority,SqliteStore};
use anyhow::{ensure,Context,Result};
use chrono::{DateTime,Utc};
use rusqlite::{params,Connection,OptionalExtension};
use serde::{Deserialize,Serialize};
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub policy:String,
    pub cohort:TechnicalCohortAuthority,
    pub scope:Scope,
    pub genesis:String,
    pub mint:String,
    pub route:String,
    pub max_cycles:u32,
    pub buy_lamports:u64,
    pub gross_buy_cap:u64,
    pub fee_cap:u64,
    pub loss_cap:u64,
    pub total_fee_cap:u64,
    pub priority_fee_cap:u64,
    pub native_floor:u64,
    pub personal_usdc_raw:u64,
    pub slippage_bps:u64,
}
impl Authority {
    pub(crate) fn view(&self)->crate::bounded_session::Authority{
        crate::bounded_session::Authority{model:crate::bounded_session::Model{
            policy:self.policy.clone(),session_id:self.cohort.run_id.clone(),max_cycles:self.max_cycles,
            gross_buy_cap:self.gross_buy_cap,fee_cap:self.fee_cap,cycles:vec![]},
            cohort:self.cohort.clone(),scope:self.scope.clone(),genesis:self.genesis.clone()}
    }
    fn validate(&self)->Result<()>{
        ensure!(self.policy=="real_bounded_usdc_session_v1" && self.genesis=="5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
            && self.mint==crate::OWNER_EXIT_MINT && self.route=="jupiter_swap_instructions"
            && self.scope.bot_run_id==self.cohort.run_id && self.scope.source_buy_intent_id==self.cohort.run_id
            && self.scope.run(&self.scope.bot_wallet)?==self.cohort.run_id
            && self.cohort.wallet_ids==[self.scope.source_wallet.clone()]
            && self.cohort.mint_policy==crate::native_buy::CLASSIC_SPL_MINT_POLICY
            && self.cohort.max_buy_count==self.max_cycles && (1..=32).contains(&self.max_cycles)
            && self.cohort.activated_at<self.cohort.deadline
            && self.cohort.deadline-self.cohort.activated_at<=chrono::Duration::hours(4)
            && self.buy_lamports==10_000_000 && self.gross_buy_cap>0
            && self.gross_buy_cap<=u64::from(self.max_cycles)*self.buy_lamports
            && self.fee_cap>0 && self.total_fee_cap==100_000
            && self.priority_fee_cap<=50_000 && self.priority_fee_cap<=self.total_fee_cap
            && (1..=20_000_000).contains(&self.loss_cap) && self.native_floor==0
            && self.personal_usdc_raw==100 && self.slippage_bps<=50,
            "real_session_authority");
        Ok(())
    }
}
pub(crate) fn active(c:&Connection)->Result<bool>{
    let exists:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='real_session_sessions')",[],|r|r.get(0))?;
    Ok(exists && c.query_row("SELECT EXISTS(SELECT 1 FROM real_session_sessions)",[],|r|r.get(0))?)
}
pub(crate) fn load(c:&Connection)->Result<Option<Authority>>{
    if !active(c)? {return Ok(None)}
    let raw:String=c.query_row("SELECT identity_json FROM real_session_sessions WHERE singleton=1",[],|r|r.get(0))?;
    let a:Authority=serde_json::from_str(&raw)?;a.validate()?;Ok(Some(a))
}
fn register(c:&Connection,a:&Authority,now:DateTime<Utc>)->Result<()>{
    a.validate()?;
    ensure!(!crate::local_bounded_session::active(c)?,"real_session_local_authority_forbidden");
    if let Some(old)=load(c)? {ensure!(old==*a,"real_session_identity_conflict");return Ok(())}
    ensure!(now>=a.cohort.activated_at && now<a.cohort.deadline,"real_session_activation_window");
    let existing:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM execution_tiny_experiment)
        OR EXISTS(SELECT 1 FROM native_buy_technical_cohort)
        OR EXISTS(SELECT 1 FROM controlled_receipt_lot_scope)
        OR EXISTS(SELECT 1 FROM positions WHERE state='open' AND accounting_bucket='execution_canary')
        OR EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch)
        OR EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs)",[],|r|r.get(0))?;
    ensure!(!existing,"real_session_prior_authority_or_obligation");
    c.execute("INSERT INTO real_session_sessions VALUES(1,?1,?2,?3,?4)",params![a.cohort.run_id,
        serde_json::to_string(a)?,serde_json::to_string(&a.cohort)?,serde_json::to_string(&a.scope)?])?;
    Ok(())
}
impl SqliteStore {
    pub fn register_real_session(&self,a:&Authority,now:DateTime<Utc>)->Result<()>{
        self.with_immediate_transaction_retry("register real session",|c|register(c,a,now))
    }
    pub fn real_session_scope(&self)->Result<Option<Scope>>{
        if !active(&self.conn)? {return Ok(None)}
        crate::bounded_session::state::scope(&self.conn)
    }
    pub fn real_source_event(&self,signature:&str)->Result<copybot_core_types::association_delivery::AdmissionFacts>{
        let i=crate::association_inbox::identity(&self.conn,signature)?.context("real_session_source_identity")?;
        ensure!(!i.conflict && !i.recovery,"real_session_source_conflict_or_recovery");Ok(i.admission)
    }
}
impl AssociationInbox {
    pub fn register_real_session(&mut self,a:&Authority,now:DateTime<Utc>)->Result<()>{
        let tx=self.conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        register(&tx,a,now)?;tx.commit()?;Ok(())
    }
}
