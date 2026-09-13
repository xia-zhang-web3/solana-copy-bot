//! Typed, single-owner transfer into the canonical dispatch and tiny reservation.
#[path = "rpc_owned_sell_identity.rs"]
pub mod identity;
use super::{recheck_state, Handoff};
use crate::{
    association_inbox::InboxLimits, ExecutionCanaryDispatch, ExecutionDispatchClaim,
    SqliteDiscoveryStore, TinyBudgetClaim,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prepared {
    pub handoff: Handoff,
    pub experiment: crate::tiny_experiment::OwnedExperimentSnapshot,
    pub limits: (usize, usize, u64),
    pub payload: String,
    pub message_sha256: String,
    pub total_fee: u64,
    pub priority_fee: u64,
}
impl Prepared {
    pub fn order_id(&self) -> String {
        format!("exec-canary:{}", self.handoff.order_id)
    }
    pub fn inbox_limits(&self) -> InboxLimits {
        InboxLimits {
            count: self.limits.0,
            bytes: self.limits.1,
            busy_ms: self.limits.2,
        }
    }
    pub(crate) fn matches_saved(&self, c: &Connection) -> Result<bool> {
        let h = &self.handoff;
        Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs WHERE intent_id=?1 AND owner=?2 AND order_id=?3 AND config_sha256=?4 AND snapshot=?5 AND authority=?6 AND quote=?7 AND signature=?8 AND position_id=?9 AND experiment_id=?10 AND wallet=?11 AND deadline=?12 AND fee_reserve=100000 AND state='unsigned_prepared' AND unsigned_payload=?13 AND message_sha256=?14 AND total_fee=?15 AND priority_fee=?16)",params![h.intent_id,h.owner,h.order_id,h.config_sha256,serde_json::to_string(&h.snapshot)?,h.authority,serde_json::to_string(&h.quote)?,h.snapshot.sell.facts.signature,h.snapshot.quote.position_id,h.experiment_id,h.wallet,h.deadline.to_rfc3339(),self.payload,self.message_sha256,self.total_fee,self.priority_fee],|r|r.get(0))?)
    }
    pub(crate) fn recheck(&self, c: &Connection, now: DateTime<Utc>) -> Result<()> {
        required(c)?;
        ensure!(
            crate::tiny_experiment::owned_snapshot(c)? == self.experiment,
            "owned_sell_experiment_changed"
        );
        ensure!(self.matches_saved(c)?, "owned_sell_prepared_changed");
        let quote: String = c.query_row(
            "SELECT record FROM ordered_sell_quote_results WHERE intent_id=?1",
            [&self.handoff.intent_id],
            |r| r.get(0),
        )?;
        ensure!(
            serde_json::from_str::<crate::ordered_sell_quote::QuoteObservation>(&quote)?
                == self.handoff.quote,
            "owned_sell_quote_changed"
        );
        ensure!(c.query_row("SELECT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=?1 AND intent_id=?2 AND owner='provider_order_strict_v1')",params![self.handoff.snapshot.sell.facts.signature,self.handoff.intent_id],|r|r.get::<_,bool>(0))?,"owned_sell_signature_owner");
        ensure!(
            !c.query_row(
                "SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches WHERE intent_id=?1)",
                [&self.handoff.intent_id],
                |r| r.get::<_, bool>(0)
            )?,
            "owned_sell_already_consumed"
        );
        recheck_state(
            c,
            &self.handoff,
            self.inbox_limits(),
            now,
            "unsigned_prepared",
        )
    }
}
pub(crate) fn required(c: &Connection) -> Result<()> {
    let applied:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version='0080_rpc_owned_sell_dispatch.sql')",[],|r|r.get(0))?;
    ensure!(applied, "owned_sell_dispatch_schema_required");
    c.prepare("SELECT intent_id,handoff_owner,order_id,prepared,dispatch,consumed_at FROM rpc_owned_sell_dispatches LIMIT 0")?;
    Ok(())
}
impl SqliteDiscoveryStore {
    pub fn recheck_owned_sell_prepared(&self, p: &Prepared, now: DateTime<Utc>) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        p.recheck(&tx, now)?;
        tx.commit()?;
        Ok(())
    }
    pub fn claim_owned_sell_dispatch(
        &self,
        p: &Prepared,
        d: &ExecutionCanaryDispatch,
        budget: &TinyBudgetClaim,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionDispatchClaim> {
        let h = &p.handoff;
        let b = &h.snapshot.quote;
        ensure!(
            d.order_id == p.order_id()
                && d.signal_id == h.intent_id
                && d.client_order_id == h.owner
                && d.wallet == h.wallet
                && d.token == b.mint
                && d.side == "sell"
                && d.attempt == 1
                && d.message_sha256 == p.message_sha256
                && !d.tx_signature.is_empty()
                && d.transaction_sha256.len() == 64
                && !d.route.is_empty(),
            "owned_sell_dispatch_identity"
        );
        self.with_immediate_transaction_retry("transfer owned SELL dispatch",|conn| {
            required(conn)?;
            if let Some(old)=self.load_execution_canary_dispatch(&d.order_id)? {
                ensure!(old==*d,"dispatch_identity_conflict");
                identity::owned(conn,&d.order_id)?.context("owned_sell_transfer_missing")?;
                return Ok(ExecutionDispatchClaim::Existing);
            }
            let now=clock()?;
            p.recheck(conn,now)?;
            ensure!(self.load_execution_canary_order(&d.order_id)?.is_none(),"owned_sell_order_conflict");
            ensure!(self.execution_canary_receipt_submit_block_reason(&d.order_id,&d.token,"sell")?.is_none(),"dispatch_receipt_blocked");
            conn.execute("INSERT INTO execution_order_sources(identity_id,owned_sell_intent_id) VALUES(?1,?1)",[&h.intent_id])?;
            conn.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt,simulation_status) VALUES(?1,?2,?3,?4,'execution_canary_simulated',?5,1,'passed')",params![d.order_id,d.signal_id,d.route,now.to_rfc3339(),d.client_order_id])?;
            crate::execution_canary_dispatch::insert(conn,d,now)?;
            ensure!(conn.execute("INSERT INTO rpc_owned_sell_dispatches(intent_id,handoff_owner,order_id,prepared,dispatch,consumed_at) VALUES(?1,?2,?3,?4,?5,?6)",params![h.intent_id,h.owner,d.order_id,serde_json::to_string(p)?,serde_json::to_string(d)?,now.to_rfc3339()])?==1,"owned_sell_transfer_cas");
            // Exclusion is authorized by this exact link inside the same transaction.
            crate::execution_canary_dispatch::mark_submitted(conn,d,now)?;
            crate::tiny_experiment::reserve_transferred(conn,d,budget,now)?;
            ensure!(clock()? < h.deadline,"owned_sell_deadline");
            ensure!(p.matches_saved(conn)?,"owned_sell_prepared_changed");
            identity::owned(conn,&d.order_id)?.context("owned_sell_transfer_readback")?;
            Ok(ExecutionDispatchClaim::New)
        })
    }
    pub fn owned_sell_dispatch_ids(&self, limit: u32) -> Result<Vec<String>> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='rpc_owned_sell_dispatches')",
            [],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(vec![]);
        }
        required(&self.conn)?;
        let mut stmt=self.conn.prepare("SELECT d.order_id FROM rpc_owned_sell_dispatches d JOIN execution_canary_unresolved_dispatch u ON u.order_id=d.order_id LEFT JOIN execution_canary_reconcile_attempts a ON a.order_id=d.order_id ORDER BY COALESCE(a.last_attempt_at,''),d.order_id LIMIT ?1")?;
        let ids = stmt
            .query_map([limit.min(4)], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ids)
    }
}
