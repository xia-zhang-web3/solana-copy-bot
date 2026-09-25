//! Unsigned reservation: no signal timestamp, fill, fee payment or cash write.
//! There is deliberately no rearm/release API. Interrupted preparation stays owned.
use crate::{
    association_inbox::InboxLimits,
    ordered_sell_quote::{QuoteObservation, QuoteOutcome},
    rpc_owned_sell_snapshot::{self, OwnedSellSnapshot},
    SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
#[path = "rpc_owned_sell_dispatch.rs"]
pub mod dispatch;
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Handoff {
    pub intent_id: String,
    pub order_id: String,
    pub owner: String,
    pub snapshot: OwnedSellSnapshot,
    pub config_sha256: String,
    pub authority: String,
    pub quote: QuoteObservation,
    pub experiment_id: String,
    pub wallet: String,
    pub deadline: DateTime<Utc>,
}
impl SqliteDiscoveryStore {
    pub fn check_owned_sell_budget_policy(
        &self,
        id: &str,
        wallet: &str,
        token: &str,
        position: &str,
        protected: bool,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        let e = crate::tiny_experiment::check_owned_sell_owner(&tx, id, wallet, token, position, now)?;
        cohort_sell_deadline(&tx, e.buy_order_id.as_deref().unwrap_or(""), id, token, now)?;
        let mode: String = tx.query_row(
            "SELECT policy_mode FROM execution_tiny_experiment WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            mode == if protected {
                "protected_native_capital"
            } else {
                "decoded_amount"
            },
            "tiny_capital_mode_conflict"
        );
        tx.commit()?;
        Ok(())
    }
    pub fn has_owned_sell_handoff(&self, id: &str) -> Result<bool> {
        required(&self.conn)?;
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs WHERE intent_id=?1)",
            [id],
            |r| r.get(0),
        )?)
    }
    pub fn reserve_owned_sell_handoff(
        &self,
        snapshot: &OwnedSellSnapshot,
        quote: &QuoteObservation,
        limits: InboxLimits,
        identity: &str,
        authority: &str,
        experiment: &str,
        wallet: &str,
        mut clock: impl FnMut() -> DateTime<Utc>,
    ) -> Result<Handoff> {
        crate::ordered_sell_quote::schema::durable_writer(&self.conn)?;
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        required(&tx)?;
        ensure!(
            rpc_owned_sell_snapshot::read(&tx, &snapshot.quote, limits)? == *snapshot,
            "owned_sell_snapshot_changed"
        );
        ensure!(
            snapshot.facts.iter().all(|f| f.wallet_pubkey == wallet),
            "owned_sell_receipt_wallet"
        );
        let b = &snapshot.quote;
        let now = clock();
        let sig = &snapshot.sell.facts.signature;
        let claimed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM source_sell_signature_claims WHERE signature=?1 AND intent_id=?2 AND owner='provider_order_strict_v1')",params![sig,b.intent_id],|r|r.get(0))?;
        ensure!(claimed, "owned_sell_signature_owner");
        ensure!(
            quote.binding.as_ref() == Some(b)
                && quote.outcome == QuoteOutcome::Current
                && crate::ordered_sell_quote::fresh(quote, now),
            "owned_sell_quote_stale"
        );
        let persisted: String = tx.query_row(
            "SELECT record FROM ordered_sell_quote_results WHERE intent_id=?1",
            [&b.intent_id],
            |r| r.get(0),
        )?;
        ensure!(
            serde_json::from_str::<QuoteObservation>(&persisted)? == *quote,
            "owned_sell_quote_changed"
        );
        ensure!(
            identity.len() == 64 && authority.len() <= limits.bytes.min(1 << 20),
            "owned_sell_authority_binding"
        );
        let e = crate::tiny_experiment::prepare_owned_sell(
            &tx,
            experiment,
            wallet,
            &b.mint,
            &b.position_id,
            now,
        )?;
        ensure!(
            snapshot.receipts.len() == 1
                && snapshot.receipts[0].contributor.order_id
                    == e.buy_order_id.as_deref().unwrap_or(""),
            "owned_sell_experiment_origins"
        );
        let cohort_deadline = cohort_sell_deadline(
            &tx, e.buy_order_id.as_deref().unwrap_or(""), experiment, &b.mint, now,
        )?;
        if cohort_deadline.is_some() {
            let reserved: i64 = tx.query_row(
                "SELECT count(*) FROM rpc_owned_sell_handoffs WHERE experiment_id=?1",
                [experiment], |r|r.get(0))?;
            ensure!(reserved < 1, "technical_cohort_sell_limit");
        }
        let deadline = (quote.http_started.context("owned_sell_quote_clock")?
            + chrono::Duration::milliseconds(crate::ordered_sell_quote::MAX_QUOTE_AGE_MS))
        .min(e.deadline)
        .min(cohort_deadline.unwrap_or(e.deadline));
        ensure!(now < deadline, "owned_sell_deadline");
        let h = Handoff {
            intent_id: b.intent_id.clone(),
            order_id: format!("rpc-owned-sell:{}", sig),
            owner: uuid::Uuid::new_v4().to_string(),
            snapshot: snapshot.clone(),
            config_sha256: identity.into(),
            authority: authority.into(),
            quote: quote.clone(),
            experiment_id: experiment.into(),
            wallet: wallet.into(),
            deadline,
        };
        let changed=tx.execute("INSERT INTO rpc_owned_sell_handoffs(intent_id,signature,position_id,order_id,owner,experiment_id,wallet,config_sha256,snapshot,authority,quote,reserved_at,deadline,fee_reserve,state) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,100000,'preparing')",params![h.intent_id,sig,b.position_id,h.order_id,h.owner,experiment,wallet,identity,serde_json::to_string(snapshot)?,authority,serde_json::to_string(quote)?,now.to_rfc3339(),deadline.to_rfc3339()])?;
        ensure!(changed == 1, "owned_sell_reservation_lost");
        tx.execute(
            "UPDATE execution_tiny_experiment SET last_decision_at=?1 WHERE singleton=1",
            [now.to_rfc3339()],
        )?;
        ensure!(clock() < deadline, "owned_sell_deadline");
        tx.commit()?;
        self.recheck_owned_sell_handoff(&h, limits, clock())?;
        Ok(h)
    }
    pub fn recheck_owned_sell_handoff(
        &self,
        h: &Handoff,
        l: InboxLimits,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        recheck(&tx, h, l, now)?;
        tx.commit()?;
        Ok(())
    }
    pub fn complete_owned_sell_handoff(
        &self,
        h: &Handoff,
        l: InboxLimits,
        payload: &str,
        message: &str,
        total: u64,
        priority: u64,
        mut clock: impl FnMut() -> DateTime<Utc>,
    ) -> Result<()> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        recheck(&tx, h, l, clock())?;
        ensure!(
            payload.len() <= 1644
                && message.len() == 64
                && total <= crate::TINY_TRANSACTION_FEE
                && priority <= crate::TINY_PRIORITY_FEE
                && priority <= total,
            "owned_sell_fee_or_payload"
        );
        let n=tx.execute("UPDATE rpc_owned_sell_handoffs SET state='unsigned_prepared',unsigned_payload=?1,message_sha256=?2,total_fee=?3,priority_fee=?4 WHERE intent_id=?5 AND owner=?6 AND state='preparing'",params![payload,message,total,priority,h.intent_id,h.owner])?;
        ensure!(n == 1, "owned_sell_completion_cas");
        ensure!(clock() < h.deadline, "owned_sell_deadline");
        tx.commit()?;
        let saved:Option<String>=self.conn.query_row("SELECT unsigned_payload FROM rpc_owned_sell_handoffs WHERE intent_id=?1 AND owner=?2 AND state='unsigned_prepared'",params![h.intent_id,h.owner],|r|r.get(0)).optional()?;
        ensure!(
            saved.as_deref() == Some(payload),
            "owned_sell_completion_readback"
        );
        ensure!(clock() < h.deadline, "owned_sell_deadline");
        Ok(())
    }
}
fn recheck(
    c: &rusqlite::Connection,
    h: &Handoff,
    l: InboxLimits,
    now: DateTime<Utc>,
) -> Result<()> {
    recheck_state(c, h, l, now, "preparing")
}
fn recheck_state(
    c: &rusqlite::Connection,
    h: &Handoff,
    l: InboxLimits,
    now: DateTime<Utc>,
    state: &str,
) -> Result<()> {
    required(c)?;
    ensure!(
        now < h.deadline && crate::ordered_sell_quote::fresh(&h.quote, now),
        "owned_sell_deadline"
    );
    ensure!(
        rpc_owned_sell_snapshot::read(c, &h.snapshot.quote, l)? == h.snapshot,
        "owned_sell_snapshot_changed"
    );
    let same:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs WHERE intent_id=?1 AND owner=?2 AND order_id=?3 AND config_sha256=?4 AND snapshot=?5 AND authority=?6 AND quote=?7 AND signature=?8 AND position_id=?9 AND experiment_id=?10 AND wallet=?11 AND deadline=?12 AND fee_reserve=100000 AND state=?13)",params![h.intent_id,h.owner,h.order_id,h.config_sha256,serde_json::to_string(&h.snapshot)?,h.authority,serde_json::to_string(&h.quote)?,h.snapshot.sell.facts.signature,h.snapshot.quote.position_id,h.experiment_id,h.wallet,h.deadline.to_rfc3339(),state],|r|r.get(0))?;
    ensure!(same, "owned_sell_owner_changed");
    let e = crate::tiny_experiment::check_owned_sell_owner(
        c,
        &h.experiment_id,
        &h.wallet,
        &h.snapshot.quote.mint,
        &h.snapshot.quote.position_id,
        now,
    )?;
    if let Some(deadline) = cohort_sell_deadline(
        c, e.buy_order_id.as_deref().unwrap_or(""), &h.experiment_id,
        &h.snapshot.quote.mint, now,
    )? {
        ensure!(h.deadline <= deadline, "technical_cohort_sell_deadline_binding");
        let reserved: i64 = c.query_row(
            "SELECT count(*) FROM rpc_owned_sell_handoffs WHERE experiment_id=?1",
            [&h.experiment_id], |r|r.get(0))?;
        ensure!(reserved <= 1, "technical_cohort_sell_limit");
    }
    Ok(())
}

fn cohort_sell_deadline(
    c: &rusqlite::Connection, buy_order_id: &str, experiment: &str,
    mint: &str, now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    if !crate::native_buy::cohort::available(c)? { return Ok(None); }
    let origin: Option<(String,String,String)> = c.query_row(
        "SELECT b.run_id,b.mint,b.policy_identity FROM orders o
         JOIN copy_signals s ON s.signal_id=o.signal_id AND s.side='buy'
         JOIN native_buy_decisions d ON d.signal_id=o.signal_id
         JOIN native_buy_cohort_decisions b ON b.signature=d.signature
         WHERE o.order_id=?1",
        [buy_order_id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((run_id,source_mint,policy)) = origin else { return Ok(None); };
    let authority = crate::native_buy::cohort::load(c)?
        .context("technical_cohort_sell_authority_missing")?;
    ensure!(run_id == authority.run_id && experiment == run_id
        && source_mint == mint && policy == authority.policy_identity,
        "technical_cohort_sell_origin");
    ensure!(now >= authority.activated_at && now < authority.deadline,
        "technical_cohort_sell_deadline");
    Ok(Some(authority.deadline))
}
pub(crate) fn required(c: &rusqlite::Connection) -> Result<()> {
    let applied:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version='0079_rpc_owned_sell_handoff.sql')",[],|r|r.get(0))?;
    ensure!(applied, "owned_sell_schema_required");
    let sql: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='rpc_owned_sell_handoffs'",
        [],
        |r| r.get(0),
    )?;
    let expected = include_str!("../../../migrations/0079_rpc_owned_sell_handoff.sql")
        .split_once("CREATE TABLE")
        .context("owned_sell_schema_ddl")?
        .1;
    ensure!(
        sql.trim().trim_end_matches(';')
            == format!("CREATE TABLE{}", expected.trim_end().trim_end_matches(';')),
        "owned_sell_schema_changed"
    );
    Ok(())
}
