//! Retain the single fee hold while replacing only a proven refused preparation.
use super::{cohort_sell_deadline, Handoff};
use crate::{
    association_inbox::InboxLimits, ordered_sell_quote::QuoteObservation,
    rpc_owned_sell_snapshot::OwnedSellSnapshot, SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

fn available(c: &Connection) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version='0094_owned_sell_simulation_recovery.sql')", [], |r| r.get(0))?)
}
fn unsent(c: &Connection, id: &str, owner: &str) -> Result<bool> {
    super::dispatch::required(c)?;
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs h
        WHERE h.intent_id=?1 AND h.owner=?2 AND h.state='preparing'
        AND h.unsigned_payload IS NULL AND h.message_sha256 IS NULL
        AND h.total_fee IS NULL AND h.priority_fee IS NULL AND h.fee_reserve=100000
        AND NOT EXISTS(SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id)
        AND NOT EXISTS(SELECT 1 FROM orders o WHERE o.order_id='exec-canary:'||h.order_id)
        AND NOT EXISTS(SELECT 1 FROM execution_canary_dispatch d WHERE d.order_id='exec-canary:'||h.order_id))",
        params![id,owner], |r| r.get(0))?)
}
fn refused(c: &Connection, id: &str) -> Result<Option<Handoff>> {
    if !available(c)? {
        return Ok(None);
    }
    let wire: Option<String> = c.query_row("SELECT f.handoff FROM owned_sell_simulation_refusals f
        JOIN rpc_owned_sell_handoffs h ON h.intent_id=f.intent_id AND h.owner=f.owner
        WHERE f.intent_id=?1 AND NOT EXISTS(SELECT 1 FROM owned_sell_preparation_recoveries r WHERE r.old_owner=f.owner)",
        [id], |r| r.get(0)).optional()?;
    wire.map(|s| serde_json::from_str(&s).map_err(Into::into))
        .transpose()
}
pub(super) fn ensure_not_refused(c: &Connection, h: &Handoff) -> Result<()> {
    if available(c)? {
        let refused:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM owned_sell_simulation_refusals WHERE intent_id=?1 AND owner=?2)",
            params![h.intent_id,h.owner],|r|r.get(0))?;
        ensure!(!refused, "owned_sell_simulation_refused_owner");
    }
    Ok(())
}
fn unchanged(c: &Connection, h: &Handoff) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs WHERE intent_id=?1 AND owner=?2
        AND order_id=?3 AND config_sha256=?4 AND snapshot=?5 AND authority=?6 AND quote=?7
        AND signature=?8 AND position_id=?9 AND experiment_id=?10 AND wallet=?11 AND deadline=?12)",
        params![
            h.intent_id,
            h.owner,
            h.order_id,
            h.config_sha256,
            serde_json::to_string(&h.snapshot)?,
            h.authority,
            serde_json::to_string(&h.quote)?,
            h.snapshot.sell.facts.signature,
            h.snapshot.quote.position_id,
            h.experiment_id,
            h.wallet,
            h.deadline.to_rfc3339()
        ],
        |r| r.get(0),
    )?)
}
/// Quote scheduling may revisit only this explicit pre-signing refusal.
pub(crate) fn quote_allowed(c: &Connection, id: &str, now: DateTime<Utc>) -> Result<bool> {
    let Some(h) = refused(c, id)? else {
        return Ok(false);
    };
    if !unsent(c, id, &h.owner)? || !unchanged(c, &h)? {
        return Ok(false);
    }
    let e = crate::tiny_experiment::check_owned_sell_owner(
        c,
        &h.experiment_id,
        &h.wallet,
        &h.snapshot.quote.mint,
        &h.snapshot.quote.position_id,
        now,
    )?;
    cohort_sell_deadline(
        c,
        e.buy_order_id.as_deref().unwrap_or(""),
        &h.experiment_id,
        &h.snapshot.quote.mint,
        now,
    )?;
    Ok(true)
}
impl SqliteDiscoveryStore {
    /// Called only by the unsigned runner on a typed, validated simulation rejection.
    /// Completion/signing/dispatch share this owner's CAS; any winner excludes this write.
    pub fn record_owned_sell_simulation_refusal(
        &self,
        h: &Handoff,
        l: InboxLimits,
        payload_sha256: &str,
        slot: u64,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        ensure!(available(&tx)?, "owned_sell_recovery_schema_required");
        // This records a completed negative read, never authorizes execution.
        // A delayed refusal must remain recordable after its old quote expires.
        ensure!(
            unchanged(&tx, h)?
                && now >= h.quote.http_ended
                && serde_json::to_string(h)?.len() <= l.bytes.min(1 << 20),
            "owned_sell_refusal_owner"
        );
        ensure!(
            unsent(&tx, &h.intent_id, &h.owner)?,
            "owned_sell_no_send_unproven"
        );
        ensure!(
            payload_sha256.len() == 64
                && payload_sha256.bytes().all(|b| b.is_ascii_hexdigit())
                && slot >= h.snapshot.sell.facts.slot,
            "owned_sell_refusal_identity"
        );
        tx.execute("INSERT INTO owned_sell_simulation_refusals(intent_id,owner,handoff,payload_sha256,simulation_slot,refused_at)
            VALUES(?1,?2,?3,?4,?5,?6)",params![h.intent_id,h.owner,serde_json::to_string(h)?,payload_sha256,slot,now.to_rfc3339()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn recover_owned_sell_handoff(
        &self,
        s: &OwnedSellSnapshot,
        q: &QuoteObservation,
        l: InboxLimits,
        identity: &str,
        authority: &str,
        experiment: &str,
        wallet: &str,
        mut clock: impl FnMut() -> DateTime<Utc>,
    ) -> Result<Handoff> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let b = &s.quote;
        let old = refused(&tx, &b.intent_id)?.context("owned_sell_refusal_missing")?;
        let now = clock();
        ensure!(
            quote_allowed(&tx, &b.intent_id, now)?,
            "owned_sell_no_send_unproven"
        );
        ensure!(
            old.config_sha256 == identity
                && old.experiment_id == experiment
                && old.wallet == wallet
                && serde_json::to_string(&old.snapshot)? == serde_json::to_string(s)?,
            "owned_sell_recovery_identity"
        );
        ensure!(
            crate::rpc_owned_sell_snapshot::read(&tx, b, l)? == *s,
            "owned_sell_snapshot_changed"
        );
        ensure!(
            q.binding.as_ref() == Some(b)
                && q.outcome == crate::ordered_sell_quote::QuoteOutcome::Current
                && crate::ordered_sell_quote::fresh(q, now)
                && q.http_started.is_some_and(|t| t > old.quote.http_ended),
            "owned_sell_recovery_fresh_quote"
        );
        let saved: String = tx.query_row(
            "SELECT record FROM ordered_sell_quote_results WHERE intent_id=?1",
            [&b.intent_id],
            |r| r.get(0),
        )?;
        ensure!(
            serde_json::from_str::<QuoteObservation>(&saved)? == *q,
            "owned_sell_quote_changed"
        );
        ensure!(
            authority.len() <= l.bytes.min(1 << 20),
            "owned_sell_authority_binding"
        );
        let e = crate::tiny_experiment::check_owned_sell_owner(
            &tx,
            experiment,
            wallet,
            &b.mint,
            &b.position_id,
            now,
        )?;
        let cap = cohort_sell_deadline(
            &tx,
            e.buy_order_id.as_deref().unwrap_or(""),
            experiment,
            &b.mint,
            now,
        )?
        .unwrap_or(e.deadline)
        .min(e.deadline);
        let deadline = (q.http_started.context("owned_sell_quote_clock")?
            + chrono::Duration::milliseconds(crate::ordered_sell_quote::MAX_QUOTE_AGE_MS))
        .min(cap);
        ensure!(now < deadline, "owned_sell_deadline");
        let h = Handoff {
            owner: uuid::Uuid::new_v4().to_string(),
            snapshot: s.clone(),
            quote: q.clone(),
            authority: authority.into(),
            deadline,
            ..old.clone()
        };
        let n=tx.execute("UPDATE rpc_owned_sell_handoffs SET owner=?1,snapshot=?2,authority=?3,quote=?4,deadline=?5
            WHERE intent_id=?6 AND owner=?7 AND state='preparing' AND unsigned_payload IS NULL",
            params![h.owner,serde_json::to_string(s)?,authority,serde_json::to_string(q)?,deadline.to_rfc3339(),b.intent_id,old.owner])?;
        ensure!(n == 1, "owned_sell_recovery_cas");
        tx.execute("INSERT INTO owned_sell_preparation_recoveries(old_owner,new_owner,handoff,recovered_at) VALUES(?1,?2,?3,?4)",
            params![old.owner,h.owner,serde_json::to_string(&h)?,now.to_rfc3339()])?;
        // No reservation release, new order, signal, spend or clock renewal.
        ensure!(clock() < deadline, "owned_sell_deadline");
        tx.commit()?;
        self.recheck_owned_sell_handoff(&h, l, clock())?;
        Ok(h)
    }
}
