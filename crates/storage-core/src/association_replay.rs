//! Cursor provenance is the committed immutable inbox event, not receipt/send state.
use crate::association_inbox::AssociationInbox;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{Delivery, DeliveryEvent};
use copybot_core_types::association_recovery::{BlockCheckpoint, DurableCheckpoint, ReplayScope};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
#[path = "association_replay_schema.rs"]
mod schema;
#[path = "association_replay_validate.rs"]
mod validate;

impl AssociationInbox {
    pub fn configure_replay_scope(&mut self, scope: &ReplayScope) -> Result<()> {
        schema::required(&self.conn)?;
        ensure!(scope.valid(), "association_replay_scope_invalid");
        let wire = serde_json::to_string(scope)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO association_replay_cursor(id,scope,head) VALUES(1,?1,NULL)",
            [&wire],
        )?;
        let saved: String = tx.query_row(
            "SELECT scope FROM association_replay_cursor WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(saved == wire, "association_replay_scope_changed");
        crate::association_inbox::check_budget(&tx, self.limits, self.mode)?;
        tx.commit()?;
        let saved: String = self.conn.query_row(
            "SELECT scope FROM association_replay_cursor WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(saved == wire, "association_replay_scope_readback");
        Ok(())
    }
    pub fn replay_checkpoint(&self, scope: &ReplayScope) -> Result<Option<DurableCheckpoint>> {
        schema::required(&self.conn)?;
        let tx = self.conn.unchecked_transaction()?;
        let mut value = read(&tx, scope, self.limits.bytes)?;
        if let Some(ref mut checkpoint) = value {
            validate::block(&tx, &checkpoint.block, self.limits)?;
            validate::event(&tx, checkpoint)?;
            checkpoint.from_slot =
                checkpoint
                    .from_slot
                    .min(validate::floor(&tx, &checkpoint.block, self.limits)?);
            checkpoint.overlap = validate::overlap(&tx, checkpoint, self.limits)?;
        }
        tx.commit()?;
        Ok(value)
    }
}
fn read(c: &Connection, scope: &ReplayScope, bytes: usize) -> Result<Option<DurableCheckpoint>> {
    ensure!(scope.valid(), "association_replay_scope_invalid");
    let size: (String, Option<usize>) = c.query_row(
        "SELECT scope,length(CAST(head AS BLOB)) FROM association_replay_cursor WHERE id=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        size.0 == serde_json::to_string(scope)?,
        "association_replay_scope_changed"
    );
    ensure!(
        size.1.is_none_or(|n| n <= bytes),
        "association_replay_head_bound"
    );
    let wire: Option<String> = c.query_row(
        "SELECT head FROM association_replay_cursor WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    let head: Option<DurableCheckpoint> = wire
        .map(|v| serde_json::from_str(&v).context("association_replay_head_decode"))
        .transpose()?;
    ensure!(
        head.as_ref().is_none_or(|h| h.block.scope == *scope),
        "association_replay_head_scope_changed"
    );
    Ok(head)
}
pub(crate) fn record(
    c: &Connection,
    d: &Delivery,
    limits: crate::association_inbox::InboxLimits,
) -> Result<Option<String>> {
    let DeliveryEvent::ParentCheckpoint(block) = &d.event else {
        return Ok(None);
    };
    schema::required(c)?;
    validate::block(c, block, limits)?;
    let prior = read(c, &block.scope, limits.bytes)?;
    if let Some(ref old) = prior {
        validate::event(c, old)?;
        validate::block(c, &old.block, limits)?;
        if block.observation.child.slot < old.block.observation.child.slot {
            return Ok(Some(serde_json::to_string(old)?));
        }
        validate::linked(
            c,
            &block.observation.child,
            &old.block.observation.child,
            limits,
        )?;
    }
    let from_slot = validate::floor(c, block, limits)?;
    let head = DurableCheckpoint {
        session: d.session.clone(),
        sequence: d.sequence,
        block: block.clone(),
        from_slot,
        overlap: vec![],
    };
    let wire = serde_json::to_string(&head)?;
    ensure!(wire.len() <= limits.bytes, "association_replay_head_bound");
    c.execute(
        "UPDATE association_replay_cursor SET head=?1 WHERE id=1",
        [&wire],
    )?;
    verify(c, &wire)?;
    Ok(Some(wire))
}
pub(crate) fn verify(c: &Connection, expected: &str) -> Result<()> {
    let saved: String = c.query_row(
        "SELECT head FROM association_replay_cursor WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    ensure!(saved == expected, "association_replay_commit_readback");
    Ok(())
}
pub(crate) fn usage(c: &Connection) -> Result<(usize, usize)> {
    if !schema::available(c)? {
        return Ok((0, 0));
    }
    Ok(c.query_row("SELECT count(*),coalesce(sum(512+length(CAST(scope AS BLOB))+coalesce(length(CAST(head AS BLOB)),0)),0) FROM association_replay_cursor", [], |r| Ok((r.get(0)?,r.get(1)?)))?)
}
