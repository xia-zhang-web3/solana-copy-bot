//! Durable SELL preparation v1, used by the actual observation consumer.
//! Preparation is evidence only. There is no execution bridge in this module.
use crate::association_inbox::{AssociationInbox, InboxIdentity, InboxLimits};
pub use crate::association_sell_types::*;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{AdmissionFacts, CandidateGeneration};
use rusqlite::Connection;
#[path = "association_sell_automatic.rs"]
mod automatic;
#[path = "association_sell_evaluate.rs"]
mod evaluate;
#[path = "association_sell_financial.rs"]
mod financial;
#[path = "association_sell_order.rs"]
mod order;
#[path = "association_parent_graph.rs"]
pub(crate) mod parent_graph;
#[path = "association_sell_rows.rs"]
mod rows;
#[path = "association_sell_schema.rs"]
pub mod schema;
#[path = "association_sell_shadow.rs"]
mod shadow;
#[path = "association_shadow_recovery.rs"]
pub(crate) mod shadow_recovery;
#[path = "association_sell_work.rs"]
mod work;
pub(crate) use rows::Readback;
const SOL: &str = "So11111111111111111111111111111111111111112";
fn anchor_identity(i: &InboxIdentity) -> AnchorIdentity {
    AnchorIdentity {
        admission: i.admission.clone(),
        first_session: i.first_session.clone(),
        first_sequence: i.first_sequence,
    }
}
impl AssociationInbox {
    /// A fresh SQLite snapshot is mandatory on every read for use. The cached
    /// evaluations are explicitly historical; callers cannot request a ready bool.
    pub fn sell_preparation(&self, signature: &str) -> Result<Option<ValidatedPreparation>> {
        let tx = self.conn.unchecked_transaction()?;
        let result = on_connection(&tx, signature, self.limits)?;
        tx.commit()?;
        Ok(result)
    }
    /// One continuation unit in the same off-thread writer. No wait for missing
    /// dependencies, no upfront inbox materialization and no task per dependent SELL.
    pub fn recover_sell_preparation(&mut self) -> Result<()> {
        crate::association_inbox::schema::required(&self.conn)?;
        schema::required(&self.conn)?;
        parent_graph::schema::required(&self.conn)?;
        crate::association_inbox::ordered::required(&self.conn, self.mode)?;
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut proof = Readback::new(self.mode);
        work::step(&tx, self.limits, &mut proof)?;
        crate::association_inbox::check_budget(&tx, self.limits, self.mode)?;
        proof.verify(&tx)?;
        tx.commit()?;
        proof.verify(&self.conn)
    }
    pub fn has_sell_preparation_work(&self) -> Result<bool> {
        Ok(work::pending(&self.conn)?
            || (self.mode == crate::association_inbox::ConsumerMode::ProviderOrderStrictV1
                && shadow_recovery::pending(&self.conn)?))
    }
}
pub(crate) fn initialize(c: &Connection, proof: &mut Readback) -> Result<()> {
    work::initialize(c, proof)
}
pub(crate) fn event(
    c: &Connection,
    signature: Option<&str>,
    fresh: bool,
    observed: (i64, u32),
    l: InboxLimits,
    proof: &mut Readback,
) -> Result<()> {
    if let Some(s) = signature {
        rows::prepare(c, s, fresh, fresh.then_some(observed), l, proof)?;
        work::enqueue(c, s, proof)?;
    }
    work::step(c, l, proof)
}

pub(crate) fn parent(
    c: &Connection,
    d: &copybot_core_types::association_delivery::Delivery,
    proof: &mut Readback,
) -> Result<()> {
    parent_graph::put(c, d, proof)
}

/// Shared evaluator; callers own exactly one transaction and its connection.
pub(crate) fn on_connection(
    c: &Connection,
    signature: &str,
    limits: InboxLimits,
) -> Result<Option<ValidatedPreparation>> {
    crate::association_inbox::schema::required(c)?;
    schema::required(c)?;
    parent_graph::schema::required(c)?;
    Ok(match rows::load(c, signature)? {
        None => None,
        Some((first, initial, latest)) => Some(ValidatedPreparation {
            current: evaluate::evaluate(c, &first, limits)?,
            first,
            historical_initial: initial,
            historical_latest: latest,
        }),
    })
}
