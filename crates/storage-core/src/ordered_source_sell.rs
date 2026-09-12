//! Shared strict staging for explicit callers and the automatic durable consumer.
//! A fresh positive result is evidence at return, not a signing/promotion permit.
use crate::association_inbox::AssociationInbox;
use anyhow::{ensure, Result};
#[path = "ordered_source_sell_ownership.rs"]
pub(crate) mod ownership;
#[path = "ordered_source_sell_policy.rs"]
pub(crate) mod policy;
#[path = "ordered_source_sell_rows.rs"]
pub(crate) mod rows;
#[path = "ordered_source_sell_schema.rs"]
pub mod schema;
#[path = "ordered_source_sell_types.rs"]
mod types;
pub use types::*;

fn canonical(signature: &str) -> String {
    format!("source-sell:{signature}")
}
fn stage_refusal(d: OrderedSellDecision) -> OrderedSellStage {
    match d {
        OrderedSellDecision::Unknown(r) => OrderedSellStage::Unknown(r),
        OrderedSellDecision::Blocked(r) => OrderedSellStage::Blocked(r),
        OrderedSellDecision::ValidatedNow => unreachable!("positive decision is handled by caller"),
    }
}
impl AssociationInbox {
    /// Loads trusted first/current itself, validates and inserts on one connection
    /// in one IMMEDIATE transaction. No caller-provided verdict or financial list.
    pub fn stage_ordered_source_sell_intent(
        &mut self,
        signature: &str,
        policy_name: &str,
    ) -> Result<OrderedSellStage> {
        if policy_name != PROVIDER_ORDER_STRICT_V1 {
            return Ok(OrderedSellStage::Unknown(
                OrderedSellReason::UnsupportedPolicy,
            ));
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = stage_on_connection(&tx, signature, self.limits)?;
        if matches!(
            result,
            OrderedSellStage::Inserted(_) | OrderedSellStage::Existing(_)
        ) {
            tx.commit()?;
        }
        Ok(result)
    }
    /// Immutable history only. Never use this lookup as a current permit.
    pub fn load_ordered_source_sell_intent_history(
        &self,
        id: &str,
    ) -> Result<Option<OrderedSourceSellIntent>> {
        let tx = self.conn.unchecked_transaction()?;
        schema::required(&tx)?;
        let result = rows::load(&tx, id)?;
        tx.commit()?;
        Ok(result)
    }
    /// Re-evaluate by immutable ID in one fresh snapshot. No saved evaluation is
    /// authoritative and a late refusal never rewrites, deletes or rebinds history.
    pub fn revalidate_ordered_source_sell_intent(&self, id: &str) -> Result<OrderedSellDecision> {
        let tx = self.conn.unchecked_transaction()?;
        schema::required(&tx)?;
        let Some(intent) = rows::load(&tx, id)? else {
            return Ok(OrderedSellDecision::Unknown(
                OrderedSellReason::MissingIntent,
            ));
        };
        let signature = &intent.first.sell.admission.facts.signature;
        let Some(p) =
            crate::association_sell_preparation::on_connection(&tx, signature, self.limits)?
        else {
            return Ok(OrderedSellDecision::Unknown(
                OrderedSellReason::MissingPreparation,
            ));
        };
        let result = if intent.first != p.first {
            OrderedSellDecision::Blocked(OrderedSellReason::PreparationChanged)
        } else if ownership::legacy_exists(&tx, signature)? {
            OrderedSellDecision::Blocked(OrderedSellReason::SourceSignatureClaimed)
        } else {
            policy::check(&tx, &p)?
        };
        tx.commit()?;
        Ok(result)
    }
}

/// Caller owns the IMMEDIATE transaction. Every attempt evaluates fresh policy,
/// including Existing; history never substitutes for current admissibility.
pub(crate) fn stage_on_connection(
    c: &rusqlite::Connection,
    signature: &str,
    limits: crate::association_inbox::InboxLimits,
) -> Result<OrderedSellStage> {
    schema::required(c)?;
    let Some(p) = crate::association_sell_preparation::on_connection(c, signature, limits)? else {
        return Ok(OrderedSellStage::Unknown(
            OrderedSellReason::MissingPreparation,
        ));
    };
    let id = canonical(signature);
    let old = rows::load(c, &id)?;
    if old.as_ref().is_some_and(|i| i.first != p.first) {
        return Ok(OrderedSellStage::Blocked(
            OrderedSellReason::PreparationChanged,
        ));
    }
    let decision = policy::check(c, &p)?;
    if decision != OrderedSellDecision::ValidatedNow {
        return Ok(stage_refusal(decision));
    }
    let owner = ownership::owner(c, signature)?;
    if ownership::legacy_exists(c, signature)? || owner.as_deref() == Some("legacy") {
        return Ok(OrderedSellStage::Blocked(
            OrderedSellReason::SourceSignatureClaimed,
        ));
    }
    let result = if let Some(old) = old {
        OrderedSellStage::Existing(Box::new(old))
    } else {
        ensure!(owner.is_none(), "ordered SELL claim without intent");
        let intent = OrderedSourceSellIntent {
            version: 1,
            intent_id: id.clone(),
            policy: PROVIDER_ORDER_STRICT_V1.into(),
            first: p.first,
            staged_evaluation: p.current,
            staged_at: chrono::Utc::now(),
        };
        ownership::claim(c, signature)?;
        rows::insert(c, &intent)?;
        OrderedSellStage::Inserted(Box::new(intent))
    };
    Ok(result)
}
