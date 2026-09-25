//! Fresh strict evidence plus all durable successful BUY receipt operands.
use crate::{
    association_inbox::InboxLimits, association_sell_preparation::ReceiptAnchor,
    ordered_sell_quote::QuoteBinding, ExecutionCanaryReceiptFacts, SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::AdmissionFacts;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedSellSnapshot {
    pub quote: QuoteBinding,
    pub sell: AdmissionFacts,
    pub receipts: Vec<ReceiptAnchor>,
    /// Exact receipt_fingerprint is serialized in receipts; runtime facts are freshly read.
    #[serde(skip)]
    pub facts: Vec<ExecutionCanaryReceiptFacts>,
}
pub(crate) fn read(c: &Connection, b: &QuoteBinding, l: InboxLimits) -> Result<OwnedSellSnapshot> {
    let current = crate::ordered_sell_quote::snapshot::read_with_preparation(
        c,
        &b.intent_id,
        l,
        &b.endpoint,
    )?;
    ensure!(
        current.as_ref().ok().map(|(binding, _)| binding) == Some(b),
        "owned_sell_snapshot_changed"
    );
    let (_, p) = current.expect("validated owned SELL snapshot");
    let mut facts = vec![];
    for r in &p.current.current_contributors {
        let f = crate::receipt_facts_rows::load(c, &r.contributor.order_id)?
            .context("owned_sell_receipt_missing")?;
        crate::receipt_facts_identity::validate_identity(c, &f)?;
        facts.push(f);
    }
    ensure!(!facts.is_empty(), "owned_sell_receipts_empty");
    Ok(OwnedSellSnapshot {
        quote: b.clone(),
        sell: p.first.sell.admission,
        receipts: p.current.current_contributors,
        facts,
    })
}
impl SqliteDiscoveryStore {
    pub fn owned_sell_snapshot(
        &self,
        b: &QuoteBinding,
        l: InboxLimits,
    ) -> Result<OwnedSellSnapshot> {
        let tx = self.conn.unchecked_transaction()?;
        let r = read(&tx, b, l)?;
        tx.commit()?;
        Ok(r)
    }
}
