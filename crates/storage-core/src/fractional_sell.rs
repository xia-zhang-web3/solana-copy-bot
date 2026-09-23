//! One immutable proof decision per native intent; no rearm after an unknown producer read.
#[path = "fractional_inventory.rs"]
pub mod inventory;
use super::{rows, snapshot, QuoteBinding, QuoteClaim};
use crate::{association_inbox::InboxLimits, rpc_owned_sell_snapshot, SqliteDiscoveryStore};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub version: u8,
    pub inventory: inventory::Inventory,
    pub owned_raw: u64,
    pub selected_raw: u64,
    pub receipt_order: String,
    pub receipt_signature: String,
    pub generation: String,
    pub decision_id: String,
    pub producer_identity: String,
}
fn present(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='fractional_sell_decisions')",
        [],
        |r| r.get(0),
    )?)
}
pub(super) fn apply(
    c: &Connection,
    mut base: QuoteBinding,
) -> Result<std::result::Result<QuoteBinding, String>> {
    if !present(c)? {
        return Ok(Ok(base));
    }
    let saved: Option<(String, String, Option<String>)> = c
        .query_row(
            "SELECT base_binding,state,decision FROM fractional_sell_decisions WHERE intent_id=?1",
            [&base.intent_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((original, state, decision)) = saved else {
        return Ok(Ok(base));
    };
    if crate::execution_canary_receipt::pending_token_order(c, &base.mint, None)?.is_some() {
        return Ok(Err("fraction_prior_receipt_pending".into()));
    }
    if serde_json::from_str::<QuoteBinding>(&original)? != base {
        return Ok(Err("fraction_generation_changed".into()));
    }
    if state != "proven" {
        return Ok(Err("fraction_inventory_unknown_or_zero".into()));
    }
    let d: Binding =
        serde_json::from_str(decision.as_deref().context("fraction_decision_missing")?)?;
    let intent = crate::ordered_source_sell::rows::load(c, &base.intent_id)?
        .context("fraction_native_intent")?;
    let a = &intent.first.sell.admission.facts;
    ensure!(
        d.version == 1
            && d.owned_raw == base.raw
            && d.generation == base.snapshot_version
            && d.inventory.signature == a.signature
            && d.inventory.slot == a.slot
            && d.inventory.wallet == a.wallet
            && d.inventory.wallet == base.source_wallet
            && d.inventory.mint == base.mint,
        "fraction_persisted_binding"
    );
    let selected = inventory::allocate(
        &[base.raw],
        d.inventory.numerator,
        d.inventory.denominator.parse()?,
    )?[0];
    ensure!(
        selected > 0 && selected == d.selected_raw,
        "fraction_persisted_amount"
    );
    base.version = 2;
    base.raw = selected;
    base.fractional = Some(d);
    Ok(Ok(base))
}
impl SqliteDiscoveryStore {
    /// Reservation occurs before the producer's first RPC. A crash/Unknown is terminal
    /// for this source intent. It does not release a financial hold or resend anything.
    pub fn begin_fractional_sell(
        &self,
        claim: &QuoteClaim,
        l: InboxLimits,
        now: DateTime<Utc>,
        producer_identity: &str,
    ) -> Result<crate::rpc_owned_sell_snapshot::OwnedSellSnapshot> {
        ensure!(present(&self.conn)?, "fraction_migration_required");
        ensure!(producer_identity.len() == 64, "fraction_producer_identity");
        self.recheck_strict_sell_quote(claim, l, now)?;
        ensure!(
            crate::execution_canary_receipt::pending_token_order(
                &self.conn,
                &claim.binding.mint,
                None
            )?
            .is_none(),
            "fraction_prior_receipt_pending"
        );
        let s = self.owned_sell_snapshot(&claim.binding, l)?;
        ensure!(
            s.receipts.len() == 1 && s.facts.len() == 1,
            "fraction_lot_model_unsupported"
        );
        ensure!(
            s.facts[0].side == "buy"
                && s.facts[0].token == claim.binding.mint
                && s.facts[0]
                    .token_delta
                    .as_ref()
                    .is_some_and(|q| q.raw > 0 && q.decimals == claim.binding.decimals),
            "fraction_real_buy_required"
        );
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        ensure!(
            rpc_owned_sell_snapshot::read(&tx, &claim.binding, l)? == s,
            "fraction_generation_changed"
        );
        let count: i64 =
            tx.query_row("SELECT count(*) FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
        ensure!(count < 128, "fraction_decision_capacity");
        let original = serde_json::to_string(&claim.binding)?;
        tx.execute("INSERT INTO fractional_sell_decisions(intent_id,base_binding,claim_owner,decision_id,state,producer_identity) VALUES(?1,?2,?3,?4,'collecting',?5)", params![claim.intent_id,original,claim.owner,uuid::Uuid::new_v4().to_string(),producer_identity])?;
        tx.commit()?;
        Ok(s)
    }
    pub fn recheck_fractional_collection(
        &self,
        claim: &QuoteClaim,
        l: InboxLimits,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        collection(&tx, claim, l, now)?;
        tx.commit()?;
        Ok(())
    }
    /// The verifier derives N/D from complete raw program pages + canonical full-prefix
    /// and the native source facts. There is no public set-N/D or set-selected-raw API.
    pub fn complete_fractional_sell(
        &self,
        claim: &QuoteClaim,
        evidence: &inventory::Evidence,
        l: InboxLimits,
        now: DateTime<Utc>,
    ) -> Result<QuoteClaim> {
        let evidence_wire = inventory::encode_evidence(evidence)?;
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        collection(&tx, claim, l, now)?;
        let intent = crate::ordered_source_sell::rows::load(&tx, &claim.intent_id)?
            .context("fraction_native_intent")?;
        let a = &intent.first.sell.admission.facts;
        let prep = crate::association_sell_preparation::on_connection(&tx, &a.signature, l)?
            .context("fraction_preparation_missing")?;
        ensure!(
            prep.current.current_contributors.len() == 1,
            "fraction_lot_model_unsupported"
        );
        let receipt = &prep.current.current_contributors[0];
        let facts = crate::receipt_facts_rows::load(&tx, &receipt.contributor.order_id)?
            .context("fraction_receipt_missing")?;
        crate::receipt_facts_identity::validate_identity(&tx, &facts)?;
        let exact = a
            .exact_amounts
            .as_ref()
            .context("fraction_source_exact_missing")?;
        ensure!(
            a.wallet == claim.binding.source_wallet
                && a.token_in == claim.binding.mint
                && a.token_out == claim.binding.output_mint,
            "fraction_native_source_identity"
        );
        let proof = inventory::verify(
            evidence,
            &claim.binding,
            &a.signature,
            a.slot,
            exact.amount_in_raw.parse()?,
            &facts.wallet_pubkey,
        )?;
        use copybot_core_types::association_delivery::Terminal;
        let terminal = prep
            .current
            .anchors
            .iter()
            .find(|x| x.signature == a.signature)
            .and_then(|x| x.terminal.as_ref())
            .context("fraction_native_bank_missing")?;
        let Terminal::ProviderAsserted(bank) = terminal else {
            anyhow::bail!("fraction_native_bank_unknown")
        };
        ensure!(
            bank.slot == proof.slot
                && bank.blockhash == proof.bank_hash
                && bank.transaction_index as usize == proof.target_index,
            "fraction_native_bank_conflict"
        );
        ensure!(
            prep.current
                .parent_paths
                .iter()
                .flat_map(|p| &p.edges)
                .any(|e| e.child.slot == proof.slot
                    && e.child.hash == proof.bank_hash
                    && e.parent.slot == proof.parent
                    && e.parent.hash == proof.parent_hash),
            "fraction_native_parent_conflict"
        );
        let selected = inventory::allocate(
            &[claim.binding.raw],
            proof.numerator,
            proof.denominator.parse()?,
        )?[0];
        let id: String = tx.query_row(
            "SELECT decision_id FROM fractional_sell_decisions WHERE intent_id=?1",
            [&claim.intent_id],
            |r| r.get(0),
        )?;
        let producer_identity: String = tx.query_row(
            "SELECT producer_identity FROM fractional_sell_decisions WHERE intent_id=?1",
            [&claim.intent_id],
            |r| r.get(0),
        )?;
        let decision = Binding {
            version: 1,
            inventory: proof,
            owned_raw: claim.binding.raw,
            selected_raw: selected,
            receipt_order: receipt.contributor.order_id.clone(),
            receipt_signature: receipt.contributor.tx_signature.clone(),
            generation: claim.binding.snapshot_version.clone(),
            decision_id: id,
            producer_identity,
        };
        let bytes: i64 = tx.query_row(
            "SELECT coalesce(sum(length(CAST(evidence AS BLOB))),0) FROM fractional_sell_decisions",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            bytes as usize + evidence_wire.len() <= 64 << 20,
            "fraction_evidence_capacity"
        );
        tx.execute("UPDATE fractional_sell_decisions SET decision=?1,state=?2,evidence=?4 WHERE intent_id=?3 AND state='collecting'",params![serde_json::to_string(&decision)?, if selected == 0 {"zero"} else {"proven"},claim.intent_id,evidence_wire])?;
        if selected == 0 {
            tx.commit()?;
            anyhow::bail!("fraction_zero_allocation");
        }
        let binding = apply(&tx, claim.binding.clone())?.map_err(anyhow::Error::msg)?;
        let old = rows::load(&tx, &claim.intent_id)?.context("fraction_claim_missing")?;
        let mut row = old.clone();
        row.binding = Some(serde_json::to_string(&binding)?);
        rows::save(&tx, &claim.intent_id, Some(&old), &row)?;
        rows::budget(&tx, l)?;
        tx.commit()?;
        let mut out = claim.clone();
        out.binding = binding;
        self.recheck_strict_sell_quote(&out, l, now)?;
        Ok(out)
    }
}
fn collection(
    c: &Connection,
    claim: &QuoteClaim,
    l: InboxLimits,
    now: DateTime<Utc>,
) -> Result<()> {
    ensure!(
        now < claim.lease_until && claim.binding.fractional.is_none(),
        "fraction_collection_deadline"
    );
    ensure!(
        crate::execution_canary_receipt::pending_token_order(c, &claim.binding.mint, None)?
            .is_none(),
        "fraction_prior_receipt_pending"
    );
    let base = snapshot::read_base(c, &claim.intent_id, l, &claim.binding.endpoint)?
        .map_err(anyhow::Error::msg)?;
    ensure!(base == claim.binding, "fraction_generation_changed");
    let row = rows::load(c, &claim.intent_id)?.context("fraction_claim_missing")?;
    ensure!(
        row.owner == claim.owner
            && row.attempt == claim.attempt
            && row.record.is_none()
            && row.binding.as_deref() == Some(serde_json::to_string(&claim.binding)?.as_str()),
        "fraction_claim_changed"
    );
    let same: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM fractional_sell_decisions WHERE intent_id=?1 AND claim_owner=?2 AND base_binding=?3 AND state='collecting')", params![claim.intent_id,claim.owner,serde_json::to_string(&claim.binding)?], |r| r.get(0))?;
    ensure!(same, "fraction_decision_changed");
    Ok(())
}
