//! Honest test-lot fraction from two canonical SOURCE receipts plus native SELL.
use super::*;
use crate::controlled_receipt_lot::{self as custody, Scope, SourceReceipts};
use serde_json::Value;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptProof {
    pub policy: String,
    pub source_run_id: String,
    pub bot_run_id: String,
    pub source_wallet: String,
    pub bot_wallet: String,
    pub mint: String,
    pub source_buy_order_id: String,
    pub source_buy_signature: String,
    pub source_buy_slot: u64,
    pub source_acquired_raw: u64,
    pub source_sell_order_id: String,
    pub source_sell_signature: String,
    pub source_sell_slot: u64,
    pub source_sold_raw: u64,
    pub source_remaining_before_raw: u64,
    pub bot_buy_order_id: String,
    pub bot_buy_signature: String,
    pub bot_owned_raw: u64,
    pub bot_token_account: String,
}
fn proof(
    c: &Connection,
    b: &QuoteBinding,
    s: &Scope,
    r: &SourceReceipts,
    account: &str,
) -> Result<ReceiptProof> {
    let (_, run, actual) = custody::load(c)?.context("receipt_lot_bot_scope_missing")?;
    ensure!(
        actual == *s
            && run == s.bot_run_id
            && b.source_wallet == s.source_wallet
            && b.source_signature == r.buy.signature
            && b.mint == inventory::USDC
            && b.decimals == 6,
        "receipt_lot_bot_scope_changed"
    );
    let crate::ExecutionCanaryBuyAttribution::Open(attribution) =
        crate::execution_canary_buy_attribution::read_on_conn(c, &b.mint)?
    else {
        anyhow::bail!("receipt_lot_bot_position_missing")
    };
    ensure!(
        attribution.position_id == b.position_id
            && attribution.proven_contributors.len() == 1
            && attribution.unproven_links.is_empty(),
        "receipt_lot_bot_contributors"
    );
    let source_raw: String = c.query_row(
        "SELECT admission FROM association_inbox_identities WHERE signature=?1",
        [&r.buy.signature],
        |row| row.get(0),
    )?;
    let source: copybot_core_types::association_delivery::AdmissionFacts =
        serde_json::from_str(&source_raw)?;
    ensure!(
        source.facts.signature == r.buy.signature
            && source.facts.slot == r.buy.slot
            && source.facts.wallet == s.source_wallet
            && source.facts.token_in == "So11111111111111111111111111111111111111112"
            && source.facts.token_out == b.mint
            && source
                .facts
                .exact_amounts
                .as_ref()
                .is_some_and(|x| x.amount_out_raw == r.buy.amount_raw.to_string()
                    && x.amount_out_decimals == 6
                    && x.amount_in_raw == "10000000"
                    && x.amount_in_decimals == 9),
        "receipt_lot_source_buy_event_amount"
    );
    let id = &attribution.proven_contributors[0].order_id;
    let f = crate::receipt_facts_rows::load(c, id)?.context("receipt_lot_bot_buy_missing")?;
    crate::receipt_facts_identity::validate_identity(c, &f)?;
    let origin = crate::buy_fill_identity::source(c, &f.order_id, &b.mint)?
        .context("receipt_lot_bot_order_missing")?;
    ensure!(
        f.wallet_pubkey == s.bot_wallet
            && f.side == "buy"
            && f.token == b.mint
            && origin.status == crate::EXECUTION_STATUS_CANARY_CONFIRMED
            && origin.signal_id == format!("native-buy-v1:{}", r.buy.signature)
            && f.token_delta
                .is_some_and(|q| q.decimals == 6 && q.raw >= i128::from(b.raw)),
        "receipt_lot_bot_buy_lineage"
    );
    Ok(ReceiptProof {
        policy: custody::POLICY.into(),
        source_run_id: s.source_run_id.clone(),
        bot_run_id: s.bot_run_id.clone(),
        source_wallet: s.source_wallet.clone(),
        bot_wallet: s.bot_wallet.clone(),
        mint: b.mint.clone(),
        source_buy_order_id: r.buy.order_id.clone(),
        source_buy_signature: r.buy.signature.clone(),
        source_buy_slot: r.buy.slot,
        source_acquired_raw: r.buy.amount_raw,
        source_sell_order_id: r.sell.order_id.clone(),
        source_sell_signature: r.sell.signature.clone(),
        source_sell_slot: r.sell.slot,
        source_sold_raw: r.sell.amount_raw,
        source_remaining_before_raw: r.remaining_before_raw,
        bot_buy_order_id: f.order_id,
        bot_buy_signature: f.tx_signature,
        bot_owned_raw: b.raw,
        bot_token_account: account.into(),
    })
}
pub(crate) fn recheck(c: &Connection, b: &QuoteBinding, d: &Binding) -> Result<()> {
    let p = d
        .controlled_receipt
        .as_ref()
        .context("receipt_lot_proof_missing")?;
    let (_, _, s) = custody::load(c)?.context("receipt_lot_scope_missing")?;
    let r = SqliteDiscoveryStore::controlled_source_receipts(
        &s,
        &p.source_sell_signature,
        p.source_sell_slot,
    )?
    .context("receipt_lot_source_receipt_pending")?;
    ensure!(
        proof(c, b, &s, &r, &p.bot_token_account)? == *p
            && d.protected_execution_baseline.is_none()
            && d.inventory.contract == custody::POLICY
            && d.inventory.numerator == r.sell.amount_raw
            && d.inventory.denominator == r.remaining_before_raw.to_string()
            && d.receipt_order == p.bot_buy_order_id
            && d.receipt_signature == p.bot_buy_signature,
        "receipt_lot_persisted_proof_changed"
    );
    Ok(())
}
pub fn available(value: &Value, account: &str, wallet: &str, amount: u64, slot: u64) -> Result<()> {
    ensure!(
        value["context"]["slot"].as_u64().is_some_and(|v| v >= slot),
        "receipt_lot_wallet_stale"
    );
    let rows = value["value"]
        .as_array()
        .context("receipt_lot_wallet_inventory")?;
    ensure!(rows.len() <= 1000, "receipt_lot_wallet_inventory_bound");
    let found = rows
        .iter()
        .filter(|r| r["pubkey"] == account)
        .collect::<Vec<_>>();
    ensure!(
        found.len() == 1,
        "receipt_lot_canonical_account_missing_or_duplicate"
    );
    let row = found[0];
    let parsed = &row["account"]["data"]["parsed"];
    let info = &parsed["info"];
    ensure!(
        row["account"]["owner"] == inventory::TOKEN
            && parsed["type"] == "account"
            && info["owner"] == wallet
            && info["mint"] == inventory::USDC
            && info["tokenAmount"]["decimals"] == 6,
        "receipt_lot_wallet_identity"
    );
    let text = info["tokenAmount"]["amount"]
        .as_str()
        .context("receipt_lot_wallet_amount")?;
    let held = text.parse::<u64>()?;
    ensure!(
        held.to_string() == text && held >= amount,
        "receipt_lot_token_shortage"
    );
    Ok(())
}
impl SqliteDiscoveryStore {
    pub fn complete_controlled_receipt_sell(
        &self,
        claim: &QuoteClaim,
        accounts: &Value,
        canonical: &str,
        l: InboxLimits,
        now: DateTime<Utc>,
    ) -> Result<QuoteClaim> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        collection(&tx, claim, l, now)?;
        let (_, _, scope) = custody::load(&tx)?.context("receipt_lot_scope_missing")?;
        let intent = crate::ordered_source_sell::rows::load(&tx, &claim.intent_id)?
            .context("receipt_lot_native_intent")?;
        let a = &intent.first.sell.admission.facts;
        let r = Self::controlled_source_receipts(&scope, &a.signature, a.slot)?
            .context("receipt_lot_source_receipt_pending")?;
        let exact = a
            .exact_amounts
            .as_ref()
            .context("receipt_lot_native_exact_missing")?;
        ensure!(
            a.wallet == scope.source_wallet
                && a.token_in == inventory::USDC
                && a.token_out == "So11111111111111111111111111111111111111112"
                && exact.amount_in_raw == r.sell.amount_raw.to_string()
                && exact.amount_in_decimals == 6,
            "receipt_lot_native_sell_changed"
        );
        let prep = crate::association_sell_preparation::on_connection(&tx, &a.signature, l)?
            .context("receipt_lot_preparation")?;
        use copybot_core_types::association_delivery::Terminal;
        let Some(Terminal::ProviderAsserted(bank)) = prep
            .current
            .anchors
            .iter()
            .find(|x| x.signature == a.signature)
            .and_then(|x| x.terminal.as_ref())
        else {
            anyhow::bail!("receipt_lot_native_bank_unknown")
        };
        let edge = prep
            .current
            .parent_paths
            .iter()
            .flat_map(|p| &p.edges)
            .find(|e| e.child.slot == bank.slot && e.child.hash == bank.blockhash)
            .context("receipt_lot_native_parent_missing")?;
        ensure!(
            bank.slot == a.slot && bank.signature == a.signature,
            "receipt_lot_native_bank_changed"
        );
        let receipt = proof(&tx, &claim.binding, &scope, &r, canonical)?;
        available(
            accounts,
            canonical,
            &scope.bot_wallet,
            claim.binding.raw,
            a.slot,
        )?;
        let selected = inventory::allocate(
            &[claim.binding.raw],
            r.sell.amount_raw,
            u128::from(r.remaining_before_raw),
        )?[0];
        ensure!(
            selected > 0 && selected <= claim.binding.raw,
            "receipt_lot_selected_amount"
        );
        let inventory = inventory::Inventory {
            contract: custody::POLICY.into(),
            signature: a.signature.clone(),
            wallet: a.wallet.clone(),
            mint: claim.binding.mint.clone(),
            slot: a.slot,
            parent: edge.parent.slot,
            bank_hash: bank.blockhash.clone(),
            parent_hash: edge.parent.hash.clone(),
            target_index: usize::try_from(bank.transaction_index)?,
            numerator: r.sell.amount_raw,
            denominator: r.remaining_before_raw.to_string(),
        };
        let (id,producer):(String,String)=tx.query_row("SELECT decision_id,producer_identity FROM fractional_sell_decisions WHERE intent_id=?1",[&claim.intent_id],|row|Ok((row.get(0)?,row.get(1)?)))?;
        let decision = Binding {
            version: 1,
            inventory,
            owned_raw: claim.binding.raw,
            selected_raw: selected,
            receipt_order: receipt.bot_buy_order_id.clone(),
            receipt_signature: receipt.bot_buy_signature.clone(),
            generation: claim.binding.snapshot_version.clone(),
            decision_id: id,
            producer_identity: producer,
            protected_execution_baseline: None,
            controlled_receipt: Some(receipt),
        };
        let evidence = serde_json::to_string(accounts)?;
        ensure!(
            evidence.len() <= 1 << 20,
            "receipt_lot_wallet_evidence_bound"
        );
        let bytes:i64=tx.query_row("SELECT COALESCE(SUM(length(CAST(evidence AS BLOB))),0) FROM fractional_sell_decisions WHERE intent_id<>?1",[&claim.intent_id],|r|r.get(0))?;
        ensure!(
            u64::try_from(bytes)?.saturating_add(evidence.len() as u64) <= 64 << 20,
            "fraction_evidence_capacity"
        );
        ensure!(tx.execute("UPDATE fractional_sell_decisions SET decision=?1,state='proven',evidence=?2 WHERE intent_id=?3 AND state='collecting'",params![serde_json::to_string(&decision)?,evidence,claim.intent_id])?==1,"receipt_lot_decision_CAS");
        let binding = apply(&tx, claim.binding.clone())?.map_err(anyhow::Error::msg)?;
        let old = rows::load(&tx, &claim.intent_id)?.context("receipt_lot_claim_missing")?;
        let mut row = old.clone();
        row.binding = Some(serde_json::to_string(&binding)?);
        rows::save(&tx, &claim.intent_id, Some(&old), &row)?;
        rows::budget(&tx, l)?;
        tx.commit()?;
        let mut out = claim.clone();
        out.binding = binding;
        ensure!(
            self.recheck_strict_sell_quote(&out, l, now)?,
            "receipt_lot_postcommit_changed"
        );
        Ok(out)
    }
}
