//! Bounded native verifier of the accepted two-program parent/full-prefix contract.
//! Inputs are raw provider evidence, never a caller's N/D or PROVEN flag.
#[path = "fractional_inventory_rows.rs"]
mod rows;
#[path = "fractional_inventory_schema.rs"]
mod schema;
use super::QuoteBinding;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
/// Bound allocation during serialization, not only after allocating an oversized String.
pub fn encode_evidence(evidence: &Evidence) -> Result<String> {
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > (32 << 20) - self.0.len() {
                return Err(std::io::Error::other("fraction_evidence_bound"));
            }
            let needed = self.0.len() + bytes.len();
            if needed > self.0.capacity() {
                let capacity = needed.max((self.0.capacity().max(8192) * 2).min(32 << 20));
                self.0
                    .try_reserve_exact(capacity - self.0.len())
                    .map_err(std::io::Error::other)?;
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Bounded(Vec::new());
    serde_json::to_writer(&mut out, evidence)?;
    Ok(String::from_utf8(out.0)?)
}
pub const CONTRACT: &str = "whole_wallet_parent_program_fraction_v1";
pub const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN22: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub program: String,
    pub cursor: Option<String>,
    pub response: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub version: u8,
    pub slot: u64,
    pub block: Value,
    pub parent: Value,
    pub pages: Vec<Page>,
    /// Current execution-wallet inventory, never the source denominator fallback.
    pub execution_accounts: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub contract: String,
    pub signature: String,
    pub wallet: String,
    pub mint: String,
    pub slot: u64,
    pub parent: u64,
    pub bank_hash: String,
    pub parent_hash: String,
    pub target_index: usize,
    pub numerator: u64,
    pub denominator: String,
}
/// Exact independent per-lot floors. Production currently rejects multi-origin tiny lots.
pub fn allocate(lots: &[u64], n: u64, d: u128) -> Result<Vec<u64>> {
    ensure!(
        n > 0 && d >= u128::from(n) && lots.len() <= 128,
        "fraction_ratio_or_lots"
    );
    lots.iter()
        .map(|h| Ok(u64::try_from(u128::from(*h) * u128::from(n) / d)?))
        .collect()
}
pub(super) fn verify(
    e: &Evidence,
    b: &QuoteBinding,
    signature: &str,
    slot: u64,
    raw: u64,
    wallet: &str,
) -> Result<Inventory> {
    ensure!(
        e.version == 1 && e.slot == slot && slot > 0,
        "fraction_slot_binding"
    );
    let block = &e.block;
    let parent = block["parentSlot"]
        .as_u64()
        .context("fraction_parent_missing")?;
    ensure!(parent < slot, "fraction_parent_order");
    let hash = rows::key(&block["blockhash"])?;
    let parent_hash = rows::key(&block["previousBlockhash"])?;
    ensure!(
        e.parent["blockhash"] == parent_hash,
        "fraction_parent_bank_conflict"
    );
    let mut accounts = BTreeMap::new();
    let mut seen = BTreeSet::new();
    ensure!(
        !e.pages.is_empty() && e.pages.len() <= 32,
        "fraction_pages_bound"
    );
    for program in [TOKEN22, TOKEN] {
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        let mut complete = false;
        for page in e.pages.iter().filter(|p| p.program == program) {
            ensure!(!complete && page.cursor == cursor, "fraction_page_chain");
            let r = &page.response;
            ensure!(
                r["context"]["slot"].as_u64() == Some(parent),
                "fraction_page_slot"
            );
            let values = r["value"].as_array().context("fraction_page_values")?;
            ensure!(values.len() <= 1000, "fraction_page_bound");
            for v in values {
                let (address, mint, amount, decimals) =
                    rows::account(v, &b.source_wallet, Some(program))?;
                ensure!(
                    seen.insert(address.clone()) && seen.len() <= 32000,
                    "fraction_duplicate_account"
                );
                if mint == b.mint {
                    ensure!(decimals == b.decimals, "fraction_decimals");
                    accounts.insert(address, amount);
                }
            }
            let next = r.get("pageKey").context("fraction_page_terminal_missing")?;
            cursor = if next.is_null() {
                None
            } else {
                let s = next
                    .as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 1024)
                    .context("fraction_cursor")?;
                ensure!(
                    !values.is_empty() && cursors.insert(s.to_owned()),
                    "fraction_cursor_cycle"
                );
                Some(s.into())
            };
            complete = cursor.is_none();
        }
        ensure!(complete, "fraction_program_chain_incomplete");
    }
    ensure!(
        e.pages
            .iter()
            .all(|p| p.program == TOKEN || p.program == TOKEN22),
        "fraction_program"
    );
    let txs = block["transactions"]
        .as_array()
        .context("fraction_full_block_missing")?;
    ensure!(txs.len() <= 20000, "fraction_block_bound");
    let hits = txs
        .iter()
        .enumerate()
        .filter(|(_, t)| t["transaction"]["signatures"][0] == signature)
        .collect::<Vec<_>>();
    ensure!(hits.len() == 1, "fraction_target_missing_duplicate");
    let (index, target) = hits[0];
    for t in &txs[..index] {
        rows::advance(t, b, &mut accounts)?;
    }
    let (keys, pre, post) = rows::balances(target, b)?;
    ensure!(target["meta"]["err"].is_null(), "fraction_target_failed");
    let source_account = rows::source(target, &keys, b, raw)?;
    for field in ["preTokenBalances", "postTokenBalances"] {
        for v in target["meta"][field].as_array().unwrap() {
            let address = &keys[v["accountIndex"].as_u64().unwrap() as usize];
            if accounts.contains_key(address) {
                ensure!(
                    v["owner"] == b.source_wallet && v["mint"] == b.mint,
                    "fraction_target_owner_change"
                );
            }
        }
    }
    for (address, amount) in &pre {
        ensure!(
            accounts.get(address).copied().unwrap_or(0) == *amount,
            "fraction_target_pre_conflict"
        );
    }
    let before = pre
        .get(&source_account)
        .context("fraction_source_account_absent")?;
    let after = post.get(&source_account).copied().unwrap_or(0);
    ensure!(
        before.checked_sub(after) == Some(raw),
        "fraction_source_amount_conflict"
    );
    for a in pre.keys().chain(post.keys()) {
        if *a != source_account {
            ensure!(pre.get(a) == post.get(a), "fraction_source_extra_movement");
        }
    }
    let denominator = accounts.values().try_fold(0u128, |a, b| {
        a.checked_add(u128::from(*b))
            .context("fraction_sum_overflow")
    })?;
    ensure!(
        denominator >= u128::from(raw) && raw > 0,
        "fraction_denominator"
    );
    // Supported custody model: one execution token account, exact receipt-owned H.
    // A short wallet is a refusal, never min(H,wallet) or a new amount selection.
    let own = &e.execution_accounts;
    ensure!(
        own["context"]["slot"].as_u64().is_some_and(|s| s >= slot),
        "fraction_wallet_context"
    );
    let values = own["value"]
        .as_array()
        .context("fraction_wallet_inventory")?;
    ensure!(values.len() == 1, "fraction_wallet_model_unsupported");
    let (_, mint, held, decimals) = rows::account(&values[0], wallet, None)?;
    ensure!(
        mint == b.mint && held == b.raw && decimals == b.decimals,
        "fraction_wallet_owned_conflict"
    );
    Ok(Inventory {
        contract: CONTRACT.into(),
        signature: signature.into(),
        wallet: b.source_wallet.clone(),
        mint: b.mint.clone(),
        slot,
        parent,
        bank_hash: hash,
        parent_hash,
        target_index: index,
        numerator: raw,
        denominator: denominator.to_string(),
    })
}
