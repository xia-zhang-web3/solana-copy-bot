//! Gross independent quote marks. Coverage is checked before adding amounts.
use crate::execution_canary_quote_pnl_wallet::{WalletSellQuoteProof, WalletTokenBalance};
use crate::execution_wallet_quote::{raw_amount, WalletQuoteRequest};
use copybot_storage_core::ExecutionCanaryOwnedPosition;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QuoteCoverage {
    pub scope: &'static str,
    pub complete: bool,
    pub known_items: u64,
    pub unknown_items: u64,
    pub quoted_value_sol: Option<f64>,
    pub unknown_reasons: Vec<String>,
}
impl QuoteCoverage {
    pub(crate) fn from_values(
        scope: &'static str,
        cohort_complete: bool,
        values: &[Option<f64>],
        reasons: Vec<String>,
    ) -> Self {
        let known_items = values.iter().filter(|v| v.is_some()).count() as u64;
        let complete = cohort_complete && known_items == values.len() as u64;
        Self {
            scope,
            complete,
            known_items,
            unknown_items: values.len() as u64 - known_items,
            quoted_value_sol: complete.then(|| values.iter().flatten().sum()),
            unknown_reasons: reasons,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BotPositionMark {
    pub position_id: String,
    pub token: String,
    pub amount_raw: Option<String>,
    pub decimals: Option<u8>,
    pub token_account: Option<String>,
    pub binding_reason: Option<String>,
    pub quoted_value_sol: Option<f64>,
    pub unknown_reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BotRemainderMark {
    pub positions_loaded: bool,
    pub open_cost_sol: Option<f64>,
    pub coverage: QuoteCoverage,
    pub positions: Vec<BotPositionMark>,
}
pub(crate) fn bound_quote<'a>(
    owner: &str,
    balance: &WalletTokenBalance,
    proofs: &'a [WalletSellQuoteProof],
) -> Option<&'a WalletSellQuoteProof> {
    let request = WalletQuoteRequest::for_balance(owner, balance);
    let mut matches = proofs.iter().filter(|p| p.request == request);
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}
pub(crate) fn quote_value(quote: Option<&WalletSellQuoteProof>) -> Option<f64> {
    let q = quote.filter(|q| q.status == "ok")?;
    let value = raw_amount(q.out_amount_raw.as_deref()?)? as f64 / 1e9;
    (q.out_sol == Some(value)).then_some(value)
}
pub(crate) fn quote_reason(quote: Option<&WalletSellQuoteProof>) -> Option<String> {
    if quote_value(quote).is_some() {
        return None;
    }
    Some(
        match quote {
            None => "quote_missing_or_ambiguous",
            Some(q) if q.status == "no_route" => "quote_no_route",
            Some(q) if q.status == "missing" => "quote_missing",
            Some(_) => "quote_invalid_or_error",
        }
        .into(),
    )
}
pub(crate) fn bot_marks(
    owner: &str,
    inventory_complete: bool,
    balances: &[WalletTokenBalance],
    positions: Result<&[ExecutionCanaryOwnedPosition], &str>,
    proofs: &[WalletSellQuoteProof],
) -> BotRemainderMark {
    let loaded = positions.is_ok();
    let mut reasons = Vec::new();
    if !loaded {
        reasons.push("bot_positions_read_failed".into());
    }
    if !inventory_complete {
        reasons.push("wallet_inventory_incomplete".into());
    }
    let positions = positions.unwrap_or(&[]);
    let marks: Vec<_> = positions
        .iter()
        .map(|position| {
            let accounts: Vec<_> = balances
                .iter()
                .filter(|b| b.mint == position.token)
                .collect();
            let same_mint = positions
                .iter()
                .filter(|p| p.token == position.token)
                .count();
            let same_id = positions
                .iter()
                .filter(|p| p.position_id == position.position_id)
                .count();
            let reason = if !inventory_complete {
                Some("wallet_inventory_incomplete")
            } else if same_mint != 1 || same_id != 1 {
                Some("ambiguous_bot_positions")
            } else if accounts.len() > 1 {
                Some("ambiguous_wallet_accounts")
            } else if accounts.is_empty() {
                Some("unmatched_bot_position")
            } else if position.qty_exact.is_none() {
                Some("missing_exact_bot_quantity")
            } else {
                let qty = position.qty_exact.as_ref().expect("checked");
                let row = accounts[0];
                if raw_amount(&row.amount_raw) != Some(qty.raw()) || row.decimals != qty.decimals()
                {
                    Some("bot_wallet_quantity_mismatch")
                } else {
                    None
                }
            };
            let quote = reason
                .is_none()
                .then(|| bound_quote(owner, accounts[0], proofs))
                .flatten();
            let unknown = reason.map(str::to_owned).or_else(|| quote_reason(quote));
            if let Some(r) = &unknown {
                reasons.push(r.clone());
            }
            BotPositionMark {
                position_id: position.position_id.clone(),
                token: position.token.clone(),
                amount_raw: position.qty_exact.as_ref().map(|q| q.raw().to_string()),
                decimals: position.qty_exact.as_ref().map(|q| q.decimals()),
                token_account: reason.is_none().then(|| accounts[0].token_account.clone()),
                binding_reason: reason.map(str::to_owned),
                quoted_value_sol: quote_value(quote),
                unknown_reason: unknown,
            }
        })
        .collect();
    reasons.sort();
    reasons.dedup();
    reasons.truncate(16);
    let values: Vec<_> = marks.iter().map(|p| p.quoted_value_sol).collect();
    BotRemainderMark {
        positions_loaded: loaded,
        open_cost_sol: loaded.then(|| positions.iter().map(|p| p.cost_sol).sum()),
        coverage: QuoteCoverage::from_values(
            "full_exact_bot_remainder_gross_independent_quotes",
            loaded && inventory_complete,
            &values,
            reasons,
        ),
        positions: marks,
    }
}
