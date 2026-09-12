//! A quote is evidence for one local request, never a mint-wide price.
use crate::execution_canary_quote_pnl_wallet::{WalletSellQuoteProof, WalletTokenBalance};
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::Value;

pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalletQuoteRequest {
    pub owner: String,
    pub token_account: String,
    pub input_mint: String,
    pub amount_raw: String,
    pub decimals: u8,
    pub output_mint: String,
}
impl WalletQuoteRequest {
    pub fn for_balance(owner: &str, balance: &WalletTokenBalance) -> Self {
        Self {
            owner: owner.into(),
            token_account: balance.token_account.clone(),
            input_mint: balance.mint.clone(),
            amount_raw: balance.amount_raw.clone(),
            decimals: balance.decimals,
            output_mint: SOL_MINT.into(),
        }
    }
}
pub(crate) fn raw_amount(raw: &str) -> Option<u64> {
    (!raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()))
        .then(|| raw.parse().ok())
        .flatten()
}
pub(crate) fn quote_sell(
    client: &Client,
    base_url: &str,
    request: WalletQuoteRequest,
    slippage_bps: u32,
) -> WalletSellQuoteProof {
    let mut proof = WalletSellQuoteProof {
        request,
        status: "error".into(),
        http_status: None,
        error: None,
        error_code: None,
        out_amount_raw: None,
        out_sol: None,
        price_impact_pct: None,
        route_labels: Vec::new(),
    };
    if base_url.is_empty() {
        proof.status = "missing".into();
        proof.error = Some("missing_quote_base_url".into());
        return proof;
    }
    let response = client
        .get(format!("{}/quote", base_url.trim_end_matches('/')))
        .query(&[
            ("inputMint", proof.request.input_mint.clone()),
            ("outputMint", proof.request.output_mint.clone()),
            ("amount", proof.request.amount_raw.clone()),
            ("slippageBps", slippage_bps.to_string()),
        ])
        .send();
    let Ok(response) = response else {
        proof.error = Some("quote_request_failed".into());
        return proof;
    };
    let status = response.status();
    proof.http_status = Some(status.as_u16());
    let Ok(value) = response.json::<Value>() else {
        proof.error = Some("quote_invalid_json".into());
        return proof;
    };
    if !status.is_success() {
        proof.error_code = value["errorCode"]
            .as_str()
            .map(|s| s.chars().take(128).collect());
        proof.status = if proof.error_code.as_deref() == Some("NO_ROUTES_FOUND") {
            "no_route"
        } else {
            "error"
        }
        .into();
        proof.error = Some("quote_http_failure".into());
        return proof;
    }
    if value["inputMint"].as_str() != Some(proof.request.input_mint.as_str())
        || value["outputMint"].as_str() != Some(proof.request.output_mint.as_str())
        || value["inAmount"].as_str().and_then(raw_amount) != raw_amount(&proof.request.amount_raw)
    {
        proof.error = Some("quote_response_identity_mismatch".into());
        return proof;
    }
    let Some(amount) = value["outAmount"].as_str().and_then(raw_amount) else {
        proof.error = Some("quote_invalid_out_amount".into());
        return proof;
    };
    proof.status = "ok".into();
    proof.out_amount_raw = Some(amount.to_string());
    proof.out_sol = Some(amount as f64 / 1e9);
    proof.price_impact_pct = value["priceImpactPct"].as_str().map(str::to_owned);
    proof.route_labels = value["routePlan"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| r["swapInfo"]["label"].as_str())
        .take(16)
        .map(|s| s.chars().take(128).collect())
        .collect();
    proof
}
