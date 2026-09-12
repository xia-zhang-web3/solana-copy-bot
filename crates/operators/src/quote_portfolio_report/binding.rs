use super::decimals;
use super::input::{raw, QuoteRef, Side};
use crate::quote_portfolio as k;
use anyhow::{ensure, Context, Result};
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};

const SOL: &str = "So11111111111111111111111111111111111111112";

pub fn mint(value: &str) -> Result<k::Mint> {
    let bytes = bs58::decode(value).into_vec().context("invalid mint")?;
    let result: k::Mint = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("mint must be 32 bytes"))?;
    ensure!(
        bs58::encode(result).into_string() == value,
        "noncanonical mint"
    );
    Ok(result)
}
pub fn resolve(
    store: &SqliteStore,
    q: &QuoteRef,
    transition_unix_ms: u64,
) -> Result<(k::ExactQuote, Value)> {
    q.provenance.validate()?;
    ensure!(
        !q.event_id.is_empty() && !q.wallet_id.is_empty(),
        "missing quote identity"
    );
    let row = store
        .load_execution_quote_canary_event_by_id(&q.event_id)
        .context("read-only quote lookup/schema unavailable")?
        .context("source quote missing")?;
    ensure!(
        row.event_id == q.event_id && row.quote_status == "ok",
        "quote identity/status mismatch"
    );
    let side = match q.side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    };
    ensure!(
        row.side == side && row.token == q.mint,
        "quote side/mint mismatch"
    );
    ensure!(
        row.wallet_id == q.wallet_id
            && row.signal_id == q.signal_id
            && row.shadow_closed_trade_id.map(|v| v.to_string()) == q.shadow_closed_trade_id,
        "quote wallet/signal/closed-trade identity mismatch"
    );
    let ts =
        chrono::DateTime::parse_from_rfc3339(&q.request_ts).context("invalid quote request_ts")?;
    ensure!(row.request_ts == ts, "quote request time mismatch");
    let time_evidence = super::time_binding::check(
        row.http_request_started_ts,
        row.quote_response_available_ts,
        transition_unix_ms,
    )?;
    let input = raw(&q.input_raw)?;
    let output = raw(&q.output_raw)?;
    ensure!(
        row.quote_in_amount_raw.as_deref().map(raw).transpose()? == Some(input)
            && row.quote_out_amount_raw.as_deref().map(raw).transpose()? == Some(output),
        "quote exact raw mismatch/missing"
    );
    let response_text = row
        .quote_response_json
        .as_deref()
        .context("quote response/decimals missing")?;
    ensure!(
        response_text.len() <= 65_536,
        "quote response exceeds binding limit"
    );
    // Typed deserialization also refuses duplicate binding fields.
    let response = decimals::parse(response_text)
        .context("quote response bindings/decimals missing or ambiguous")?;
    let (input_mint, output_mint) = match q.side {
        Side::Buy => (SOL, q.mint.as_str()),
        Side::Sell => (q.mint.as_str(), SOL),
    };
    ensure!(
        response.input_mint == input_mint
            && response.output_mint == output_mint
            && raw(&response.in_amount)? == input
            && raw(&response.out_amount)? == output,
        "quote response binding mismatch"
    );
    ensure!(
        q.mint != SOL && q.decimals <= 19,
        "invalid token mint/decimals"
    );
    let decimals_evidence = decimals::resolve(store, q, &response)?;
    let quote = k::ExactQuote {
        position_id: q.position_id.clone(),
        mint: mint(&q.mint)?,
        decimals: q.decimals,
        direction: super::convert::side(q.side),
        input,
        output: k::Knowledge::Known(output),
        provenance: super::convert::origin(&q.provenance),
    };
    Ok((
        quote,
        json!({
            "state":"verified_db_fields", "event_id":row.event_id,
            "wallet_id":row.wallet_id, "signal_id":row.signal_id,
            "shadow_closed_trade_id":row.shadow_closed_trade_id.map(|v| v.to_string()),
            "request_ts":row.request_ts.to_rfc3339(), "quote_status":row.quote_status,
            "mint":row.token, "side":row.side, "input_raw":row.quote_in_amount_raw,
            "output_raw":row.quote_out_amount_raw, "decimals":q.decimals.to_string(),
            "decimals_evidence":decimals_evidence, "time_binding":time_evidence,
            "response_text":response_text,
            "position_association":"caller_assertion_only",
            "costs":"caller_operands_only; sampled priority fee is not charged fee proof",
            "history":"row read for this invocation; retry rows may be updated; not append-only"
        }),
    ))
}
