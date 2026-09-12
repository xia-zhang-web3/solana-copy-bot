use super::{ExecutionConfirmedBuyFill, ExecutionConfirmedFill};
use anyhow::{anyhow, ensure, Result};
use copybot_core_types::{Lamports, TokenQuantity};
use copybot_storage_core::{ExecutionCanaryReceiptFacts, ExecutionCanaryReceiptProof};

pub(super) struct ActualConfirmedFill {
    pub fill: ExecutionConfirmedFill,
    pub net_lamports: Lamports,
}

/// BUY compatibility conversion after durable facts. SELL uses the signed storage
/// settlement path and cannot derive a swap price from wallet native cash.
pub(super) fn confirmed_fill_from_facts(
    facts: &ExecutionCanaryReceiptFacts,
    proof: &ExecutionCanaryReceiptProof,
) -> Result<ActualConfirmedFill> {
    ensure!(
        facts.side == "buy",
        "receipt_sell_requires_signed_settlement"
    );
    let token = facts.token_delta.ok_or_else(|| {
        anyhow!(facts
            .token_coverage_reason
            .clone()
            .unwrap_or_else(|| "receipt_token_coverage_unresolved".into()))
    })?;
    ensure!(token.raw > 0, "receipt_buy_token_delta_unsupported");
    let cash = facts.wallet_native_delta.as_i128();
    ensure!(cash < 0, "receipt_buy_sol_delta_unsupported");
    let raw = u64::try_from(token.raw).map_err(|_| anyhow!("receipt_token_delta_overflow"))?;
    let net = cash
        .checked_neg()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(|| anyhow!("receipt_sol_delta_overflow"))?;
    let qty_exact = TokenQuantity::new(raw, token.decimals);
    let qty = qty_exact.as_f64();
    ensure!(
        qty.is_finite() && qty > 0.0,
        "receipt_token_quantity_unsupported"
    );
    let fill_ts = facts
        .block_time
        .and_then(|t| chrono::DateTime::from_timestamp(t, 0))
        .unwrap_or(proof.confirmed_at);
    Ok(ActualConfirmedFill {
        fill: ExecutionConfirmedFill::Buy(ExecutionConfirmedBuyFill {
            order_id: facts.order_id.clone(),
            token: facts.token.clone(),
            qty,
            qty_exact: Some(qty_exact),
            cost_sol: net as f64 / 1e9,
            fill_ts,
        }),
        net_lamports: Lamports::new(net),
    })
}
