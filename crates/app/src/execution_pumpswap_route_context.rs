use crate::execution_pumpswap_accounts::parse_pubkey;
use crate::execution_route_plan::route_plan_has_pump_fun_amm;
use crate::execution_solana_tx::PubkeyBytes;
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use anyhow::{anyhow, Result};
use serde_json::Value;

pub(crate) fn plan_has_pumpswap_direct_route(plan: &ExecutionTransactionPlan) -> bool {
    if !(plan.side.eq_ignore_ascii_case("buy") || plan.side.eq_ignore_ascii_case("sell")) {
        return false;
    }
    route_plan_has_pump_fun_amm(plan.metadata.route_plan_json.as_deref())
        || (plan.side.eq_ignore_ascii_case("sell")
            && route_plan_has_pump_fun_amm(plan.entry_route_plan_json.as_deref()))
}

pub(crate) fn extract_pumpswap_amm_key(plan: &ExecutionTransactionPlan) -> Result<PubkeyBytes> {
    for raw in [
        plan.metadata.route_plan_json.as_deref(),
        plan.metadata.quote_response_json.as_deref(),
        plan.entry_route_plan_json.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(key) = extract_amm_key_from_json(raw)? {
            return parse_pubkey(&key, "PumpSwap ammKey");
        }
    }
    anyhow::bail!("PumpSwap direct builder missing route ammKey")
}

fn extract_amm_key_from_json(raw: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|error| anyhow!("invalid PumpSwap route JSON: {error}"))?;
    let route_plan = value
        .as_array()
        .or_else(|| value.get("routePlan").and_then(Value::as_array));
    let Some(items) = route_plan else {
        return Ok(None);
    };
    Ok(items.iter().find_map(|item| {
        let swap = item.get("swapInfo")?;
        let label = swap.get("label").and_then(Value::as_str)?;
        if !label.eq_ignore_ascii_case("Pump.fun Amm") {
            return None;
        }
        swap.get("ammKey")
            .and_then(Value::as_str)
            .map(str::to_string)
    }))
}
