use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::{anyhow, Result};
use serde_json::Value;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const REQUEST_KIND_SWAP_INSTRUCTIONS: &str = "jupiter_swap_instructions_blueprint";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionSwapBlueprint {
    pub(crate) request_kind: String,
    pub(crate) quote_event_id: String,
    pub(crate) wallet_pubkey: Option<String>,
    pub(crate) input_mint: String,
    pub(crate) output_mint: String,
    pub(crate) input_amount_raw: String,
    pub(crate) output_amount_raw: String,
    pub(crate) slippage_bps: f64,
    pub(crate) priority_fee_lamports: u64,
    pub(crate) route_labels: Vec<String>,
}

pub(crate) fn build_execution_swap_blueprint(
    request: &ExecutionSubmitRequest,
) -> Result<ExecutionSwapBlueprint> {
    let metadata = &request.metadata;
    let quote_event_id =
        required_metadata_string(metadata.quote_event_id.as_deref(), "quote_event_id")?;
    let input_amount_raw = required_positive_raw_amount(
        metadata.quote_in_amount_raw.as_deref(),
        "quote_in_amount_raw",
    )?;
    let output_amount_raw = required_positive_raw_amount(
        metadata.quote_out_amount_raw.as_deref(),
        "quote_out_amount_raw",
    )?;
    if request.slippage_tolerance_bps > 5_000 {
        return Err(anyhow!("invalid slippage_tolerance_bps for swap blueprint"));
    }
    let slippage_bps = request.slippage_tolerance_bps as f64;
    let priority_fee_lamports = metadata
        .priority_fee_lamports
        .ok_or_else(|| anyhow!("missing priority_fee_lamports for swap blueprint"))?;
    let route_plan_json =
        required_metadata_string(metadata.route_plan_json.as_deref(), "route_plan_json")?;
    let route_labels = extract_route_labels(&route_plan_json)?;
    let token = required_metadata_string(Some(request.token.as_str()), "token")?;
    let (input_mint, output_mint) = match request.side.to_ascii_lowercase().as_str() {
        "buy" => (WSOL_MINT.to_string(), token),
        "sell" => (token, WSOL_MINT.to_string()),
        side => return Err(anyhow!("unsupported swap blueprint side {side}")),
    };
    let wallet_pubkey = non_empty_string(request.wallet_pubkey.as_str())
        .or_else(|| non_empty_string(&request.wallet_id));

    Ok(ExecutionSwapBlueprint {
        request_kind: REQUEST_KIND_SWAP_INSTRUCTIONS.to_string(),
        quote_event_id,
        wallet_pubkey,
        input_mint,
        output_mint,
        input_amount_raw,
        output_amount_raw,
        slippage_bps,
        priority_fee_lamports,
        route_labels,
    })
}

pub(crate) fn validate_execution_swap_blueprint_for_simulation(
    blueprint: &ExecutionSwapBlueprint,
) -> Result<()> {
    required_metadata_string(Some(blueprint.quote_event_id.as_str()), "quote_event_id")?;
    required_metadata_string(Some(blueprint.input_mint.as_str()), "input_mint")?;
    required_metadata_string(Some(blueprint.output_mint.as_str()), "output_mint")?;
    required_positive_raw_amount(
        Some(blueprint.input_amount_raw.as_str()),
        "input_amount_raw",
    )?;
    required_positive_raw_amount(
        Some(blueprint.output_amount_raw.as_str()),
        "output_amount_raw",
    )?;
    if !blueprint.slippage_bps.is_finite()
        || blueprint.slippage_bps < 0.0
        || blueprint.slippage_bps > 5_000.0
    {
        return Err(anyhow!(
            "invalid slippage_bps for swap simulation blueprint"
        ));
    }
    if blueprint.route_labels.is_empty() {
        return Err(anyhow!(
            "missing route labels for swap simulation blueprint"
        ));
    }
    Ok(())
}

fn extract_route_labels(route_plan_json: &str) -> Result<Vec<String>> {
    let value: Value = serde_json::from_str(route_plan_json)
        .map_err(|error| anyhow!("invalid route_plan_json for swap blueprint: {error}"))?;
    let Some(items) = value.as_array() else {
        return Err(anyhow!(
            "route_plan_json must be an array for swap blueprint"
        ));
    };
    let labels: Vec<String> = items
        .iter()
        .filter_map(route_label_from_value)
        .filter(|label| !label.trim().is_empty())
        .collect();
    if labels.is_empty() {
        return Err(anyhow!(
            "route_plan_json has no route labels for swap blueprint"
        ));
    }
    Ok(labels)
}

fn route_label_from_value(value: &Value) -> Option<String> {
    value
        .get("swapInfo")
        .and_then(|swap| swap.get("label"))
        .or_else(|| value.get("label"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn required_metadata_string(value: Option<&str>, field: &str) -> Result<String> {
    non_empty_string(value.unwrap_or_default())
        .ok_or_else(|| anyhow!("missing {field} for swap blueprint"))
}

fn required_positive_raw_amount(value: Option<&str>, field: &str) -> Result<String> {
    let raw = required_metadata_string(value, field)?;
    let amount = raw
        .parse::<u128>()
        .map_err(|error| anyhow!("invalid {field} for swap blueprint: {error}"))?;
    if amount == 0 {
        return Err(anyhow!("{field} must be positive for swap blueprint"));
    }
    Ok(raw)
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
