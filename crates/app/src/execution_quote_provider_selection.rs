use crate::execution_quote_canary_helpers::QUOTE_STATUS_OK;
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::{Context, Result};
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryProviderSampleInsert, SqliteStore,
    PROVIDER_GENERIC_METIS, PROVIDER_GENERIC_PUBLIC, PROVIDER_PUMP_FUN_PAID,
};
use serde_json::Value;

pub(crate) const QUOTE_SOURCE_EVENT: &str = "execution_quote_canary_event";
pub(crate) const QUOTE_SOURCE_GENERIC_METIS: &str = "execution_quote_canary_provider:generic_metis";
pub(crate) const QUOTE_SOURCE_GENERIC_PUBLIC: &str =
    "execution_quote_canary_provider:generic_public";
pub(crate) const QUOTE_SOURCE_PUMP_FUN_PAID: &str = "execution_quote_canary_provider:pump_fun_paid";

pub(crate) fn selected_execution_build_plan_metadata(
    store: &SqliteStore,
    event: ExecutionQuoteCanaryEventInsert,
) -> Result<ExecutionBuildPlanMetadata> {
    let event_id = event.event_id.clone();
    let generic =
        store.load_execution_quote_canary_provider_sample(&event_id, PROVIDER_GENERIC_METIS)?;
    let public =
        store.load_execution_quote_canary_provider_sample(&event_id, PROVIDER_GENERIC_PUBLIC)?;
    let pump =
        store.load_execution_quote_canary_provider_sample(&event_id, PROVIDER_PUMP_FUN_PAID)?;

    if let Some(sample) = pump.filter(pump_fun_bonding_curve_quote_is_usable) {
        return Ok(metadata_from_provider_sample(
            &event,
            sample,
            QUOTE_SOURCE_PUMP_FUN_PAID,
        ));
    }
    if let Some((source, sample)) = best_generic_provider_sample(&generic, &public) {
        return Ok(metadata_from_provider_sample(
            &event,
            sample.clone(),
            source,
        ));
    }
    if event.quote_status == QUOTE_STATUS_OK {
        return Ok(metadata_from_quote_event(
            event,
            Some(QUOTE_SOURCE_GENERIC_METIS),
        ));
    }
    if let Some(sample) = public.filter(provider_quote_is_buildable_without_fee_account) {
        return Ok(metadata_from_provider_sample(
            &event,
            sample,
            QUOTE_SOURCE_GENERIC_PUBLIC,
        ));
    }
    Ok(metadata_from_quote_event(event, Some(QUOTE_SOURCE_EVENT)))
}

fn provider_quote_is_ok(sample: &ExecutionQuoteCanaryProviderSampleInsert) -> bool {
    sample.quote_status == QUOTE_STATUS_OK
}

fn provider_quote_is_buildable_without_fee_account(
    sample: &ExecutionQuoteCanaryProviderSampleInsert,
) -> bool {
    provider_quote_is_ok(sample)
        && !quote_response_requires_fee_account(sample.quote_response_json.as_deref())
}

fn best_generic_provider_sample<'a>(
    generic: &'a Option<ExecutionQuoteCanaryProviderSampleInsert>,
    public: &'a Option<ExecutionQuoteCanaryProviderSampleInsert>,
) -> Option<(&'static str, &'a ExecutionQuoteCanaryProviderSampleInsert)> {
    let mut best: Option<(&'static str, &ExecutionQuoteCanaryProviderSampleInsert)> = None;
    for (source, sample) in [
        (QUOTE_SOURCE_GENERIC_METIS, generic.as_ref()),
        (QUOTE_SOURCE_GENERIC_PUBLIC, public.as_ref()),
    ] {
        let Some(sample) = sample.filter(|sample| provider_quote_has_finite_slippage(sample))
        else {
            continue;
        };
        best = match best {
            Some((best_source, best_sample))
                if best_sample
                    .slippage_bps
                    .expect("filtered finite slippage")
                    .total_cmp(&sample.slippage_bps.expect("filtered finite slippage"))
                    .is_le() =>
            {
                Some((best_source, best_sample))
            }
            _ => Some((source, sample)),
        };
    }
    best
}

fn provider_quote_has_finite_slippage(sample: &ExecutionQuoteCanaryProviderSampleInsert) -> bool {
    provider_quote_is_buildable_without_fee_account(sample)
        && sample.slippage_bps.is_some_and(f64::is_finite)
}

fn pump_fun_bonding_curve_quote_is_usable(
    sample: &ExecutionQuoteCanaryProviderSampleInsert,
) -> bool {
    provider_quote_is_ok(sample)
        && sample
            .quote_response_json
            .as_deref()
            .and_then(|raw| pump_fun_is_completed(raw).ok())
            == Some(false)
}

fn pump_fun_is_completed(raw: &str) -> Result<bool> {
    let value: Value = serde_json::from_str(raw).context("invalid pump.fun quote response JSON")?;
    value
        .pointer("/quote/meta/isCompleted")
        .and_then(Value::as_bool)
        .context("missing pump.fun quote meta.isCompleted")
}

pub(crate) fn quote_response_requires_fee_account(raw: Option<&str>) -> bool {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return false;
    };
    value.get("platformFee").is_some_and(|fee| !fee.is_null())
}

fn metadata_from_provider_sample(
    event: &ExecutionQuoteCanaryEventInsert,
    sample: ExecutionQuoteCanaryProviderSampleInsert,
    source: &str,
) -> ExecutionBuildPlanMetadata {
    let mut metadata = metadata_from_quote_event(event.clone(), Some(source));
    metadata.quote_status = Some(sample.quote_status);
    metadata.quote_in_amount_raw = sample.quote_in_amount_raw;
    metadata.quote_out_amount_raw = sample.quote_out_amount_raw;
    metadata.quote_response_json = sample.quote_response_json;
    metadata.quote_price_sol = sample.quote_price_sol;
    metadata.price_impact_pct = sample.price_impact_pct;
    metadata.route_plan_json = sample.route_plan_json;
    metadata.slippage_bps = sample.slippage_bps;
    metadata.decision_status = sample.decision_status;
    metadata.decision_reason = sample.decision_reason;
    metadata
}

pub(crate) fn metadata_from_quote_event(
    event: ExecutionQuoteCanaryEventInsert,
    quote_source: Option<&str>,
) -> ExecutionBuildPlanMetadata {
    let priority_fee_source = if event.priority_fee_status.is_some()
        || event.priority_fee_lamports.is_some()
        || event.priority_fee_json.is_some()
    {
        Some(QUOTE_SOURCE_EVENT.to_string())
    } else {
        None
    };
    ExecutionBuildPlanMetadata {
        quote_source: quote_source
            .or(Some(QUOTE_SOURCE_EVENT))
            .map(ToString::to_string),
        quote_event_id: Some(event.event_id),
        quote_status: Some(event.quote_status),
        quote_in_amount_raw: event.quote_in_amount_raw,
        quote_out_amount_raw: event.quote_out_amount_raw,
        quote_response_json: event.quote_response_json,
        quote_price_sol: event.quote_price_sol,
        price_impact_pct: event.price_impact_pct,
        route_plan_json: event.route_plan_json,
        priority_fee_source,
        priority_fee_status: event.priority_fee_status,
        priority_fee_lamports: event.priority_fee_lamports,
        priority_fee_json: event.priority_fee_json,
        slippage_bps: event.slippage_bps,
        decision_status: event.decision_status,
        decision_reason: event.decision_reason,
    }
}
