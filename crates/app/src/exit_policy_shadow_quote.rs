use crate::execution_quote_canary_helpers::{
    apply_quote_sample_to_event, duration_ms_between, finalize_quote_decision, price_sol_per_token,
    quote_canary_slippage_limit_bps, quote_slippage_bps_for_sell, raw_amount_to_ui, short_error,
    ui_amount_to_raw_string, QUOTE_STATUS_ERROR, QUOTE_STATUS_OK, SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryRecordOutcome, ShadowLotRow, SqliteStore,
};

#[derive(Debug, Clone)]
pub(crate) struct ExitPolicyShadowQuoteDiagnostic {
    config: ExecutionConfig,
    http: reqwest::Client,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ExitPolicyShadowQuoteSummary {
    pub(crate) checked: u64,
    pub(crate) inserted: u64,
    pub(crate) existing: u64,
    pub(crate) quote_ok: u64,
    pub(crate) quote_error: u64,
    pub(crate) last_event_id: Option<String>,
}

impl ExitPolicyShadowQuoteDiagnostic {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub(crate) async fn process_tick(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<ExitPolicyShadowQuoteSummary> {
        if !self.config.exit_policy_shadow_quote_enabled {
            return Ok(ExitPolicyShadowQuoteSummary::default());
        }
        let hold_minutes = self.config.exit_policy_shadow_quote_hold_minutes.max(1);
        let cutoff = now - Duration::minutes(hold_minutes.min(i64::MAX as u64) as i64);
        let event_prefix = exit_policy_shadow_quote_event_prefix(hold_minutes);
        let candidates = store.list_exit_policy_shadow_quote_candidates(
            cutoff,
            &event_prefix,
            self.config.exit_policy_shadow_quote_batch_limit.max(1),
        )?;
        let mut summary = ExitPolicyShadowQuoteSummary::default();
        for lot in candidates {
            summary.checked += 1;
            let event_id = format!("{event_prefix}{}", lot.id);
            let event = self
                .quote_lot_event(lot, event_id.clone(), hold_minutes, now)
                .await;
            match store.record_execution_quote_canary_event(&event)? {
                ExecutionQuoteCanaryRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionQuoteCanaryRecordOutcome::Existing => summary.existing += 1,
            }
            if event.quote_status == QUOTE_STATUS_OK {
                summary.quote_ok += 1;
            } else {
                summary.quote_error += 1;
            }
            summary.last_event_id = Some(event_id);
        }
        Ok(summary)
    }

    async fn quote_lot_event(
        &self,
        lot: ShadowLotRow,
        event_id: String,
        hold_minutes: u64,
        now: DateTime<Utc>,
    ) -> ExecutionQuoteCanaryEventInsert {
        let trigger_ts =
            lot.opened_ts + Duration::minutes(hold_minutes.min(i64::MAX as u64) as i64);
        let mut event = base_event(&lot, event_id, trigger_ts, now);
        match self.quote_lot(&lot).await {
            Ok(quote) => apply_quote_to_lot_event(&lot, &mut event, quote),
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        finalize_quote_decision(
            &mut event,
            quote_canary_slippage_limit_bps(&self.config, SIDE_SELL),
        );
        event
    }

    async fn quote_lot(&self, lot: &ShadowLotRow) -> Result<LotQuote> {
        let (amount_raw, token_decimals) = quote_amount(&self.http, &self.config, lot).await?;
        let sample = fetch_quote_sample(
            &self.http,
            &self.config,
            &lot.token,
            SOL_MINT,
            &amount_raw,
            quote_canary_slippage_limit_bps(&self.config, SIDE_SELL),
        )
        .await?;
        Ok(LotQuote {
            sample,
            token_decimals,
        })
    }
}

impl ExitPolicyShadowQuoteSummary {
    pub(crate) fn has_activity(&self) -> bool {
        self.checked > 0 || self.inserted > 0 || self.existing > 0
    }
}

fn exit_policy_shadow_quote_event_prefix(hold_minutes: u64) -> String {
    format!("quote:exit-policy:backstop{hold_minutes}m:")
}

fn base_event(
    lot: &ShadowLotRow,
    event_id: String,
    trigger_ts: DateTime<Utc>,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id,
        signal_id: None,
        shadow_closed_trade_id: None,
        wallet_id: lot.wallet_id.clone(),
        token: lot.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_ERROR.to_string(),
        request_ts: now,
        signal_ts: Some(trigger_ts),
        decision_delay_ms: duration_ms_between(trigger_ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(lot.cost_sol),
        quote_in_amount_raw: lot.qty_exact.as_ref().map(|qty| qty.raw().to_string()),
        quote_out_amount_raw: None,
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: price_sol_per_token(lot.cost_sol, lot.qty),
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}

struct LotQuote {
    sample: crate::execution_quote_canary_helpers::QuoteSample,
    token_decimals: u8,
}

fn apply_quote_to_lot_event(
    lot: &ShadowLotRow,
    event: &mut ExecutionQuoteCanaryEventInsert,
    quote: LotQuote,
) {
    apply_quote_sample_to_event(event, quote.sample);
    event.shadow_price_sol = price_sol_per_token(lot.cost_sol, lot.qty);
    let token_qty = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), quote.token_decimals);
    let out_sol = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), 9);
    event.quote_price_sol =
        out_sol.and_then(|sol| token_qty.and_then(|qty| price_sol_per_token(sol, qty)));
    event.slippage_bps = quote_slippage_bps_for_sell(event.quote_price_sol, event.shadow_price_sol);
    event.leader_notional_sol = Some(lot.cost_sol);
}

async fn quote_amount(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    lot: &ShadowLotRow,
) -> Result<(String, u8)> {
    if let Some(qty_exact) = lot.qty_exact.as_ref() {
        return Ok((qty_exact.raw().to_string(), qty_exact.decimals()));
    }
    let decimals = resolve_spl_token_decimals(http, config, &lot.token, None)
        .await
        .ok_or_else(|| anyhow!("missing token decimals for exit policy shadow quote"))?;
    let raw = ui_amount_to_raw_string(lot.qty, decimals)
        .context("failed to convert shadow lot quantity for exit policy quote")?;
    Ok((raw, decimals))
}
