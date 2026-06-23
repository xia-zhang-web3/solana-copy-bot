use crate::execution_quote_canary_helpers::{
    apply_quote_sample_to_event, duration_ms_between, price_sol_per_token,
    quote_canary_slippage_limit_bps, quote_slippage_bps_for_sell, raw_amount_to_ui, short_error,
    ui_amount_to_raw_string, QUOTE_STATUS_ERROR, QUOTE_STATUS_OK, SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionCanaryCloseCandidate, ExecutionQuoteCanaryEventInsert,
    ExecutionQuoteCanaryRecordOutcome, SqliteStore,
};

const MARKET_EXIT_SHADOW_QUOTE_EVENT_PREFIX: &str = "quote:market-exit-shadow-diag:";

#[derive(Debug, Clone)]
pub(crate) struct MarketExitShadowQuoteDiagnostic {
    config: ExecutionConfig,
    http: reqwest::Client,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct MarketExitShadowQuoteSummary {
    pub(crate) checked: u64,
    pub(crate) inserted: u64,
    pub(crate) existing: u64,
    pub(crate) quote_ok: u64,
    pub(crate) quote_error: u64,
    pub(crate) last_event_id: Option<String>,
}

impl MarketExitShadowQuoteDiagnostic {
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
    ) -> Result<MarketExitShadowQuoteSummary> {
        if !self.config.market_exit_shadow_quote_enabled {
            return Ok(MarketExitShadowQuoteSummary::default());
        }
        let max_age_seconds = self
            .config
            .market_exit_shadow_quote_max_close_age_seconds
            .max(1);
        let since = now - Duration::seconds(max_age_seconds.min(i64::MAX as u64) as i64);
        let candidates = store
            .list_market_exit_shadow_quote_candidates(
                since,
                MARKET_EXIT_SHADOW_QUOTE_EVENT_PREFIX,
                self.config.market_exit_shadow_quote_batch_limit.max(1),
            )
            .context("failed loading market exit shadow quote candidates")?;
        let mut summary = MarketExitShadowQuoteSummary::default();
        for close in candidates {
            summary.checked += 1;
            let event_id = market_exit_shadow_quote_event_id(close.id);
            let event = self.quote_close_event(close, event_id.clone(), now).await;
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

    async fn quote_close_event(
        &self,
        close: ExecutionCanaryCloseCandidate,
        event_id: String,
        now: DateTime<Utc>,
    ) -> ExecutionQuoteCanaryEventInsert {
        let mut event = base_event(&close, event_id, now);
        match self.quote_close(&close).await {
            Ok(quote) => apply_quote_to_close_event(&close, &mut event, quote),
            Err(error) => {
                event.quote_status = QUOTE_STATUS_ERROR.to_string();
                event.error = Some(short_error(&error));
            }
        }
        event
    }

    async fn quote_close(&self, close: &ExecutionCanaryCloseCandidate) -> Result<CloseQuote> {
        let (amount_raw, token_decimals) = quote_amount(&self.http, &self.config, close).await?;
        let sample = fetch_quote_sample(
            &self.http,
            &self.config,
            &close.token,
            SOL_MINT,
            &amount_raw,
            quote_canary_slippage_limit_bps(&self.config, SIDE_SELL),
        )
        .await?;
        Ok(CloseQuote {
            sample,
            token_decimals,
        })
    }
}

impl MarketExitShadowQuoteSummary {
    pub(crate) fn has_activity(&self) -> bool {
        self.checked > 0 || self.inserted > 0 || self.existing > 0
    }
}

pub(crate) fn market_exit_shadow_quote_event_id(close_id: i64) -> String {
    format!("{MARKET_EXIT_SHADOW_QUOTE_EVENT_PREFIX}{close_id}")
}

fn base_event(
    close: &ExecutionCanaryCloseCandidate,
    event_id: String,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id,
        signal_id: None,
        shadow_closed_trade_id: None,
        wallet_id: close.wallet_id.clone(),
        token: close.token.clone(),
        side: SIDE_SELL.to_string(),
        quote_status: QUOTE_STATUS_ERROR.to_string(),
        request_ts: now,
        signal_ts: Some(close.closed_ts),
        decision_delay_ms: duration_ms_between(close.closed_ts, now),
        quote_latency_ms: None,
        leader_notional_sol: Some(close.exit_value_sol),
        quote_in_amount_raw: close.qty_raw.clone(),
        quote_out_amount_raw: None,
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: price_sol_per_token(close.exit_value_sol, close.qty),
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

struct CloseQuote {
    sample: crate::execution_quote_canary_helpers::QuoteSample,
    token_decimals: u8,
}

fn apply_quote_to_close_event(
    close: &ExecutionCanaryCloseCandidate,
    event: &mut ExecutionQuoteCanaryEventInsert,
    quote: CloseQuote,
) {
    apply_quote_sample_to_event(event, quote.sample);
    event.shadow_price_sol = price_sol_per_token(close.exit_value_sol, close.qty);
    let token_qty = raw_amount_to_ui(event.quote_in_amount_raw.as_deref(), quote.token_decimals);
    let out_sol = raw_amount_to_ui(event.quote_out_amount_raw.as_deref(), 9);
    event.quote_price_sol =
        out_sol.and_then(|sol| token_qty.and_then(|qty| price_sol_per_token(sol, qty)));
    event.slippage_bps = quote_slippage_bps_for_sell(event.quote_price_sol, event.shadow_price_sol);
    event.leader_notional_sol = Some(close.exit_value_sol);
}

async fn quote_amount(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    close: &ExecutionCanaryCloseCandidate,
) -> Result<(String, u8)> {
    if let (Some(raw), Some(decimals)) = (close.qty_raw.as_ref(), close.qty_decimals) {
        return Ok((raw.clone(), decimals));
    }
    let decimals = resolve_spl_token_decimals(http, config, &close.token, close.qty_decimals)
        .await
        .ok_or_else(|| anyhow!("missing token decimals for market exit shadow quote"))?;
    let raw = ui_amount_to_raw_string(close.qty, decimals)
        .context("failed to convert market close quantity for quote")?;
    Ok((raw, decimals))
}
