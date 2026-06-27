use crate::execution_quote_canary_helpers::{
    price_sol_per_token, quote_canary_slippage_limit_bps, raw_amount_to_ui,
    ui_amount_to_raw_string, SIDE_SELL, SOL_MINT,
};
use crate::execution_quote_canary_rpc::resolve_spl_token_decimals;
use crate::execution_quote_http::fetch_quote_sample;
use crate::quote_price_sanity::{
    quote_value_to_cost_ratio, raw_amount_mismatch_error, QUOTE_RATIO_MAX,
};
use copybot_config::ExecutionConfig;
use copybot_storage_core::ShadowLotRow;

#[derive(Debug, Clone)]
pub(crate) struct StaleCloseQuotePricer {
    config: ExecutionConfig,
    http: reqwest::Client,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StaleCloseQuoteAttempt {
    Disabled,
    MissingAmount,
    InvalidQuote { reason: String },
    Outlier { reason: String },
    Error { error: String },
    Priced(StaleCloseQuotePrice),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StaleCloseQuotePrice {
    pub(crate) exit_price_sol: f64,
    pub(crate) quote_in_amount_raw: String,
    pub(crate) quote_out_amount_raw: String,
    pub(crate) quote_in_tokens: f64,
    pub(crate) quote_out_sol: f64,
    pub(crate) quote_latency_ms: u64,
    pub(crate) price_impact_pct: Option<f64>,
}

impl StaleCloseQuotePricer {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    pub(crate) async fn quote_exit_price(&self, lot: &ShadowLotRow) -> StaleCloseQuoteAttempt {
        if !self.config.quote_canary_enabled {
            return StaleCloseQuoteAttempt::Disabled;
        }

        let Some((amount_raw, decimals)) = self.quote_amount(lot).await else {
            return StaleCloseQuoteAttempt::MissingAmount;
        };
        if let Some(reason) =
            raw_amount_mismatch_error(&amount_raw, Some(decimals), lot.qty, "stale quote")
        {
            return StaleCloseQuoteAttempt::Outlier { reason };
        }
        let slippage_bps = quote_canary_slippage_limit_bps(&self.config, SIDE_SELL);
        let quote = match fetch_quote_sample(
            &self.http,
            &self.config,
            &lot.token,
            SOL_MINT,
            &amount_raw,
            slippage_bps,
        )
        .await
        {
            Ok(quote) => quote,
            Err(error) => {
                return StaleCloseQuoteAttempt::Error {
                    error: crate::execution_quote_canary_helpers::short_error(&error),
                };
            }
        };

        let quote_in_tokens = raw_amount_to_ui(Some(&quote.in_amount), decimals);
        let quote_out_sol = raw_amount_to_ui(Some(&quote.out_amount), 9);
        let exit_price_sol = quote_out_sol
            .and_then(|out| quote_in_tokens.and_then(|input| price_sol_per_token(out, input)));
        let Some(exit_price_sol) = exit_price_sol.filter(|value| value.is_finite() && *value > 0.0)
        else {
            return StaleCloseQuoteAttempt::InvalidQuote {
                reason: "missing_positive_exit_price".to_string(),
            };
        };
        if quote.in_amount != amount_raw {
            return StaleCloseQuoteAttempt::Outlier {
                reason: format!(
                    "quote_input_amount_mismatch expected={amount_raw} actual={}",
                    quote.in_amount
                ),
            };
        }
        let Some(value_ratio) =
            quote_value_to_cost_ratio(quote_out_sol.unwrap_or(0.0), lot.cost_sol)
        else {
            return StaleCloseQuoteAttempt::InvalidQuote {
                reason: "missing_quote_value_to_cost_ratio".to_string(),
            };
        };
        if value_ratio > QUOTE_RATIO_MAX {
            return StaleCloseQuoteAttempt::Outlier {
                reason: format!("quote_value_to_cost_ratio_out_of_bounds ratio={value_ratio:.6}"),
            };
        }
        StaleCloseQuoteAttempt::Priced(StaleCloseQuotePrice {
            exit_price_sol,
            quote_in_amount_raw: quote.in_amount,
            quote_out_amount_raw: quote.out_amount,
            quote_in_tokens: quote_in_tokens.unwrap_or(0.0),
            quote_out_sol: quote_out_sol.unwrap_or(0.0),
            quote_latency_ms: quote.latency_ms,
            price_impact_pct: quote.price_impact_pct,
        })
    }

    async fn quote_amount(&self, lot: &ShadowLotRow) -> Option<(String, u8)> {
        if let Some(qty_exact) = lot.qty_exact {
            return Some((qty_exact.raw().to_string(), qty_exact.decimals()));
        }
        let decimals =
            resolve_spl_token_decimals(&self.http, &self.config, &lot.token, None).await?;
        ui_amount_to_raw_string(lot.qty, decimals).map(|raw| (raw, decimals))
    }
}

impl StaleCloseQuoteAttempt {
    pub(crate) fn unavailable_reason_code(&self) -> &'static str {
        match self {
            StaleCloseQuoteAttempt::Disabled => "quote_disabled",
            StaleCloseQuoteAttempt::MissingAmount => "quote_missing_amount",
            StaleCloseQuoteAttempt::InvalidQuote { .. } => "quote_invalid_response",
            StaleCloseQuoteAttempt::Outlier { .. } => "quote_outlier",
            StaleCloseQuoteAttempt::Error { .. } => "quote_error",
            StaleCloseQuoteAttempt::Priced(_) => "quote_priced",
        }
    }

    pub(crate) fn unavailable_error(&self) -> Option<&str> {
        match self {
            StaleCloseQuoteAttempt::InvalidQuote { reason } => Some(reason),
            StaleCloseQuoteAttempt::Outlier { reason } => Some(reason),
            StaleCloseQuoteAttempt::Error { error } => Some(error),
            _ => None,
        }
    }
}
