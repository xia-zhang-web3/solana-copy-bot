use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Lamports(u64);

impl Lamports {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SignedLamports(i128);

impl SignedLamports {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: i128) -> Self {
        Self(value)
    }

    pub const fn as_i128(self) -> i128 {
        self.0
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }

    pub fn checked_abs_lamports(self) -> Option<Lamports> {
        self.0.checked_abs()?.try_into().ok().map(Lamports)
    }
}

impl From<Lamports> for SignedLamports {
    fn from(value: Lamports) -> Self {
        Self(i128::from(value.as_u64()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenQuantity {
    raw: u64,
    decimals: u8,
}

impl TokenQuantity {
    pub const fn new(raw: u64, decimals: u8) -> Self {
        Self { raw, decimals }
    }

    pub const fn raw(self) -> u64 {
        self.raw
    }

    pub const fn decimals(self) -> u8 {
        self.decimals
    }

    pub fn as_f64(self) -> f64 {
        self.raw as f64 / 10f64.powi(i32::from(self.decimals))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactAmountParseError {
    field: &'static str,
    raw: String,
}

impl ExactAmountParseError {
    fn new(field: &'static str, raw: String) -> Self {
        Self { field, raw }
    }
}

impl fmt::Display for ExactAmountParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid exact raw amount for {}: {:?}",
            self.field, self.raw
        )
    }
}

impl Error for ExactAmountParseError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExactSwapAmounts {
    pub amount_in_raw: String,
    pub amount_in_decimals: u8,
    pub amount_out_raw: String,
    pub amount_out_decimals: u8,
}

impl ExactSwapAmounts {
    pub fn amount_in_quantity(&self) -> Result<TokenQuantity, ExactAmountParseError> {
        parse_token_quantity(
            "amount_in_raw",
            &self.amount_in_raw,
            self.amount_in_decimals,
        )
    }

    pub fn amount_out_quantity(&self) -> Result<TokenQuantity, ExactAmountParseError> {
        parse_token_quantity(
            "amount_out_raw",
            &self.amount_out_raw,
            self.amount_out_decimals,
        )
    }
}

fn parse_token_quantity(
    field: &'static str,
    raw: &str,
    decimals: u8,
) -> Result<TokenQuantity, ExactAmountParseError> {
    raw.parse::<u64>()
        .map(|value| TokenQuantity::new(value, decimals))
        .map_err(|_| ExactAmountParseError::new(field, raw.to_string()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub wallet: String,
    pub dex: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub signature: String,
    pub slot: u64,
    pub ts_utc: DateTime<Utc>,
    #[serde(default)]
    pub exact_amounts: Option<ExactSwapAmounts>,
}

#[derive(Debug, Clone)]
pub struct WalletMetricRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
    pub pnl: f64,
    pub win_rate: f64,
    pub trades: u32,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub score: f64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct WalletUpsertRow {
    pub wallet_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CopySignalRow {
    pub signal_id: String,
    pub wallet_id: String,
    pub side: String,
    pub token: String,
    pub notional_sol: f64,
    pub notional_lamports: Option<Lamports>,
    pub notional_origin: String,
    pub ts: DateTime<Utc>,
    pub status: String,
}

pub const COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS: &str = "leader_exact_lamports";
pub const COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE: &str = "leader_approximate";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenQualityCacheRow {
    pub mint: String,
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenQualityRpcRow {
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
}

#[cfg(test)]
mod tests;
