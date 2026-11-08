use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const INVENTORY_CONTRACT_VERSION: u8 = 2;
pub(crate) const UNKNOWN_TOKEN_2022_VALUE: &str = "live_portfolio_token_2022_valuation_unknown";

/// Complete account parsing is distinct from supported observed-price valuation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2LiveInventoryEvidence {
    pub contract_version: u8,
    pub classic_accounts: usize,
    pub token_2022_accounts: usize,
    pub sol_slot: u64,
    pub classic_slot: u64,
    pub token_2022_slot: u64,
    pub token_2022_positive_positions: u32,
    pub unvalued_token_positions: u32,
    pub known_classic_value_sol: f64,
    #[serde(default)]
    pub known_token_2022_value_sol: f64,
    #[serde(default)]
    pub known_supported_token_value_sol: f64,
    #[serde(default)]
    pub valued_token_2022_positions: u32,
    #[serde(default)]
    pub valued_supported_positions: u32,
    pub valuation_basis: DiscoveryV2LiveValuationBasis,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryV2LiveValuationBasis {
    ClassicObservedPriceQualitySubtotal,
    SupportedObservedPriceQualitySubtotal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TokenProgram {
    Classic,
    Token2022,
}

impl TokenProgram {
    pub(crate) fn id(self) -> &'static str {
        match self {
            Self::Classic => "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            Self::Token2022 => "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        }
    }

    pub(crate) fn parsed_name(self) -> &'static str {
        match self {
            Self::Classic => "spl-token",
            Self::Token2022 => "spl-token-2022",
        }
    }
}

/// Only bounded categories may escape the RPC boundary; no URL or payload errors.
#[derive(Debug, Clone, Copy)]
pub(crate) enum InventoryFailure {
    Transport,
    Protocol,
    Malformed,
    Unsupported,
    Conflict,
    Budget,
}

impl InventoryFailure {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Transport => "live_portfolio_rpc_unavailable",
            Self::Protocol => "live_portfolio_rpc_invalid_response",
            Self::Malformed => "live_portfolio_inventory_malformed",
            Self::Unsupported => "live_portfolio_inventory_unsupported",
            Self::Conflict => "live_portfolio_inventory_conflict",
            Self::Budget => "live_portfolio_account_budget_exceeded",
        }
    }
}

impl fmt::Display for InventoryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason())
    }
}

impl std::error::Error for InventoryFailure {}

#[derive(Debug, Clone)]
pub(crate) struct LiveTokenPosition {
    pub mint: String,
    pub amount: f64,
    pub program: TokenProgram,
    pub token_2022_accounts: Vec<Token2022InventoryAccount>,
    pub token_2022_profile: Option<crate::token2022_profile::Token2022PositionProfile>,
    pub token_2022_collection_failure: Option<Token2022ProfileCollectionFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Token2022ProfileCollectionFailure {
    Unavailable,
    Malformed,
    Missing,
    SlotBehind,
    Budget,
}

#[derive(Debug, Clone)]
pub(crate) struct Token2022InventoryAccount {
    pub pubkey: String,
    pub raw_amount: u64,
    pub decimals: u8,
    pub state: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LivePortfolioSnapshot {
    pub sol_balance: f64,
    pub token_positions: Vec<LiveTokenPosition>,
    pub classic_accounts: usize,
    pub token_2022_accounts: usize,
    pub sol_slot: u64,
    pub classic_slot: u64,
    pub token_2022_slot: u64,
}
