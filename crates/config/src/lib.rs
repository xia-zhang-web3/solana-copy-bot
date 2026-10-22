#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

mod association_delivery;
mod http_recovery;
pub use association_delivery::{
    validate_association_delivery, validate_delivery_source, AssociationDeliveryConfig,
    DeliveryBudget,
};
pub use http_recovery::HttpRecoveryConfig;
mod discovery_v2_identity;
mod env_parsing;
mod loader;
mod native_fresh_buy;
mod owned_sell_preparation;
mod owner_exit;
mod owner_source_copy_gate;
mod owner_technical_buy;
mod risk_validation;
mod schema;
mod technical_cohort;
mod tiny_experiment;
pub use native_fresh_buy::{
    native_first_buy_activation, NativeFreshBuyConfig, PROCESSED_SLOT_FENCE_AVAILABILITY_V1,
};
pub use owned_sell_preparation::{
    owned_sell_dispatch, owned_sell_flags, validate_owned_sell_preparation,
    OwnedSellPreparationConfig, ProtectedBotUsdcBaselineConfig, PROTECTED_BOT_USDC_100_V1,
    RPC_FINALIZED_OWNED_SELL_V1,
};
pub use owner_exit::{
    validate_owner_exit, OwnerExitConfig, OWNER_EXIT_V1, OWNER_ROUNDTRIP_EXIT_V1,
};
pub use owner_source_copy_gate::{
    validate_owner_source_copy_gate, OwnerSourceCopyGateConfig, CONFIRMED_NATIVE_BOT_BUY_V1,
};
pub use owner_technical_buy::{
    owner_execution_roundtrip, validate_owner_technical_buy, OwnerTechnicalBuyConfig,
    OWNER_EXECUTION_ROUNDTRIP_V1, OWNER_TECHNICAL_BUY_V1,
};
pub use technical_cohort::{
    validate_technical_cohort, TechnicalCohortConfig, CLASSIC_SPL_MINT_V1, TECHNICAL_COHORT_V1,
};
pub use tiny_experiment::{TinyExperimentConfig, TinyPolicyMode};

pub use self::discovery_v2_identity::{
    discovery_v2_policy_fingerprint, DiscoveryV2PolicyFingerprintInput,
    DISCOVERY_V2_SCORING_SOURCE, DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_CLOSED_TRADES,
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MAX_HOLD_SECONDS,
    DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_FAST_LOSS_MIN_ENTRY_SOL, DISCOVERY_V2_SHADOW_FEEDBACK_MAX_PNL_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_MAX_ROI, DISCOVERY_V2_SHADOW_FEEDBACK_MIN_CLOSED_TRADES,
    DISCOVERY_V2_SHADOW_FEEDBACK_MIN_ENTRY_SOL, DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_STALE_COPY_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_STALE_COPY_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_WINDOW_HOURS, DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS,
    DISCOVERY_V2_TOKEN_ROLLING_MARKET_WINDOW_SECONDS,
};
pub use self::env_parsing::normalize_ingestion_source;
pub use self::loader::{load_from_env_or_default, load_from_path};
pub use self::schema::{
    AppConfig, DiscoveryConfig, ExecutionConfig, HistoryRetentionConfig, IngestionConfig,
    RecentRawJournalConfig, RiskConfig, RuntimeRestoreOpsConfig, ShadowConfig, SqliteConfig,
    SystemConfig,
};

#[cfg(test)]
mod tests;

mod controlled_receipt_lot;
pub use controlled_receipt_lot::{
    validate_controlled_receipt_lot, ControlledReceiptLotConfig, CONTROLLED_RECEIPT_LOT_V1,
    TRANSACTION_OWNED_WSOL_V1,
};
