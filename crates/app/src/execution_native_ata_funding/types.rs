use crate::execution_native_rpc::rent_types::ClassicAtaFundingFacts;
use crate::execution_native_setup::types::{AssociatedInitialState, NativeSetupInterpretation};
use crate::execution_solana_tx::PubkeyBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtaFundingIssue {
    InitialState(AssociatedInitialState),
    PriorAccountWrite,
    UnsupportedToken2022State,
    MissingToken2022Rent,
    PriorOpaqueInstruction,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtaFundingAmount {
    Known(u64),
    Unresolved(AtaFundingIssue),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExplicitAtaCoverage {
    Complete,
    Partial,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AtaFundingRow {
    pub(crate) requirement_index: usize,
    pub(crate) payer: PubkeyBytes,
    pub(crate) account: PubkeyBytes,
    pub(crate) amount: AtaFundingAmount,
}
/// Conditional explicit ATA funding; Token2022 requires the opt-in collector/planner. Never a route/BUY approval.
/// Fee, transfers, WSOL, CPI, refunds and the nine unavailable budget fields stay separate.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ClassicAtaFundingPlan<'a> {
    pub(crate) facts: &'a ClassicAtaFundingFacts,
    /// Classic interpretation only. Supported Token2022 funding reparses bound raw rows.
    pub(crate) setup: NativeSetupInterpretation<'a>,
    pub(crate) rows: Vec<AtaFundingRow>,
    /// Sum of known rows paid by expected_wallet, not a full debit when coverage is partial.
    pub(crate) known_wallet_payer_lamports: u128,
    pub(crate) known_wallet_token2022_payer_lamports: u128,
    /// Coverage of ALL explicit ATA rows, including foreign payers. Not whole-route coverage.
    pub(crate) explicit_ata_coverage: ExplicitAtaCoverage,
}
