use crate::execution_priority_fee_wire::EncodedPriorityFee;
use crate::execution_solana_tx::PubkeyBytes;
use crate::execution_transaction_wire::{DecodedInstruction, MessageBinding};

/// Coverage of decoded operands only. Neither variant establishes a spending bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FundingCoverage {
    ExplicitOperandsOnly,
    PartialWithStateOrUnsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferRelation {
    WalletToOther,
    OtherToWallet,
    WalletToSelf,
    OtherToOther,
}

/// Each state request is bound to the full ordered instruction and its resolved roles.
/// These are encoded addresses, not verified owners, initialized accounts or refunds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FundingOperation {
    ComputeBudget,
    SystemTransfer {
        from: PubkeyBytes,
        to: PubkeyBytes,
        lamports: u64,
        relation: TransferRelation,
    },
    AssociatedTokenCreateIdempotent {
        payer: PubkeyBytes,
        associated_account: PubkeyBytes,
        owner: PubkeyBytes,
        mint: PubkeyBytes,
        token_program: PubkeyBytes,
        unresolved: AccountFactsRequired,
    },
    SyncNative {
        account: PubkeyBytes,
        unresolved: AccountFactsRequired,
    },
    CloseTokenAccount {
        account: PubkeyBytes,
        destination: PubkeyBytes,
        authority: PubkeyBytes,
        unresolved: AccountFactsRequired,
    },
    Unresolved {
        reason: UnsupportedFundingInstruction,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountFactsRequired {
    // ATA creation does not establish existence, address derivation, token program
    // account layout, payer balance, rent or the cost of an idempotent invocation.
    AssociatedAddressExistenceOwnerMintTokenProgramRentAndPayer,
    TokenProgramOwnerMintNativeReserveAndLamports,
    TokenProgramOwnerMintBalanceAuthorityAndDestination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsupportedFundingInstruction {
    SystemOpcode,
    AssociatedTokenOpcode,
    ClassicTokenOpcode,
    ProgramSemanticsAndCpiExpenses,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FundingRequirement {
    pub(crate) instruction: DecodedInstruction,
    pub(crate) operation: FundingOperation,
}

/// Offline operands cannot populate any of these. Kept explicitly unavailable for
/// the next exact-message RPC budget consumer; this is not a reserve-ready API.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UnavailableNativeBudget {
    pub(crate) base_fee_lamports: Option<u128>,
    pub(crate) full_message_fee_lamports: Option<u128>,
    pub(crate) rent_lamports: Option<u128>,
    pub(crate) refunds_lamports: Option<u128>,
    pub(crate) full_native_debit_lamports: Option<u128>,
    pub(crate) net_native_debit_lamports: Option<u128>,
    pub(crate) peak_native_funding_lamports: Option<u128>,
    pub(crate) available_sol_lamports: Option<u128>,
    pub(crate) pending_exposure_lamports: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeFundingRequirements {
    pub(crate) binding: MessageBinding,
    pub(crate) expected_wallet: PubkeyBytes,
    pub(crate) requirements: Vec<FundingRequirement>,
    /// Sum of NOMINAL encoded System Transfer arguments whose source is the wallet,
    /// INCLUDING self-transfers. Not actual/net/peak debit, a spending bound or balance.
    pub(crate) nominal_wallet_source_transfer_operands_lamports: u128,
    pub(crate) encoded_priority_fee: EncodedPriorityFee,
    pub(crate) coverage: FundingCoverage,
    pub(crate) unavailable_budget: UnavailableNativeBudget,
}
