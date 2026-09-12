use crate::execution_native_rpc::types::NativeFundingRpcFacts;
use crate::execution_solana_tx::PubkeyBytes;

/// Initial observations only. No execution simulation, funding bound or refund promise.
/// One borrowed provenance source retains the full requirements, fee, raw observations,
/// confirmed/floor and independent slots/timings without duplicating bodies per reference.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NativeSetupInterpretation<'a> {
    pub(crate) facts: &'a NativeFundingRpcFacts,
    pub(crate) initial_accounts: Vec<InitialSetupAccount>,
    /// Same order/length as facts.requirements.requirements, including unresolved CPI.
    pub(crate) instructions: Vec<SetupInstruction>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SetupInstruction {
    pub(crate) requirement_index: usize,
    /// Index in initial_accounts; repeated instructions share the initial observation.
    pub(crate) initial_account_index: Option<usize>,
    pub(crate) interpretation: SetupOperation,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SetupOperation {
    /// Original operands/unresolved reason remain in the referenced requirement.
    NotSetup,
    Associated(AssociatedInitialState),
    Sync(SyncInitialState),
    Close(CloseInitialFacts),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InitialSetupAccount {
    pub(crate) pubkey: PubkeyBytes,
    /// Index in facts.accounts.value (one independent accounts observation).
    pub(crate) observation_index: usize,
    pub(crate) observed_lamports: Option<u64>,
    pub(crate) state: InitialAccountState,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InitialAccountState {
    Absent,
    SystemOwnedEmpty,
    Classic {
        token: ClassicTokenAccount,
        native: NativeInitialState,
    },
    Unsupported(AccountIssue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountIssue {
    Executable,
    ProgramOwner,
    SystemDataNotEmpty,
    ClassicLength,
    ClassicState,
    DelegateTag,
    NativeTag,
    CloseAuthorityTag,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClassicTokenAccount {
    pub(crate) mint: PubkeyBytes,
    pub(crate) token_owner: PubkeyBytes,
    pub(crate) amount: u64,
    pub(crate) delegate: Option<PubkeyBytes>,
    pub(crate) state: ClassicAccountState,
    pub(crate) native_reserve: Option<u64>,
    pub(crate) delegated_amount: u64,
    pub(crate) close_authority: Option<PubkeyBytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClassicAccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeInitialState {
    NonNative,
    /// Arithmetic on initial lamports only, NOT spendable SOL or a future refund.
    Wsol {
        lamports_minus_reserve: u64,
        synced: bool,
    },
    MintReserveMismatch,
    ReserveExceedsLamports,
    AmountExceedsBacking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssociatedInitialState {
    WrongDerivedAddress,
    UnsupportedTokenProgram,
    CreationRequiredAbsent,
    CreationCandidateSystemPrefunded,
    /// Identity only, including Frozen. Does not validate mint/rent/payer/CPI or swap.
    ExistingIdentityMatch,
    Uninitialized,
    WrongTokenOwner,
    WrongMint,
    UnsupportedInitialAccount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncInitialState {
    NoClassicInitialState,
    Uninitialized,
    NonNative,
    InconsistentNative,
    /// Includes the initial Frozen state; never claims sync/swap will succeed.
    ObservedWsol,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CloseInitialFacts {
    pub(crate) effective_authority: Option<PubkeyBytes>,
    pub(crate) authority_relation: CloseAuthorityRelation,
    pub(crate) authority_is_expected_wallet: bool,
    pub(crate) source_equals_destination: bool,
    pub(crate) destination_is_expected_wallet: bool,
    pub(crate) balance: CloseInitialBalance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseAuthorityRelation {
    NoClassicInitialState,
    /// Key comparison only. Signatures/multisig authorization are not established.
    MatchesEffectiveKey,
    ForeignKey,
    UnsupportedSpecialOwner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseInitialBalance {
    NoClassicInitialState,
    Uninitialized,
    ObservedNative,
    NonNativeZero,
    NonNativePositive,
    InconsistentNative,
}
