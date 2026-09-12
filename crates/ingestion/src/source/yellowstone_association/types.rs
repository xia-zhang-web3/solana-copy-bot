use super::super::{
    yellowstone_block_association as association, yellowstone_facts, yellowstone_message_time,
};
use std::{sync::Arc, time::Duration};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransaction};

pub(in crate::source) use association::{
    AssociationRefusal, ProviderBlockTime, ProviderContainingBlock,
};
pub(in crate::source) use yellowstone_message_time::YellowstoneMessageTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::source) struct Session {
    pub id: [u8; 16],
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::source) struct Context {
    pub session: Session,
    pub offset: Duration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::source) struct ResultId(pub u64);

/// Borrowed input remains caller-owned on every refusal. No clock inference.
pub(in crate::source) enum Input<'a> {
    Transaction(&'a SubscribeUpdateTransaction, YellowstoneMessageTime),
    Block(&'a SubscribeUpdateBlock),
    Tick,
    End,
    Reset(Session),
}
#[derive(Debug, PartialEq, Eq)]
pub(in crate::source) enum Admission {
    Transaction(ResultId),
    Duplicate(ResultId),
    NotChecked { used_program_fallback: bool },
    Block,
    Control,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::source) enum Rejection {
    Busy,
    StaleSession,
    RegressingOffset,
    Ended,
    InvalidReset,
    InputTooLarge,
    InputBounds(AssociationRefusal),
    FactsDecodeError,
    HistoryCapacity,
    BlockCapacity,
    MetadataCapacity,
    OutputCapacity,
    IdExhausted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::source) enum UnresolvedReason {
    Expired,
    PendingCapacity,
    EndOfStream,
    SessionReset,
    ConflictingTransaction,
    Association(AssociationRefusal),
    ConflictingAssertions,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::source) enum Resolution {
    ProviderAsserted {
        assertion: ProviderContainingBlock,
        block_time: ProviderBlockTime,
    },
    Unresolved(UnresolvedReason),
}
#[derive(Debug)]
pub(in crate::source) struct CheckedTransaction {
    pub context: Context,
    pub facts: yellowstone_facts::YellowstoneSwapFacts,
    pub message_time: YellowstoneMessageTime,
    pub used_program_fallback: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::source) enum LateEvidence {
    ConflictingTransaction,
    Association(AssociationRefusal),
    ProviderAssertion {
        assertion: ProviderContainingBlock,
        block_time: ProviderBlockTime,
    },
}
#[derive(Debug)]
pub(in crate::source) enum Outcome {
    Terminal {
        id: ResultId,
        checked: Arc<CheckedTransaction>,
        info: yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
        resolution: Resolution,
    },
    /// A correction/late-evidence notice referencing the original result. It
    /// never re-emits swap facts as a second financial event or upgrades Unknown.
    Late {
        id: ResultId,
        session: Session,
        signature: String,
        slot: u64,
        original: Resolution,
        evidence: LateEvidence,
    },
}
#[derive(Debug)]
pub(in crate::source) struct OutputBatch {
    pub outcomes: Vec<Outcome>,
    pub complete: bool,
    /// Logical encoded/metadata charge, not heap size or RSS.
    pub charged_bytes: usize,
}
