use super::super::yellowstone_association as a;
use super::super::yellowstone_message_time::{
    CreatedAtUnavailable as M, YellowstoneMessageTime as T,
};
use copybot_core_types::association_delivery::*;
use prost::Message;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;
pub(super) fn info(v: &SubscribeUpdateTransactionInfo) -> InfoIdentity {
    InfoIdentity {
        encoded: v.encode_to_vec(),
        float_bits: v
            .meta
            .iter()
            .flat_map(|m| m.pre_token_balances.iter().chain(&m.post_token_balances))
            .map(|r| r.ui_token_amount.as_ref().map(|a| a.ui_amount.to_bits()))
            .collect(),
    }
}
pub(super) fn admission(
    c: &a::CheckedTransaction,
    i: &SubscribeUpdateTransactionInfo,
) -> AdmissionFacts {
    let f = &c.facts;
    let mut programs = f.program_ids.clone();
    programs.sort();
    AdmissionFacts {
        facts: CheckedFacts {
            signature: f.signature.clone(),
            slot: f.slot,
            wallet: f.signer.clone(),
            token_in: f.token_in.clone(),
            token_out: f.token_out.clone(),
            amount_in_bits: f.amount_in.to_bits(),
            amount_out_bits: f.amount_out.to_bits(),
            exact_amounts: f.exact_amounts.clone(),
            programs,
            dex: f.dex_hint.clone(),
            program_fallback: c.used_program_fallback,
        },
        info: info(i),
        message_time: message_time(c.message_time),
    }
}
fn assertion(v: &a::ProviderContainingBlock, t: &a::ProviderBlockTime) -> ProviderAssertion {
    ProviderAssertion {
        slot: v.slot,
        blockhash: v.blockhash.clone(),
        signature: bs58::encode(v.signature).into_string(),
        transaction_index: v.transaction_index,
        block_time: match t {
            a::ProviderBlockTime::AvailableBlockTime(t) => BlockTime::Seconds(t.timestamp()),
            a::ProviderBlockTime::Missing => BlockTime::Missing,
            a::ProviderBlockTime::OutOfRange(n) => BlockTime::OutOfRange(*n),
        },
    }
}
pub(super) fn terminal(v: &a::Resolution) -> Terminal {
    match v {
        a::Resolution::ProviderAsserted {
            assertion: a,
            block_time,
        } => Terminal::ProviderAsserted(assertion(a, block_time)),
        a::Resolution::Unresolved(r) => Terminal::Unresolved(match r {
            a::UnresolvedReason::Expired => Unresolved::Expired,
            a::UnresolvedReason::PendingCapacity => Unresolved::PendingCapacity,
            a::UnresolvedReason::EndOfStream => Unresolved::EndOfStream,
            a::UnresolvedReason::SessionReset => Unresolved::SessionReset,
            a::UnresolvedReason::ConflictingTransaction => Unresolved::ConflictingTransaction,
            a::UnresolvedReason::ConflictingAssertions => Unresolved::ConflictingAssertions,
            a::UnresolvedReason::Association(r) => Unresolved::Association(format!("{r:?}")),
        }),
    }
}
pub(super) fn late(v: &a::LateEvidence) -> Late {
    match v {
        a::LateEvidence::ConflictingTransaction => Late::ConflictingTransaction,
        a::LateEvidence::Association(r) => Late::Association(format!("{r:?}")),
        a::LateEvidence::ProviderAssertion {
            assertion: a,
            block_time,
        } => Late::ProviderAssertion(assertion(a, block_time)),
    }
}

pub(super) fn message_time(time: T) -> MessageTime {
    match time {
        T::AvailableCreatedAt(t) => MessageTime::CreatedAt {
            seconds: t.timestamp(),
            nanos: t.timestamp_subsec_nanos(),
        },
        T::UnresolvedCreatedAt(m) => match m {
            M::Missing => MessageTime::Missing,
            M::InvalidNanos(n) => MessageTime::InvalidNanos(n),
            M::OutOfRangeSeconds(s) => MessageTime::OutOfRangeSeconds(s),
        },
    }
}
