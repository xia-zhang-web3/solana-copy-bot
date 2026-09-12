use super::*;
use std::sync::Arc;
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransaction};

pub(super) struct Record {
    pub tx: SubscribeUpdateTransaction,
    pub checked: Arc<CheckedTransaction>,
    pub encoded_bytes: usize,
    pub metadata_bytes: usize,
    pub output_bytes: usize,
    pub terminal: Option<(Duration, Resolution)>,
    // Ready for Unresolved delivery, never counted as a waiting transaction.
    // This single in-flight admission is reserved in the history budget.
    pub forced: Option<UnresolvedReason>,
    pub late_reported: bool,
}
pub(super) struct Block {
    pub value: SubscribeUpdateBlock,
    pub offset: Duration,
    pub encoded_bytes: usize,
    pub metadata_bytes: usize,
}
pub(super) enum Cause {
    Transaction(ResultId),
    Conflict(ResultId),
    Block(u64, usize),
    Tick,
    End,
    Reset(Session),
}
pub(super) struct Action {
    pub cause: Cause,
    pub cursor: Option<ResultId>,
}
impl YellowstoneAssociation<'_> {
    pub(super) fn retained(&self, r: &Record, now: Duration) -> bool {
        r.terminal
            .as_ref()
            .is_none_or(|(at, _)| !limits::expired(now, *at, self.limits.history_ttl))
    }
    pub(super) fn prune(&mut self, now: Duration) {
        let ttl = self.limits.history_ttl;
        self.records.retain(|_, r| {
            r.terminal
                .as_ref()
                .is_none_or(|(at, _)| !limits::expired(now, *at, ttl))
        });
        self.signatures
            .retain(|_, id| self.records.contains_key(id));
        let ttl = self.limits.block_ttl;
        self.blocks.retain(|_, blocks| {
            blocks.retain(|b| !limits::expired(now, b.offset, ttl));
            !blocks.is_empty()
        });
    }
    pub(super) fn metadata_at(&self, now: Duration) -> usize {
        self.records
            .values()
            .filter(|r| self.retained(r, now))
            .map(|r| r.metadata_bytes)
            .sum::<usize>()
            + self
                .blocks
                .values()
                .flatten()
                .filter(|b| !limits::expired(now, b.offset, self.limits.block_ttl))
                .map(|b| b.metadata_bytes)
                .sum::<usize>()
    }
}
