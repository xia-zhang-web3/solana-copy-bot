//! Online provider-message association only. No event-time/canonical/finalized
//! claim and no RawSwapObservation/SwapEvent or runtime consumer.
//!
//! push borrows caller-owned inputs. Drain to completion before the next push;
//! Busy never consumes the input. Output chunks have explicit count/byte limits.
//! Completed identity retention is bounded; after expiry/reset there is no
//! global exactly-once guarantee. Only the first late evidence notice is emitted
//! per retained result. Callers must preserve terminal outcomes and notices.
mod admission;
mod blocks;
mod drain;
mod evaluate;
pub(in crate::source) mod limits;
mod state;
mod types;

use super::{
    yellowstone_block_association as association, yellowstone_facts::decode_yellowstone_swap_facts,
};
pub(in crate::source) use limits::{InvalidLimits, Limits};
use state::*;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};
pub(in crate::source) use types::*;

/// Borrowed immutable parser policy: no runtime defaults or telemetry mutation.
pub(in crate::source) struct Programs<'a> {
    pub interested: &'a HashSet<String>,
    pub raydium: &'a HashSet<String>,
    pub pumpswap: &'a HashSet<String>,
}
pub(in crate::source) struct YellowstoneAssociation<'a> {
    limits: Limits,
    programs: Programs<'a>,
    admission_wallets: Option<&'a HashSet<String>>,
    session: Session,
    offset: Duration,
    ended: bool,
    next_id: u64,
    records: BTreeMap<ResultId, Record>,
    signatures: HashMap<String, ResultId>,
    blocks: BTreeMap<u64, Vec<Block>>,
    action: Option<Action>,
}
impl<'a> YellowstoneAssociation<'a> {
    /// First checked facts are observable immediately after successful admission,
    /// before draining a possibly already-ready terminal. No decoder duplication.
    pub(in crate::source) fn admitted(
        &self,
        id: ResultId,
    ) -> Option<(
        &CheckedTransaction,
        &yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo,
    )> {
        self.records.get(&id).map(|r| {
            (
                r.checked.as_ref(),
                r.tx.transaction.as_ref().expect("admitted Info"),
            )
        })
    }

    pub(in crate::source) fn new(
        session: Session,
        limits: Limits,
        programs: Programs<'a>,
    ) -> Result<Self, InvalidLimits> {
        let limits = limits.validate()?;
        // Fallback program IDs can become facts metadata. Bound the borrowed
        // policy before decoder79 can copy it (caller allocation remains external).
        let policy_bytes = [programs.interested, programs.raydium, programs.pumpswap]
            .into_iter()
            .flat_map(|s| s.iter())
            .try_fold(0usize, |n, p| n.checked_add(p.len())?.checked_add(64));
        if policy_bytes.is_none_or(|n| n > limits.metadata_bytes) {
            return Err(InvalidLimits::Overflow);
        }
        Ok(Self {
            limits,
            programs,
            admission_wallets: None,
            session,
            offset: Duration::ZERO,
            ended: false,
            next_id: 0,
            records: BTreeMap::new(),
            signatures: HashMap::new(),
            blocks: BTreeMap::new(),
            action: None,
        })
    }
    pub(in crate::source) fn restrict_wallets(&mut self, wallets: &'a HashSet<String>) {
        self.admission_wallets = Some(wallets);
    }
}
