//! Replay requests and observed overlap are distinct. No SQLite or trading decisions.
use super::{super::YellowstoneGrpcSource, convert};
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use copybot_core_types::association_recovery::{DurableCheckpoint, ReplayScope};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

pub fn replay_scope(config: &IngestionConfig, wallets: &HashSet<String>) -> Result<ReplayScope> {
    let runtime = YellowstoneGrpcSource::new(config)?.runtime_config;
    let sorted = |values: &HashSet<String>| {
        let mut v: Vec<_> = values.iter().cloned().collect();
        v.sort();
        v
    };
    let scope = ReplayScope {
        policy: "durable_checkpoint_replay_v1".into(),
        wallets: sorted(wallets),
        programs: sorted(&runtime.interested_program_ids),
        raydium_programs: sorted(&runtime.raydium_program_ids),
        pumpswap_programs: sorted(&runtime.pumpswap_program_ids),
    };
    ensure!(scope.valid(), "replay_scope_invalid");
    Ok(scope)
}
#[derive(Clone)]
pub(super) struct RecoveryCursor {
    pub(super) scope: ReplayScope,
    head: Arc<Mutex<Option<DurableCheckpoint>>>,
    limits: copybot_config::DeliveryBudget,
}
impl RecoveryCursor {
    pub(super) fn new(
        scope: ReplayScope,
        head: Option<DurableCheckpoint>,
        limits: copybot_config::DeliveryBudget,
    ) -> Result<Self> {
        ensure!(scope.valid(), "replay_scope_invalid");
        if let Some(ref h) = head {
            validate(&scope, h, &limits)?;
        }
        Ok(Self {
            scope,
            head: Arc::new(Mutex::new(head)),
            limits,
        })
    }
    pub(super) fn snapshot(&self) -> Result<Option<DurableCheckpoint>> {
        Ok(self
            .head
            .lock()
            .map_err(|_| anyhow::anyhow!("replay_cursor_poisoned"))?
            .clone())
    }
    pub(super) fn acknowledge(&self, head: DurableCheckpoint) -> Result<()> {
        validate(&self.scope, &head, &self.limits)?;
        let mut current = self
            .head
            .lock()
            .map_err(|_| anyhow::anyhow!("replay_cursor_poisoned"))?;
        if let Some(old) = current.as_ref() {
            ensure!(
                head.block.observation.child.slot >= old.block.observation.child.slot,
                "replay_cursor_regression"
            );
            if head.block.observation.child.slot == old.block.observation.child.slot {
                ensure!(
                    head.block.observation.child == old.block.observation.child,
                    "replay_cursor_conflict"
                );
            }
        }
        *current = Some(head);
        Ok(())
    }
}
fn validate(
    scope: &ReplayScope,
    h: &DurableCheckpoint,
    limits: &copybot_config::DeliveryBudget,
) -> Result<()> {
    ensure!(
        h.block.scope == *scope
            && h.block.observation.issue.is_none()
            && h.block.observation.expected_issue().is_none()
            && h.from_slot > 0
            && h.from_slot <= h.block.observation.parent.slot
            && h.block.executed_transaction_count == h.block.supplied_transaction_count,
        "replay_cursor_identity"
    );
    let mut remaining = limits.bytes;
    let mut seen = HashSet::new();
    ensure!(h.overlap.len() <= limits.count, "replay_overlap_bound");
    for a in &h.overlap {
        ensure!(
            scope.wallets.contains(&a.facts.wallet)
                && a.facts.slot >= h.from_slot
                && a.facts.slot <= h.block.observation.child.slot
                && seen.insert(&a.facts.signature),
            "replay_overlap_identity"
        );
        remaining = remaining
            .checked_sub(serde_json::to_vec(a)?.len() + 512)
            .context("replay_overlap_bound")?;
    }
    Ok(())
}
pub(in crate::source) struct RecoveryGate {
    expected: Option<DurableCheckpoint>,
    anchor_seen: bool,
    parent_seen: bool,
    last: Option<BlockKey>,
    seen: HashMap<u64, ParentObservation>,
    bound: usize,
}
pub(super) fn definitive_history_code(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::OutOfRange
            | tonic::Code::InvalidArgument
            | tonic::Code::Unimplemented
            | tonic::Code::NotFound
    )
}
impl RecoveryGate {
    pub(in crate::source) fn new(expected: Option<DurableCheckpoint>, bound: usize) -> Self {
        Self {
            parent_seen: false,
            anchor_seen: expected.is_none(),
            expected,
            last: None,
            seen: HashMap::new(),
            bound,
        }
    }
    pub(in crate::source) fn ready(&self) -> bool {
        self.anchor_seen
    }
    pub(in crate::source) fn from_slot(&self) -> Option<u64> {
        self.expected.as_ref().map(|h| h.from_slot)
    }
    pub(in crate::source) fn block(&mut self, block: &SubscribeUpdateBlock) -> Result<bool> {
        let p = super::parent::observation(block);
        ensure!(p.issue.is_none(), "replay_parent_malformed");
        let mut ready = true;
        if let Some(h) = self.expected.as_ref().filter(|_| !self.anchor_seen) {
            if self.last.is_none() {
                ensure!(p.child.slot == h.from_slot, "replay_overlap_start_missing");
            }
            if p.child == h.block.observation.parent {
                self.parent_seen = true;
            }
            if p.child.slot == h.block.observation.child.slot {
                ensure!(
                    p == h.block.observation && self.parent_seen,
                    "replay_anchor_or_overlap_mismatch"
                );
                ensure!(
                    block.executed_transaction_count == block.transactions.len() as u64,
                    "replay_anchor_incomplete"
                );
                for claim in &h.block.claims {
                    let mut matches = block
                        .transactions
                        .iter()
                        .filter(|i| bs58::encode(&i.signature).into_string() == claim.signature);
                    let info = matches.next().context("replay_anchor_info_missing")?;
                    ensure!(
                        matches.next().is_none()
                            && info.index == claim.transaction_index
                            && convert::info(info) == claim.info,
                        "replay_anchor_info_changed"
                    );
                }
                self.anchor_seen = true;
            } else {
                ensure!(
                    p.child.slot < h.block.observation.child.slot,
                    "replay_history_unavailable"
                );
                ready = false;
            }
        }
        if let Some(last) = self.last.as_ref() {
            if p.child.slot <= last.slot {
                ensure!(
                    self.seen.get(&p.child.slot) == Some(&p),
                    "replay_overlap_branch_mismatch"
                );
                return Ok(ready);
            }
            ensure!(p.parent == *last, "replay_parent_gap");
        }
        if self.seen.len() >= self.bound {
            if let Some(oldest) = self.seen.keys().copied().min() {
                self.seen.remove(&oldest);
            }
        }
        self.seen.insert(p.child.slot, p.clone());
        self.last = Some(p.child);
        Ok(ready)
    }
}
