//! Actual protected handoff. Capture commits precede inbox writes and receiver ACK.
//! Neither this consumer nor a recovered observation has financial authority.
use crate::protected_capture::{checked_protected_info_equivalent, protected_stream_swap};
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::association_delivery::{
    CandidateGeneration, Delivery, DeliveryEvent, MessageTime, ProtectedSourceKind, SessionGap,
};
use copybot_core_types::association_recovery::{DurableCheckpoint, ReplayScope};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    capture_scope::{CaptureStore, ProtectedScope},
};

/// Reconcile the same-DB gap after a raw/live receipt commits but before its
/// protected identity link commits. An already delivered live row keeps its
/// origin; a later complete block cannot turn it into a recovery-first action.
pub fn restore_stream_links(config: &IngestionConfig, store: &mut CaptureStore) -> Result<()> {
    crate::protected_capture::restore_stream(config, store)?;
    loop {
        let batch = store.unbound_stream_receipts(128)?;
        if batch.is_empty() {
            return Ok(());
        }
        for row in batch {
            let revision = store.stream_receipt_scope(row.seq)?;
            let (evidence, observation) = crate::protected_capture::reconstruct_stream(
                config,
                &row.raw,
                (row.received_at * 1e6).round() as i64,
            )?;
            store.commit_evidence(revision, &evidence)?;
            store.commit_observation(
                revision,
                &observation,
                Some(row.seq),
                checked_protected_info_equivalent,
            )?;
        }
    }
}

pub struct ProtectedDelivery {
    pub store: CaptureStore,
    pub scope: ProtectedScope,
    pub replay_scope: ReplayScope,
    inbox: AssociationInbox,
    config: IngestionConfig,
}
impl ProtectedDelivery {
    /// Ordinary input remains global, but an identity first recovered without
    /// timely stream delivery cannot later be promoted into the fresh app path.
    pub fn ordinary_live(
        &mut self,
        packet: &copybot_core_types::association_delivery::OrdinaryLive,
    ) -> Result<Option<copybot_core_types::SwapEvent>> {
        let result = (|| {
            if let Some(prior) = self.store.protected_prior(&packet.facts.signature)? {
                ensure!(
                    prior.observation.facts.as_ref() == Some(&packet.facts),
                    "ordinary_live_protected_identity_conflict"
                );
                if prior.receipt.recovery_first {
                    return Ok(None);
                }
                ensure!(
                    packet.info_hash == prior.observation.info_hash,
                    "ordinary_live_protected_info_conflict"
                );
            }
            crate::protected_capture::ordinary_live_swap(&self.config, packet)
        })();
        if result.is_err() {
            let _ = self.store.fail_closed("ordinary_live_identity_failed");
        }
        result
    }
    pub fn open(
        config: &IngestionConfig,
        store: CaptureStore,
        scope: ProtectedScope,
    ) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        let policy = config
            .capture_recovery
            .as_ref()
            .context("protected policy missing")?;
        ensure!(
            scope.revision > 0
                && scope.revision as usize <= policy.max_scope_revisions
                && scope.wallets.len() <= policy.max_scope_wallets,
            "protected scope configured bound"
        );
        let directory = std::path::Path::new(&policy.checkpoint_directory);
        std::fs::create_dir_all(directory)?;
        ensure!(
            directory.canonicalize()? == directory,
            "protected checkpoint directory must be canonical"
        );
        let name = format!(
            "{}-revision-{}.db",
            store.durable_capture_identity()?,
            scope.revision
        );
        let path = directory.join(name);
        ensure!(!path.is_symlink(), "protected checkpoint symlink forbidden");
        // Each revision uses a fresh, separately owned schema. Reuse the existing
        // transactional migration runner; never migrate the consumed capture,
        // virtual or main runtime database here.
        let mut database = copybot_storage_core::SqliteStore::open(&path)?;
        database.run_migrations(std::path::Path::new(&policy.migrations_directory))?;
        drop(database);
        let l = config
            .yellowstone_association
            .as_ref()
            .context("protected limits missing")?;
        let mut inbox = AssociationInbox::open(
            path,
            InboxLimits {
                count: l.inbox.count,
                bytes: l.inbox.bytes,
                busy_ms: l.sqlite_busy_ms,
            },
        )?;
        let replay_scope = crate::replay_scope(config, &scope.wallets.iter().cloned().collect())?;
        inbox.configure_replay_scope(&replay_scope)?;
        Ok(Self {
            store,
            scope,
            replay_scope,
            inbox,
            config: config.clone(),
        })
    }
    pub fn restored_checkpoint(&self) -> Result<Option<DurableCheckpoint>> {
        self.inbox.replay_checkpoint(&self.replay_scope)
    }
    /// Cross-DB handoff is retryable only by immutable identity. Every receipt
    /// and outbox is durable/read back before the dependent inbox checkpoint.
    pub fn persist(&mut self, delivery: &Delivery) -> Result<Option<DurableCheckpoint>> {
        let result = self.persist_inner(delivery);
        if result.is_err() {
            // Preserve the primary error. An unknown writer is never used for
            // a cleanup write; the durable handoff is reconciled on restart.
            let _ = self.store.fail_closed("protected_delivery_failed");
        }
        result
    }
    fn persist_inner(&mut self, delivery: &Delivery) -> Result<Option<DurableCheckpoint>> {
        if matches!(&delivery.event, DeliveryEvent::OrdinaryLive(_)) {
            // The real app handler persists ordinary swaps to main raw storage.
            // Do not retain the global live universe in the protected inbox.
            return Ok(None);
        }
        let mut handoff = None;
        match &delivery.event {
            DeliveryEvent::Session(
                SessionGap::Transport
                | SessionGap::End
                | SessionGap::Reset
                | SessionGap::Rejected(_)
                | SessionGap::Recovery,
            ) => {
                self.store.gap("protected_source_discontinuity", false)?;
            }
            DeliveryEvent::ProtectedEvidence(e) => {
                self.store.commit_evidence(self.scope.revision, e)?;
                handoff = Some((e.sha256.clone(), None));
            }
            DeliveryEvent::ProtectedObservation(o) => {
                ensure!(
                    self.scope.wallets.binary_search(&o.wallet).is_ok(),
                    "protected observed wallet outside revision"
                );
                let prior = o
                    .signature
                    .as_deref()
                    .map(|s| self.store.protected_prior(s))
                    .transpose()?
                    .flatten();
                let live_seq = if o.source_kind == ProtectedSourceKind::Stream
                    && !prior.as_ref().is_some_and(|p| p.receipt.recovery_first)
                {
                    let evidence = self.store.evidence(&o.evidence_sha256)?;
                    let source_at = match o.message_time {
                        MessageTime::CreatedAt { seconds, nanos } => {
                            Some(seconds as f64 + nanos as f64 / 1e9)
                        }
                        _ => None,
                    };
                    let receipt = self.store.receive(
                        o.signature.as_deref(),
                        &o.wallet,
                        o.slot,
                        &evidence.raw,
                        &o.transaction_fingerprint,
                        evidence.available_at_micros as f64 / 1e6,
                        source_at,
                    )?;
                    if let Some(receipt) = &receipt {
                        if receipt.stage == "RECEIVED" {
                            let swap = protected_stream_swap(&self.config, o)?;
                            self.store.finish(
                                receipt.seq,
                                swap.as_ref(),
                                if swap.is_some() {
                                    "decoded_and_committed"
                                } else {
                                    "decoder_rejected_or_unavailable_time"
                                },
                            )?;
                        }
                    }
                    receipt.map(|r| r.seq)
                } else {
                    prior.as_ref().and_then(|p| p.receipt.live_seq)
                };
                let receipt = self.store.commit_observation(
                    self.scope.revision,
                    o,
                    live_seq,
                    checked_protected_info_equivalent,
                )?;
                handoff = Some((o.evidence_sha256.clone(), Some(receipt.id)));
            }
            DeliveryEvent::ProtectedHandoff { .. } => {
                anyhow::bail!("source_cannot_claim_committed_protected_handoff")
            }
            _ => {}
        }
        let projection = handoff
            .map(|(evidence_sha256, observation_id)| -> Result<_> {
                let mut projected = delivery.clone();
                projected.event = DeliveryEvent::ProtectedHandoff {
                    capture_identity: self.store.durable_capture_identity()?,
                    scope_revision: self.scope.revision,
                    evidence_sha256,
                    observation_id,
                };
                Ok(projected)
            })
            .transpose()?;
        self.inbox.persist(
            projection.as_ref().unwrap_or(delivery),
            &CandidateGeneration::Unknown,
        )?;
        if matches!(&delivery.event, DeliveryEvent::ParentCheckpoint(_)) {
            self.inbox.replay_checkpoint(&self.replay_scope)
        } else {
            Ok(None)
        }
    }
    /// A caller may ACK only after this method returns true. A pending forward
    /// outbox delays membership ACK/retirement, never upgrades admission.
    pub fn commit_checkpoint(&mut self, head: &DurableCheckpoint, now: f64) -> Result<bool> {
        let result = self.commit_checkpoint_inner(head, now);
        if result.is_err() {
            let _ = self.store.fail_closed("protected_checkpoint_failed");
        }
        result
    }
    fn commit_checkpoint_inner(&mut self, head: &DurableCheckpoint, now: f64) -> Result<bool> {
        if !self.store.verify_forward_dispositions()? {
            return Ok(false);
        }
        if self
            .store
            .planned_scope()?
            .is_some_and(|s| s.revision == self.scope.revision)
        {
            if self.scope.epoch != self.store.consumer_epoch() {
                self.store.recover_planned(&self.scope, head)?;
            } else {
                self.store.commit_scope(&self.scope, head, now)?;
            }
        } else {
            self.store.record_checkpoint(self.scope.revision, head)?;
        }
        Ok(true)
    }
}
