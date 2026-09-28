use super::super::super::yellowstone_message_time::YellowstoneMessageTime;
use super::super::recovery::{RecoveryCursor, RecoveryGate};
use super::*;
use anyhow::{ensure, Context};
use copybot_core_types::association_recovery::{BlockCheckpoint, CheckpointClaim};
use prost::Message;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdateBlock, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
};

pub(super) struct BlockRecovery {
    cursor: RecoveryCursor,
    gate: RecoveryGate,
    held: Vec<(SubscribeUpdateTransaction, YellowstoneMessageTime)>,
    bytes: usize,
    count_bound: usize,
    byte_bound: usize,
    block_bound: usize,
    known: std::collections::HashMap<String, AdmissionFacts>,
}
impl Bridge<'_> {
    pub(in crate::source::durable) fn enable_recovery(
        &mut self,
        cursor: RecoveryCursor,
        limits: &AssociationDeliveryConfig,
    ) -> Result<()> {
        let saved = cursor.snapshot()?;
        let known = saved
            .as_ref()
            .map(|h| {
                h.overlap
                    .iter()
                    .map(|a| (a.facts.signature.clone(), a.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let gate = RecoveryGate::new(saved, limits.blocks.count);
        self.recovery = Some(BlockRecovery {
            cursor,
            gate,
            held: vec![],
            bytes: 0,
            count_bound: limits.pending.count,
            byte_bound: limits.pending.bytes,
            block_bound: limits.blocks.count,
            known,
        });
        Ok(())
    }
    pub(in crate::source::durable) fn begin_replay(&mut self) -> Result<Option<u64>> {
        let Some(r) = self.recovery.as_mut() else {
            return Ok(None);
        };
        let saved = r.cursor.snapshot()?;
        r.known = saved
            .as_ref()
            .map(|h| {
                h.overlap
                    .iter()
                    .map(|a| (a.facts.signature.clone(), a.clone()))
                    .collect()
            })
            .unwrap_or_default();
        r.gate = RecoveryGate::new(saved, r.block_bound);
        r.held.clear();
        r.bytes = 0;
        Ok(r.gate.from_slot())
    }
    pub(in crate::source::durable) fn recovery_enabled(&self) -> bool {
        self.recovery.is_some()
    }
    pub(in crate::source::durable) fn replay_waiting_anchor(&self) -> bool {
        self.recovery.as_ref().is_some_and(|r| !r.gate.ready())
    }
    fn selected(&self, info: &SubscribeUpdateTransactionInfo) -> bool {
        let signature = bs58::encode(&info.signature).into_string();
        if self.adapter.has_signature(&signature) {
            return true;
        }
        let Some(r) = self.recovery.as_ref() else {
            return false;
        };
        let Some(message) = info.transaction.as_ref().and_then(|t| t.message.as_ref()) else {
            return false;
        };
        let Some(header) = message.header.as_ref() else {
            return false;
        };
        message
            .account_keys
            .iter()
            .take(header.num_required_signatures as usize)
            .any(|key| {
                r.cursor
                    .scope
                    .wallets
                    .contains(&bs58::encode(key).into_string())
            })
    }
    pub(super) async fn recovery_transaction(
        &mut self,
        at: u64,
        tx: &SubscribeUpdateTransaction,
        time: YellowstoneMessageTime,
    ) -> Result<()> {
        if let Some(info) = tx.transaction.as_ref() {
            self.verify_known(at, tx.slot, info, time).await?;
        }
        if self.recovery.as_ref().is_some_and(|r| !r.gate.ready()) {
            self.hold_selected(tx, time)?;
            return Ok(());
        }
        self.push(at, a::Input::Transaction(tx, time)).await
    }
    async fn verify_known(
        &mut self,
        at: u64,
        slot: u64,
        info: &SubscribeUpdateTransactionInfo,
        time: YellowstoneMessageTime,
    ) -> Result<()> {
        let signature = bs58::encode(&info.signature).into_string();
        let original = self
            .recovery
            .as_ref()
            .expect("recovery")
            .known
            .get(&signature)
            .cloned();
        if let Some(original) = original {
            let observed = convert::info(info);
            if original.facts.slot != slot || original.info != observed {
                self.emit(
                    at,
                    DeliveryEvent::Duplicate {
                        original,
                        observed_info: observed,
                        observed_slot: slot,
                        message_time: convert::message_time(time),
                    },
                )
                .await?;
                anyhow::bail!("replay_durable_info_conflict");
            }
        }
        Ok(())
    }
    fn hold_selected(
        &mut self,
        tx: &SubscribeUpdateTransaction,
        time: YellowstoneMessageTime,
    ) -> Result<()> {
        if !tx
            .transaction
            .as_ref()
            .is_some_and(|info| self.selected(info))
        {
            return Ok(());
        }
        let charge = tx
            .encoded_len()
            .checked_add(512)
            .context("replay_hold_bound")?;
        let r = self.recovery.as_mut().expect("recovery");
        ensure!(
            r.held.len() < r.count_bound && charge <= r.byte_bound.saturating_sub(r.bytes),
            "replay_hold_bound"
        );
        r.bytes += charge;
        r.held.push((tx.clone(), time));
        Ok(())
    }
    pub(super) async fn recovery_block(
        &mut self,
        at: u64,
        block: &SubscribeUpdateBlock,
    ) -> Result<()> {
        // Reject partial/ambiguous producer blocks before association can emit
        // provider assertions for previously pending transactions.
        ensure!(
            block.executed_transaction_count == block.transactions.len() as u64,
            "replay_incomplete_block"
        );
        let mut indices = std::collections::HashSet::new();
        let mut signatures = std::collections::HashSet::new();
        for info in &block.transactions {
            ensure!(
                info.index < block.executed_transaction_count
                    && indices.insert(info.index)
                    && signatures.insert(bs58::encode(&info.signature).into_string()),
                "replay_block_indices_or_signatures"
            );
        }
        ensure!(
            self.recovery
                .as_ref()
                .expect("recovery")
                .known
                .values()
                .filter(|a| a.facts.slot == block.slot)
                .all(|a| signatures.contains(&a.facts.signature)),
            "replay_durable_info_missing"
        );
        for info in &block.transactions {
            self.verify_known(
                at,
                block.slot,
                info,
                YellowstoneMessageTime::from_created_at(None),
            )
            .await?;
        }
        let ready = self
            .recovery
            .as_mut()
            .expect("recovery")
            .gate
            .block(block)?;
        // Full original input first: retained signatures must see changed Info,
        // even when that changed Info now belongs to an unrelated signer.
        self.push(at, a::Input::Block(block)).await?;
        let observation = super::super::parent::observation(block);
        if !ready {
            for info in &block.transactions {
                let tx = SubscribeUpdateTransaction {
                    transaction: Some(info.clone()),
                    slot: block.slot,
                };
                self.hold_selected(&tx, YellowstoneMessageTime::from_created_at(None))?;
            }
            return self.emit(at, DeliveryEvent::Parent(observation)).await;
        }
        let held = {
            let r = self.recovery.as_mut().expect("recovery");
            r.bytes = 0;
            std::mem::take(&mut r.held)
        };
        for (tx, time) in held {
            self.push(at, a::Input::Transaction(&tx, time)).await?;
        }
        let mut claims = vec![];
        for info in &block.transactions {
            if !self.selected(info) {
                continue;
            }
            let tx = SubscribeUpdateTransaction {
                transaction: Some(info.clone()),
                slot: block.slot,
            };
            self.push(
                at,
                a::Input::Transaction(&tx, YellowstoneMessageTime::from_created_at(None)),
            )
            .await?;
            let signature = bs58::encode(&info.signature).into_string();
            if self.adapter.has_signature(&signature) {
                claims.push(CheckpointClaim {
                    signature,
                    transaction_index: info.index,
                    info: convert::info(info),
                });
            }
        }
        let scope = self
            .recovery
            .as_ref()
            .expect("recovery")
            .cursor
            .scope
            .clone();
        self.emit(
            at,
            DeliveryEvent::ParentCheckpoint(BlockCheckpoint {
                scope,
                observation,
                executed_transaction_count: block.executed_transaction_count,
                supplied_transaction_count: block.transactions.len() as u64,
                claims,
            }),
        )
        .await
    }
}
