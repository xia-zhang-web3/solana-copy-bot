//! One bounded evidence handoff before checkpoint ACK; no synthetic event clock.
use super::{convert, Bridge};
use crate::source::{
    yellowstone_facts::decode_yellowstone_swap_facts,
    yellowstone_message_time::YellowstoneMessageTime,
};
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::*;
use prost::Message;
use sha2::{Digest, Sha256};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlock,
    SubscribeUpdateTransaction,
};

pub fn checked_protected_info_equivalent(a: &InfoIdentity, b: &InfoIdentity) -> bool {
    crate::source::http_recovery::identity::info_equivalent(a, b)
}
pub fn checked_protected_info_fingerprint(info: &InfoIdentity) -> Result<String> {
    let value = yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo::decode(
        info.encoded.as_slice(),
    )?;
    let mut normalized = convert::info(&value);
    // Prost omits default signed zero; the explicit IEEE witness remains identity.
    normalized.float_bits = info.float_bits.clone();
    ensure!(
        checked_protected_info_equivalent(info, &normalized),
        "protected_info_normalization_conflict"
    );
    Ok(hash(&serde_json::to_vec(&normalized)?))
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn evidence(
    kind: ProtectedSourceKind,
    slot: u64,
    raw: Vec<u8>,
    block_time: Option<i64>,
) -> ProtectedEvidence {
    ProtectedEvidence {
        source_kind: kind,
        slot,
        sha256: hash(&raw),
        raw,
        available_at_micros: chrono::Utc::now().timestamp_micros(),
        block_time,
        anchor: None,
    }
}
impl Bridge<'_> {
    pub(super) async fn protected_stream(
        &mut self,
        at: u64,
        update: &SubscribeUpdate,
    ) -> Result<()> {
        if !self.protected {
            return Ok(());
        }
        let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
            return Ok(());
        };
        let Some(info) = tx.transaction.as_ref() else {
            anyhow::bail!("protected_unidentified_transaction");
        };
        let Some(wallet) = info
            .transaction
            .as_ref()
            .and_then(|t| t.message.as_ref())
            .and_then(|m| m.account_keys.first())
            .filter(|k| k.len() == 32)
            .map(|k| bs58::encode(k).into_string())
        else {
            anyhow::bail!("protected_unidentified_wallet");
        };
        if !self
            .runtime
            .admission_wallets
            .as_ref()
            .is_some_and(|s| s.contains(&wallet))
        {
            return Ok(());
        }
        let proof = evidence(
            ProtectedSourceKind::Stream,
            tx.slot,
            update.encode_to_vec(),
            None,
        );
        let evidence_hash = proof.sha256.clone();
        self.emit(at, DeliveryEvent::ProtectedEvidence(proof))
            .await?;
        let time = YellowstoneMessageTime::from_created_at(update.created_at.as_ref());
        let observation = observation(
            self.runtime,
            tx,
            time,
            ProtectedSourceKind::Stream,
            evidence_hash,
            None,
        )?;
        self.emit(at, DeliveryEvent::ProtectedObservation(observation))
            .await
    }
    pub(super) async fn protected_block(
        &mut self,
        at: u64,
        block: &SubscribeUpdateBlock,
        proof: Option<ProtectedEvidence>,
    ) -> Result<()> {
        if !self.protected {
            return Ok(());
        }
        let proof = proof.context("protected_complete_block_evidence_required")?;
        ensure!(
            proof.slot == block.slot
                && proof.source_kind != ProtectedSourceKind::Stream
                && hash(&proof.raw) == proof.sha256,
            "protected_block_evidence_identity"
        );
        let evidence_hash = proof.sha256.clone();
        let kind = proof.source_kind;
        self.emit(at, DeliveryEvent::ProtectedEvidence(proof))
            .await?;
        for info in &block.transactions {
            let signature = bs58::encode(&info.signature).into_string();
            if let Some((checked, prior)) = self.adapter.admitted_signature(&signature) {
                ensure!(
                    checked.facts.slot == block.slot
                        && checked_protected_info_equivalent(
                            &convert::info(prior),
                            &convert::info(info)
                        ),
                    "protected_complete_block_checked_info_conflict"
                );
            }
            let selected = info
                .transaction
                .as_ref()
                .and_then(|t| t.message.as_ref())
                .and_then(|m| m.account_keys.first())
                .map(|k| bs58::encode(k).into_string())
                .is_some_and(|w| {
                    self.runtime
                        .admission_wallets
                        .as_ref()
                        .is_some_and(|s| s.contains(&w))
                });
            if !selected {
                continue;
            }
            let tx = SubscribeUpdateTransaction {
                transaction: Some(info.clone()),
                slot: block.slot,
                bank_id: block.bank_id,
            };
            let observation = observation(
                self.runtime,
                &tx,
                YellowstoneMessageTime::RecoveredBlock {
                    block_time: block.block_time.as_ref().map(|t| t.timestamp),
                },
                kind,
                evidence_hash.clone(),
                Some(ProtectedBlockIdentity {
                    blockhash: block.blockhash.clone(),
                    parent_slot: block.parent_slot,
                    parent_blockhash: block.parent_blockhash.clone(),
                    transaction_index: info.index,
                    executed_transaction_count: block.executed_transaction_count,
                    block_time: block.block_time.as_ref().map(|t| t.timestamp),
                }),
            )?;
            self.emit(at, DeliveryEvent::ProtectedObservation(observation))
                .await?;
        }
        Ok(())
    }
}

fn observation(
    runtime: &crate::source::YellowstoneRuntimeConfig,
    tx: &SubscribeUpdateTransaction,
    time: YellowstoneMessageTime,
    kind: ProtectedSourceKind,
    evidence_hash: String,
    block: Option<ProtectedBlockIdentity>,
) -> Result<ProtectedObservation> {
    let info = tx.transaction.as_ref().context("protected_missing_info")?;
    let wallet = info
        .transaction
        .as_ref()
        .and_then(|t| t.message.as_ref())
        .and_then(|m| m.account_keys.first())
        .filter(|k| k.len() == 32)
        .map(|k| bs58::encode(k).into_string())
        .context("protected_unidentified_wallet")?;
    let decoded = decode_yellowstone_swap_facts(
        tx,
        &runtime.interested_program_ids,
        &runtime.raydium_program_ids,
        &runtime.pumpswap_program_ids,
    );
    let facts = if kind == ProtectedSourceKind::Stream {
        decoded.facts.ok().flatten()
    } else {
        decoded
            .facts
            .context("protected_recovered_facts_decode_error")?
    }
    .map(|f| convert::checked_facts(&f, decoded.used_program_fallback));
    let identity = convert::info(info);
    Ok(ProtectedObservation {
        signature: crate::source::yellowstone_proto::decode_signature_from_proto(info),
        wallet,
        slot: tx.slot,
        token_quantity: facts
            .as_ref()
            .and_then(|f| checked_token_quantity(runtime, tx, f)),
        facts,
        info_hash: checked_protected_info_fingerprint(&identity)?,
        info: identity,
        message_time: convert::message_time(time),
        source_kind: kind,
        evidence_sha256: evidence_hash,
        block,
        transaction_fingerprint: hash(&tx.encode_to_vec()),
    })
}

/// Reconstruct only a previously committed stream envelope, preserving its real
/// local availability. This does not start/fence a consumer or create admission.
pub fn reconstruct_stream(
    config: &copybot_config::IngestionConfig,
    raw: &[u8],
    available_at_micros: i64,
) -> Result<(ProtectedEvidence, ProtectedObservation)> {
    ensure!(
        raw.len() <= 8_388_608 && available_at_micros >= 0,
        "protected_restore_raw_bound"
    );
    let update = SubscribeUpdate::decode(raw)?;
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        anyhow::bail!("protected_restore_requires_transaction");
    };
    let runtime = crate::source::YellowstoneGrpcSource::new(config)?.runtime_config;
    let mut proof = evidence(ProtectedSourceKind::Stream, tx.slot, raw.to_vec(), None);
    proof.available_at_micros = available_at_micros;
    let observed = observation(
        &runtime,
        tx,
        YellowstoneMessageTime::from_created_at(update.created_at.as_ref()),
        ProtectedSourceKind::Stream,
        proof.sha256.clone(),
        None,
    )?;
    Ok((proof, observed))
}

fn checked_token_quantity(
    runtime: &crate::source::YellowstoneRuntimeConfig,
    tx: &SubscribeUpdateTransaction,
    f: &CheckedFacts,
) -> Option<ProtectedTokenQuantity> {
    let info = tx.transaction.as_ref()?;
    let meta = info.meta.as_ref()?;
    let message = info.transaction.as_ref()?.message.as_ref()?;
    let (input, amount_in, output, amount_out) =
        crate::source::yellowstone_proto::infer_swap_from_proto_balances_with_attribution(
            meta,
            0,
            &f.wallet,
            || {
                crate::source::native_attribution::proto::infer(
                    message,
                    meta,
                    &f.wallet,
                    &runtime.pumpswap_program_ids,
                )
            },
        )?;
    if input != f.token_in || output != f.token_out {
        return None;
    }
    let (side, mint, amount) =
        if input != crate::source::SOL_MINT && output == crate::source::SOL_MINT {
            ("SELL", input, amount_in)
        } else if input == crate::source::SOL_MINT && output != crate::source::SOL_MINT {
            ("BUY", output, amount_out)
        } else {
            return None;
        };
    let raw = amount.raw_amount?;
    if raw.parse::<u64>().ok()? == 0 {
        return None;
    }
    Some(ProtectedTokenQuantity {
        side: side.into(),
        mint,
        raw,
        decimals: amount.decimals?,
    })
}
