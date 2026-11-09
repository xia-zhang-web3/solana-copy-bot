//! Ordinary program-stream intake shares the same bounded reader and source.
//! Historical full blocks and held association replay never call this entrypoint.
use super::{convert, Bridge};
use crate::source::{
    yellowstone_facts::decode_yellowstone_swap_facts,
    yellowstone_message_time::YellowstoneMessageTime,
};
use anyhow::Result;
use copybot_core_types::association_delivery::{DeliveryEvent, MessageTime, OrdinaryLive};
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

impl Bridge<'_> {
    /// Only the original live frames buffered before the HTTP anchor use this
    /// cutoff. Older ones must not be re-admitted as protected fresh events.
    pub(in crate::source::durable) async fn buffered_original_update(
        &mut self,
        at: u64,
        anchor_slot: u64,
        update: &SubscribeUpdate,
    ) -> Result<()> {
        if matches!(update.update_oneof.as_ref(),
            Some(UpdateOneof::Transaction(tx)) if tx.slot > anchor_slot)
        {
            self.update(at, update).await
        } else {
            self.ordinary_stream(at, update).await
        }
    }

    pub(in crate::source::durable) async fn ordinary_stream(
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
        let time = YellowstoneMessageTime::from_created_at(update.created_at.as_ref());
        if !matches!(convert::message_time(time), MessageTime::CreatedAt { .. }) {
            return Ok(());
        }
        // Buffered original frames after HTTP do not re-enter protected admission,
        // but a conflicting retained identity must not become ordinary fresh input.
        if self.recovery.is_some() {
            if let Some(info) = tx.transaction.as_ref() {
                self.verify_known(at, tx.slot, info, time).await?;
            }
        }
        let decoded = decode_yellowstone_swap_facts(
            tx,
            &self.runtime.interested_program_ids,
            &self.runtime.raydium_program_ids,
            &self.runtime.pumpswap_program_ids,
        );
        if decoded.used_program_fallback {
            self.runtime
                .telemetry
                .note_parse_fallback("missing_program_ids_fallback");
        }
        let Ok(Some(facts)) = decoded.facts else {
            return Ok(());
        };
        let packet = OrdinaryLive {
            facts: convert::checked_facts(&facts, decoded.used_program_fallback),
            message_time: convert::message_time(time),
            info_hash: super::protected::checked_protected_info_fingerprint(&convert::info(
                tx.transaction.as_ref().expect("decoded transaction Info"),
            ))?,
        };
        if crate::protected_capture::checked_stream_swap(
            &packet.facts,
            &packet.message_time,
            self.runtime.raydium_program_ids.iter().cloned().collect(),
            self.runtime.pumpswap_program_ids.iter().cloned().collect(),
        )
        .is_none()
        {
            return Ok(());
        }
        self.emit(at, DeliveryEvent::OrdinaryLive(packet)).await
    }
}
