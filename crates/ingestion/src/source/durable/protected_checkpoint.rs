//! Protected evidence wrappers; default delivery retains its original clock/path.
use super::Bridge;
use crate::source::yellowstone_message_time::YellowstoneMessageTime;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use prost::Message;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

impl Bridge<'_> {
    pub(super) async fn recovery_block(
        &mut self,
        at: u64,
        block: &SubscribeUpdateBlock,
    ) -> Result<()> {
        self.recovery_block_time(
            at,
            block,
            YellowstoneMessageTime::from_created_at(None),
            None,
        )
        .await
    }
    pub(super) async fn recovery_block_with_evidence(
        &mut self,
        at: u64,
        block: &SubscribeUpdateBlock,
        proof: Option<ProtectedEvidence>,
    ) -> Result<()> {
        self.recovery_block_time(
            at,
            block,
            YellowstoneMessageTime::RecoveredBlock {
                block_time: block.block_time.as_ref().map(|t| t.timestamp),
            },
            proof,
        )
        .await
    }
    pub(in crate::source::durable) async fn http_block(
        &mut self,
        at: u64,
        block: &SubscribeUpdateBlock,
    ) -> Result<()> {
        self.recovery_block_time(
            at,
            block,
            YellowstoneMessageTime::RecoveredBlock {
                block_time: block.block_time.as_ref().map(|t| t.timestamp),
            },
            None,
        )
        .await
    }
    pub(in crate::source::durable) async fn http_block_with_evidence(
        &mut self,
        at: u64,
        block: &SubscribeUpdateBlock,
        raw_response: &[u8],
        provider_block_time: Option<i64>,
    ) -> Result<()> {
        if !self.protected {
            return self.http_block(at, block).await;
        }
        let proof = super::protected::evidence(
            ProtectedSourceKind::ConfirmedHttp,
            block.slot,
            raw_response.to_vec(),
            provider_block_time,
        );
        self.recovery_block_with_evidence(at, block, Some(proof))
            .await
    }
    pub(in crate::source::durable) async fn http_anchor_with_evidence(
        &mut self,
        at: u64,
        selected: &SubscribeUpdateBlock,
        live: &SubscribeUpdateBlock,
        raw_http: &[u8],
        http_block_time: Option<i64>,
        selected_http: bool,
        comparison: &str,
    ) -> Result<()> {
        if !self.protected {
            return self.http_block(at, selected).await;
        }
        let live_proof = super::protected::evidence(
            ProtectedSourceKind::CompleteLiveBlock,
            live.slot,
            live.encode_to_vec(),
            live.block_time.as_ref().map(|t| t.timestamp),
        );
        let http_proof = super::protected::evidence(
            ProtectedSourceKind::ConfirmedHttp,
            selected.slot,
            raw_http.to_vec(),
            http_block_time,
        );
        let selected_source = if selected_http {
            ProtectedSourceKind::ConfirmedHttp
        } else {
            ProtectedSourceKind::CompleteLiveBlock
        };
        let (mut selected_proof, other_proof) = if selected_http {
            (http_proof, live_proof)
        } else {
            (live_proof, http_proof)
        };
        selected_proof.anchor = Some(ProtectedAnchorLink {
            other_evidence_sha256: other_proof.sha256.clone(),
            comparison: comparison.into(),
            selected_source,
        });
        if self.protected {
            // The other exact representation must commit before the selected
            // proof's checked link is read back by storage.
            self.emit(at, DeliveryEvent::ProtectedEvidence(other_proof))
                .await?;
        }
        self.recovery_block_with_evidence(at, selected, Some(selected_proof))
            .await
    }
}
