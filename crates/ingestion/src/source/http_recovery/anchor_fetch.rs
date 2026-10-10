//! Only the live anchor retains pre-decoder evidence. Other history stays unchanged.
use super::{anchor_capture::Capture, block, ConfirmedHttpRecovery, RecoveredBlock};
use anyhow::Result;
use serde_json::json;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

impl ConfirmedHttpRecovery {
    pub(crate) async fn anchor_block(
        &self,
        slot: u64,
        grpc: &SubscribeUpdateBlock,
        directory: Option<&str>,
    ) -> Result<RecoveredBlock> {
        let capture = Capture::begin(directory)?;
        let Capture::Full(mut pair) = capture else {
            let mut recovered = self.block(slot).await?;
            recovered.anchor_evidence = capture;
            return Ok(recovered);
        };
        pair.grpc(grpc)?;
        let response = self
            .request_with_evidence(
                "getBlock",
                json!([slot,{
                    "commitment":"confirmed","encoding":"json","transactionDetails":"full",
                    "maxSupportedTransactionVersion":1,"rewards":true
                }]),
                Some(&mut pair),
            )
            .await;
        let (result, raw_response) = match response {
            Ok(value) => value,
            Err(error) => {
                pair.refused(grpc, "http_response", &error)?;
                return Err(error);
            }
        };
        pair.http_raw(&raw_response)?;
        let block = match block::parse(slot, &result) {
            Ok(block) => block,
            Err(error) => {
                pair.refused(grpc, "http_normalization", &error)?;
                return Err(error);
            }
        };
        pair.http_normalized(&block)?;
        Ok(RecoveredBlock {
            block,
            raw_response,
            anchor_evidence: Capture::Full(pair),
        })
    }
}
