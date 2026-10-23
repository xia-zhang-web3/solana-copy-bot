//! One-use representation choice minted only by the current bounded RPC fetch.
use super::{anchor_diagnostic, complete_anchor, identity, ConfirmedHttpRecovery, RecoveredBlock};
use anyhow::{ensure, Result};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

/// Private fields and borrowed live input prevent arbitrary/replayed normalization
/// from being presented as a newly fetched complete anchor. It is not Clone.
pub(crate) struct CurrentAnchor<'a> {
    recovered: RecoveredBlock,
    live: &'a SubscribeUpdateBlock,
    context: String,
    http_selected: bool,
}
pub(crate) struct SelectedAnchor<'a>(CurrentAnchor<'a>);
impl<'a> CurrentAnchor<'a> {
    /// Consumes even a refused choice; a different association session cannot reuse it.
    pub(crate) fn for_context(self, context: &str) -> Result<SelectedAnchor<'a>> {
        ensure!(
            self.context == context,
            "http_anchor_stale_association_context"
        );
        Ok(SelectedAnchor(self))
    }
}
impl SelectedAnchor<'_> {
    pub(crate) fn block(&self) -> &SubscribeUpdateBlock {
        if self.0.http_selected {
            &self.0.recovered.block
        } else {
            self.0.live
        }
    }
}
impl ConfirmedHttpRecovery {
    pub(crate) async fn current_anchor<'a>(
        &self,
        slot: u64,
        live: &'a SubscribeUpdateBlock,
        directory: Option<&str>,
        context: String,
    ) -> Result<CurrentAnchor<'a>> {
        ensure!(
            slot == live.slot && !context.is_empty(),
            "http_anchor_current_slot_context"
        );
        let mut recovered = self.anchor_block(slot, live, directory).await?;
        // The existing request validates response id, full getBlock shape, bounds,
        // broker clock and post-body deadline. No retained response is reloaded here.
        let http_selected = if identity::block_equivalent(&recovered.block, live) {
            anchor_diagnostic::admit_recovered(directory, live, &mut recovered)?;
            false
        } else {
            if !complete_anchor::eligible(&recovered.block, live) {
                anchor_diagnostic::admit_recovered(directory, live, &mut recovered)?;
                anyhow::bail!("http_recovery_live_anchor_conflict");
            }
            anchor_diagnostic::complete_http(directory, live, &mut recovered)?;
            true
        };
        Ok(CurrentAnchor {
            recovered,
            live,
            context,
            http_selected,
        })
    }
}
