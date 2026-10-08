//! Only the complete-block recovery owner may establish a durable closed prefix.
use super::*;

impl YellowstoneAssociation<'_> {
    pub(in crate::source) fn retire_complete_blocks_through(
        &mut self,
        slot: u64,
    ) -> Result<usize, Rejection> {
        if self.action.is_some() {
            return Err(Rejection::Busy);
        }
        if self
            .records
            .values()
            .any(|r| r.tx.slot <= slot && r.terminal.is_none())
        {
            return Err(Rejection::BlockRetirementPending);
        }
        // No pruning of records/signatures/Info, late flags, TTLs or clocks.
        let before = self.blocks.values().map(Vec::len).sum::<usize>();
        self.blocks.retain(|block_slot, _| *block_slot > slot);
        self.closed_block_watermark = Some(
            self.closed_block_watermark
                .map_or(slot, |old| old.max(slot)),
        );
        let after = self.blocks.values().map(Vec::len).sum::<usize>();
        Ok(before - after)
    }
}
