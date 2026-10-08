//! Durable closed-prefix ownership, separate from replay validation.
use super::*;

impl Bridge<'_> {
    pub(super) fn retire_acknowledged_complete_blocks(&mut self) -> Result<()> {
        let r = self.recovery.as_ref().expect("recovery");
        if !r.gate.ready() {
            return Ok(());
        }
        let Some(emitted) = r.last_checkpoint else {
            return Ok(());
        };
        let Some(first_sequence) = r.first_checkpoint_sequence else {
            return Ok(());
        };
        let Some(committed) = r.cursor.snapshot()? else {
            return Ok(());
        };
        // Slot-only ACK checks would accept an inherited same-slot durable head.
        if committed.session != self.name || committed.sequence < first_sequence {
            return Ok(());
        }
        ensure!(
            r.gate
                .observed_child_matches(&committed.block.observation.child),
            "complete_block_retirement_ack_identity"
        );
        if committed.block.observation.child.slot == emitted {
            ensure!(
                r.last_checkpoint_child.as_ref() == Some(&committed.block.observation.child),
                "complete_block_retirement_ack_identity"
            );
        }
        match self
            .adapter
            .retire_complete_blocks_through(emitted.min(committed.block.observation.child.slot))
        {
            Ok(_)
            | Err(crate::source::yellowstone_association::Rejection::BlockRetirementPending) => {
                Ok(())
            }
            Err(r) => Err(anyhow::anyhow!("complete_block_retirement_refused: {r:?}")),
        }
    }
}
