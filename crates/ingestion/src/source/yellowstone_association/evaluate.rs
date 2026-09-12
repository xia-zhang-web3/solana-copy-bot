use super::*;

impl YellowstoneAssociation<'_> {
    fn associate(
        &self,
        r: &Record,
        block: &Block,
    ) -> Result<association::AssociatedSwap, AssociationRefusal> {
        association::associate_yellowstone_transaction(
            &r.tx,
            &block.value,
            self.programs.interested,
            self.programs.raydium,
            self.programs.pumpswap,
        )
    }
    pub(super) fn pending_resolution(
        &self,
        id: ResultId,
        r: &Record,
        cause: &Cause,
    ) -> Option<Resolution> {
        use UnresolvedReason as Reason;
        let forced = if let Some(reason) = &r.forced {
            Some(reason.clone())
        } else if limits::expired(
            self.offset,
            r.checked.context.offset,
            self.limits.pending_ttl,
        ) {
            Some(Reason::Expired)
        } else {
            match cause {
                Cause::End => Some(Reason::EndOfStream),
                Cause::Reset(_) => Some(Reason::SessionReset),
                Cause::Conflict(target) if *target == id => Some(Reason::ConflictingTransaction),
                _ => None,
            }
        };
        if let Some(reason) = forced {
            return Some(Resolution::Unresolved(reason));
        }
        let relevant = match cause {
            Cause::Transaction(target) => *target == id,
            Cause::Block(slot, _) => *slot == r.tx.slot,
            _ => false,
        };
        if !relevant {
            return None;
        }
        let mut selected = None;
        for b in self.blocks.get(&r.tx.slot).into_iter().flatten() {
            match self.associate(r, b) {
                Ok(value) => {
                    let resolution = Resolution::ProviderAsserted {
                        assertion: value.provider_assertion,
                        block_time: value.block_time,
                    };
                    if selected.as_ref().is_some_and(|prior| prior != &resolution) {
                        return Some(Resolution::Unresolved(Reason::ConflictingAssertions));
                    }
                    selected = Some(resolution);
                }
                Err(AssociationRefusal::NotFoundInMessage) => {}
                Err(reason) => return Some(Resolution::Unresolved(Reason::Association(reason))),
            }
        }
        selected
    }
    pub(super) fn late_evidence(
        &self,
        id: ResultId,
        r: &Record,
        original: &Resolution,
        cause: &Cause,
    ) -> Option<LateEvidence> {
        if r.late_reported {
            return None;
        }
        match cause {
            Cause::Conflict(target) if *target == id => {
                if matches!(
                    original,
                    Resolution::Unresolved(UnresolvedReason::ConflictingTransaction)
                ) {
                    None
                } else {
                    Some(LateEvidence::ConflictingTransaction)
                }
            }
            Cause::Block(slot, index) if *slot == r.tx.slot => {
                let b = &self.blocks[slot][*index];
                match self.associate(r, b) {
                    Ok(value) => {
                        let resolution = Resolution::ProviderAsserted {
                            assertion: value.provider_assertion.clone(),
                            block_time: value.block_time,
                        };
                        (original != &resolution).then_some(LateEvidence::ProviderAssertion {
                            assertion: value.provider_assertion,
                            block_time: value.block_time,
                        })
                    }
                    Err(AssociationRefusal::NotFoundInMessage) => None,
                    Err(reason) => Some(LateEvidence::Association(reason)),
                }
            }
            _ => None,
        }
    }
}
