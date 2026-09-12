use super::*;
use std::{
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

impl YellowstoneAssociation<'_> {
    /// Progresses one bounded chunk. No internal output queue or block cloning.
    /// A paused cursor is resumed before another input can be admitted. Caller
    /// retention of returned chunks is outside this adapter's memory budget.
    pub(in crate::source) fn drain(&mut self) -> OutputBatch {
        let mut outcomes = Vec::new();
        let mut charged_bytes = 0;
        while let Some(action) = &self.action {
            let next = match action.cursor {
                None => self.records.first_key_value(),
                Some(cursor) => self.records.range((Excluded(cursor), Unbounded)).next(),
            };
            let Some((&id, r)) = next else {
                self.finish_action();
                break;
            };
            // Reserve conservatively before evaluating. A single record's max
            // output charge was validated at admission, so drain cannot wedge.
            if outcomes.len() == self.limits.outputs.count
                || r.output_bytes > self.limits.outputs.encoded_bytes - charged_bytes
            {
                break;
            }
            let outcome = match &r.terminal {
                None => self
                    .pending_resolution(id, r, &action.cause)
                    .map(|resolution| Outcome::Terminal {
                        id,
                        checked: Arc::clone(&r.checked),
                        info: r.tx.transaction.as_ref().expect("admitted Info").clone(),
                        resolution,
                    }),
                Some((_, original)) => {
                    self.late_evidence(id, r, original, &action.cause)
                        .map(|evidence| Outcome::Late {
                            id,
                            session: r.checked.context.session,
                            signature: r.checked.facts.signature.clone(),
                            slot: r.tx.slot,
                            original: original.clone(),
                            evidence,
                        })
                }
            };
            let r = self.records.get_mut(&id).expect("cursor record");
            if let Some(outcome) = outcome {
                match &outcome {
                    Outcome::Terminal { resolution, .. } => {
                        r.terminal = Some((self.offset, resolution.clone()));
                        r.forced = None;
                    }
                    Outcome::Late { .. } => r.late_reported = true,
                }
                charged_bytes += r.output_bytes;
                outcomes.push(outcome);
            }
            self.action.as_mut().expect("active cursor").cursor = Some(id);
        }
        OutputBatch {
            outcomes,
            complete: self.action.is_none(),
            charged_bytes,
        }
    }
    fn finish_action(&mut self) {
        match self.action.take().expect("active action").cause {
            Cause::End => {
                self.ended = true;
                self.blocks.clear();
            }
            Cause::Reset(next) => {
                self.records.clear();
                self.signatures.clear();
                self.blocks.clear();
                self.session = next;
                self.offset = Duration::ZERO;
                self.ended = false;
            }
            _ => {}
        }
    }
}
