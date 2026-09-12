use super::*;
use prost::Message;
use std::sync::Arc;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

impl YellowstoneAssociation<'_> {
    /// On Err the borrowed input is not consumed and state/clock do not change.
    pub(in crate::source) fn push(
        &mut self,
        context: Context,
        input: Input<'_>,
    ) -> Result<Admission, Rejection> {
        if self.action.is_some() {
            return Err(Rejection::Busy);
        }
        if context.session != self.session {
            return Err(Rejection::StaleSession);
        }
        if context.offset < self.offset {
            return Err(Rejection::RegressingOffset);
        }
        if self.ended && !matches!(input, Input::Reset(_)) {
            return Err(Rejection::Ended);
        }
        let (admission, cause) = match input {
            Input::Transaction(tx, time) => self.admit_transaction(context, tx, time)?,
            Input::Block(block) => self.admit_block(context.offset, block)?,
            Input::Tick => (Admission::Control, Cause::Tick),
            Input::End => (Admission::Control, Cause::End),
            Input::Reset(next) => {
                if next.generation <= self.session.generation {
                    return Err(Rejection::InvalidReset);
                }
                (Admission::Control, Cause::Reset(next))
            }
        };
        self.prune(context.offset);
        self.offset = context.offset;
        self.action = Some(Action {
            cause,
            cursor: None,
        });
        Ok(admission)
    }

    fn admit_transaction(
        &mut self,
        context: Context,
        tx: &SubscribeUpdateTransaction,
        time: YellowstoneMessageTime,
    ) -> Result<(Admission, Cause), Rejection> {
        let info = tx.transaction.as_ref().ok_or(Rejection::InputBounds(
            AssociationRefusal::MissingExpectedInfo,
        ))?;
        association::bounds::check_info(info, association::InfoSide::Expected)
            .map_err(Rejection::InputBounds)?;
        if info.signature.len() != 64 {
            return Err(Rejection::InputBounds(
                AssociationRefusal::InvalidSignatureLength(info.signature.len()),
            ));
        }
        let bytes = tx.encoded_len();
        if bytes > self.limits.input_bytes {
            return Err(Rejection::InputTooLarge);
        }
        let signature = bs58::encode(&info.signature).into_string();
        if let Some(id) = self.signatures.get(&signature).copied() {
            let r = &self.records[&id];
            if self.retained(r, context.offset) {
                let expected = r.tx.transaction.as_ref().expect("admitted Info");
                let same = r.tx.slot == tx.slot
                    && expected.encode_to_vec() == info.encode_to_vec()
                    && association::same_float_bits(expected, info);
                return Ok((
                    Admission::Duplicate(id),
                    if same {
                        Cause::Tick
                    } else {
                        Cause::Conflict(id)
                    },
                ));
            }
        }
        let decoded = decode_yellowstone_swap_facts(
            tx,
            self.programs.interested,
            self.programs.raydium,
            self.programs.pumpswap,
        );
        let Some(facts) = decoded.facts.map_err(|_| Rejection::FactsDecodeError)? else {
            return Ok((
                Admission::NotChecked {
                    used_program_fallback: decoded.used_program_fallback,
                },
                Cause::Tick,
            ));
        };
        let metadata = 1024
            + [
                &facts.signature,
                &facts.signer,
                &facts.token_in,
                &facts.token_out,
                &facts.dex_hint,
            ]
            .into_iter()
            .map(|s| s.len())
            .sum::<usize>()
            + facts
                .program_ids
                .iter()
                .map(|s| s.len() + 32)
                .sum::<usize>();
        // Includes enough charge for the largest terminal/late notice (two
        // bounded provider assertions). Arc avoids copying facts on output.
        let output_bytes = bytes
            .checked_add(metadata)
            .and_then(|n| n.checked_add(512))
            .ok_or(Rejection::OutputCapacity)?;
        if output_bytes > self.limits.outputs.encoded_bytes {
            return Err(Rejection::OutputCapacity);
        }
        let live: Vec<_> = self
            .records
            .values()
            .filter(|r| self.retained(r, context.offset))
            .collect();
        if live.len() >= self.limits.history.count
            || bytes
                > self
                    .limits
                    .history
                    .encoded_bytes
                    .saturating_sub(live.iter().map(|r| r.encoded_bytes).sum())
        {
            return Err(Rejection::HistoryCapacity);
        }
        if metadata
            > self
                .limits
                .metadata_bytes
                .saturating_sub(self.metadata_at(context.offset))
        {
            return Err(Rejection::MetadataCapacity);
        }
        let pending_count = live
            .iter()
            .filter(|r| r.terminal.is_none() && r.forced.is_none())
            .count();
        let pending_bytes: usize = live
            .iter()
            .filter(|r| r.terminal.is_none() && r.forced.is_none())
            .map(|r| r.encoded_bytes)
            .sum();
        let forced = (pending_count >= self.limits.pending.count
            || bytes
                > self
                    .limits
                    .pending
                    .encoded_bytes
                    .saturating_sub(pending_bytes))
        .then_some(UnresolvedReason::PendingCapacity);
        let next = self.next_id.checked_add(1).ok_or(Rejection::IdExhausted)?;
        let id = ResultId(self.next_id);
        self.prune(context.offset);
        self.records.insert(
            id,
            Record {
                tx: tx.clone(),
                checked: Arc::new(CheckedTransaction {
                    context,
                    facts,
                    message_time: time,
                    used_program_fallback: decoded.used_program_fallback,
                }),
                encoded_bytes: bytes,
                metadata_bytes: metadata,
                output_bytes,
                terminal: None,
                forced,
                late_reported: false,
            },
        );
        self.signatures.insert(signature, id);
        self.next_id = next;
        Ok((Admission::Transaction(id), Cause::Transaction(id)))
    }
}
