use super::*;
use prost::Message;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

impl YellowstoneAssociation<'_> {
    pub(super) fn admit_block(
        &mut self,
        now: Duration,
        block: &SubscribeUpdateBlock,
    ) -> Result<(Admission, Cause), Rejection> {
        if block.transactions.len() > association::MAX_BLOCK_TRANSACTIONS {
            return Err(Rejection::InputBounds(
                AssociationRefusal::TooManyTransactions(block.transactions.len()),
            ));
        }
        // Already-decoded input is caller-owned. encoded_len walks it without
        // allocation; transport and deserialization must enforce their own caps.
        let bytes = block.encoded_len();
        if bytes > self.limits.input_bytes {
            return Err(Rejection::InputTooLarge);
        }
        if let Some(blocks) = self.blocks.get(&block.slot) {
            for retained in blocks
                .iter()
                .filter(|b| !limits::expired(now, b.offset, self.limits.block_ttl))
            {
                if retained.value.encode_to_vec() == block.encode_to_vec()
                    && retained
                        .value
                        .transactions
                        .iter()
                        .zip(&block.transactions)
                        .all(|(a, b)| association::same_float_bits(a, b))
                {
                    return Ok((Admission::Block, Cause::Tick));
                }
            }
        }
        let live = self
            .blocks
            .values()
            .flatten()
            .filter(|b| !limits::expired(now, b.offset, self.limits.block_ttl));
        let (count, total) = live.fold((0, 0), |(n, sum), b| (n + 1, sum + b.encoded_bytes));
        if count >= self.limits.blocks.count
            || bytes > self.limits.blocks.encoded_bytes.saturating_sub(total)
        {
            return Err(Rejection::BlockCapacity);
        }
        // Logical metadata charge for containers/indexes, separate from encoded
        // bytes. Neither charge is an RSS guarantee for caller-decoded protobufs.
        let metadata = 512 + block.transactions.len() * 128;
        if metadata
            > self
                .limits
                .metadata_bytes
                .saturating_sub(self.metadata_at(now))
        {
            return Err(Rejection::MetadataCapacity);
        }
        self.prune(now);
        let slot = block.slot;
        let blocks = self.blocks.entry(slot).or_default();
        let index = blocks.len();
        blocks.push(Block {
            value: block.clone(),
            offset: now,
            encoded_bytes: bytes,
            metadata_bytes: metadata,
        });
        Ok((Admission::Block, Cause::Block(slot, index)))
    }
}
