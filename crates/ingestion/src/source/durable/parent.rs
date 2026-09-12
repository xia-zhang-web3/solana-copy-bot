use super::super::yellowstone_block_association::valid_blockhash;
use copybot_core_types::association_parent::{issue, BlockKey, ParentObservation};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
/// Called only after successful Block admission, including empty filtered blocks.
pub(super) fn observation(b: &SubscribeUpdateBlock) -> ParentObservation {
    let child = BlockKey {
        slot: b.slot,
        hash: b.blockhash.clone(),
    };
    let parent = BlockKey {
        slot: b.parent_slot,
        hash: b.parent_blockhash.clone(),
    };
    let issue = issue(&child, &parent, valid_blockhash);
    ParentObservation {
        child,
        parent,
        issue,
    }
}
