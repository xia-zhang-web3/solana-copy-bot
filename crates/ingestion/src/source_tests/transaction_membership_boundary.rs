use super::*;
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};

#[test]
fn transaction_membership_decoded_frame_is_seen_before_business_rejection() {
    let raw = vec![9; 64];
    let signature = bs58::encode(&raw).into_string();
    let slot = (1..100_000)
        .find(|s| crate::transaction_membership::selected(&signature, *s))
        .unwrap();
    let tx = SubscribeUpdateTransaction {
        slot,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: raw,
            ..Default::default()
        }),
        ..Default::default()
    };
    let update = SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Transaction(tx.clone())),
        ..Default::default()
    };
    let capture = Capture::new(true, 8);
    capture.observe_update(&update);
    let empty = HashSet::new();
    let parsed = crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
        &tx, &empty, &empty, &empty,
    );
    assert!(parsed.facts.is_err() || parsed.facts.unwrap().is_none());
    capture.close();
    let snapshot = capture.snapshot();
    assert_eq!(snapshot["records"][0]["decoded"]["count"], 1);
    assert_eq!(snapshot["records"][0]["journal_inserted"]["count"], 0);
}
