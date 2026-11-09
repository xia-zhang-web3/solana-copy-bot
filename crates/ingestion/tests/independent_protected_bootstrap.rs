//! Reviewer probe: prior scope's durable chain anchor is not a new-scope ACK.
#[path = "protected_recovery/support.rs"]
mod support;
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use copybot_core_types::association_recovery::{BlockCheckpoint, DurableCheckpoint};
use copybot_ingestion::{replay_scope, DeliveryReceiver};
use std::collections::HashSet;

#[tokio::test(flavor = "current_thread")]
async fn reviewer_old_scope_bootstrap_cannot_ack_new_wallet_coverage() {
    let temp = tempfile::tempdir().unwrap();
    let config = support::config(&temp.path().join("capture.db")).unwrap();
    let old_wallet = bs58::encode([3_u8; 32]).into_string();
    let new_wallet = bs58::encode([4_u8; 32]).into_string();
    let old_scope = replay_scope(&config, &HashSet::from([old_wallet])).unwrap();
    let anchor = DurableCheckpoint {
        session: "review-prior-scope".into(),
        sequence: 1,
        from_slot: 6,
        overlap: vec![],
        block: BlockCheckpoint {
            scope: old_scope,
            observation: ParentObservation {
                child: BlockKey {
                    slot: 7,
                    hash: support::key(7),
                },
                parent: BlockKey {
                    slot: 6,
                    hash: support::key(6),
                },
                issue: None,
            },
            executed_transaction_count: 0,
            supplied_transaction_count: 0,
            claims: vec![],
        },
    };
    // Current-thread executor has not yielded to the transport task. It is
    // aborted before any local/external connect can run: this is a cursor probe.
    let receiver = DeliveryReceiver::start_protected_transition(
        &config,
        "review-new-scope".into(),
        HashSet::from([new_wallet]),
        None,
        None,
        Some(anchor.clone()),
    )
    .unwrap();
    assert!(receiver
        .http_continuity_hold()
        .unwrap()
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(receiver.continuity_snapshot().context.checkpoint.is_none());
    assert!(
        receiver.acknowledge_checkpoint(anchor).is_err(),
        "old scope checkpoint must not ACK new wallet scope"
    );
    assert!(receiver
        .http_continuity_hold()
        .unwrap()
        .load(std::sync::atomic::Ordering::Acquire));
    drop(receiver);
}
