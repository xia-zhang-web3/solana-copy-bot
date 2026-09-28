use crate::source::durable::capture_scope::CaptureScope;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
};
use yellowstone_grpc_proto::prelude::*;
fn info(signer: u8, mention: u8) -> SubscribeUpdateTransactionInfo {
    SubscribeUpdateTransactionInfo {
        signature: vec![7; 64],
        transaction: Some(Transaction {
            message: Some(Message {
                header: Some(MessageHeader {
                    num_required_signatures: 1,
                    ..Default::default()
                }),
                account_keys: vec![vec![signer; 32], vec![mention; 32]],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn scope(durable: Vec<Vec<u8>>, hold: Option<Arc<AtomicBool>>) -> Arc<CaptureScope> {
    CaptureScope::new(
        &[bs58::encode([3; 32]).into_string()],
        durable.into_iter(),
        HashSet::new(),
        hold,
    )
}
#[test]
fn queued_owned_signature_keeps_changed_foreign_info_without_a_later_block() {
    let scope = scope(vec![], None);
    let original = info(3, 4);
    let foreign = info(4, 3);
    assert!(scope.keep(&original));
    assert!(
        !scope.keep(&foreign),
        "mentioning a source is not ownership"
    );
    let queued = scope.track(&original.signature);
    assert!(
        scope.keep(&foreign),
        "reader may be ahead of first admission"
    );
    scope.replace_known(HashSet::from([original.signature.clone()]));
    drop(queued);
    assert!(scope.keep(&foreign), "admitted conflict survives dequeue");
    scope.replace_known(HashSet::new());
    assert!(
        !scope.keep(&foreign),
        "expired evidence does not grow reader history"
    );
    let restored = scope_with_durable(original.signature);
    assert!(
        restored.keep(&foreign),
        "durable overlap cannot be hidden by changed owner"
    );
}
fn scope_with_durable(signature: Vec<u8>) -> Arc<CaptureScope> {
    scope(vec![signature], None)
}
#[test]
fn malformed_ownership_is_delivered_for_fail_closed_validation() {
    let scope = scope(vec![], None);
    let mut bad = info(4, 5);
    bad.transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .header = None;
    assert!(scope.keep(&bad));
    bad = info(4, 5);
    bad.signature.pop();
    assert!(scope.keep(&bad));
    bad = info(4, 5);
    bad.transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .header
        .as_mut()
        .unwrap()
        .num_required_signatures = 3;
    assert!(scope.keep(&bad));
}
#[test]
fn interruption_wins_a_verified_anchor_release_race() {
    for _ in 0..32 {
        let hold = Arc::new(AtomicBool::new(true));
        let scope = scope(vec![], Some(hold.clone()));
        let barrier = Arc::new(Barrier::new(3));
        let clear = {
            let s = scope.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                s.clear_verified();
            })
        };
        let end = {
            let s = scope.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                s.interrupted();
            })
        };
        barrier.wait();
        clear.join().unwrap();
        end.join().unwrap();
        assert!(hold.load(Ordering::Acquire));
        scope.clear_verified();
        assert!(
            hold.load(Ordering::Acquire),
            "an ended connection is never released"
        );
    }
    let hold = Arc::new(AtomicBool::new(true));
    let live = scope(vec![], Some(hold.clone()));
    live.clear_verified();
    assert!(!hold.load(Ordering::Acquire));
}
#[tokio::test]
async fn dropping_pending_reader_closes_hold_without_waiting_for_scheduled_abort() {
    let hold = Arc::new(AtomicBool::new(false));
    let scope = scope(vec![], Some(hold.clone()));
    let reader = crate::source::durable::TestReader::pending_for_lifetime_test(scope);
    assert!(!hold.load(Ordering::Acquire));
    drop(reader);
    assert!(
        hold.load(Ordering::Acquire),
        "no yield may precede financial hold"
    );
}
