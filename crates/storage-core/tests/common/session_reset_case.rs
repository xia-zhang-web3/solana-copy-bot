#![allow(dead_code)]
#[path = "association.rs"]
pub mod f;
use copybot_core_types::{association_delivery::*, association_parent::*, association_recovery::*};
pub use copybot_storage_core::association_inbox::AssociationInbox;

pub fn events() -> Vec<Delivery> {
    serde_json::from_str(include_str!("../fixtures/session_reset_partial.json")).unwrap()
}
pub fn scope() -> ReplayScope {
    let DeliveryEvent::ParentCheckpoint(b) = events().remove(0).event else {
        panic!()
    };
    b.scope
}
pub fn assertion() -> ProviderAssertion {
    let DeliveryEvent::Terminal {
        result: Terminal::ProviderAsserted(a),
        ..
    } = events().pop().unwrap().event
    else {
        panic!()
    };
    a
}
pub fn signature() -> String {
    assertion().signature
}
pub fn checkpoint() -> Delivery {
    let all = events();
    let DeliveryEvent::ParentCheckpoint(previous) = &all[0].event else {
        panic!()
    };
    let DeliveryEvent::Terminal { expected, .. } = &all.last().unwrap().event else {
        panic!()
    };
    let a = assertion();
    Delivery {
        session: all.last().unwrap().session.clone(),
        sequence: 72,
        arrival_offset_ns: 72,
        event: DeliveryEvent::ParentCheckpoint(BlockCheckpoint {
            scope: previous.scope.clone(),
            observation: ParentObservation {
                child: BlockKey {
                    slot: a.slot,
                    hash: a.blockhash,
                },
                parent: previous.observation.child.clone(),
                issue: None,
            },
            executed_transaction_count: 1,
            supplied_transaction_count: 1,
            claims: vec![CheckpointClaim {
                signature: a.signature,
                transaction_index: a.transaction_index,
                info: expected.info.clone(),
            }],
        }),
    }
}
pub fn open(path: &std::path::Path) -> AssociationInbox {
    let mut inbox = AssociationInbox::open(path, f::limits()).unwrap();
    inbox.configure_replay_scope(&scope()).unwrap();
    inbox
}
pub fn feed(inbox: &mut AssociationInbox, all: &[Delivery]) {
    for d in all {
        inbox.persist(d, &CandidateGeneration::Unknown).unwrap();
    }
}
