#[path = "common/association.rs"]
mod f;
use copybot_core_types::{association_delivery::*, association_parent::*, association_recovery::*};
use copybot_storage_core::association_inbox::AssociationInbox;
use f::*;
fn key(slot: u64) -> BlockKey {
    BlockKey {
        slot,
        hash: format!(
            "{}{}",
            "1".repeat(31),
            b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"[slot as usize] as char
        ),
    }
}
fn scope() -> ReplayScope {
    ReplayScope {
        policy: "durable_checkpoint_replay_v1".into(),
        wallets: vec!["leader".into()],
        programs: vec!["program".into()],
        raydium_programs: vec!["ray".into()],
        pumpswap_programs: vec!["pump".into()],
    }
}
fn checkpoint(slot: u64, parent: u64, claimed: bool) -> Delivery {
    let observation = ParentObservation {
        child: key(slot),
        parent: key(parent),
        issue: None,
    };
    event(
        slot + 20,
        DeliveryEvent::ParentCheckpoint(BlockCheckpoint {
            scope: scope(),
            observation,
            executed_transaction_count: 10,
            supplied_transaction_count: 10,
            claims: if claimed {
                vec![CheckpointClaim {
                    signature: facts().facts.signature,
                    transaction_index: 9,
                    info: facts().info,
                }]
            } else {
                vec![]
            },
        }),
    )
}
fn asserted() -> Terminal {
    let Terminal::ProviderAsserted(mut a) = result() else {
        unreachable!()
    };
    a.blockhash = key(7).hash;
    Terminal::ProviderAsserted(a)
}
#[test]
fn checkpoint_only_after_claim_commit_and_readback_survives_ack_loss() {
    let (_tmp, path) = db();
    let mut inbox = AssociationInbox::open(&path, limits()).unwrap();
    inbox.configure_replay_scope(&scope()).unwrap();
    inbox
        .persist(&admission(0), &CandidateGeneration::Unknown)
        .unwrap();
    assert!(inbox
        .persist(&checkpoint(7, 6, true), &CandidateGeneration::Unknown)
        .is_err());
    assert!(inbox.replay_checkpoint(&scope()).unwrap().is_none());
    inbox
        .persist(&terminal(1, asserted()), &CandidateGeneration::Unknown)
        .unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TRIGGER ignore_cursor BEFORE UPDATE ON association_replay_cursor BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(inbox
        .persist(&checkpoint(7, 6, true), &CandidateGeneration::Unknown)
        .is_err());
    assert!(inbox.replay_checkpoint(&scope()).unwrap().is_none());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM association_inbox_events", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        2
    );
    conn.execute_batch("DROP TRIGGER ignore_cursor;").unwrap();
    inbox
        .persist(&checkpoint(7, 6, true), &CandidateGeneration::Unknown)
        .unwrap();
    let committed = inbox.replay_checkpoint(&scope()).unwrap().unwrap();
    assert_eq!(committed.overlap, vec![facts()]);
    drop(inbox); // commit-before-ACK process loss: no received progress survives.
    let mut inbox = AssociationInbox::open(&path, limits()).unwrap();
    inbox.configure_replay_scope(&scope()).unwrap();
    assert_eq!(
        inbox.replay_checkpoint(&scope()).unwrap().unwrap(),
        committed
    );
    inbox
        .persist(&checkpoint(7, 6, true), &candidate("different-on-retry"))
        .unwrap();
    assert_eq!(
        inbox.replay_checkpoint(&scope()).unwrap().unwrap(),
        committed
    );
    assert_eq!(
        inbox
            .identity(&facts().facts.signature)
            .unwrap()
            .unwrap()
            .first_session,
        "session-A"
    );
}
#[test]
fn incomplete_scope_info_index_unknown_and_gap_never_advance_cursor() {
    for mode in ["partial", "scope", "float", "index", "unknown", "gap"] {
        let (_tmp, path) = db();
        let mut inbox = AssociationInbox::open(&path, limits()).unwrap();
        inbox.configure_replay_scope(&scope()).unwrap();
        inbox
            .persist(&admission(0), &CandidateGeneration::Unknown)
            .unwrap();
        inbox
            .persist(
                &terminal(
                    1,
                    if mode == "unknown" {
                        Terminal::Unresolved(Unresolved::SessionReset)
                    } else {
                        asserted()
                    },
                ),
                &CandidateGeneration::Unknown,
            )
            .unwrap();
        let mut d = checkpoint(7, 6, true);
        let DeliveryEvent::ParentCheckpoint(ref mut b) = d.event else {
            unreachable!()
        };
        match mode {
            "partial" => b.supplied_transaction_count = 1,
            "scope" => b.scope.raydium_programs = vec!["different-partition".into()],
            "float" => b.claims[0].info.float_bits[0] = Some(0.0f64.to_bits()),
            "index" => b.claims[0].transaction_index = 8,
            "gap" => {
                inbox
                    .persist(&checkpoint(5, 4, false), &CandidateGeneration::Unknown)
                    .unwrap();
            }
            _ => {}
        }
        assert!(
            inbox.persist(&d, &CandidateGeneration::Unknown).is_err(),
            "{mode}"
        );
        let head = inbox.replay_checkpoint(&scope()).unwrap();
        if mode == "gap" {
            assert_eq!(head.unwrap().block.observation.child, key(5));
        } else {
            assert!(head.is_none(), "{mode}");
        }
    }
}
#[test]
fn later_durable_pending_lowers_restored_floor_without_upgrading_unknown() {
    let (_tmp, path) = db();
    let mut inbox = AssociationInbox::open(&path, limits()).unwrap();
    inbox.configure_replay_scope(&scope()).unwrap();
    inbox
        .persist(&checkpoint(7, 6, false), &CandidateGeneration::Unknown)
        .unwrap();
    assert_eq!(
        inbox
            .replay_checkpoint(&scope())
            .unwrap()
            .unwrap()
            .from_slot,
        6
    );
    let mut a = facts();
    a.facts.slot = 4;
    inbox
        .persist(
            &event(80, DeliveryEvent::Admission(a.clone())),
            &CandidateGeneration::Unknown,
        )
        .unwrap();
    assert_eq!(
        inbox
            .replay_checkpoint(&scope())
            .unwrap()
            .unwrap()
            .from_slot,
        4
    );
    assert_eq!(
        inbox.replay_checkpoint(&scope()).unwrap().unwrap().overlap,
        vec![a.clone()]
    );
    drop(inbox);
    let mut inbox = AssociationInbox::open(&path, limits()).unwrap();
    assert!(
        inbox
            .identity(&a.facts.signature)
            .unwrap()
            .unwrap()
            .recovery
    );
    assert_eq!(
        inbox
            .replay_checkpoint(&scope())
            .unwrap()
            .unwrap()
            .from_slot,
        4
    );
    inbox
        .persist(
            &event(
                81,
                DeliveryEvent::Terminal {
                    signature: a.facts.signature.clone(),
                    expected: a,
                    result: Terminal::Unresolved(Unresolved::SessionReset),
                },
            ),
            &CandidateGeneration::Unknown,
        )
        .unwrap();
    // Explicit terminal UNKNOWN remains unrecoverable; floor promises pending only.
    assert!(matches!(
        inbox
            .identity(&facts().facts.signature)
            .unwrap()
            .unwrap()
            .terminal,
        Some(Terminal::Unresolved(_))
    ));
}
#[test]
fn tampered_head_scope_and_cursor_schema_fail_closed() {
    let (_tmp, path) = db();
    let mut inbox = AssociationInbox::open(&path, limits()).unwrap();
    inbox.configure_replay_scope(&scope()).unwrap();
    inbox
        .persist(&checkpoint(7, 6, false), &CandidateGeneration::Unknown)
        .unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE association_replay_cursor SET head=json_set(head,'$.block.scope.programs[0]','other')",[]).unwrap();
    assert!(inbox.replay_checkpoint(&scope()).is_err());
    conn.execute_batch("DROP TRIGGER association_replay_cursor_no_delete;")
        .unwrap();
    assert!(inbox.configure_replay_scope(&scope()).is_err());
}
