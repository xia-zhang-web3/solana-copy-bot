#[path = "common/b97_automatic.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::{association_parent::*, association_recovery::*};
use copybot_storage_core::association_sell_preparation::Check;
use f::*;

fn checkpoint(
    session: &str,
    sequence: u64,
    scope: &ReplayScope,
    claims: Vec<CheckpointClaim>,
) -> Delivery {
    let claimed = !claims.is_empty();
    Delivery {
        session: session.into(),
        sequence,
        arrival_offset_ns: sequence,
        event: DeliveryEvent::ParentCheckpoint(BlockCheckpoint {
            scope: scope.clone(),
            observation: edge(
                key(if claimed { 42 } else { 41 }, if claimed { 3 } else { 2 }),
                key(if claimed { 41 } else { 40 }, if claimed { 2 } else { 1 }),
            ),
            executed_transaction_count: if claimed { 3 } else { 0 },
            supplied_transaction_count: if claimed { 3 } else { 0 },
            claims,
        }),
    }
}

#[test]
fn only_recovered_checkpoint_wakes_drained_existing_sell_once() -> Result<()> {
    let mut f = new()?;
    let scope = ReplayScope {
        policy: "durable_checkpoint_replay_v1".into(),
        wallets: vec!["execution-wallet".into(), "leader".into()],
        programs: vec!["fixture".into()],
        raydium_programs: vec![],
        pumpswap_programs: vec![],
    };
    f.inbox.configure_replay_scope(&scope)?;
    f.inbox.persist(
        &checkpoint("origin", 0, &scope, vec![]),
        &CandidateGeneration::Unknown,
    )?;
    let source = facts("leaderbuy", "leader", true);
    f.admit(source.clone())?;
    f.terminal(&source, 0, 42, &hash(3))?;
    let our = facts(&f.our, "execution-wallet", true);
    f.admit(our.clone())?;
    f.event(
        DeliveryEvent::Terminal {
            signature: f.our.clone(),
            expected: our.clone(),
            result: Terminal::Unresolved(Unresolved::SessionReset),
        },
        CandidateGeneration::Unknown,
    )?;
    let sell = facts("sell", "leader", false);
    f.admit(sell.clone())?;
    f.terminal(&sell, 2, 42, &hash(3))?;
    f.drain()?;
    pair(&f, 0)?;
    let first = f.read()?.first;
    assert!(!f.inbox.has_sell_preparation_work()?);
    let mut recovered = our.clone();
    recovered.message_time = MessageTime::RecoveredBlock { block_time: None };
    for (sequence, event) in [
        (
            100,
            DeliveryEvent::Session(SessionGap::StartedContinuityUnknown),
        ),
        (101, DeliveryEvent::Admission(recovered.clone())),
        (
            102,
            DeliveryEvent::Terminal {
                signature: f.our.clone(),
                expected: recovered,
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: 42,
                    blockhash: hash(3),
                    signature: f.our.clone(),
                    transaction_index: 1,
                    block_time: BlockTime::Missing,
                }),
            },
        ),
    ] {
        f.inbox.persist(
            &Delivery {
                session: "recovered".into(),
                sequence,
                arrival_offset_ns: sequence,
                event,
            },
            &CandidateGeneration::Unknown,
        )?;
    }
    f.drain()?;
    pair(&f, 0)?;
    assert!(!f.inbox.has_sell_preparation_work()?);
    assert!(!f.inbox.take_ordered_sell_wake());
    let d = checkpoint(
        "recovered",
        103,
        &scope,
        vec![CheckpointClaim {
            signature: f.our.clone(),
            transaction_index: 1,
            info: our.info,
        }],
    );
    f.inbox.persist(&d, &CandidateGeneration::Unknown)?;
    f.drain()?;
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::ProviderOrderedWithinBlock
    );
    assert_eq!(f.read()?.first, first);
    pair(&f, 1)?;
    assert!(f.inbox.take_ordered_sell_wake());
    assert!(!f.inbox.take_ordered_sell_wake());
    f.inbox.persist(&d, &CandidateGeneration::Unknown)?;
    f.drain()?;
    pair(&f, 1)?;
    assert!(!f.inbox.take_ordered_sell_wake());
    Ok(())
}
