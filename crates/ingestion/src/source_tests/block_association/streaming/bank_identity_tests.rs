//! Synthetic protocol fixtures; not historical Alpenglow or provider evidence.
use super::*;

#[test]
fn alpenglow_same_bank_duplicate_is_one_checked_admission() -> Result<()> {
    let (mut tx, mut block) = pair(true, false)?;
    tx_mut(&mut tx).bank_id = 41;
    block.bank_id = 41;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    assert_eq!(
        a.push(context(0), tx_input(&tx)),
        Ok(Admission::Transaction(ResultId(0)))
    );
    assert!(drain(&mut a).is_empty());
    assert_eq!(
        a.push(context(1), tx_input(&tx)),
        Ok(Admission::Duplicate(ResultId(0)))
    );
    assert!(drain(&mut a).is_empty());
    let out = feed(&mut a, 2, Input::Block(&block));
    assert!(matches!(
        terminal(&out[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    assert_eq!(out.len(), 1);
    assert!(feed(&mut a, 3, tx_input(&tx)).is_empty());
    tx_mut(&mut tx).bank_id = 42;
    assert_eq!(
        a.push(context(4), tx_input(&tx)),
        Ok(Admission::Duplicate(ResultId(0)))
    );
    assert!(matches!(
        &drain(&mut a)[..],
        [Outcome::Late {
            evidence: LateEvidence::ConflictingTransaction,
            ..
        }]
    ));
    Ok(())
}

#[test]
fn alpenglow_same_slot_other_bank_cannot_supply_raw_association() -> Result<()> {
    let (mut tx, mut block) = pair(true, false)?;
    tx_mut(&mut tx).bank_id = 41;
    block.bank_id = 42;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, tx_input(&tx));
    assert_eq!(
        associate_yellowstone_transaction(
            transaction(&tx),
            &block,
            &source.runtime_config.interested_program_ids,
            &source.runtime_config.raydium_program_ids,
            &source.runtime_config.pumpswap_program_ids
        )
        .unwrap_err(),
        Refusal::BankMismatch
    );
    assert!(feed(&mut a, 1, Input::Block(&block)).is_empty());
    block.bank_id = 41;
    block.blockhash = bs58::encode([81u8; 32]).into_string();
    let out = feed(&mut a, 2, Input::Block(&block));
    assert!(matches!(terminal(&out[0]).1,
        Resolution::ProviderAsserted { assertion, .. } if assertion.blockhash == block.blockhash));
    assert_eq!(out.len(), 1);
    Ok(())
}

#[test]
fn alpenglow_raw_signature_in_two_known_banks_is_conflict_not_second_admission() -> Result<()> {
    let (mut tx, _) = pair(true, false)?;
    tx_mut(&mut tx).bank_id = 41;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, tx_input(&tx));
    tx_mut(&mut tx).bank_id = 42;
    assert_eq!(
        a.push(context(1), tx_input(&tx)),
        Ok(Admission::Duplicate(ResultId(0)))
    );
    let out = drain(&mut a);
    unresolved(&out[0], UnresolvedReason::ConflictingTransaction);
    assert_eq!(out.len(), 1);
    assert!(feed(&mut a, 2, tx_input(&tx)).is_empty());
    Ok(())
}

#[test]
fn alpenglow_reused_bank_id_after_reconnect_never_joins_old_session_block() -> Result<()> {
    let (mut tx, mut block) = pair(true, false)?;
    tx_mut(&mut tx).bank_id = 41;
    block.bank_id = 41;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(&mut a, 0, Input::Block(&block));
    let next = Session {
        id: session().id,
        generation: 2,
    };
    feed(&mut a, 1, Input::Reset(next));
    assert_eq!(
        a.push(context(2), tx_input(&tx)),
        Err(Rejection::StaleSession)
    );
    let current = |ns| Context {
        session: next,
        offset: Duration::from_nanos(ns),
    };
    assert!(matches!(
        a.push(current(0), tx_input(&tx)),
        Ok(Admission::Transaction(_))
    ));
    assert!(
        drain(&mut a).is_empty(),
        "old connection's bank cannot resolve new input"
    );
    a.push(current(1), Input::Block(&block)).unwrap();
    let out = drain(&mut a);
    assert_eq!(out.len(), 1);
    assert!(matches!(
        terminal(&out[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    // Bank ids do not enter durable assertions/business identities: the actual
    // signature is unchanged; storage's existing cross-session idempotency wins.
    assert_eq!(
        terminal(&out[0]).0.facts.signature,
        bs58::encode(&transaction(&tx).transaction.as_ref().unwrap().signature).into_string()
    );
    Ok(())
}

#[test]
fn alpenglow_legacy_wire_without_additive_field_keeps_exact_info_path() -> Result<()> {
    let (mut update, mut block) = pair(true, false)?;
    tx_mut(&mut update).bank_id = 0;
    let wire = transaction(&update).encode_to_vec();
    let legacy = SubscribeUpdateTransaction::decode(wire.as_slice())?;
    assert_eq!(legacy.bank_id, 0);
    block.bank_id = 42;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    feed(
        &mut a,
        0,
        Input::Transaction(&legacy, YellowstoneMessageTime::from_created_at(None)),
    );
    let out = feed(&mut a, 1, Input::Block(&block));
    assert_eq!(out.len(), 1);
    assert!(matches!(
        terminal(&out[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    let mut additive = legacy;
    additive.bank_id = 42;
    let decoded = SubscribeUpdateTransaction::decode(additive.encode_to_vec().as_slice())?;
    assert_eq!(decoded.bank_id, 42);
    assert_eq!(decoded.transaction, transaction(&update).transaction);
    Ok(())
}

#[test]
fn alpenglow_bank_ids_are_not_cross_provider_checkpoint_identity() -> Result<()> {
    let (_, mut grpc) = pair(true, false)?;
    // pair() is a filtered association message. This comparison requires a
    // complete synthetic block with valid execution indices, not 100000/1.
    grpc.executed_transaction_count = grpc.transactions.len() as u64;
    for (index, info) in grpc.transactions.iter_mut().enumerate() {
        info.index = index as u64;
    }
    grpc.bank_id = 41;
    let mut http = grpc.clone();
    http.bank_id = 0;
    assert!(crate::source::http_recovery::identity::block_equivalent(
        &grpc, &http
    ));
    http.blockhash = bs58::encode([82u8; 32]).into_string();
    assert!(!crate::source::http_recovery::identity::block_equivalent(
        &grpc, &http
    ));
    Ok(())
}

#[tokio::test]
async fn alpenglow_reconnect_reused_bank_retains_one_sqlite_signature_identity() -> Result<()> {
    use super::super::delivery_tests as delivery;
    use copybot_core_types::association_delivery::{CandidateGeneration, DeliveryEvent};
    use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
    let (mut tx, mut block) = pair(true, false)?;
    tx_mut(&mut tx).bank_id = 41;
    block.bank_id = 41;
    let (events, failed) = delivery::run(
        delivery::policy(),
        vec![
            delivery::input(0, tx.clone()),
            delivery::input(1, delivery::block(block.clone())),
            crate::ReplayInput::Reset(2),
            delivery::input(3, tx),
            delivery::input(4, delivery::block(block)),
            crate::ReplayInput::End(5),
        ],
    )
    .await;
    assert!(!failed);
    let admissions: Vec<_> = events
        .iter()
        .filter_map(|d| match &d.event {
            DeliveryEvent::Admission(a) => Some((d.session.clone(), a.facts.signature.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        admissions.len(),
        2,
        "separate connection generations have separate observations"
    );
    assert_ne!(admissions[0].0, admissions[1].0);
    assert_eq!(admissions[0].1, admissions[1].1);
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("bank-identity.sqlite");
    let mut store = copybot_storage_core::SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    drop(store);
    let mut inbox = AssociationInbox::open(
        &path,
        InboxLimits {
            count: 20000,
            bytes: 128 << 20,
            busy_ms: 100,
        },
    )?;
    for event in &events {
        inbox.persist(event, &CandidateGeneration::Unknown)?;
    }
    let row = inbox.identity(&admissions[0].1)?.unwrap();
    assert_eq!(row.first_session, admissions[0].0);
    assert!(!row.conflict);
    let db = rusqlite::Connection::open(&path)?;
    let count: u64 = db.query_row(
        "SELECT count(*) FROM association_inbox_identities",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        count, 1,
        "reusing node-local bank id never creates a second business identity"
    );
    Ok(())
}
