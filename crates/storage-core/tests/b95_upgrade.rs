#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{
    association_inbox::AssociationInbox, association_sell_preparation::*,
    shadow_lot_origin::schema, SqliteStore,
};
use rusqlite::Connection;
use std::path::Path;

fn old_db(path: &Path) -> Result<()> {
    if let Ok(source) = std::env::var("B95_PRE_FIX_DB") {
        std::fs::copy(source, path)?;
        return Ok(());
    }
    let migrations = tempfile::tempdir()?;
    for e in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"))? {
        let e = e?;
        let name = e.file_name();
        if name.to_string_lossy().ends_with(".sql")
            && name.to_string_lossy().as_ref() < schema::MIGRATION
        {
            std::fs::copy(e.path(), migrations.path().join(name))?;
        }
    }
    let mut s = SqliteStore::open(path)?;
    s.run_migrations(migrations.path())?;
    drop(s);
    let c = Connection::open(path)?;
    let tx = c.unchecked_transaction()?;
    // sqlite dump order is alphabetical, not foreign-key order; validate at commit.
    tx.pragma_update(None, "defer_foreign_keys", true)?;
    tx.execute_batch(include_str!("fixtures/b95_accepted93r1_data.sql"))?;
    tx.commit()?;
    assert_eq!(
        c.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
            .get::<_, i64>(
            0
        ))?,
        0
    );
    Ok(())
}
#[test]
fn b95_true_accepted93r1_upgrade_reopen_replay_preserves_first_and_fingerprint_invalidation(
) -> Result<()> {
    let d = tempfile::tempdir()?;
    let path = d.path().join("upgrade.sqlite");
    old_db(&path)?;
    let c = Connection::open(&path)?;
    assert_eq!(
        c.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
            .get::<_, String>(
            0
        ))?,
        "0070_owned_sell_amount_proof.sql"
    );
    let (first,initial,latest):(String,String,String)=c.query_row("SELECT first_binding,initial_evaluation,latest_evaluation FROM association_sell_preparations WHERE signature='sell'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let before: FirstBinding = serde_json::from_str(&first)?;
    let history: Evaluation = serde_json::from_str(&latest)?;
    assert_eq!(before.pending_buys.len(), 1);
    assert!(history.shadow.is_none());
    assert!(matches!(before.witness, FirstWitness::Selected(_)));
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?,
        1
    );
    drop(store);
    let mut inbox = AssociationInbox::open(&path, f::limits())?;
    let actual = inbox.sell_preparation("sell")?.unwrap();
    assert_eq!(actual.first, before);
    assert!(actual.current.pending_buys.is_empty());
    assert_ne!(
        actual.current.contributors_fingerprint,
        before.contributors_fingerprint
    );
    assert_eq!(
        actual.current.selected_chain,
        Check::Blocked(Reason::FinancialSetChanged)
    );
    // Re-deliver the exact old persisted event through the actual idempotent writer.
    let wire: String = c.query_row(
        "SELECT delivery FROM association_inbox_events ORDER BY sequence LIMIT 1",
        [],
        |r| r.get(0),
    )?;
    let delivery: Delivery = serde_json::from_str(&wire)?;
    let candidate = match &delivery.event {
        DeliveryEvent::Admission(a) => inbox.identity(&a.facts.signature)?.unwrap().candidate,
        _ => CandidateGeneration::Unknown,
    };
    inbox.persist(&delivery, &candidate)?;
    while inbox.has_sell_preparation_work()? {
        inbox.recover_sell_preparation()?;
    }
    drop(inbox);
    let inbox = AssociationInbox::open(&path, f::limits())?;
    let p = inbox.sell_preparation("sell")?.unwrap();
    assert_eq!(p.first, before);
    assert!(p.current.pending_buys.is_empty());
    assert_eq!(
        p.current.selected_chain,
        Check::Blocked(Reason::FinancialSetChanged)
    );
    let raw:(String,String)=c.query_row("SELECT first_binding,initial_evaluation FROM association_sell_preparations WHERE signature='sell'",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
    assert_eq!(raw, (first, initial));
    assert!(p.historical_initial.shadow.is_none());
    assert_eq!(p.current.trade_authority, "trade_authority_none");
    Ok(())
}
