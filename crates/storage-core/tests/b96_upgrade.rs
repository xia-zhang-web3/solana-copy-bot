use anyhow::Result;
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    ordered_source_sell::*,
    SqliteStore,
};
#[test]
#[ignore = "requires actual accepted95 executable exported 0071 database"]
fn b96_actual_0071_to_0072_reopen_preserves_first_legacy_and_old_json() -> Result<()> {
    let input = std::path::PathBuf::from(std::env::var("B96_0071_DB")?);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("upgrade.sqlite");
    std::fs::copy(input, &path)?;
    let c = rusqlite::Connection::open(&path)?;
    let first: String = c.query_row(
        "SELECT first_binding FROM association_sell_preparations WHERE signature='sell'",
        [],
        |r| r.get(0),
    )?;
    let initial: String = c.query_row(
        "SELECT initial_evaluation FROM association_sell_preparations WHERE signature='sell'",
        [],
        |r| r.get(0),
    )?;
    assert!(!initial.contains("\"shadow\""));
    assert_eq!(
        c.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
            .get::<_, String>(
            0
        ))?,
        "0071_shadow_lot_origins.sql"
    );
    let l = InboxLimits {
        count: 1000,
        bytes: 8 << 20,
        busy_ms: 100,
    };
    let mut pre = AssociationInbox::open(&path, l)?;
    assert!(pre
        .stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)
        .is_err());
    drop(pre);
    let mut store = SqliteStore::open(&path)?;
    let legacy = store
        .load_execution_source_sell_intent("source-sell:legacy-sell")?
        .unwrap();
    let migrations = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    assert_eq!(store.run_migrations(migrations)?, 1);
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(migrations)?, 0);
    let after = store
        .load_execution_source_sell_intent("source-sell:legacy-sell")?
        .unwrap();
    assert_eq!(after.intent_id, legacy.intent_id);
    assert_eq!(after.event.ts_utc, legacy.event.ts_utc);
    assert_eq!(after.buy_witness, legacy.buy_witness);
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM source_sell_signature_claims",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0,
        "no historical backfill"
    );
    let mut inbox = AssociationInbox::open(&path, l)?;
    assert_eq!(
        c.query_row(
            "SELECT first_binding FROM association_sell_preparations WHERE signature='sell'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        first
    );
    assert_eq!(
        c.query_row(
            "SELECT initial_evaluation FROM association_sell_preparations WHERE signature='sell'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        initial
    );
    assert!(inbox
        .sell_preparation("sell")?
        .unwrap()
        .historical_initial
        .shadow
        .is_none());
    let actual = inbox.stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)?;
    let OrderedSellStage::Inserted(i) = actual else {
        panic!("upgrade actual {actual:?}")
    };
    drop(inbox);
    let inbox = AssociationInbox::open(&path, l)?;
    assert_eq!(
        inbox.revalidate_ordered_source_sell_intent(&i.intent_id)?,
        OrderedSellDecision::ValidatedNow
    );
    c.execute(
        "DELETE FROM execution_source_sell_intents WHERE intent_id='source-sell:legacy-sell'",
        [],
    )?;
    assert_eq!(
        c.query_row(
            "SELECT owner FROM source_sell_signature_claims WHERE signature='legacy-sell'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "legacy"
    );
    Ok(())
}
