#[path = "common/b95_shadow.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{shadow_lot_origin::schema, SqliteStore};
use f::*;
#[test]
fn b95_origin_cannot_rebind_by_replay_update_replace_or_restart() -> Result<()> {
    let mut f = within()?;
    let id = lot(&mut f, "origin", 42, "block", 4)?;
    let original = f.db.store.shadow_lot_origin(id)?.unwrap();
    for sql in ["UPDATE shadow_lot_origins SET signal_id='substitute'",
        "DELETE FROM shadow_lot_origins",
        "INSERT OR REPLACE INTO shadow_lot_origins SELECT lot_id,signal_id,origin FROM shadow_lot_origins"] {
        assert!(f.db.conn()?.execute_batch(sql).is_err());
    }
    assert!(insert(&f, &facts("origin", "leader", true)).is_err());
    assert_eq!(f.db.store.list_shadow_lots("leader", "mint")?.len(), 1);
    reopen(&mut f)?;
    assert_eq!(f.db.store.shadow_lot_origin(id)?.unwrap(), original);
    Ok(())
}
#[test]
fn b95_storage_rejects_substituted_anchor_or_signal_and_keeps_existing_lot() -> Result<()> {
    let mut f = within()?;
    let id = lot(&mut f, "origin", 42, "block", 4)?;
    let mut a = facts("other-origin", "leader", true);
    f.admit(a.clone())?;
    a.facts.amount_out_bits = 8.0f64.to_bits();
    assert!(insert(&f, &a).is_err());
    let swap = swap(&f, &facts("origin", "leader", true));
    assert!(f
        .db
        .store
        .insert_shadow_buy_lot(
            &swap,
            "shadow:wrong:leader:buy:mint",
            3.5,
            Some(TokenQuantity::new(3500, 3)),
            0.000001
        )
        .is_err());
    assert_eq!(f.db.store.list_shadow_lots("leader", "mint")?.len(), 1);
    assert_eq!(
        f.db.store.shadow_lot_origin(id)?.unwrap().signature,
        "origin"
    );
    Ok(())
}
#[test]
fn b95_schema_guard_rejects_missing_or_altered_origin_objects() -> Result<()> {
    for object in [
        "shadow_lot_origin_no_replace",
        "shadow_lot_origin_no_update",
        "idx_shadow_lots_pair_qty_id",
    ] {
        let f = within()?;
        let kind = if object.starts_with("idx") {
            "INDEX"
        } else {
            "TRIGGER"
        };
        f.db.conn()?
            .execute_batch(&format!("DROP {kind} {object}"))?;
        assert!(f.read().is_err());
        let mut store = SqliteStore::open(&f.db.path)?;
        assert!(store
            .run_migrations(std::path::Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../migrations"
            )))
            .is_err());
    }
    let f = within()?;
    f.db.conn()?.execute(
        "DELETE FROM schema_migrations WHERE version=?1",
        [schema::MIGRATION],
    )?;
    assert!(f.read().is_err());
    Ok(())
}
#[test]
fn b95_new_migration_fresh_install_idempotent_and_legacy_apis_unanchored() -> Result<()> {
    let mut f = within()?;
    assert_eq!(
        f.db.store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?,
        0
    );
    let id = f.db.store.insert_shadow_lot_exact_with_risk_context(
        "leader",
        "mint",
        1.0,
        None,
        1.0,
        "quarantined_legacy",
        f.db.now,
    )?;
    assert!(f.db.store.shadow_lot_origin(id)?.is_none());
    assert_eq!(
        f.db.store.list_shadow_lots("leader", "mint")?[0].risk_context,
        "quarantined_legacy"
    );
    assert_eq!(
        evidence(&f)?.lots[0].relation,
        copybot_storage_core::association_sell_shadow_types::ShadowRelation::Unknown(
            copybot_storage_core::association_sell_preparation::Reason::MissingShadowOrigin
        )
    );
    Ok(())
}
