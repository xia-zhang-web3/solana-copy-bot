#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::AssociationInbox, association_sell_preparation::*};
use f::*;
#[test]
fn b90_trigger_ignore_every_new_insert_prevents_ack_and_rolls_back() -> Result<()> {
    for table in [
        "association_sell_preparations",
        "association_sell_dependencies",
        "association_sell_work",
    ] {
        let mut f = F::new()?;
        f.anchors()?;
        f.drain()?;
        let before = f.inbox.usage()?;
        let sql = f.db.conn()?;
        sql.execute_batch(&format!(
            "CREATE TRIGGER injected BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END;"
        ))?;
        assert!(f.admit(facts("sell", "leader", false)).is_err(), "{table}");
        assert!(f.inbox.identity("sell")?.is_none());
        assert!(f.inbox.sell_preparation("sell")?.is_none());
        assert_eq!(f.inbox.usage()?, before);
    }
    Ok(())
}
#[test]
fn b90_trigger_ignore_latest_and_late_pin_and_cursor_prevents_ack() -> Result<()> {
    for target in ["latest_evaluation", "first_identity", "after_signature"] {
        let mut f = F::new()?;
        f.sell()?;
        f.drain()?;
        let table = match target {
            "latest_evaluation" => "association_sell_preparations",
            "first_identity" => "association_sell_dependencies",
            _ => "association_sell_work",
        };
        f.db.conn()?.execute_batch(&format!("CREATE TRIGGER ignored BEFORE UPDATE OF {target} ON {table} BEGIN SELECT RAISE(IGNORE); END;"))?;
        let a = facts("leaderbuy", "leader", true);
        assert!(f.admit(a).is_err(), "{target}");
        assert!(f.inbox.identity("leaderbuy")?.is_none());
    }
    Ok(())
}
#[test]
fn b90_preparation_schema_missing_corrupt_and_deleted_dependency_fail_closed() -> Result<()> {
    for ddl in [
        "DROP TABLE association_sell_preparations",
        "DROP INDEX association_sell_dependency_anchor",
        "DROP TRIGGER association_sell_first_immutable",
        "DELETE FROM schema_migrations WHERE version='0068_association_sell_preparation.sql'",
    ] {
        let f = F::new()?;
        f.db.conn()?.execute_batch(ddl)?;
        assert!(AssociationInbox::open(&f.db.path, limits()).is_err());
    }
    let mut f = F::new()?;
    f.anchors()?;
    f.sell()?;
    f.db.conn()?
        .execute_batch("DROP TRIGGER association_sell_dependency_no_delete")?;
    f.db.conn()?.execute(
        "DELETE FROM association_sell_dependencies WHERE anchor_signature='leaderbuy'",
        [],
    )?;
    assert!(f.read().is_err());
    Ok(())
}
#[test]
fn b90_historical_89_without_binding_stays_unknown_and_bootstrap_is_resumable() -> Result<()> {
    let mut f = F::new()?;
    // Emulate a row written by accepted89 before0068 was installed: no new
    // binding has ever existed. Migration recovery must not invent that snapshot.
    let a = facts("sell", "leader", false);
    let candidate = f.db.store.association_candidate(&a.facts);
    f.db.conn()?.execute("INSERT INTO association_inbox_identities(signature,admission,candidate,first_session,first_sequence,conflict,recovery) VALUES('sell',?1,?2,'historical89',0,0,0)",rusqlite::params![serde_json::to_string(&a)?,serde_json::to_string(&candidate)?])?;
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    f.inbox.recover_sell_preparation()?;
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    f.drain()?;
    let p = f.read()?;
    assert_eq!(
        p.first.witness,
        FirstWitness::Unknown(Reason::HistoricalNoSnapshot)
    );
    assert_eq!(p.first.app_dequeue_clock, None);
    assert_eq!(
        p.current.selected_chain,
        Check::Unknown(Reason::HistoricalNoSnapshot)
    );
    Ok(())
}
#[test]
fn b90_bounded_dependencies_durable_cursor_late_anchor_does_not_wait_for_whole_queue() -> Result<()>
{
    let mut f = F::new()?;
    for n in 0..7 {
        let a = facts(&format!("sell-{n}"), "leader", false);
        f.admit(a.clone())?;
        f.terminal(&a, 3, 42, "block")?;
    }
    f.drain()?;
    let source = facts("leaderbuy", "leader", true);
    f.admit(source.clone())?; // this ACK commits one of seven dependent evaluations
    let pending: i64 =
        f.db.conn()?
            .query_row("SELECT count(*) FROM association_sell_work", [], |r| {
                r.get(0)
            })?;
    assert!(pending > 0);
    assert!(f.inbox.identity("leaderbuy")?.is_some());
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    let steps = f.drain()?;
    assert!(steps >= 6);
    let count:i64=f.db.conn()?.query_row("SELECT count(*) FROM association_sell_dependencies WHERE anchor_signature='leaderbuy' AND first_identity IS NOT NULL",[],|r|r.get(0))?;
    assert_eq!(count, 7);
    Ok(())
}
#[test]
fn b90_count_and_bytes_include_preparation_dependencies_and_continuation_n_plus_one() -> Result<()>
{
    let mut f = F::new()?;
    f.anchors()?;
    f.drain()?;
    // Same physical preimages and fixed dequeue clock prevent unrelated length drift.
    let base = tempfile::tempdir()?;
    let snap = base.path().join("base.sqlite");
    f.db.conn()?
        .execute("VACUUM INTO ?1", [snap.to_string_lossy().as_ref()])?;
    let before = f.inbox.usage()?;
    f.admit(facts("sell", "leader", false))?;
    let (n, b) = f.inbox.usage()?;
    assert!(n > before.0 + 2 && b > before.1);
    for (count, bytes, success) in [(n, b, true), (n - 1, b, false), (n, b - 1, false)] {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("copy.sqlite");
        std::fs::copy(&snap, &path)?;
        let mut inbox = AssociationInbox::open(
            &path,
            copybot_storage_core::association_inbox::InboxLimits {
                count,
                bytes,
                ..limits()
            },
        )?;
        let a = facts("sell", "leader", false);
        let candidate = f.db.store.association_candidate(&a.facts);
        let d = Delivery {
            session: "synthetic".into(),
            sequence: f.seq - 1,
            arrival_offset_ns: f.seq - 1,
            event: DeliveryEvent::Admission(a),
        };
        let result = inbox.persist_at(
            &d,
            &candidate,
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        );
        assert_eq!(result.is_ok(), success, "{result:?}");
        assert_eq!(inbox.usage()?, if success { (n, b) } else { before });
    }
    Ok(())
}
