#[path = "common/association_parent_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::AssociationInbox, association_sell_preparation::*};
use f::*;
#[test]
fn b91_graph_insert_ignore_rejects_before_ack_and_rolls_back() -> Result<()> {
    for table in ["association_parent_blocks", "association_parent_hashes"] {
        let mut f = F::new()?;
        let before = f.inbox.usage()?;
        let financial = f.db.snapshot()?;
        f.db.conn()?.execute_batch(&format!(
            "CREATE TRIGGER ignored BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END;"
        ))?;
        assert!(put(&mut f, graph()[0].clone()).is_err(), "{table}");
        assert_eq!(f.inbox.usage()?, before);
        assert_eq!(f.db.snapshot()?, financial);
    }
    for table in ["association_parent_dependencies", "association_parent_work"] {
        let mut f = F::new()?;
        put(&mut f, graph()[0].clone())?;
        f.db.conn()?.execute_batch(&format!(
            "CREATE TRIGGER ignored BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END;"
        ))?;
        assert!(anchors(&mut f, false).is_err(), "{table}");
        assert!(f.inbox.identity("sell")?.unwrap().terminal.is_none());
        assert_eq!(
            f.db.conn()?
                .query_row("SELECT count(*) FROM association_inbox_events", [], |r| r
                    .get::<_, i64>(
                    0
                ))?,
            6
        );
    }
    Ok(())
}
#[test]
fn b91_graph_update_ignore_never_acks_conflict_or_cursor_change() -> Result<()> {
    for table in [
        "association_parent_blocks",
        "association_parent_hashes",
        "association_parent_work",
    ] {
        let mut f = F::new()?;
        ready(&mut f)?;
        let before = f.inbox.usage()?;
        f.db.conn()?.execute_batch(&format!(
            "CREATE TRIGGER ignored BEFORE UPDATE ON {table} BEGIN SELECT RAISE(IGNORE); END;"
        ))?;
        let changed = if table == "association_parent_hashes" {
            edge(key(43, 3), key(30, 1))
        } else {
            edge(key(42, 3), key(31, 57))
        };
        assert!(put(&mut f, changed).is_err(), "{table}");
        assert_eq!(f.inbox.usage()?, before);
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::ProviderOrderedAcrossBlocks
        );
    }
    Ok(())
}
#[test]
fn b91_missing_graph_schema_and_forged_validation_tag_fail_closed() -> Result<()> {
    for sql in [
        "DROP TABLE association_parent_blocks",
        "DROP INDEX association_parent_dependency_hash",
        "DROP INDEX association_parent_pending",
        "DROP TRIGGER association_parent_first_immutable",
        "DELETE FROM schema_migrations WHERE version='0069_association_parent_graph.sql'",
    ] {
        let f = F::new()?;
        f.db.conn()?.execute_batch(sql)?;
        assert!(AssociationInbox::open(&f.db.path, limits()).is_err());
    }
    let mut f = F::new()?;
    let before = f.inbox.usage()?;
    let mut malformed = graph()[0].clone();
    malformed.parent.hash = "0".repeat(32);
    assert!(put(&mut f, malformed).is_err());
    assert_eq!(f.inbox.usage()?, before);
    Ok(())
}
#[test]
fn b91_duplicate_edge_is_idempotent_and_first_evidence_cannot_be_deleted() -> Result<()> {
    let mut f = F::new()?;
    ready(&mut f)?;
    let c = f.db.conn()?;
    let before: Vec<(String, String)> = c
        .prepare(
            "SELECT block_key,first_observation FROM association_parent_blocks ORDER BY block_key",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for e in graph() {
        put(&mut f, e)?;
    }
    f.drain()?;
    let after: Vec<(String, String)> = c
        .prepare(
            "SELECT block_key,first_observation FROM association_parent_blocks ORDER BY block_key",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(before, after);
    for table in [
        "association_parent_blocks",
        "association_parent_hashes",
        "association_parent_dependencies",
        "association_parent_work",
    ] {
        assert!(c.execute(&format!("DELETE FROM {table}"), []).is_err());
    }
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::ProviderOrderedAcrossBlocks
    );
    Ok(())
}
#[test]
fn b91_position_replacement_does_not_rebind_even_when_parent_paths_are_complete() -> Result<()> {
    let mut f = F::new()?;
    ready(&mut f)?;
    let first = f.read()?.first;
    let c = f.db.conn()?;
    c.execute("UPDATE positions SET state='closed' WHERE token='mint'", [])?;
    c.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,accounting_bucket) VALUES('replacement-B','mint',1,1,'2026-09-09T00:00:01Z','open','execution_canary')",[])?;
    let before = f.db.snapshot()?;
    for e in graph() {
        put(&mut f, e)?;
    }
    f.drain()?;
    let p = f.read()?;
    assert_eq!(p.first, first);
    assert_eq!(
        p.current.selected_chain,
        Check::Blocked(Reason::GenerationChanged)
    );
    assert_eq!(f.db.snapshot()?, before);
    Ok(())
}
