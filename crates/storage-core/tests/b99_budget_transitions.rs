#[path = "common/buy_attribution_fixture.rs"]
mod f;
#[path = "common/b99_slow_oracle.rs"]
mod oracle;
use anyhow::Result;
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
use rusqlite::params;
fn limits() -> InboxLimits {
    InboxLimits {
        count: 10000,
        bytes: 128 << 20,
        busy_ms: 10,
    }
}
fn check(i: &AssociationInbox, c: &rusqlite::Connection, strict: bool) -> Result<()> {
    assert_eq!(i.usage()?, oracle::usage(c, strict)?);
    Ok(())
}
#[test]
fn b99_all_twelve_domains_utf8_nul_nullable_growth_shrink_and_reservations() -> Result<()> {
    let db = f::Db::new()?;
    let i = AssociationInbox::open_ordered_sell_consumer(&db.path, limits())?;
    let observation = AssociationInbox::open(&db.path, limits())?;
    let c = db.conn()?;
    let text = "é\0末";
    for (label, sql) in [
        ("identity", "INSERT INTO association_inbox_identities(signature,admission,candidate,first_session,first_sequence,terminal,conflict,recovery) VALUES('id',?1,?1,?1,0,NULL,0,0)"),
        ("event", "INSERT INTO association_inbox_events VALUES(?1,0,?1)"),
        ("preparation", "INSERT INTO association_sell_preparations VALUES('sell',1,?1,?1,?1,'trade_authority_none')"),
        ("dependency", "INSERT INTO association_sell_dependencies VALUES('sell',?1,NULL)"),
        ("sell work", "INSERT INTO association_sell_work VALUES(?1,'')"),
        ("parent block", "INSERT INTO association_parent_blocks VALUES(?1,?1,?1,0,NULL)"),
        ("parent hash", "INSERT INTO association_parent_hashes VALUES(?1,?1,NULL)"),
        ("parent dependency", "INSERT INTO association_parent_dependencies VALUES('sell',?1)"),
        ("parent work", "INSERT INTO association_parent_work VALUES(?1,'',1)"),
        ("intent", "INSERT INTO ordered_source_sell_intents VALUES('source-sell:strict','strict',1,'provider_order_strict_v1',json_quote(?1))"),
        ("claim", "INSERT INTO source_sell_signature_claims VALUES(?1,'provider_order_strict_v1','source-sell:'||?1)"),
    ] {
        c.execute(sql, [text])?;
        check(&i, &c, true).map_err(|e| anyhow::anyhow!("{label}: {e:#}"))?;
        check(&observation, &c, false)?;
    }
    // Bootstrap existed before the identity; every domain is now populated.
    assert_eq!(i.usage()?.0, 12);
    assert_eq!(observation.usage()?.0, 10);
    let unchanged = i.usage()?;
    c.execute(
        "INSERT INTO source_sell_signature_claims VALUES('legacy','legacy','source-sell:legacy')",
        [],
    )?;
    assert_eq!(i.usage()?, unchanged);
    let long = "長\0".repeat(41);
    let before = i.usage()?;
    c.execute("INSERT INTO association_inbox_identities(signature,admission,candidate,first_session,first_sequence,terminal,conflict,recovery) VALUES(?1,'','','',1,NULL,0,0)", [&long])?;
    assert_eq!(i.usage()?.1 - before.1, 512 + long.len() + long.len() - 2);
    check(&i, &c, true)?;
    let before = i.usage()?;
    c.execute(
        "INSERT INTO association_sell_dependencies VALUES(?1,?2,NULL)",
        params![long, text],
    )?;
    assert_eq!(
        i.usage()?.1 - before.1,
        512 + 2 * long.len() + 2 * text.len() + long.len() - 4
    );
    check(&i, &c, true)?;
    c.execute(
        "INSERT INTO association_parent_dependencies VALUES(?1,?2)",
        params![long, text],
    )?;
    check(&i, &c, true)?;
    for payload in [&long, text, ""] {
        c.execute(
            "UPDATE association_sell_preparations SET latest_evaluation=?1",
            [payload],
        )?;
        check(&i, &c, true)?;
    }
    for sql in [
        "UPDATE association_inbox_identities SET terminal=?1 WHERE signature='id'",
        "UPDATE association_sell_dependencies SET first_identity=?1 WHERE sell_signature='sell'",
        "UPDATE association_parent_blocks SET contradiction=?1",
        "UPDATE association_parent_hashes SET contradiction_slot=?1",
        "UPDATE association_sell_work SET after_signature=?1",
        "UPDATE association_parent_work SET after_signature=?1,pending=0",
        "UPDATE association_sell_bootstrap SET after_signature=?1,complete=1",
    ] {
        c.execute(sql, [&long])?;
        check(&i, &c, true)?;
    }
    let reserved = i.usage()?;
    c.execute_batch("UPDATE association_sell_work SET after_signature=''; UPDATE association_parent_work SET after_signature='',pending=1; UPDATE association_sell_bootstrap SET after_signature='',complete=0;")?;
    assert_eq!(i.usage()?, reserved);
    c.execute("DELETE FROM association_sell_work", [])?;
    assert_eq!(i.usage()?.0, reserved.0 - 1);
    check(&i, &c, true)?;
    // Completed parent work is retained and charged.
    c.execute("UPDATE association_parent_work SET pending=0", [])?;
    check(&i, &c, true)?;
    let before = i.usage()?;
    c.execute(
        "INSERT OR IGNORE INTO association_inbox_events VALUES(?1,0,'ignored')",
        [text],
    )?;
    assert_eq!(i.usage()?, before);
    Ok(())
}
#[test]
fn b99_two_open_consumers_observe_only_committed_index_movements() -> Result<()> {
    let db = f::Db::new()?;
    db.conn()?.pragma_update(None, "journal_mode", "WAL")?;
    let a = AssociationInbox::open(&db.path, limits())?;
    let b = AssociationInbox::open(&db.path, limits())?;
    let c = db.conn()?;
    let d = db.conn()?;
    for n in 0..6 {
        let w = if n % 2 == 0 { &c } else { &d };
        let before = a.usage()?;
        w.execute_batch("BEGIN IMMEDIATE")?;
        w.execute(
            "INSERT INTO association_inbox_events VALUES('external',?1,'é'||char(0)||'last')",
            [n],
        )?;
        assert_eq!(a.usage()?, before);
        assert_eq!(b.usage()?, before);
        w.execute_batch(if n % 3 == 0 { "ROLLBACK" } else { "COMMIT" })?;
        check(&a, &c, false)?;
        check(&b, &c, false)?;
    }
    let fresh = AssociationInbox::open(&db.path, limits())?;
    check(&fresh, &c, false)?;
    Ok(())
}
