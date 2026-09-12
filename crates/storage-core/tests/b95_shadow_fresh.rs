#[path = "common/b95_shadow.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{association_sell_preparation::*, association_sell_shadow_types::*};
use f::*;
#[test]
fn b95_fresh_add_risk_close_other_pairs_and_empty_complete_after_reopen() -> Result<()> {
    let mut f = within()?;
    assert_eq!(
        evidence(&f)?,
        ShadowEvidence {
            scan: ShadowScan::Complete,
            lots: vec![]
        }
    );
    for (w, t) in [("other", "mint"), ("leader", "other")] {
        f.db.store.insert_shadow_lot(w, t, 1.0, 1.0, f.db.now)?;
    }
    assert!(evidence(&f)?.lots.is_empty());
    let first = f.read()?.first;
    let id = lot(&mut f, "origin", 42, "block", 4)?;
    reopen(&mut f)?;
    assert_eq!(evidence(&f)?.lots[0].lot_id, id);
    f.db.conn()?.execute(
        "UPDATE shadow_lots SET risk_context='quarantined_legacy',qty=1.5 WHERE id=?1",
        [id],
    )?;
    let e = evidence(&f)?;
    assert_eq!(e.lots[0].risk_context, "quarantined_legacy");
    assert_eq!(e.lots[0].qty_bits, 1.5f64.to_bits());
    f.db.store
        .close_shadow_lots_fifo_atomic_exact("close", "leader", "mint", 3.5, None, 1.0, f.db.now)?;
    reopen(&mut f)?;
    assert!(evidence(&f)?.lots.is_empty());
    assert_eq!(f.read()?.first, first);
    assert!(
        f.db.store.shadow_lot_origin(id)?.is_some(),
        "origin survives lot deletion"
    );
    Ok(())
}
#[test]
fn b95_count_and_bytes_never_report_partial_scan_as_empty_complete() -> Result<()> {
    for many in [true, false] {
        let f = within()?;
        if many {
            for _ in 0..1001 {
                f.db.store
                    .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
            }
        } else {
            f.db.store
                .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
            f.db.conn()?.execute(
                "UPDATE shadow_lots SET risk_context=?1",
                ["x".repeat(8 << 20)],
            )?;
        }
        let e = evidence(&f)?;
        assert!(matches!(
            e.scan,
            ShadowScan::Unknown(Reason::LookupBound | Reason::ParentTraversalBound)
        ));
        assert!(e.lots.is_empty());
    }
    Ok(())
}
#[test]
fn b95_shared_parent_traversal_budget_cannot_prove_after() -> Result<()> {
    let mut f = F::new()?;
    ready(&mut f)?;
    lot(&mut f, "origin", 1000, &hash(8), 0)?;
    // Use a smaller read budget after the fixture was persisted. Inbox itself still fits.
    let c = f.db.conn()?;
    let hash_n = |n: u64| {
        let alphabet = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
        let mut v = n;
        let mut suffix = vec![];
        while v > 0 {
            suffix.push(alphabet[(v % 58) as usize] as char);
            v /= 58;
        }
        format!(
            "{}{}",
            "1".repeat(32 - (64 - n.leading_zeros() as usize).div_ceil(8)),
            suffix.iter().rev().collect::<String>()
        )
    };
    let minimum = f.inbox.usage()?.0;
    for n in 61..=1000 {
        let child = copybot_core_types::association_parent::BlockKey {
            slot: n,
            hash: hash_n(n),
        };
        // A direct controlled DB insert tests traversal cap without enqueuing huge histories.
        let parent = if n == 61 {
            key(60, 6)
        } else {
            copybot_core_types::association_parent::BlockKey {
                slot: n - 1,
                hash: hash_n(n - 1),
            }
        };
        let child = if n == 1000 { key(1000, 8) } else { child };
        let e = edge(child.clone(), parent);
        c.execute("INSERT INTO association_parent_blocks(block_key,first_observation,first_session,first_sequence) VALUES(?1,?2,'bounds',1)",rusqlite::params![serde_json::to_string(&child)?,serde_json::to_string(&e)?])?;
    }
    // Open would enforce durable inbox budget, so keep its original 1000-unit reader;
    // repeated node/path charges exhaust before reaching SELL.
    assert!(minimum < 1000);
    let e = evidence(&f)?;
    assert!(
        e.scan != ShadowScan::Complete
            || e.lots[0].relation == ShadowRelation::Unknown(Reason::ParentTraversalBound)
    );
    Ok(())
}
#[test]
fn b95_new_evidence_roundtrip_and_old_json_are_historical_unknown() -> Result<()> {
    let mut f = within()?;
    lot(&mut f, "origin", 42, "block", 4)?;
    f.drain()?;
    f.admit(facts("sell", "leader", false))?;
    let current = f.read()?.current;
    let mut wire = serde_json::to_value(&current)?;
    let round: Evaluation = serde_json::from_value(wire.clone())?;
    assert_eq!(round, current);
    wire.as_object_mut().unwrap().remove("shadow");
    let old: Evaluation = serde_json::from_value(wire)?;
    assert!(old.shadow.is_none());
    assert!(!serde_json::to_string(&old)?.contains("\"shadow\""));
    reopen(&mut f)?;
    assert_eq!(f.read()?.historical_latest.shadow, current.shadow);
    assert_eq!(f.read()?.current.shadow, current.shadow);
    assert_eq!(current.trade_authority, "trade_authority_none");
    Ok(())
}
