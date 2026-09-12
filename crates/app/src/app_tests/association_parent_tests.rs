use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
};
use anyhow::Result;
use copybot_storage_core::association_sell_preparation::*;
#[tokio::test]
#[ignore = "explicit synthetic protobuf cross-block matrix"]
async fn b91_actual_mixed_forks_missing_malformed_and_index() -> Result<()> {
    for case in [
        "mixed-source",
        "mixed-sell",
        "fork",
        "missing",
        "malformed",
        "cycle",
        "wrong-index",
        "reversed-index",
        "endpoint-hash",
        "hash-slot",
    ] {
        let m = p::meta(case)?;
        let db = f::Db::new(&format!("b91-{case}"))?;
        s::seed(&db, &m)?;
        p::stage(&db, case, &m, p::frames(&m), "matrix", false).await?;
        let r = p::read(&db, &m)?;
        let expected = match case {
            "mixed-source" | "mixed-sell" => Check::ProviderOrderedAcrossBlocks,
            "hash-slot" => Check::Blocked(Reason::ParentHashSlotConflict),
            "reversed-index" => Check::Blocked(Reason::NonIncreasingIndex),
            "fork" | "endpoint-hash" => Check::Blocked(Reason::ParentBranchMismatch),
            "missing" => Check::Unknown(Reason::ParentMissingData),
            "malformed" | "cycle" => Check::Blocked(Reason::ParentMalformed),
            _ => Check::ProviderOrderedAcrossBlocks,
        };
        // Equal indices in different blocks are legal; the reversed-index case
        // separately exercises strict indices in one containing block.
        assert_eq!(r.current.selected_chain, expected, "{case}");
        if case.starts_with("mixed") {
            assert!(r.current.parent_paths.len() < 3);
        }
        p::save(&db, &m, "matrix")?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "late empty-block frontier through actual consumer with fresh adapter"]
async fn b91_actual_gap_late_edge_and_duplicate_subsets() -> Result<()> {
    let case = "multi";
    let m = p::meta(case)?;
    let db = f::Db::new("b91-gap")?;
    s::seed(&db, &m)?;
    let names = p::frames(&m)
        .into_iter()
        .filter(|n| n != "block-110")
        .collect();
    p::stage(&db, case, &m, names, "gap", false).await?;
    let r = p::read(&db, &m)?;
    assert_eq!(r.current.selected_chain, Check::Unknown(Reason::ParentGap));
    let first = r.first;
    p::save(&db, &m, "gap")?;
    p::stage(&db, case, &m, vec!["block-110".into()], "late", true).await?;
    let r = p::read(&db, &m)?;
    assert_eq!(r.first, first);
    assert_eq!(r.current.selected_chain, Check::ProviderOrderedAcrossBlocks);
    assert_eq!(r.current.parent_paths.len(), 3);
    p::save(&db, &m, "late")?;
    p::stage(
        &db,
        case,
        &m,
        vec!["duplicate-110".into(), "duplicate-120".into()],
        "duplicate",
        true,
    )
    .await?;
    let r = p::read(&db, &m)?;
    assert_eq!(r.first, first);
    assert_eq!(r.current.selected_chain, Check::ProviderOrderedAcrossBlocks);
    assert_eq!(
        db.sql
            .query_row("SELECT count(*) FROM association_parent_blocks", [], |r| {
                r.get::<_, i64>(0)
            })?,
        6
    );
    p::save(&db, &m, "duplicate")?;
    Ok(())
}
#[tokio::test]
#[ignore = "parent-only conflict each used edge after TTL reset and restart"]
async fn b91_actual_parent_only_conflicts_invalidate_historical_positive() -> Result<()> {
    for child in [110, 120, 130, 140, 150] {
        let case = "multi";
        let m = p::meta(case)?;
        let db = f::Db::new(&format!("b91-conflict-{child}"))?;
        s::seed(&db, &m)?;
        p::stage(&db, case, &m, p::frames(&m), "positive", false).await?;
        let r = p::read(&db, &m)?;
        assert_eq!(r.current.selected_chain, Check::ProviderOrderedAcrossBlocks);
        let first = r.first;
        let positive = serde_json::to_string(&r.current)?;
        p::stage(
            &db,
            case,
            &m,
            vec![format!("conflict-{child}")],
            "conflict",
            true,
        )
        .await?;
        // Force a historical cache positive: current API must traverse the DB snapshot.
        db.sql.execute(
            "UPDATE association_sell_preparations SET latest_evaluation=?1",
            [&positive],
        )?;
        let r = p::read(&db, &m)?;
        assert_eq!(r.first, first);
        assert_eq!(
            r.current.selected_chain,
            Check::Blocked(Reason::ParentConflict)
        );
        assert!(r.current.anchors.iter().all(|a| !a.conflict));
        p::stage(
            &db,
            case,
            &m,
            vec![format!("duplicate-{child}")],
            "correct-replay",
            true,
        )
        .await?;
        let r = p::read(&db, &m)?;
        assert_eq!(r.first, first);
        assert_eq!(
            r.current.selected_chain,
            Check::Blocked(Reason::ParentConflict)
        );
        p::save(&db, &m, "conflict")?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "first witness is immutable through real consumer revalidation"]
async fn b91_actual_unknown_first_and_a_to_b_keep_binding() -> Result<()> {
    for unknown in [true, false] {
        let case = "multi";
        let m = p::meta(case)?;
        let db = f::Db::new(&format!("b91-binding-{unknown}"))?;
        if !unknown {
            s::seed(&db, &m)?;
        }
        p::stage(&db, case, &m, p::frames(&m), "initial", false).await?;
        let first = p::read(&db, &m)?.first;
        if unknown {
            s::seed(&db, &m)?;
        } else {
            db.sql.execute("UPDATE positions SET state='closed'", [])?;
            db.position("position-B", m["our"]["token_out"].as_str().unwrap())?;
        }
        // The explicit fixture mutation above is outside the measured delivery interval.
        p::stage(&db, case, &m, vec!["duplicate-110".into()], "recheck", true).await?;
        let r = p::read(&db, &m)?;
        assert_eq!(r.first, first);
        assert_eq!(
            r.current.selected_chain,
            if unknown {
                Check::Unknown(Reason::InitialCandidateUnknown)
            } else {
                Check::Blocked(Reason::GenerationChanged)
            }
        );
        p::save(&db, &m, "binding")?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "positive and late parent conflict within the same actual adapter lifetime"]
async fn b91_actual_same_adapter_ttl_and_reset_do_not_erase_parent_evidence() -> Result<()> {
    use copybot_ingestion::ReplayInput;
    for reset in [false, true] {
        let m = p::meta("multi")?;
        let db = f::Db::new(&format!("b91-same-adapter-{reset}"))?;
        s::seed(&db, &m)?;
        let before = s::snapshot(&db)?;
        let config = f::config(&m);
        let path = p::root("multi");
        let db_path = db.path.clone();
        let names = p::frames(&m);
        let (mut consumer, tx) = f::start(&db, &config, "same-adapter").await?;
        let producer = tokio::spawn(async move {
            for (i, name) in names.iter().enumerate() {
                tx.send(ReplayInput::Update {
                    offset_ns: i as u64 + 1,
                    payload: std::fs::read(path.join(format!("{name}.pb")))?,
                })
                .await?;
            }
            let c = rusqlite::Connection::open(db_path)?;
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let positive:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM association_sell_preparations WHERE json_extract(latest_evaluation,'$.selected_chain')='ProviderOrderedAcrossBlocks')",[],|r|r.get(0))?;
                if positive {
                    break;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "positive was never committed"
                );
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            let first: String = c.query_row(
                "SELECT first_binding FROM association_sell_preparations",
                [],
                |r| r.get(0),
            )?;
            // Both transitions happen AFTER positive on this very adapter.
            let at = 121_000_000_000;
            tx.send(if reset {
                ReplayInput::Reset(at)
            } else {
                ReplayInput::Tick(at)
            })
            .await?;
            tx.send(ReplayInput::Update {
                offset_ns: at + 1,
                payload: std::fs::read(path.join("conflict-110.pb"))?,
            })
            .await?;
            tx.send(ReplayInput::End(at + 2)).await?;
            Ok::<String, anyhow::Error>(first)
        });
        f::drain(&mut consumer, &db).await?;
        let first = producer.await??;
        let r = p::read(&db, &m)?;
        assert_eq!(serde_json::to_string(&r.first)?, first);
        assert_eq!(
            r.current.selected_chain,
            Check::Blocked(Reason::ParentConflict)
        );
        assert!(r.current.anchors.iter().all(|a| !a.conflict));
        assert_eq!(s::snapshot(&db)?, before);
        p::save(&db, &m, "same-adapter")?;
    }
    Ok(())
}
