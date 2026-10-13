//! Changed snapshot identity boundary only. Saved37 groups are ignored without the private immutable corpus.
#[path = "common/strict_quote_fixture.rs"]
mod model;
#[path = "common/saved37_fractional_fixture.rs"]
mod saved;
use anyhow::Result;
use copybot_storage_core::{ordered_sell_quote::QuoteClaimStep, SqliteStore};

#[test]
fn short_snapshot_producer_is_compact_and_rechecks_same_graph() -> Result<()> {
    let f = model::fixture()?;
    let now = chrono::Utc::now();
    let claim = model::claim(&f, now)?;
    assert!(claim
        .binding
        .snapshot_version
        .starts_with("strict_quote_snapshot_v2:sha256:"));
    assert_eq!(claim.binding.snapshot_version.len(), 96);
    assert!(f
        .db
        .store
        .recheck_strict_sell_quote(&claim, model::limits(), now)?);
    let again =
        f.db.store
            .strict_sell_quote_snapshot(model::ID, model::limits(), model::ENDPOINT)?
            .unwrap();
    assert_eq!(again, claim.binding);
    for fault in ["owner", "raw", "digest"] {
        let mut bad = claim.clone();
        match fault {
            "owner" => bad.owner = "foreign".into(),
            "raw" => bad.binding.raw += 1,
            _ => bad.binding.snapshot_version.push('x'),
        }
        assert!(f
            .db
            .store
            .recheck_strict_sell_quote(&bad, model::limits(), now)
            .is_err());
    }
    Ok(())
}
#[test]
fn unchanged_quote_binding_sqlite_byte_boundary_is_atomic() -> Result<()> {
    // The public claim reserves 4096 bytes for completion within the unchanged 131072 cap.
    for cap in [126_976usize, 126_977] {
        let f = model::fixture()?;
        let binding =
            f.db.store
                .strict_sell_quote_snapshot(model::ID, model::limits(), model::ENDPOINT)?
                .unwrap();
        let overhead = serde_json::to_string(&binding)?.len() - binding.endpoint.len();
        let endpoint = "x".repeat(cap - overhead);
        let result =
            f.db.store
                .claim_strict_sell_quote(saved::limits(), &endpoint, chrono::Utc::now)?;
        if cap == 126_976 {
            let QuoteClaimStep::Claimed(claim) = result else {
                panic!("claim absent: {result:?}")
            };
            assert_eq!(serde_json::to_string(&claim.binding)?.len(), cap);
            // Explicit malformed TEMP bytes verify the exact SQL cap; they confer no claim/permit.
            let sql = f.db.conn()?;
            sql.execute(
                "UPDATE ordered_sell_quote_results SET binding=?1",
                ["x".repeat(131_072)],
            )?;
            let failed = sql.execute(
                "UPDATE ordered_sell_quote_results SET binding=?1",
                ["x".repeat(131_073)],
            );
            assert!(failed.unwrap_err().to_string().contains("131072"));
            let bytes: i64 = sql.query_row(
                "SELECT length(CAST(binding AS BLOB)) FROM ordered_sell_quote_results",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(bytes, 131_072);
            assert!(f
                .db
                .store
                .recheck_strict_sell_quote(&claim, saved::limits(), chrono::Utc::now())
                .is_err());
        } else {
            let QuoteClaimStep::CapacityRefused(refusal) = result else {
                panic!("expected bounded refusal: {result:?}")
            };
            assert_eq!(refusal.dimension, "record_bytes");
            assert_eq!(model::count(&f, "ordered_sell_quote_results")?, 0);
            assert_eq!(model::count(&f, "ordered_sell_quote_cursor")?, 1);
        }
    }
    Ok(())
}
#[test]
#[ignore = "requires immutable actual37 corpus; baseline RED/fixed GREEN uses explicit modeled fresh TEMP claim"]
fn actual_saved37_fresh_completion() -> Result<()> {
    let f = saved::Saved::load()?;
    let money = f.financial()?;
    let claim = f.fresh_claim()?;
    let before = f.state()?;
    let result = f.store.complete_fractional_sell(
        &claim,
        &f.evidence,
        saved::limits(),
        saved::original_clock(),
    );
    if result.is_err() {
        assert_eq!(f.state()?, before);
    }
    assert_eq!(f.financial()?, money);
    let out = result?;
    assert_eq!(out.binding.snapshot_version, f.expected_digest());
    let bytes = f.assert_bound(&out)?;
    assert_eq!(f.financial()?, money);
    let reopened = SqliteStore::open(&f.path)?;
    assert!(reopened.recheck_strict_sell_quote(&out, saved::limits(), saved::original_clock())?);
    let state = f.state()?;
    assert!(reopened
        .begin_fractional_sell(
            &claim,
            saved::limits(),
            saved::original_clock(),
            &f.producer
        )
        .is_err());
    assert_eq!(f.state()?, state);
    println!("saved37 real Rust complete_fractional_sell: binding_bytes={bytes}; N=D1067375; selected1067086; restart/no-rearm/financial_rows_unchanged PASS");
    Ok(())
}
#[test]
#[ignore = "requires immutable actual37 corpus; old consumed short claim must remain unchanged"]
fn actual_saved37_old_collecting_claim_is_not_upgraded() -> Result<()> {
    let f = saved::Saved::load()?;
    let before = f.state()?;
    let error = f
        .store
        .complete_fractional_sell(
            &f.original,
            &f.evidence,
            saved::limits(),
            saved::original_clock(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("fraction_generation_changed"),
        "{error:#}"
    );
    assert_eq!(f.state()?, before);
    Ok(())
}
#[test]
#[ignore = "requires immutable actual37 corpus; changed binding/native proof contradictions"]
fn actual_saved37_corruptions_and_unknown_restart_refuse() -> Result<()> {
    for fault in ["owner", "raw", "digest", "parent", "receipt", "wallet"] {
        let f = saved::Saved::load()?;
        let mut claim = f.fresh_claim()?;
        let mut evidence = f.evidence.clone();
        match fault {
            "owner" => claim.owner = "foreign".into(),
            "raw" => claim.binding.raw += 1,
            "digest" => claim.binding.snapshot_version.push('x'),
            "parent" => evidence.parent["blockhash"] = serde_json::json!("foreign"),
            "receipt" => {
                f.sql()?.execute(
                    "UPDATE execution_canary_receipt_proofs SET tx_signature='foreign'",
                    [],
                )?;
            }
            _ => {
                evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]
                    ["tokenAmount"]["amount"] = serde_json::json!("1067085")
            }
        }
        let before = f.state()?;
        assert!(
            f.store
                .complete_fractional_sell(
                    &claim,
                    &evidence,
                    saved::limits(),
                    saved::original_clock()
                )
                .is_err(),
            "{fault}"
        );
        assert_eq!(f.state()?, before, "{fault}");
    }
    let f = saved::Saved::load()?;
    let claim = f.fresh_claim()?;
    let before = f.state()?;
    let restarted = SqliteStore::open(&f.path)?;
    assert!(restarted
        .begin_fractional_sell(
            &claim,
            saved::limits(),
            saved::original_clock(),
            &f.producer
        )
        .is_err());
    assert_eq!(f.state()?, before);
    Ok(())
}
#[test]
#[ignore = "requires immutable actual37 corpus; explicit TEMP trigger injects postdecision write failure"]
fn actual_saved37_write_failure_rolls_back_proof_and_binding() -> Result<()> {
    let f = saved::Saved::load()?;
    let claim = f.fresh_claim()?;
    f.sql()?.execute_batch("CREATE TRIGGER saved37_abort BEFORE UPDATE OF binding ON ordered_sell_quote_results BEGIN SELECT RAISE(ABORT,'saved37_write_abort'); END;")?;
    let before = f.state()?;
    let money = f.financial()?;
    let err = f
        .store
        .complete_fractional_sell(
            &claim,
            &f.evidence,
            saved::limits(),
            saved::original_clock(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("saved37_write_abort"));
    assert_eq!(f.state()?, before);
    assert_eq!(f.financial()?, money);
    let restarted = SqliteStore::open(&f.path)?;
    assert!(restarted
        .begin_fractional_sell(
            &claim,
            saved::limits(),
            saved::original_clock(),
            &f.producer
        )
        .is_err());
    assert_eq!(f.state()?, before);
    Ok(())
}
