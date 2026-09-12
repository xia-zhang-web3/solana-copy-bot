#[path = "common/buy_attribution_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::*;
use fixture::Db;

fn read(db: &Db) -> Result<OpenPositionBuyAttribution> {
    let ExecutionCanaryBuyAttribution::Open(value) =
        db.store.load_execution_canary_buy_attribution("mint")?
    else {
        panic!("expected position")
    };
    Ok(value)
}

#[test]
fn two_sources_survive_reopen_replay_and_only_new_generation_is_read() -> Result<()> {
    let mut db = Db::new()?;
    assert_eq!(
        db.store.load_execution_canary_buy_attribution("mint")?,
        ExecutionCanaryBuyAttribution::NoOpenPosition
    );
    let a = db.seed("a", "source-a", "buy")?;
    let first = db.buy(&a)?;
    let b = db.seed("b", "source-b", "buy")?;
    db.buy(&b)?;
    db.reopen()?;
    let before = db.snapshot()?;
    let value = read(&db)?;
    assert_eq!(value.position_id, first.position.position_id);
    assert_eq!(value.token, "mint");
    assert_eq!(value.coverage, BuyAttributionCoverage::ProvenSubset);
    assert!(value.unproven_links.is_empty());
    assert_eq!(
        value
            .proven_contributors
            .iter()
            .map(|c| (
                c.order_id.as_str(),
                c.signal_id.as_str(),
                c.source_wallet.as_str(),
                c.tx_signature.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            (a.as_str(), "a", "source-a", "sig:exec-canary:a"),
            (b.as_str(), "b", "source-b", "sig:exec-canary:b")
        ]
    );
    for id in [&a, &b] {
        assert_eq!(
            db.buy(id)?.outcome,
            ExecutionCanaryPositionRecordOutcome::Existing
        );
        assert_eq!(read(&db)?, value);
        assert_eq!(db.snapshot()?, before);
    }
    db.close()?;
    let c = db.seed("c", "source-c", "buy")?;
    let new = db.buy(&c)?;
    db.reopen()?;
    let before = db.snapshot()?;
    for id in [&a, &b] {
        assert_eq!(db.buy(id)?.position.position_id, first.position.position_id);
    }
    let value = read(&db)?;
    assert_eq!(value.position_id, new.position.position_id);
    assert_eq!(value.proven_contributors.len(), 1);
    assert_eq!(value.proven_contributors[0].source_wallet, "source-c");
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn legacy_null_replay_is_unknown_and_never_guesses_or_backfills() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("b", "source-b", "buy")?;
    db.buy(&b)?;
    // Historical corruption/legacy fixture only, never a GREEN attribution input.
    db.conn()?
        .execute("UPDATE fills SET position_id=NULL WHERE order_id=?1", [&b])?;
    db.reopen()?;
    let before = db.snapshot()?;
    let value = read(&db)?;
    assert_eq!(value.coverage, BuyAttributionCoverage::ProvenSubset);
    assert_eq!(value.proven_contributors.len(), 1);
    assert_eq!(
        value.unproven_links[0].reason,
        BuyAttributionIssue::MissingDestination
    );
    assert!(db.buy(&b).unwrap_err().is::<BuyAttributionIssue>());
    assert_eq!(db.snapshot()?, before);
    db.close()?;
    let c = db.seed("c", "source-c", "buy")?;
    db.buy(&c)?;
    let before = db.snapshot()?;
    assert!(db.buy(&b).is_err());
    assert_eq!(read(&db)?.proven_contributors[0].source_wallet, "source-c");
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn no_order_orphan_and_receiptless_compatibility_are_not_proven_sources() -> Result<()> {
    let db = Db::new()?;
    let first = db.store.record_execution_canary_open_position(
        "recovery-orphan:fixture",
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        db.now,
    )?;
    let before = db.snapshot()?;
    assert!(db.links()?.is_empty());
    let value = read(&db)?;
    assert_eq!(value.coverage, BuyAttributionCoverage::Unknown);
    assert!(value.proven_contributors.is_empty());
    assert_eq!(db.snapshot()?, before);
    db.close()?;
    let a = db.seed("a", "source-a", "buy")?;
    // Actual receiptless legacy completion path retains its historical amounts.
    db.conn()?.execute(
        "DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&a],
    )?;
    db.buy(&a)?;
    db.store.record_execution_canary_confirmed_buy_fill(
        &a,
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        db.now,
    )?;
    let before = db.snapshot()?;
    let replay = db.store.record_execution_canary_open_position(
        "recovery-orphan:fixture",
        "mint",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        db.now,
    )?;
    assert_eq!(replay.position.position_id, first.position.position_id);
    let value = read(&db)?;
    assert_eq!(value.coverage, BuyAttributionCoverage::Unknown);
    assert!(value.proven_contributors.is_empty());
    assert!(value
        .unproven_links
        .iter()
        .any(|v| v.reason == BuyAttributionIssue::MissingReceiptFacts));
    assert_eq!(db.snapshot()?, before);
    Ok(())
}

#[test]
fn partial_sell_does_not_allocate_sources_or_count_buy_links_as_cash() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("b", "source-b", "buy")?;
    db.buy(&b)?;
    let at = db.now + chrono::Duration::seconds(1);
    let sources = read(&db)?;
    assert_eq!(
        db.store
            .execution_canary_sell_cash_day(at)?
            .known_events
            .events,
        0
    );
    assert_eq!(
        db.store
            .execution_canary_entry_cost(at)?
            .known_total_lamports
            .as_deref(),
        Some("0")
    );
    assert!(db
        .store
        .load_execution_canary_cash_settlement(&a)?
        .is_none());
    let sell = db.seed("sell", "source-a", "sell")?;
    let facts = db
        .store
        .load_execution_canary_receipt_facts(&sell)?
        .unwrap();
    let result = db
        .store
        .apply_execution_canary_sell_settlement(&facts, db.now)?;
    assert_eq!(result.settlement.cash_result_delta.as_i128(), -500);
    assert_eq!(result.settlement.remaining_quantity.raw(), 7000);
    assert_eq!(result.settlement.remaining_entry_basis.as_u64(), 1000);
    db.reopen()?;
    let before = db.snapshot()?;
    assert_eq!(read(&db)?, sources);
    let day = db.store.execution_canary_sell_cash_day(at)?;
    assert_eq!(day.known_events.events, 1);
    assert_eq!(day.known_events.partial_events, 1);
    assert_eq!(day.known_events.gross_negative_cash_result_lamports, "500");
    assert_eq!(
        db.store
            .execution_canary_entry_cost(at)?
            .known_total_lamports
            .as_deref(),
        Some("500")
    );
    assert!(
        db.store
            .apply_execution_canary_sell_settlement(&facts, db.now)?
            .already_accounted
    );
    for id in [&a, &b] {
        db.buy(id)?;
    }
    assert_eq!(db.snapshot()?, before);
    Ok(())
}
