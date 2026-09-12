#[path = "common/b96_ordered.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{ExecutionSellIntentOutcome, HistoryRetentionCutoffs};
use f::*;

#[test]
fn root96_legacy_record_control_without_ordered_claim() -> Result<()> {
    let f = within()?;
    let swap = observed(&f)?;
    f.db.store.activate_follow_wallet("leader", swap.ts_utc - chrono::Duration::seconds(20), "root96")?;
    let actual = f.db.store.record_execution_sell_intent(&swap)?;
    assert!(matches!(actual, ExecutionSellIntentOutcome::Inserted(_)), "control: {actual:?}");
    Ok(())
}

#[test]
fn root96_ordered_claim_blocks_actual_legacy_record_api() -> Result<()> {
    let mut f = within()?;
    let swap = observed(&f)?;
    f.db.store.activate_follow_wallet("leader", swap.ts_utc - chrono::Duration::seconds(20), "root96")?;
    let i = inserted(&mut f)?;
    assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    let before = count(&f, "copy_signals")?;
    let actual = f.db.store.record_execution_sell_intent(&swap)?;
    let after = count(&f, "copy_signals")?;
    let block = match &actual {
        ExecutionSellIntentOutcome::Inserted(s) => f.db.store.execution_sell_intent_position_block_reason(s)?,
        _ => None,
    };
    let post = fresh(&f)?;
    let output = serde_json::json!({"actual":format!("{actual:?}"),"before_signals":before,"after_signals":after,"legacy_position_guard":block,"new_revalidation":post,"intent":i});
    record("legacy-bypass", &output)?;
    println!("root96 actual legacy={actual:?}; signals {before}->{after}; position_guard={block:?}; revalidation={post:?}");
    assert!(!matches!(actual, ExecutionSellIntentOutcome::Inserted(_)), "ordered owner must block every actual legacy intent writer");
    assert_eq!(before, after);
    Ok(())
}

#[test]
fn root96_canonical_orphan_retention_does_not_release_ownership() -> Result<()> {
    let mut f = within()?;
    let signal = CopySignalRow {
        signal_id: "shadow:sell:leader:sell:mint".into(), wallet_id: "leader".into(),
        side: "sell".into(), token: "mint".into(), notional_sol: 0.00000095,
        notional_lamports: Some(Lamports::new(950)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: f.db.now, status: "execution_failed".into(),
    };
    assert!(f.db.store.insert_copy_signal(&signal)?);
    assert_eq!(refused(&mut f)?, OrderedSellStage::Blocked(OrderedSellReason::SourceSignatureClaimed));
    let before_first = f.read()?.first;
    let old = f.db.now - chrono::Duration::days(1);
    let retained = f.db.store.apply_history_retention(HistoryRetentionCutoffs {
        risk_events_before: old, copy_signals_before: f.db.now + chrono::Duration::seconds(1),
        orders_before: old, shadow_closed_trades_before: old, execution_quote_canary_before: old,
    }, true)?;
    assert_eq!(retained.copy_signals_deleted, 1, "actual normal retention must remove only the orphan");
    reopen(&mut f)?;
    assert_eq!(f.read()?.first, before_first);
    let actual = stage(&mut f)?;
    record("orphan-retention", &actual)?;
    println!("root96 actual after orphan retention: {actual:?}");
    assert_eq!(actual, OrderedSellStage::Blocked(OrderedSellReason::SourceSignatureClaimed), "canonical legacy ownership must survive normal retention");
    Ok(())
}
