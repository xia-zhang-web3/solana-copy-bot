use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent, TokenQuantity};
use copybot_storage_core::{
    ExecutionSellIntentOutcome as Outcome, ExecutionSellIntentReject as Reject, SqliteStore,
    EXECUTION_SELL_INTENT_STATUS,
};
use tempfile::{tempdir, TempDir};
const SOL: &str = "So11111111111111111111111111111111111111112";
fn setup() -> Result<(TempDir, SqliteStore, SwapEvent)> {
    let dir = tempdir()?;
    let mut s = SqliteStore::open(dir.path().join("intent.db"))?;
    s.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let ts = Utc::now() - Duration::days(3);
    s.activate_follow_wallet("leader", ts - Duration::seconds(20), "test")?;
    s.record_execution_canary_open_position(
        "owned",
        "token",
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.07,
        ts - Duration::seconds(10),
    )?;
    let swap = SwapEvent {
        wallet: "leader".into(),
        dex: "pumpswap".into(),
        token_in: "token".into(),
        token_out: SOL.into(),
        amount_in: 10.0,
        amount_out: 0.1,
        signature: "raw".into(),
        slot: 123,
        ts_utc: ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "10000".into(),
            amount_in_decimals: 3,
            amount_out_raw: "100000000".into(),
            amount_out_decimals: 9,
        }),
    };
    s.insert_observed_swap(&swap)?;
    Ok((dir, s, swap))
}
fn rejected(out: Outcome, expected: Reject) {
    assert!(
        matches!(out,Outcome::Rejected(reason) if reason==expected),
        "{out:?}"
    );
}
#[test]
fn owned_sell_intent_is_bound_to_observed_source_and_durable_position() -> Result<()> {
    for case in [
        "missing_raw",
        "wallet",
        "mint",
        "timestamp",
        "slot",
        "amount",
        "exact",
        "inactive",
        "temporal",
        "position",
        "future",
        "shadow",
        "invalid_side",
    ] {
        let (dir, s, mut swap) = setup()?;
        let c = rusqlite::Connection::open(dir.path().join("intent.db"))?;
        let expected = match case {
            "missing_raw" => {
                c.execute("DELETE FROM observed_swaps", [])?;
                Reject::ObservedEventMismatch
            }
            "wallet" => {
                swap.wallet = "other".into();
                s.activate_follow_wallet("other", swap.ts_utc - Duration::seconds(20), "test")?;
                Reject::ObservedEventMismatch
            }
            "mint" => {
                swap.token_in = "other".into();
                Reject::ObservedEventMismatch
            }
            "timestamp" => {
                swap.ts_utc += Duration::nanoseconds(1);
                Reject::ObservedEventMismatch
            }
            "slot" => {
                swap.slot += 1;
                Reject::ObservedEventMismatch
            }
            "amount" => {
                swap.amount_in += 1.0;
                Reject::ObservedEventMismatch
            }
            "exact" => {
                swap.exact_amounts.as_mut().unwrap().amount_in_raw = "10001".into();
                Reject::ObservedEventMismatch
            }
            "inactive" => {
                s.deactivate_follow_wallet("leader", swap.ts_utc + Duration::seconds(1), "test")?;
                Reject::SourceNotActive
            }
            "temporal" => {
                c.execute(
                    "UPDATE followlist SET added_at=?1",
                    [(swap.ts_utc + Duration::nanoseconds(1)).to_rfc3339()],
                )?;
                Reject::SourceTemporalMiss
            }
            "position" => {
                c.execute("DELETE FROM positions", [])?;
                Reject::NoOwnedPosition
            }
            "future" => {
                c.execute(
                    "UPDATE positions SET opened_ts=?1",
                    [(swap.ts_utc + Duration::nanoseconds(1)).to_rfc3339()],
                )?;
                Reject::SellBeforePosition
            }
            "shadow" => {
                s.insert_shadow_lot("leader", "token", 1.0, 0.01, swap.ts_utc)?;
                Reject::ShadowRiskPresent
            }
            _ => {
                swap.token_in = SOL.into();
                swap.token_out = "token".into();
                Reject::InvalidSell
            }
        };
        rejected(s.record_execution_sell_intent(&swap)?, expected);
        assert!(s
            .list_copy_signals_by_status(EXECUTION_SELL_INTENT_STATUS, 10)?
            .is_empty());
    }
    Ok(())
}
#[test]
fn owned_sell_intent_reopen_replay_and_consumers_do_not_create_shadow_accounting() -> Result<()> {
    let (dir, s, swap) = setup()?;
    let Outcome::Inserted(signal) = s.record_execution_sell_intent(&swap)? else {
        panic!("inserted")
    };
    assert_eq!(signal.ts, swap.ts_utc);
    assert_eq!(signal.notional_lamports.unwrap().as_u64(), 100_000_000);
    drop(s);
    let s = SqliteStore::open(dir.path().join("intent.db"))?;
    rejected(
        s.record_execution_sell_intent(&swap)?,
        Reject::SignalAlreadyExists,
    );
    assert_eq!(
        s.list_copy_signals_by_status(EXECUTION_SELL_INTENT_STATUS, 10)?
            .len(),
        1
    );
    assert!(s
        .list_copy_signals_by_status("shadow_recorded", 10)?
        .is_empty());
    let summary = s.shadow_signal_summary_since(swap.ts_utc - Duration::seconds(1))?;
    assert_eq!(summary.sell_signals_total, 0);
    assert_eq!(summary.open_lots, 0);
    assert!(!s.has_recent_copy_signal_for_wallet_token_side(
        "leader",
        "token",
        "sell",
        swap.ts_utc - Duration::seconds(1),
        swap.ts_utc + Duration::seconds(1)
    )?);
    // Durable admission is not lost when the original event is older than entry lookback.
    assert_eq!(
        s.list_execution_quote_canary_owned_sell_signal_candidate_ids(
            "shadow_recorded",
            Utc::now() - Duration::hours(24),
            10
        )?,
        vec![signal.signal_id.clone()]
    );
    assert!(s
        .list_execution_quote_canary_entry_candidates(
            "shadow_recorded",
            swap.ts_utc - Duration::seconds(1),
            10
        )?
        .is_empty());
    assert!(s
        .list_execution_quote_canary_close_candidates_for_signal(&signal.signal_id, 10)?
        .is_empty());
    Ok(())
}
#[test]
fn owned_sell_intent_insert_failure_rolls_back_and_retry_is_idempotent() -> Result<()> {
    let (dir, s, swap) = setup()?;
    let c = rusqlite::Connection::open(dir.path().join("intent.db"))?;
    c.execute_batch("CREATE TRIGGER reject_intent BEFORE INSERT ON copy_signals BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;")?;
    assert!(s.record_execution_sell_intent(&swap).is_err());
    assert_eq!(
        s.load_execution_canary_open_position("token")?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        7000
    );
    c.execute_batch("DROP TRIGGER reject_intent")?;
    assert!(matches!(
        s.record_execution_sell_intent(&swap)?,
        Outcome::Inserted(_)
    ));
    rejected(
        s.record_execution_sell_intent(&swap)?,
        Reject::SignalAlreadyExists,
    );
    Ok(())
}

#[test]
fn owned_sell_intent_quote_refresh_is_limited_to_unordered_intents() -> Result<()> {
    for ordinary in [false, true] {
        let (dir, s, swap) = setup()?;
        let Outcome::Inserted(signal) = s.record_execution_sell_intent(&swap)? else {
            panic!("intent")
        };
        let c = rusqlite::Connection::open(dir.path().join("intent.db"))?;
        if ordinary {
            c.execute("UPDATE copy_signals SET status='shadow_recorded'", [])?;
        }
        c.execute("INSERT INTO execution_quote_canary_events(event_id,signal_id,wallet_id,token,side,quote_status,request_ts,signal_ts) VALUES ('quote',?1,'leader','token','sell','error',?2,?2)",rusqlite::params![signal.signal_id,swap.ts_utc.to_rfc3339()])?;
        let original = s.load_execution_quote_canary_event_by_id("quote")?.unwrap();
        let mut next = original.clone();
        next.request_ts += Duration::seconds(1);
        next.quote_status = "ok".into();
        next.quote_in_amount_raw = Some("7000".into());
        s.record_execution_quote_canary_event(&next)?;
        assert_eq!(
            s.load_execution_quote_canary_event_by_id("quote")?.unwrap(),
            if ordinary {
                original.clone()
            } else {
                next.clone()
            }
        );
        let mut forged = next.clone();
        forged.signal_ts = Some(swap.ts_utc + Duration::seconds(1));
        forged.request_ts += Duration::seconds(1);
        s.record_execution_quote_canary_event(&forged)?;
        assert_eq!(
            s.load_execution_quote_canary_event_by_id("quote")?.unwrap(),
            if ordinary {
                original.clone()
            } else {
                next.clone()
            }
        );
        s.reserve_execution_canary_order(&signal.signal_id, "test-route", Utc::now())?;
        next.request_ts += Duration::seconds(2);
        next.quote_in_amount_raw = Some("9000".into());
        let before = s.load_execution_quote_canary_event_by_id("quote")?.unwrap();
        s.record_execution_quote_canary_event(&next)?;
        assert_eq!(
            s.load_execution_quote_canary_event_by_id("quote")?.unwrap(),
            before
        );
        assert!(s
            .list_execution_quote_canary_owned_sell_signal_candidate_ids(
                "shadow_recorded",
                swap.ts_utc - Duration::seconds(1),
                10
            )?
            .is_empty());
    }
    Ok(())
}
