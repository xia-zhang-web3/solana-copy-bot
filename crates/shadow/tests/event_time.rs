mod event_time_support;
use anyhow::Result;
use chrono::Duration;
use copybot_shadow::{FollowSnapshot, ShadowDropReason, ShadowService};
use event_time_support::*;

fn future_refused(delta: Duration) -> Result<()> {
    for receipt_api in [false, true] {
        for quality in [false, true] {
            for exact in [false, true] {
                let (_dir, store, service) = setup(quality)?;
                let event = buy(now() + delta, exact, "future-A");
                let cache = serde_json::to_value(store.get_token_quality_cache(TOKEN)?)?;
                let capture = Capture::default();
                let (outcome, receipt) =
                    tracing::subscriber::with_default(capture.clone(), || {
                        process(receipt_api, &service, &store, &event, &follow())
                    })?;
                counts(&store, 0, 0)?;
                assert!(receipt.is_none());
                dropped(&outcome, "lag_exceeded");
                let traces = capture.0.lock().unwrap();
                assert!(traces.iter().any(|v| v["stage"] == "future_event"
                    && v["reason"] == "lag_exceeded"
                    && v["signature"] == "future-A"));
                assert_eq!(
                    serde_json::to_value(store.get_token_quality_cache(TOKEN)?)?,
                    cache
                );
            }
        }
    }
    Ok(())
}

#[test]
fn future_one_nanosecond_rejected_by_both_apis_with_quality_on_or_off() -> Result<()> {
    future_refused(Duration::nanoseconds(1))
}

#[test]
fn future_sixty_seconds_rejected_by_both_apis_with_quality_on_or_off() -> Result<()> {
    future_refused(Duration::seconds(60))
}

#[test]
fn equality_and_fresh_times_preserve_exact_timestamp_and_receipt() -> Result<()> {
    for receipt_api in [false, true] {
        for quality in [false, true] {
            for exact in [false, true] {
                for age in [
                    Duration::zero(),
                    Duration::nanoseconds(1),
                    Duration::seconds(44),
                ] {
                    let (_dir, store, service) = setup(quality)?;
                    let event = buy(now() - age, exact, "healthy");
                    let (outcome, receipt) =
                        process(receipt_api, &service, &store, &event, &follow())?;
                    let result = recorded(&outcome);
                    assert_eq!(result.side, "buy");
                    assert_eq!(result.latency_ms, age.num_milliseconds());
                    counts(&store, 1, 1)?;
                    assert_eq!(
                        store
                            .load_copy_signal_by_signal_id(&result.signal_id)?
                            .unwrap()
                            .ts,
                        event.ts_utc
                    );
                    assert_eq!(
                        store.list_shadow_lots(WALLET, TOKEN)?[0].opened_ts,
                        event.ts_utc
                    );
                    assert_eq!(receipt.is_some(), receipt_api && exact);
                    if let Some(receipt) = receipt {
                        receipt.verify(&store, &event)?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn old_event_still_exceeds_entry_lag_with_quality_on_or_off() -> Result<()> {
    for receipt_api in [false, true] {
        for quality in [false, true] {
            let (_dir, store, service) = setup(quality)?;
            let event = buy(now() - Duration::seconds(46), true, "old");
            let (outcome, receipt) = process(receipt_api, &service, &store, &event, &follow())?;
            reason(&outcome, ShadowDropReason::LagExceeded);
            counts(&store, 0, 0)?;
            assert!(receipt.is_none());
        }
    }
    Ok(())
}

#[test]
fn refusal_does_not_poison_healthy_buy_b_or_its_receipt_after_reopen() -> Result<()> {
    for receipt_api in [false, true] {
        for quality in [false, true] {
            let (dir, store, service) = setup(quality)?;
            let a = buy(now() + Duration::seconds(60), true, "future-A");
            let (outcome, receipt) = process(receipt_api, &service, &store, &a, &follow())?;
            dropped(&outcome, "lag_exceeded");
            assert!(receipt.is_none());
            counts(&store, 0, 0)?;
            let b = buy(now() - Duration::nanoseconds(1), true, "healthy-B");
            let (outcome, receipt) = process(receipt_api, &service, &store, &b, &follow())?;
            recorded(&outcome);
            counts(&store, 1, 1)?;
            assert_eq!(receipt.is_some(), receipt_api);
            if let Some(receipt) = &receipt {
                receipt.verify(&store, &b)?;
            }
            drop(store);
            let store = copybot_storage_core::SqliteStore::open(dir.path().join("event-time.db"))?;
            if let Some(receipt) = &receipt {
                receipt.verify(&store, &b)?;
            }
            let (outcome, replay_receipt) = process(receipt_api, &service, &store, &b, &follow())?;
            reason(&outcome, ShadowDropReason::DuplicateSignal);
            assert!(replay_receipt.is_none());
            counts(&store, 1, 1)?;
        }
    }
    Ok(())
}

#[test]
fn existing_disabled_non_sol_follow_and_notional_refusals_remain_closed() -> Result<()> {
    for receipt_api in [false, true] {
        for (case, expected) in [
            ("disabled", ShadowDropReason::Disabled),
            ("non-sol", ShadowDropReason::NotSolLeg),
            ("unfollowed", ShadowDropReason::NotFollowed),
            ("promotion", ShadowDropReason::NotFollowed),
            ("notional", ShadowDropReason::BelowNotional),
        ] {
            let (_dir, store, _) = setup(true)?;
            let mut cfg = config(true);
            let mut event = buy(now() + Duration::seconds(60), false, case);
            let mut followed = follow();
            match case {
                "disabled" => cfg.enabled = false,
                "non-sol" => event.token_in = "another-token".into(),
                "unfollowed" => followed = FollowSnapshot::default(),
                "promotion" => {
                    followed
                        .promoted_at
                        .insert(WALLET.into(), event.ts_utc + Duration::nanoseconds(1));
                }
                "notional" => event.amount_in = 0.01,
                _ => unreachable!(),
            }
            let service = ShadowService::new(cfg);
            let (outcome, receipt) = process(receipt_api, &service, &store, &event, &followed)?;
            reason(&outcome, expected);
            assert!(receipt.is_none());
            counts(&store, 0, 0)?;
        }
    }
    Ok(())
}

#[test]
fn current_buy_cannot_use_future_quality_cache_through_either_api() -> Result<()> {
    for receipt_api in [false, true] {
        let (_dir, store, service) = setup(true)?;
        store.upsert_token_quality_cache(
            TOKEN,
            Some(100),
            Some(100.0),
            Some(86400),
            now() + Duration::nanoseconds(1),
        )?;
        let event = buy(now(), true, "current");
        let (outcome, receipt) = process(receipt_api, &service, &store, &event, &follow())?;
        reason(&outcome, ShadowDropReason::LowHolders);
        assert!(receipt.is_none());
        counts(&store, 0, 0)?;
        assert_eq!(
            store.get_token_quality_cache(TOKEN)?.unwrap().fetched_at,
            now() + Duration::nanoseconds(1)
        );
    }
    Ok(())
}

#[test]
fn future_sell_keeps_open_risk_exit_and_excludes_future_only_lots() -> Result<()> {
    for receipt_api in [false, true] {
        for followed in [false, true] {
            for future_lot in [false, true] {
                let (_dir, store, service) = setup(true)?;
                let opened = now() + Duration::seconds(if future_lot { 1 } else { -1 });
                store.insert_shadow_lot(WALLET, TOKEN, 100.0, 1.0, opened)?;
                let mut sell = buy(now() + Duration::nanoseconds(1), false, "future-sell");
                std::mem::swap(&mut sell.token_in, &mut sell.token_out);
                std::mem::swap(&mut sell.amount_in, &mut sell.amount_out);
                let follow = if followed {
                    follow()
                } else {
                    FollowSnapshot::default()
                };
                let (outcome, receipt) = process(receipt_api, &service, &store, &sell, &follow)?;
                assert!(receipt.is_none());
                if !followed && future_lot {
                    reason(&outcome, ShadowDropReason::NotFollowed);
                    counts(&store, 0, 1)?;
                } else {
                    let result = recorded(&outcome);
                    assert_eq!(result.side, "sell");
                    assert_eq!(result.closed_qty, if future_lot { 0.0 } else { 50.0 });
                    counts(&store, 1, 1)?;
                }
                let lots = store.list_shadow_lots(WALLET, TOKEN)?;
                assert_eq!(lots[0].qty, if future_lot { 100.0 } else { 50.0 });
                assert_eq!(lots[0].opened_ts, opened);
            }
        }
    }
    Ok(())
}
