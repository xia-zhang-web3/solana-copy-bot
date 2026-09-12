use super::source_guard_fixture::Fixture;
use anyhow::Result;

#[tokio::test]
async fn source_guard_quote_await_success_and_error_stop_publish() -> Result<()> {
    for owned in [true, false] {
        for error in [false, true] {
            let mut f = Fixture::new().await?;
            f.config.quote_canary_pump_fun_parallel_enabled = true;
            {
                let mut c = f.rpc.state.lock().unwrap();
                c.mutate_at = Some("quote");
                c.error_at = error.then_some("quote");
            }
            let refused = if owned {
                f.owned_quote()
                    .await
                    .map(|s| s.source_sell_refusals.count())
            } else {
                f.tiny().await.map(|s| s.source_sell_refusals.count())
            };
            f.finish().await?;
            assert_eq!(refused?, 1, "owned={owned}, error={error}");
            assert_eq!(f.rpc.count("quote"), 1);
            if owned {
                // Pump may already be in flight before generic completes. The new
                // barrier tests check both starts, refusal and explicit cancellation.
                assert!(f.rpc.count("fallback") <= 1);
            } else {
                assert_eq!(f.rpc.count("fallback"), 0);
            }
            assert_eq!(f.rpc.count("swap"), 0);
            assert_eq!(f.rpc.count("sendTransaction"), 0);
            assert_eq!(
                f.rpc.state.lock().unwrap().after_mutation.as_ref(),
                Some(&f.state()?)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_guard_amount_error_preserves_external_generation_change() -> Result<()> {
    let mut f = Fixture::new().await?;
    {
        let mut c = f.rpc.state.lock().unwrap();
        c.mutate_at = Some("getTokenAccountsByOwner");
        c.error_at = c.mutate_at;
    }
    let out = f.tiny().await;
    f.finish().await?;
    assert_eq!(out?.source_sell_refusals.count(), 1);
    assert_eq!(f.rpc.count("quote"), 0);
    assert_eq!(
        f.rpc.state.lock().unwrap().after_mutation.as_ref(),
        Some(&f.state()?)
    );
    Ok(())
}

#[tokio::test]
async fn source_guard_simulation_error_and_new_signature_do_not_overwrite_order() -> Result<()> {
    for retry in [false, true] {
        for mutation in ["generation", "signature"] {
            for error in [false, true] {
                let mut f = Fixture::new().await?;
                if retry {
                    f.retry()?;
                }
                {
                    let mut c = f.rpc.state.lock().unwrap();
                    c.mutate_at = Some("simulateTransaction");
                    c.mutation = mutation;
                    c.error_at = error.then_some("simulateTransaction");
                }
                let out = if retry {
                    f.retry_sweep().await
                } else {
                    f.tiny().await
                };
                f.finish().await?;
                let out = out?;
                assert_eq!(
                    out.source_sell_refusals.count(),
                    1,
                    "retry={retry}, {mutation}, error={error}: {out:?}"
                );
                assert_eq!(
                    (out.signing_envelope_built, out.failed, out.simulated),
                    (0, 0, 0)
                );
                assert_eq!(f.rpc.count("sendTransaction"), 0);
                assert_eq!(
                    f.rpc.state.lock().unwrap().after_mutation.as_ref(),
                    Some(&f.state()?)
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_guard_known_signature_stale_sell_continues_receipt_reconciliation() -> Result<()> {
    let mut f = Fixture::new().await?;
    let id = f.retry()?;
    f.f.store
        .mark_execution_canary_submitted(&id, f.f.now, "already-sent")?;
    f.f.replace(4000)?;
    let before = f.f.store.load_execution_canary_order(&id)?;
    let out = f.tiny().await;
    f.finish().await?;
    let out = out?;
    assert_eq!(out.source_sell_refusals.count(), 0, "{out:?}");
    assert!(f.rpc.count("getSignatureStatuses") > 0);
    assert_eq!(
        (f.rpc.count("quote"), f.rpc.count("sendTransaction")),
        (0, 0)
    );
    assert_eq!(f.f.store.load_execution_canary_order(&id)?, before);
    Ok(())
}

#[tokio::test]
async fn source_guard_reverse_unknown_and_schema_error_never_become_legacy() -> Result<()> {
    for corruption in ["moved", "unknown", "schema"] {
        let mut f = Fixture::new().await?;
        let sql=match corruption {
            "moved"=>"UPDATE execution_source_sell_promotions SET signal_id='wrong-marker'",
            "unknown"=>"UPDATE execution_source_sell_intents SET staged_at='invalid-time'",
            _=>"ALTER TABLE execution_source_sell_promotions RENAME COLUMN intent_id TO broken_intent",
        };
        f.f.conn()?.execute_batch(sql)?;
        let before = f.state()?;
        let quote = f.owned_quote().await;
        let tiny = f.tiny().await;
        f.finish().await?;
        if corruption == "schema" {
            assert!(quote.is_err() && tiny.is_err());
        } else {
            assert_eq!(quote?.source_sell_refusals.count(), 1);
            assert_eq!(tiny?.source_sell_refusals.count(), 1);
        }
        assert_eq!(f.rpc.state.lock().unwrap().calls.len(), 0, "{corruption}");
        assert_eq!(before, f.state()?);
    }
    Ok(())
}

#[tokio::test]
async fn source_guard_unchanged_source_still_allows_quote_fallback_control() -> Result<()> {
    for owned in [true, false] {
        let mut f = Fixture::new().await?;
        f.config.quote_canary_pump_fun_parallel_enabled = true;
        f.rpc.state.lock().unwrap().error_at = Some("quote");
        let result = if owned {
            f.owned_quote()
                .await
                .map(|s| s.source_sell_refusals.count())
        } else {
            f.tiny().await.map(|s| s.source_sell_refusals.count())
        };
        f.finish().await?;
        assert_eq!(result?, 0);
        assert_eq!(f.rpc.count("quote"), 1);
        assert_eq!(f.rpc.count("fallback"), 1, "owned={owned}");
    }
    Ok(())
}

#[tokio::test]
async fn source_guard_valid_owned_quote_preserves_original_signal_time() -> Result<()> {
    let mut f = Fixture::new().await?;
    let out = f.owned_quote().await;
    f.finish().await?;
    let out = out?;
    assert_eq!(out.close_inserted, 1, "{out:?}");
    assert_eq!(out.source_sell_refusals.count(), 0);
    assert_eq!(f.rpc.count("quote"), 1);
    let event =
        f.f.store
            .load_execution_quote_canary_event_by_id(out.last_event_id.as_deref().unwrap())?
            .unwrap();
    assert_eq!(event.signal_ts, Some(f.f.signal.ts));
    Ok(())
}
