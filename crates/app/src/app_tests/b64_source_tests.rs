use super::{
    b64_http_fixture as http,
    source_write_off_fixture::{self as old, Fixture},
};
use anyhow::Result;
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use serde_json::json;
use std::{cell::RefCell, time::Duration};
use tokio::net::TcpListener;

fn config(listener: &TcpListener) -> Result<ExecutionConfig> {
    let mut cfg = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
    cfg.quote_canary_pump_fun_parallel_enabled = true;
    Ok(cfg)
}
async fn owned(
    f: &Fixture,
    cfg: ExecutionConfig,
    signal: &CopySignalRow,
) -> Result<crate::execution_quote_canary::ExecutionQuoteCanaryTickSummary> {
    let shadow = copybot_shadow::ShadowSignalResult {
        signal_id: signal.signal_id.clone(),
        wallet_id: signal.wallet_id.clone(),
        side: signal.side.clone(),
        token: signal.token.clone(),
        notional_sol: signal.notional_sol,
        latency_ms: 0,
        closed_qty: 0.0,
        realized_pnl_sol: 0.0,
        has_open_lots_after_signal: Some(false),
    };
    crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg)
        .process_recorded_shadow_signal(&f.store, &shadow, Utc::now())
        .await
}
#[tokio::test]
async fn b64_owned_invalid_before_http_starts_neither_provider() -> Result<()> {
    let f = Fixture::new(4000)?;
    f.replace(4000)?;
    let before = old::snapshot(&f.conn()?, &[])?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let cfg = config(&listener)?;
    let (out, server) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(owned(&f, cfg, &f.signal), http::no_more(&listener))
    })
    .await?;
    server?;
    let out = out?;
    assert_eq!(out.source_sell_refusals.count(), 1);
    assert_eq!(out.source_sell_refusals.order_id(), f.signal.signal_id);
    assert_eq!(before, old::snapshot(&f.conn()?, &[])?);
    Ok(())
}
#[tokio::test]
async fn b64_owned_each_completed_branch_rechecks_even_on_error_and_cancels_other() -> Result<()> {
    for pump_first in [false, true] {
        for error in [false, true] {
            let f = Fixture::new(4000)?;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let cfg = config(&listener)?;
            let after_mutation = RefCell::new(None);
            let server = async {
                let mut pair = http::pair(&listener).await?;
                pair.sort_by_key(|r| r.pump() != pump_first);
                let first = pair.remove(0);
                f.replace(4000)?;
                *after_mutation.borrow_mut() = Some(old::snapshot(&f.conn()?, &[])?);
                let body = if error {
                    json!({"error":"provider unavailable"})
                } else {
                    first.quote()
                };
                first.reply(if error { 503 } else { 200 }, body).await?;
                pair.remove(0).cancelled().await?;
                http::no_more(&listener).await
            };
            let (out, server) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(owned(&f, cfg, &f.signal), server)
            })
            .await?;
            server?;
            let out = out?;
            assert_eq!(out.source_sell_refusals.count(), 1, "{out:?}");
            assert_eq!(out.source_sell_refusals.order_id(), f.signal.signal_id);
            assert!(out
                .last_error
                .as_ref()
                .unwrap()
                .contains("source_sell_generation_mismatch"));
            assert!(out
                .last_error
                .as_ref()
                .unwrap()
                .contains(&f.signal.signal_id));
            assert_eq!(
                (
                    out.close_inserted,
                    out.close_errors,
                    out.would_execute,
                    out.would_force_exit
                ),
                (0, 0, 0, 0)
            );
            assert_eq!(
                after_mutation.into_inner().unwrap(),
                old::snapshot(&f.conn()?, &[])?
            );
            assert!(f
                .store
                .load_execution_canary_order_by_signal(&f.signal.signal_id)?
                .is_none());
        }
    }
    Ok(())
}
#[tokio::test]
async fn b64_owned_global_storage_error_is_not_provider_fallback() -> Result<()> {
    for pump_first in [false, true] {
        let f = Fixture::new(4000)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let cfg = config(&listener)?;
        let server = async {
            let mut pair = http::pair(&listener).await?;
            pair.sort_by_key(|r| r.pump() != pump_first);
            let first = pair.remove(0);
            f.conn()?.execute_batch("ALTER TABLE execution_source_sell_promotions RENAME COLUMN intent_id TO broken_intent")?;
            first
                .reply(503, json!({"error":"HTTP failure alongside SQL failure"}))
                .await?;
            pair.remove(0).cancelled().await?;
            http::no_more(&listener).await
        };
        let (out, server) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(owned(&f, cfg, &f.signal), server)
        })
        .await?;
        server?;
        let error = out.unwrap_err();
        assert!(
            crate::execution_source_sell_guard::global_storage_error(&error),
            "{error:#}"
        );
        assert!(f
            .store
            .load_execution_quote_canary_event_by_id(&format!(
                "quote:owned-close:{}",
                f.signal.signal_id
            ))?
            .is_none());
    }
    Ok(())
}
#[tokio::test]
async fn b64_owned_healthy_source_publishes_both_actual_samples() -> Result<()> {
    let f = Fixture::new(4000)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let cfg = config(&listener)?;
    let (out, server) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            owned(&f, cfg, &f.signal),
            http::serve(
                listener,
                http::Replies {
                    pump_first: true,
                    ..Default::default()
                }
            )
        )
    })
    .await?;
    assert_eq!(server?.len(), 2);
    let out = out?;
    assert_eq!(out.close_inserted, 1);
    assert_eq!(out.source_sell_refusals.count(), 0);
    let event = f
        .store
        .load_execution_quote_canary_event_by_id(out.last_event_id.as_deref().unwrap())?
        .unwrap();
    assert_eq!(
        event.signal_id.as_deref(),
        Some(f.signal.signal_id.as_str())
    );
    assert_eq!(event.quote_in_amount_raw.as_deref(), Some("4000"));
    assert_eq!(event.quote_out_amount_raw.as_deref(), Some("300000000"));
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));
    Ok(())
}
#[tokio::test]
async fn b64_local_a_refusal_survives_successful_b_in_owned_queue() -> Result<()> {
    let f = Fixture::new(4000)?;
    f.replace(4000)?;
    let b = super::source_sell_sweep_fixture::add_signal(&f, "valid-b")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let cfg = config(&listener)?;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg);
    let now = Utc::now();
    let (out, server) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            runner.process_tick(
                &f.store,
                "shadow_recorded",
                now,
                now - chrono::Duration::seconds(5),
                10
            ),
            http::serve(listener, http::Replies::default())
        )
    })
    .await?;
    assert_eq!(server?.len(), 2);
    let out = out?;
    assert_eq!(out.source_sell_refusals.count(), 1, "{out:?}");
    assert_eq!(out.source_sell_refusals.order_id(), f.signal.signal_id);
    assert!(out
        .last_error
        .as_ref()
        .unwrap()
        .contains(&f.signal.signal_id));
    assert_eq!(out.close_inserted, 1);
    let event = f
        .store
        .load_execution_quote_canary_event_by_id(out.last_event_id.as_deref().unwrap())?
        .unwrap();
    assert_eq!(event.signal_id.as_deref(), Some(b.signal_id.as_str()));
    let mut tick = crate::execution_canary::ExecutionCanaryTickSummary::default();
    crate::execution_canary_summary::apply_quote_summary(&mut tick, out);
    let log = super::submit_refusal_fixture::capture(|| {
        crate::telemetry::record_execution_canary_tick(&tick)
    });
    assert_eq!(log["source_sell_refusal_id"], f.signal.signal_id);
    assert_eq!(
        log["source_sell_refusal_reason"],
        "source_sell_generation_mismatch"
    );
    Ok(())
}
