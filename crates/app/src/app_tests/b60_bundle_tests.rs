use super::b58_fixture::{config, Fixture};
use super::b60_http_tests::{GENERIC, PUMP};
use super::*;
use copybot_storage_core::{PROVIDER_GENERIC_METIS, PROVIDER_PUMP_FUN_PAID};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn b60_selected_alternative_provider_keeps_own_payload_and_timing() -> Result<()> {
    let f = Fixture::new("alternative")?;
    f.seed("TokenB", "buy", Utc::now() - chrono::Duration::seconds(1))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut cfg = config(format!("http://{}", listener.local_addr()?));
    cfg.quote_canary_pump_fun_parallel_enabled = true;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg);
    let tick = Utc::now();
    let (run, received) = tokio::time::timeout(Duration::from_secs(4), async {
        tokio::join!(
            runner.process_tick(
                &f.store,
                "shadow_recorded",
                tick,
                tick - chrono::Duration::seconds(30),
                10
            ),
            responses(listener, &[("200 OK", GENERIC), ("200 OK", PUMP)], 40)
        )
    })
    .await?;
    assert_eq!(run?.entry_inserted, 1);
    let received = received?;
    let event = f.event("TokenB", "buy")?;
    let generic = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_GENERIC_METIS)?
        .unwrap();
    let pump = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_PUMP_FUN_PAID)?
        .unwrap();
    assert_eq!(generic.request_ts, tick);
    assert_eq!(pump.request_ts, tick);
    assert!(generic.http_request_started_ts.unwrap() <= received[0]);
    assert!(pump.http_request_started_ts.unwrap() >= tick);
    assert!(pump.http_request_started_ts.unwrap() <= received[1]);
    assert_eq!(event.http_request_started_ts, pump.http_request_started_ts);
    assert_eq!(event.quote_response_json, pump.quote_response_json);
    assert_eq!(event.quote_latency_ms, pump.quote_latency_ms);
    assert_ne!(pump.quote_out_amount_raw, generic.quote_out_amount_raw);
    let metadata =
        crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
            &f.store,
            event.clone(),
        )?;
    assert_eq!(
        metadata.http_request_started_ts,
        pump.http_request_started_ts
    );
    assert_eq!(metadata.quote_response_json, pump.quote_response_json);
    assert_eq!(metadata.quote_request_ts, Some(tick));
    // Selection rules remain unchanged for an eligible older sample; it cannot borrow newer event time.
    let mut newer = event;
    newer.request_ts += chrono::Duration::seconds(1);
    newer.http_request_started_ts = Some(tick + chrono::Duration::seconds(1));
    let stale = crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
        &f.store, newer,
    )?;
    assert_eq!(stale.quote_response_json, pump.quote_response_json);
    assert_eq!(stale.http_request_started_ts, None);
    Ok(())
}

#[tokio::test]
async fn b60_alternative_provider_error_does_not_inherit_generic_payload_or_time() -> Result<()> {
    let f = Fixture::new("alternative-error")?;
    f.seed("TokenB", "buy", Utc::now() - chrono::Duration::seconds(1))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut cfg = config(format!("http://{}", listener.local_addr()?));
    cfg.quote_canary_pump_fun_parallel_enabled = true;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg);
    let tick = Utc::now();
    let (run, received) = tokio::time::timeout(Duration::from_secs(4), async {
        tokio::join!(
            runner.process_tick(
                &f.store,
                "shadow_recorded",
                tick,
                tick - chrono::Duration::seconds(30),
                10
            ),
            responses(listener, &[("200 OK", GENERIC), ("200 OK", "{}")], 25)
        )
    })
    .await?;
    run?;
    let received = received?;
    let event = f.event("TokenB", "buy")?;
    let pump = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_PUMP_FUN_PAID)?
        .unwrap();
    assert_eq!(pump.quote_status, "error");
    assert_eq!(pump.quote_response_json, None);
    assert_eq!(pump.quote_out_amount_raw, None);
    assert!(pump.http_request_started_ts.unwrap() >= tick);
    assert!(pump.http_request_started_ts.unwrap() <= received[1]);
    assert!(pump.quote_latency_ms.unwrap() >= 25);
    assert_ne!(pump.http_request_started_ts, event.http_request_started_ts);
    assert_eq!(event.quote_status, "ok");
    Ok(())
}

// The runner now starts both read-only requests. Bind replies to their endpoint,
// and accept both before replying; arrival order must not exchange payloads.
async fn responses(
    listener: TcpListener,
    replies: &[(&str, &str)],
    delay: u64,
) -> Result<Vec<chrono::DateTime<Utc>>> {
    assert_eq!(replies.len(), 2);
    let requests = super::b64_http_fixture::pair(&listener).await?;
    let mut received = vec![Utc::now(); 2];
    for request in requests {
        let index = usize::from(request.pump());
        received[index] = request.received;
        let (status, body) = replies[index];
        let status = status.split_whitespace().next().unwrap().parse()?;
        tokio::time::sleep(Duration::from_millis(delay)).await;
        request.reply(status, serde_json::from_str(body)?).await?;
    }
    super::b64_http_fixture::no_more(&listener).await?;
    Ok(received)
}
