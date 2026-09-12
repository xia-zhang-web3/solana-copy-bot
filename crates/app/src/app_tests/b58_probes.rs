use super::b58_fixture::{config, Fixture};
use super::b58_http::serve;
use super::*;
use crate::execution_quote_canary::ExecutionQuoteCanaryRunner;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn run(f: &Fixture, tokens: &[&str], delayed: bool) -> Result<Value> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let runner =
        ExecutionQuoteCanaryRunner::new(config(format!("http://{}", listener.local_addr()?)));
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let origin = Instant::now();
    let tick_now = Utc::now();
    let control = async {
        ready_rx.await?;
        let start = Instant::now();
        if delayed {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        let held_ms = start.elapsed().as_millis();
        release_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("server dropped barrier"))?;
        Ok::<_, anyhow::Error>(held_ms)
    };
    let (summary, traces, held_ms) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(
            runner.process_tick(
                &f.store,
                "shadow_recorded",
                tick_now,
                tick_now - chrono::Duration::seconds(30),
                10
            ),
            serve(listener, tokens, origin, ready_tx, release_rx),
            control
        )
    })
    .await?;
    let summary = summary?;
    let traces = traces?;
    let held_ms = held_ms?;
    let finished_us = origin.elapsed().as_micros();
    assert_eq!(summary.entry_inserted, if delayed { 2 } else { 1 });
    assert_eq!(summary.close_inserted, usize::from(delayed));
    assert_eq!(traces.len(), tokens.len());
    let mut observations = Vec::new();
    for (token, trace) in tokens.iter().zip(&traces) {
        let side = if *token == "TokenS" { "sell" } else { "buy" };
        let event = f.event(token, side)?;
        assert_eq!(event.request_ts, tick_now);
        assert_eq!(event.quote_status, "ok");
        assert_eq!(event.decision_status.as_deref(), Some("would_execute"));
        assert_eq!(
            event.decision_delay_ms,
            Some(
                (event.http_request_started_ts.unwrap() - event.signal_ts.unwrap())
                    .num_milliseconds() as u64
            )
        );
        let request_utc =
            chrono::DateTime::parse_from_rfc3339(trace["request_utc"].as_str().unwrap())?
                .with_timezone(&Utc);
        assert!(event.http_request_started_ts.unwrap() >= tick_now);
        assert!(event.http_request_started_ts.unwrap() <= request_utc);
        let queue_ms = (request_utc - tick_now).num_milliseconds();
        let mut observation = f.observe(token, side)?;
        observation["observed_queue_ms"] = json!(queue_ms);
        observation["source_to_http_ms"] =
            json!((request_utc - event.signal_ts.unwrap()).num_milliseconds());
        let age_utc =
            chrono::DateTime::parse_from_rfc3339(observation["age_before_utc"].as_str().unwrap())?
                .with_timezone(&Utc);
        let actual_request_age = (age_utc - request_utc).num_milliseconds();
        observation["observed_request_age_ms"] = json!(actual_request_age);
        observation["age_overstatement_ms"] =
            json!(observation["age_ms"].as_i64().unwrap() - actual_request_age);
        if delayed && *token != "TokenA" {
            assert!(queue_ms >= 350);
            assert!(observation["age_overstatement_ms"].as_i64().unwrap().abs() < 50);
            assert!((event.http_request_started_ts.unwrap() - tick_now).num_milliseconds() >= 350);
            let previous = &traces[observations.len() - 1];
            let released = chrono::DateTime::parse_from_rfc3339(
                previous["before_response_utc"].as_str().unwrap(),
            )?
            .with_timezone(&Utc);
            assert!(event.http_request_started_ts.unwrap() >= released);
            assert!(event.quote_latency_ms.unwrap() < held_ms as u64 / 2);
        }
        observations.push(observation);
    }
    if delayed {
        assert!(held_ms >= 400);
        assert!(f.event("TokenA", "buy")?.quote_latency_ms.unwrap() >= 400);
        assert!(
            traces[1]["accept_us"].as_u64().unwrap()
                >= traces[0]["response_complete_us"].as_u64().unwrap()
        );
        assert!(
            traces[2]["accept_us"].as_u64().unwrap()
                >= traces[1]["response_complete_us"].as_u64().unwrap()
        );
    } else {
        assert!(observations[0]["observed_queue_ms"].as_i64().unwrap() < 200);
    }
    Ok(
        json!({"tick_now":tick_now,"held_ms":held_ms,"tick_finished_us":finished_us,
        "handler_completed":true,"barrier_released":true,"traces":traces,"rows":observations}),
    )
}

#[tokio::test]
async fn b58_delayed_a_exposes_b_and_owned_sell_timestamps_with_fresh_b_control() -> Result<()> {
    let delayed = Fixture::new("delayed")?;
    let fresh = Fixture::new("fresh")?;
    let source_ts = Utc::now() - chrono::Duration::seconds(1);
    delayed.seed(
        "TokenA",
        "buy",
        source_ts - chrono::Duration::milliseconds(1),
    )?;
    delayed.seed("TokenB", "buy", source_ts)?;
    delayed.seed("TokenS", "sell", source_ts)?;
    fresh.seed("TokenB", "buy", source_ts)?;
    let evidence = run(&delayed, &["TokenA", "TokenB", "TokenS"], true).await?;
    delayed.capture("delayed", evidence.clone())?;
    let control = run(&fresh, &["TokenB"], false).await?;
    fresh.capture("fresh", control.clone())?;
    let b = &evidence["rows"][1];
    let c = &control["rows"][0];
    for key in [
        "signal_id",
        "signal_ts",
        "quote_in_amount_raw",
        "quote_out_amount_raw",
        "route_plan_json",
        "quote_response_json",
        "decision_status",
        "decision_reason",
    ] {
        assert_eq!(b[key], c[key], "identity/response/decision control {key}");
    }
    assert_eq!(
        evidence["traces"][1]["request_line"],
        control["traces"][0]["request_line"]
    );
    assert!(
        b["observed_queue_ms"].as_i64().unwrap() - c["observed_queue_ms"].as_i64().unwrap() >= 350
    );
    assert!(
        b["age_overstatement_ms"].as_i64().unwrap() < 100,
        "B actual quote age must exclude waiting for A"
    );
    Ok(())
}

#[tokio::test]
async fn b58_pre_http_error_has_event_timestamp_but_no_request_or_latency() -> Result<()> {
    let f = Fixture::new("pre-http-error")?;
    f.seed("TokenB", "buy", Utc::now() - chrono::Duration::seconds(1))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut config = config(format!("http://{}", listener.local_addr()?));
    config.quote_canary_buy_size_sol = 0.0;
    let runner = ExecutionQuoteCanaryRunner::new(config);
    let now = Utc::now();
    tokio::time::timeout(
        Duration::from_secs(3),
        runner.process_tick(
            &f.store,
            "shadow_recorded",
            now,
            now - chrono::Duration::seconds(30),
            10,
        ),
    )
    .await??;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    drop(listener);
    let event = f.event("TokenB", "buy")?;
    assert_eq!(event.quote_status, "error");
    assert_eq!(event.request_ts, now);
    assert_eq!(event.quote_latency_ms, None);
    assert_eq!(event.http_request_started_ts, None);
    assert_eq!(event.decision_delay_ms, None);
    assert_eq!(event.quote_out_amount_raw, None);
    f.capture(
        "pre-http-error",
        json!({"tick_now":now,"http_requests":0,"listener_closed":true,
        "rows":[f.observe("TokenB", "buy")?]}),
    )?;
    Ok(())
}
