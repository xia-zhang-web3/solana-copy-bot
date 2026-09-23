#[path = "../execution_strict_quote_http.rs"]
mod strict_http;
use super::b60_http_tests::{responses, GENERIC, PUMP};
use super::*;
use crate::execution_quote_timing::apply_error_timing;
use tokio::net::TcpListener;

#[tokio::test]
async fn b105_concrete_generic_and_pump_completion_errors_and_untimed_parser() -> Result<()> {
    for pump in [false, true] {
        for (status, body, success) in [
            ("200 OK", if pump { PUMP } else { GENERIC }, true),
            ("503 Unavailable", "down", false),
            ("200 OK", "{", false),
            ("200 OK", "{}", false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let cfg = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
            let client = reqwest::Client::new();
            let fetch = async {
                if pump {
                    crate::execution_pump_fun_quote_http::fetch_pump_fun_quote_sample(
                        &client,
                        &cfg,
                        "buy",
                        "Token",
                        "200000000",
                    )
                    .await
                } else {
                    crate::execution_quote_http::fetch_quote_sample(
                        &client,
                        &cfg,
                        super::b58_fixture::SOL,
                        "Token",
                        "200000000",
                        50,
                    )
                    .await
                }
            };
            let replies = [(status, body)];
            let (result, received) = tokio::join!(fetch, responses(listener, &replies, 30));
            let returned = Utc::now();
            let received = received?;
            if success {
                let q = result?;
                let available = q.quote_response_available_ts.unwrap();
                assert!(q.http_request_started_ts.unwrap() <= received[0]);
                assert!(received[0] + chrono::Duration::milliseconds(30) <= available);
                assert!(available <= returned);
            } else {
                let error = result.unwrap_err();
                let mut event = crate::execution_quote_canary_helpers::entry_error_event(
                    &super::execution_state_machine_tiny_submit_route::tiny_route_signal(
                        "availability-error",
                        returned,
                    ),
                    returned,
                    &anyhow::anyhow!("fixture"),
                );
                event.quote_response_available_ts = Some(returned);
                apply_error_timing(&mut event, &error);
                assert!(event.quote_response_available_ts.is_none());
            }
        }
    }
    let q = crate::execution_quote_http::quote_sample_from_json(serde_json::from_str(GENERIC)?)?;
    assert!(q.quote_response_available_ts.is_none() && q.http_request_started_ts.is_none());
    Ok(())
}

#[tokio::test]
async fn b105_generic_retry_completion_belongs_to_final_success() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let cfg = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
    let client = reqwest::Client::new();
    let replies = [
        ("400 Bad Request", "TOKEN_NOT_TRADABLE"),
        ("200 OK", GENERIC),
    ];
    let (result, received) = tokio::join!(
        crate::execution_quote_http::fetch_quote_sample(
            &client,
            &cfg,
            super::b58_fixture::SOL,
            "Token",
            "200000000",
            50
        ),
        responses(listener, &replies, 30)
    );
    let q = result?;
    let received = received?;
    assert_eq!(received.len(), 2);
    assert!(q.http_request_started_ts.unwrap() <= received[0]);
    assert!(
        q.quote_response_available_ts.unwrap() >= received[1] + chrono::Duration::milliseconds(30)
    );
    assert!(q.latency_ms >= 160);
    Ok(())
}

#[tokio::test]
async fn b105_provider_selection_preserves_independent_completion_and_old_metadata() -> Result<()> {
    use super::b64_case_fixture::run;
    use super::b64_http_fixture::Replies;
    for pump_first in [false, true] {
        for pump_error in [false, true] {
            let c = run(
                "entry",
                Replies {
                    pump_first,
                    pump_error,
                    ..Default::default()
                },
                true,
            )
            .await?;
            c.correlated(!pump_error)?;
            let selected = if pump_error {
                &c.generic
            } else {
                c.pump.as_ref().unwrap()
            };
            assert!(selected.quote_response_available_ts.is_some());
            assert_eq!(
                c.event.quote_response_available_ts,
                selected.quote_response_available_ts
            );
            let metadata =
                crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
                    &c.f.store,
                    c.event.clone(),
                )?;
            assert_eq!(
                metadata.quote_response_available_ts,
                selected.quote_response_available_ts
            );
            let mut other_version = c.event.clone();
            other_version.request_ts += chrono::Duration::seconds(1);
            let mismatched =
                crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
                    &c.f.store,
                    other_version,
                )?;
            assert!(mismatched.quote_response_available_ts.is_none());
            if pump_error {
                assert!(c
                    .pump
                    .as_ref()
                    .unwrap()
                    .quote_response_available_ts
                    .is_none());
            } else {
                assert_ne!(
                    c.generic.quote_response_available_ts,
                    selected.quote_response_available_ts
                );
            }
        }
    }
    let mut v = serde_json::to_value(
        crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default(),
    )?;
    v.as_object_mut()
        .unwrap()
        .remove("quote_response_available_ts");
    let m: crate::execution_submit_adapter::ExecutionBuildPlanMetadata = serde_json::from_value(v)?;
    assert!(m.quote_response_available_ts.is_none());
    Ok(())
}

#[tokio::test]
async fn b105_strict_headers_remain_distinct_from_full_body_and_required_validation() -> Result<()>
{
    use super::b105_availability_tests::http;
    use copybot_storage_core::ordered_sell_quote::{QuoteBinding, QuoteObservation, QuoteOutcome};
    use std::time::{Duration, Instant};
    use tokio::sync::oneshot;
    for valid in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let cfg = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
        let b = QuoteBinding {
            version: 1,
            intent_id: "i".into(),
            policy: "p".into(),
            position_id: "p".into(),
            position_opened_ts: "synthetic".into(),
            source_signature: "s".into(),
            source_wallet: "w".into(),
            mint: "Token".into(),
            output_mint: super::b58_fixture::SOL.into(),
            side: "sell".into(),
            provider: "generic_metis".into(),
            endpoint: cfg.quote_canary_base_url.clone(),
            raw: 100,
            decimals: 6,
            fractional: None,
            snapshot_version: "synthetic".into(),
        };
        let body = serde_json::json!({"inputMint":"Token","outputMint":b.output_mint,"swapMode":"ExactIn","inAmount":if valid {"100"} else {"99"},"outAmount":"5"}).to_string();
        let origin = Instant::now();
        let (got_tx, got_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = http::serve(
            listener,
            http::Reply {
                status: "200 OK",
                body,
                advertised_extra: 0,
            },
            origin,
            got_tx,
            release_rx,
        );
        let controller = async {
            got_rx.await.unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
            release_tx.send(()).unwrap();
        };
        let client = reqwest::Client::new();
        let (q, server, _) = tokio::join!(
            strict_http::fetch(&client, &cfg, &b, || Ok(true)),
            server,
            controller
        );
        let server = server?;
        assert!(q.http_response.unwrap() < server.body_release.utc);
        if valid {
            assert_eq!(q.outcome, QuoteOutcome::Current);
            assert!(server.body_release.utc <= q.quote_response_available_ts.unwrap());
            assert!(q.quote_response_available_ts.unwrap() <= q.http_ended);
        } else {
            assert_eq!(q.outcome, QuoteOutcome::Unknown);
            assert!(q.quote_response_available_ts.is_none());
        }
        let mut wire = serde_json::to_value(&q)?;
        wire.as_object_mut()
            .unwrap()
            .remove("quote_response_available_ts");
        let legacy: QuoteObservation = serde_json::from_value(wire)?;
        assert!(legacy.quote_response_available_ts.is_none());
    }
    Ok(())
}
