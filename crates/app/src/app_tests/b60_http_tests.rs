use super::*;
use crate::execution_quote_timing::QuoteAttemptResult;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub(super) const GENERIC: &str = r#"{"inAmount":"200000000","outAmount":"1000000","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#;
pub(super) const PUMP: &str = r#"{"quote":{"inAmount":"200000000","outAmount":"2000000","meta":{"isCompleted":false,"inDecimals":9,"outDecimals":6}}}"#;

pub(super) async fn responses(
    listener: TcpListener,
    replies: &[(&str, &str)],
    delay: u64,
) -> Result<Vec<chrono::DateTime<Utc>>> {
    let mut received = Vec::new();
    for (status, body) in replies {
        let (mut socket, peer) = listener.accept().await?;
        assert!(peer.ip().is_loopback());
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await?);
            anyhow::ensure!(request.len() < 16384);
        }
        received.push(Utc::now());
        tokio::time::sleep(Duration::from_millis(delay)).await;
        let reply = format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len());
        socket.write_all(reply.as_bytes()).await?;
        socket.shutdown().await?;
    }
    Ok(received)
}
async fn fetch(pump: bool, config: &ExecutionConfig) -> QuoteAttemptResult {
    let client = reqwest::Client::new();
    if pump {
        crate::execution_pump_fun_quote_http::fetch_pump_fun_quote_sample(
            &client,
            config,
            "buy",
            "TokenB",
            "200000000",
        )
        .await
    } else {
        crate::execution_quote_http::fetch_quote_sample(
            &client,
            config,
            super::b58_fixture::SOL,
            "TokenB",
            "200000000",
            50,
        )
        .await
    }
}
#[tokio::test]
async fn b60_both_http_boundaries_keep_success_and_post_start_error_timing() -> Result<()> {
    for pump in [false, true] {
        for (status, body, expected) in [
            ("200 OK", if pump { PUMP } else { GENERIC }, None),
            ("503 Unavailable", "provider down", Some("HTTP 503")),
            ("200 OK", "not json", Some("JSON decode failed")),
            ("200 OK", "{}", Some("response missing")),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let config = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
            let before = Utc::now();
            let origin = Instant::now();
            let (result, received) = tokio::time::timeout(Duration::from_secs(3), async {
                let replies = [(status, body)];
                tokio::join!(fetch(pump, &config), responses(listener, &replies, 25))
            })
            .await?;
            let received = received?;
            let (started, elapsed) = match expected {
                None => {
                    let quote = result?;
                    (quote.http_request_started_ts.unwrap(), quote.latency_ms)
                }
                Some(message) => {
                    let error = result.unwrap_err();
                    assert!(error.to_string().contains(message), "{error}");
                    let timing = error.timing.expect("post-execute error has provenance");
                    (timing.started_ts, timing.elapsed_ms)
                }
            };
            assert!(started >= before && started <= received[0]);
            assert!(elapsed >= 25 && elapsed <= origin.elapsed().as_millis() as u64);
        }
    }
    Ok(())
}
#[tokio::test]
async fn b60_pre_http_build_errors_and_transport_errors_are_distinct() -> Result<()> {
    for pump in [false, true] {
        for invalid_url in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let mut config =
                super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
            if invalid_url {
                config.quote_canary_base_url = "invalid URL".into();
            } else {
                config.quote_canary_api_key = "invalid\nheader".into();
            }
            let error = fetch(pump, &config).await.unwrap_err();
            assert!(error.timing.is_none());
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err()
            );
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let config = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
        drop(listener);
        let error = tokio::time::timeout(Duration::from_secs(3), fetch(pump, &config))
            .await?
            .unwrap_err();
        assert!(error.timing.is_some());
        assert!(error.to_string().contains("request failed"));
    }
    Ok(())
}
#[tokio::test]
async fn b60_generic_retry_retains_first_start_and_includes_backoff() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let config = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
    let (result, received) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            fetch(false, &config),
            responses(
                listener,
                &[
                    ("400 Bad Request", "TOKEN_NOT_TRADABLE"),
                    ("200 OK", GENERIC)
                ],
                25
            )
        )
    })
    .await?;
    let quote = result?;
    let received = received?;
    assert_eq!(received.len(), 2);
    assert!(quote.http_request_started_ts.unwrap() <= received[0]);
    assert!((received[1] - received[0]).num_milliseconds() >= 125);
    assert!(quote.latency_ms >= 150);
    Ok(())
}
#[test]
fn b60_old_serialized_metadata_and_unordered_clock_are_unknown() -> Result<()> {
    let mut metadata = crate::execution_submit_adapter::ExecutionBuildPlanMetadata::default();
    metadata.quote_request_ts = Some(Utc::now() - chrono::Duration::seconds(99));
    let mut old = serde_json::to_value(&metadata)?;
    old.as_object_mut()
        .unwrap()
        .remove("http_request_started_ts");
    let legacy = serde_json::from_value(old)?;
    assert_eq!(
        crate::execution_build_plan_age::quote_age_ms_at_build(&legacy),
        None
    );
    metadata.http_request_started_ts = Some(Utc::now() + chrono::Duration::seconds(10));
    assert_eq!(
        crate::execution_build_plan_age::quote_age_ms_at_build(&metadata),
        None
    );
    let source = Utc::now();
    assert_eq!(
        crate::execution_quote_timing::actual_delay(
            Some(source),
            Some(source - chrono::Duration::microseconds(1))
        ),
        None
    );
    Ok(())
}

#[tokio::test]
async fn b60_truncated_body_keeps_timing_in_both_http_boundaries() -> Result<()> {
    for pump in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let config = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
        let server = async {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await?);
                anyhow::ensure!(request.len() < 16384);
            }
            let received = Utc::now();
            tokio::time::sleep(Duration::from_millis(25)).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\nconnection: close\r\n\r\n{").await?;
            socket.shutdown().await?;
            Ok::<_, anyhow::Error>(received)
        };
        let (result, received) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(fetch(pump, &config), server)
        })
        .await?;
        let error = result.unwrap_err();
        let timing = error.timing.unwrap();
        assert!(timing.started_ts <= received?);
        assert!(timing.elapsed_ms >= 25);
        assert!(error.to_string().contains("JSON decode failed"));
    }
    Ok(())
}
