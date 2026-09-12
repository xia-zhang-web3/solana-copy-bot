use super::native_ata_fixture::*;
use super::native_rpc_fixture::{Fixture, Framing, Reply};
use super::native_rpc_reuse_fixture::ReuseFixture;
use crate::execution_native_rpc::NativeFundingRpcClient;
use anyhow::Result;
use std::time::{Duration, Instant};

#[tokio::test]
async fn native_ata_rpc_body_caps_cover_length_chunked_and_close() -> Result<()> {
    for framing in [Framing::Length, Framing::Chunked, Framing::Close] {
        for too_large in [false, true] {
            let f = Fixture::start_with_in_flight(3, move |r| {
                let mut reply = Reply::json(rpc_success(r));
                if r["method"] == RENT_METHOD {
                    reply.body.resize(16 * 1024 + usize::from(too_large), b' ');
                    reply.framing = framing;
                }
                reply
            })
            .await?;
            let result = NativeFundingRpcClient::new()?
                .collect_with_classic_ata_rent(
                    &f.endpoint,
                    Duration::from_secs(2),
                    &payload(&budget())?,
                    WALLET,
                    None,
                )
                .await;
            let trace = f.finish().await?;
            exact_calls(&trace, 3)?;
            if too_large {
                assert!(
                    format!("{:#}", result.unwrap_err()).contains("native_rpc_response_too_large")
                );
            } else {
                assert_eq!(result?.rent().lamports(), 2_039_280);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_ata_rpc_shared_deadline_and_any_error_cancel_siblings() -> Result<()> {
    for broken in [
        None,
        Some("getFeeForMessage"),
        Some("getMultipleAccounts"),
        Some(RENT_METHOD),
    ] {
        for headers in [false, true] {
            let f = Fixture::start_with_in_flight(3, move |r| {
                let mut reply = Reply::json(rpc_success(r));
                if broken.is_some_and(|m| r["method"] == m) {
                    reply.body = b"{".to_vec();
                    reply.delay = Duration::from_millis(20);
                } else {
                    reply.wait_for_cancel = true;
                    reply.headers_before_cancel = headers;
                }
                reply
            })
            .await?;
            let started = Instant::now();
            let result = NativeFundingRpcClient::new()?
                .collect_with_classic_ata_rent(
                    &f.endpoint,
                    if broken.is_none() {
                        Duration::from_millis(100)
                    } else {
                        Duration::from_secs(2)
                    },
                    &payload(&budget())?,
                    WALLET,
                    None,
                )
                .await;
            let elapsed = started.elapsed();
            let trace = f.finish().await?;
            exact_calls(&trace, 3)?;
            assert_eq!(
                trace.iter().filter(|r| r.cancellation_seen).count(),
                if broken.is_none() { 3 } else { 2 }
            );
            let error = format!("{:#}", result.unwrap_err());
            assert!(
                error.contains(if broken.is_none() {
                    "native_rpc_timeout"
                } else {
                    "native_rpc_invalid_json"
                }),
                "{error}"
            );
            eprintln!(
                "broken={broken:?} headers={headers} elapsed={elapsed:?} calls={}",
                trace.len()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_ata_rpc_no_retry_or_redirect_and_same_client_recovers() -> Result<()> {
    let client = NativeFundingRpcClient::new()?;
    for status in [307, 308, 503] {
        let target = Fixture::start(false, |r| Reply::json(rpc_success(r))).await?;
        let location = target.endpoint.clone();
        let origin = Fixture::start_with_in_flight(3, move |r| {
            let mut reply = Reply::json(rpc_success(r));
            if r["method"] == RENT_METHOD {
                reply.status = status;
                reply.location = Some(location.clone());
            }
            reply
        })
        .await?;
        let result = client
            .collect_with_classic_ata_rent(
                &origin.endpoint,
                Duration::from_secs(2),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let a = origin.finish().await;
        let b = target.finish().await;
        exact_calls(&a?, 3)?;
        assert!(b?.is_empty());
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("native_rpc_http_status"));
        assert!(!error.contains("private-endpoint-secret"));
    }
    let f = Fixture::start_with_in_flight(3, |r| Reply::json(rpc_success(r))).await?;
    let result = client
        .collect_with_classic_ata_rent(
            &f.endpoint,
            Duration::from_secs(2),
            &payload(&budget())?,
            WALLET,
            None,
        )
        .await;
    let trace = f.finish().await?;
    exact_calls(&trace, 3)?;
    result?;
    Ok(())
}

#[tokio::test]
async fn native_ata_rpc_pool_reuse_and_bundle_binding_across_messages() -> Result<()> {
    let client = NativeFundingRpcClient::new()?;
    let clone = client.clone();
    let f = ReuseFixture::with_rent(true).await?;
    let mut results = Vec::new();
    let mut payloads = Vec::new();
    for round in 0..3 {
        let p = payload(&direct(true, false, 17 + round)?)?;
        results.push(
            (if round == 1 { &clone } else { &client })
                .collect_with_classic_ata_rent(
                    &f.endpoint,
                    Duration::from_secs(2),
                    &p,
                    WALLET,
                    Some(69),
                )
                .await,
        );
        payloads.push(p);
    }
    let trace = f.finish().await?;
    let values: Vec<_> = results.into_iter().collect::<Result<_>>()?;
    assert_eq!(trace.connections, 3);
    assert_eq!(trace.requests.len(), 9);
    for socket in 1..=3 {
        assert_eq!(
            trace
                .requests
                .iter()
                .filter(|(id, _)| *id == socket)
                .count(),
            3
        );
    }
    for (i, bundle) in values.iter().enumerate() {
        assert_eq!(bundle.rent().lamports(), 2_039_280 + i as u64);
        assert_eq!(bundle.native().fee().value, Some(19_000 + i as u64));
        assert_eq!(
            bundle.native().observed_payer_lamports(),
            Some(u64::MAX - i as u64)
        );
        plan(&payloads[i], WALLET, bundle)?;
        assert!(plan(&payloads[(i + 1) % 3], WALLET, bundle).is_err());
    }
    Ok(())
}
