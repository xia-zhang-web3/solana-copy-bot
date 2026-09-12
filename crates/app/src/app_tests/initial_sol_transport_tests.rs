use super::initial_sol_rpc_fixture::FundingRpc;
use super::native_rpc_fixture::{Fixture as Rpc, Reply};
use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;
use std::time::{Duration, Instant};

#[tokio::test]
async fn initial_sol_actual_submit_shared_deadline_cancels_all_three_without_retries() -> Result<()>
{
    let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let server = Rpc::start_with_in_flight(3, |r| {
        let mut reply = Reply::json(FundingRpc::default().reply(r));
        reply.wait_for_cancel = true;
        reply.headers_before_cancel = true;
        reply
    })
    .await?;
    f.config.submit_adapter_http_url = server.endpoint.clone();
    f.config.submit_timeout_ms = 100;
    let started = Instant::now();
    let out = f.submit(&envelope).await;
    let elapsed = started.elapsed();
    f.finish().await?;
    let trace = server.finish().await?;
    let out = out?;
    assert_eq!(out.failed, 1, "{out:?}");
    let error = out.error.unwrap();
    assert!(
        error.contains("initial_sol_observations_unavailable")
            && error.contains("native_rpc_timeout"),
        "{error}"
    );
    assert!(!error.contains("private-endpoint-secret"));
    assert!(
        elapsed >= Duration::from_millis(80) && elapsed < Duration::from_millis(700),
        "{elapsed:?}"
    );
    assert_eq!(trace.len(), 3);
    assert!(trace
        .iter()
        .all(|r| r.cancellation_seen && r.completed.is_some()));
    let first_completed = trace.iter().map(|r| r.completed.unwrap()).min().unwrap();
    assert!(
        trace.iter().all(|r| r.received < first_completed),
        "requests must overlap: {trace:?}"
    );
    eprintln!(
        "B26_DEADLINE timeout_ms=100 requests=3 cancelled=3 elapsed_ms={}",
        elapsed.as_millis()
    );
    Ok(())
}

#[tokio::test]
async fn initial_sol_invalid_timeout_is_buy_only_and_never_queries_collector() -> Result<()> {
    for timeout in [0, 30_001, u64::MAX] {
        let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
        let envelope = f.build().await?.envelope.unwrap();
        let server = Rpc::start(false, |_| panic!("invalid timeout must not send HTTP")).await?;
        f.config.submit_adapter_http_url = server.endpoint.clone();
        f.config.submit_timeout_ms = timeout;
        let out = f.submit(&envelope).await;
        f.finish().await?;
        let trace = server.finish().await?;
        let out = out?;
        assert_eq!(out.failed, 1, "{out:?}");
        assert!(out.error.unwrap().contains("native_rpc_timeout_bounds"));
        assert!(trace.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_guarded_fallback_and_labels_cannot_skip_collection() -> Result<()> {
    for route in [
        Route::Metis,
        Route::Paid,
        Route::DirectFallback,
        Route::PaidFallback,
    ] {
        for enough in [false, true] {
            let mut f = Fixture::new(route, 200_000, 200_000).await?;
            f.wire.lock().unwrap().guard = Some(50_000_001);
            f.funding.lock().unwrap().balance = if enough { 1_000_000_000 } else { 0 };
            let envelope = f.build().await?.envelope.unwrap();
            let out = f.submit(&envelope).await;
            f.finish().await?;
            let out = out?;
            assert_eq!(f.sends(), usize::from(enough), "{route:?}: {out:?}");
            if !enough {
                assert!(out.error.unwrap().starts_with("initial_sol_insufficient:"));
            }
            let calls = f.calls.lock().unwrap();
            assert_eq!(
                calls
                    .iter()
                    .filter(|(_, r)| r["id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("native-funding-")))
                    .count(),
                3
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_disabled_and_sell_do_not_use_new_collector() -> Result<()> {
    for sell in [false, true] {
        let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
        if sell {
            f.make_sell()?;
        }
        let envelope = f.build().await?.envelope.unwrap();
        f.config.pretrade_min_sol_reserve = f64::NAN;
        f.config.submit_timeout_ms = 30_001; // outside collector bound, within existing transport bound
        if !sell {
            f.config.canary_tiny_submit_enabled = false;
        }
        let out = f.submit(&envelope).await;
        f.finish().await?;
        let out = out?;
        assert_eq!(out.failed, 0, "{out:?}");
        assert_eq!(f.sends(), usize::from(sell));
        assert_eq!(out.submit_disabled, usize::from(!sell));
        assert!(f.calls.lock().unwrap().iter().all(|(_, r)| !r["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("native-funding-"))));
    }
    Ok(())
}
