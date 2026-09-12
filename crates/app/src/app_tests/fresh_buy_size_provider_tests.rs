use super::execution_build_plan_refresh_contract::write_http_status_json;
use super::execution_state_machine_tiny_submit_route::tiny_route_signal;
use super::fresh_buy_size_fixture::*;
use crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata;
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata as gate;
use crate::execution_quote_provider_selection::{
    QUOTE_SOURCE_GENERIC_METIS, QUOTE_SOURCE_PUMP_FUN_PAID,
};
use anyhow::Result;
use copybot_config::ExecutionConfig;
use serde_json::json;
use tokio::io::AsyncReadExt;

#[derive(Clone, Copy)]
enum ProviderPath {
    Paid,
    Completed,
    NoRoute,
    ErrorPaid,
    ErrorGeneric,
    ErrorFallback,
}

async fn check_path(path: ProviderPath) -> Result<()> {
    for execute in [false, true] {
        let (i0, o0, i1, o1) = if execute {
            (20_000_000, 200, 10_000_000, 100)
        } else {
            (10_000_000, 100, 20_000_000, 180)
        };
        let mut old = metadata(i0, o0, 0.0);
        let starts_paid = matches!(
            path,
            ProviderPath::Paid | ProviderPath::Completed | ProviderPath::ErrorPaid
        );
        if starts_paid {
            old.quote_source = Some(QUOTE_SOURCE_PUMP_FUN_PAID.into());
        }
        let original = old.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut config = ExecutionConfig::default();
        config.canary_buy_size_sol = i1 as f64 / 1e9;
        config.quote_canary_buy_slippage_bps = 500;
        config.quote_canary_timeout_ms = 1000;
        config.quote_canary_base_url = format!("http://{}", listener.local_addr()?);
        config.quote_canary_pump_fun_parallel_enabled =
            matches!(path, ProviderPath::NoRoute | ProviderPath::ErrorFallback);
        let mut pump = json!({"quote":{"inAmount":i1.to_string(),"outAmount":o1.to_string(),
            "meta":{"isCompleted":false,"outDecimals":0,"inDecimals":9}}});
        let generic = quote(&i1.to_string(), &o1.to_string());
        let no_route =
            json!({"error":"Could not find any route","errorCode":"COULD_NOT_FIND_ANY_ROUTE"});
        let steps = match path {
            ProviderPath::Paid => vec![("/pump-fun/quote?", 200, pump)],
            ProviderPath::Completed => {
                pump["quote"]["meta"]["isCompleted"] = true.into();
                vec![("/pump-fun/quote?", 200, pump), ("/quote?", 200, generic)]
            }
            ProviderPath::NoRoute => {
                vec![("/quote?", 400, no_route), ("/pump-fun/quote?", 200, pump)]
            }
            ProviderPath::ErrorPaid => vec![("/pump-fun/quote?", 400, no_route)],
            ProviderPath::ErrorGeneric => vec![("/quote?", 400, no_route)],
            ProviderPath::ErrorFallback => vec![
                ("/quote?", 400, no_route.clone()),
                ("/pump-fun/quote?", 400, no_route),
            ],
        };
        let server = tokio::spawn(async move {
            let mut calls = 0;
            for (path, status, body) in steps {
                let mut socket = listener.accept().await.unwrap().0;
                let mut buf = [0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]);
                assert!(request.starts_with(&format!("GET {path}")));
                assert!(request.contains(&format!("amount={i1}")));
                write_http_status_json(&mut socket, status, &body.to_string()).await;
                calls += 1;
            }
            calls
        });
        let fresh = refresh_tiny_buy_build_plan_metadata(
            &reqwest::Client::new(),
            &config,
            &tiny_route_signal("fresh-size-provider", chrono::Utc::now()),
            old,
        )
        .await?;
        let calls = tokio::time::timeout(std::time::Duration::from_secs(3), server).await??;
        assert_eq!(
            calls,
            if matches!(
                path,
                ProviderPath::Completed | ProviderPath::NoRoute | ProviderPath::ErrorFallback
            ) {
                2
            } else {
                1
            }
        );
        if matches!(
            path,
            ProviderPath::ErrorPaid | ProviderPath::ErrorGeneric | ProviderPath::ErrorFallback
        ) {
            assert_eq!(fresh.quote_status.as_deref(), Some("error"));
            assert_eq!(fresh.decision_status.as_deref(), Some("unknown"));
            assert_eq!(
                fresh.decision_reason.as_deref(),
                Some("fresh_submit_quote_error")
            );
            assert_eq!(fresh.quote_price_sol, None);
            assert_eq!(fresh.slippage_bps, None);
            assert!(gate(&config, &fresh).is_some());
        } else {
            assert_quote(
                &fresh,
                i1,
                o1,
                if execute { 0.0 } else { 10_000.0 / 9.0 },
                if execute {
                    "would_execute"
                } else {
                    "would_skip"
                },
            );
            assert_eq!(gate(&config, &fresh).is_none(), execute);
            assert_eq!(
                fresh.quote_source.as_deref(),
                Some(if matches!(path, ProviderPath::Completed) {
                    QUOTE_SOURCE_GENERIC_METIS
                } else {
                    QUOTE_SOURCE_PUMP_FUN_PAID
                })
            );
            let response: serde_json::Value =
                serde_json::from_str(fresh.quote_response_json.as_deref().unwrap())?;
            assert_eq!(response.pointer("/_copybot/outDecimals"), Some(&json!(0)));
        }
        assert_eq!(fresh.quote_event_id, original.quote_event_id);
        assert_eq!(fresh.priority_fee_json, original.priority_fee_json);
        assert_eq!(fresh.priority_fee_lamports, original.priority_fee_lamports);
        assert_eq!(fresh.priority_fee_source, original.priority_fee_source);
        assert_eq!(fresh.priority_fee_status, original.priority_fee_status);
    }
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_paid_pump_price_and_gate() -> Result<()> {
    check_path(ProviderPath::Paid).await
}
#[tokio::test]
async fn fresh_buy_size_completed_pump_generic_fallback() -> Result<()> {
    check_path(ProviderPath::Completed).await
}
#[tokio::test]
async fn fresh_buy_size_generic_no_route_allowed_pump_fallback() -> Result<()> {
    check_path(ProviderPath::NoRoute).await
}
#[tokio::test]
async fn fresh_buy_size_provider_errors_remain_fail_closed() -> Result<()> {
    for path in [
        ProviderPath::ErrorPaid,
        ProviderPath::ErrorGeneric,
        ProviderPath::ErrorFallback,
    ] {
        check_path(path).await?;
    }
    Ok(())
}
