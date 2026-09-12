use super::native_funding_fixture::*;
use super::native_rpc_fixture::*;
use crate::execution_native_rpc::{
    NativeFundingRpcClient, ACCOUNTS_RESPONSE_LIMIT, FEE_RESPONSE_LIMIT,
};
use anyhow::Result;
use std::time::{Duration, Instant};

#[tokio::test]
async fn native_rpc_http_json_body_failures_are_bounded_and_redacted() -> Result<()> {
    for (case, reason) in [
        (0, "http_status"),
        (1, "invalid_json"),
        (2, "body_read"),
        (3, "response_too_large"),
    ] {
        for fee in [false, true] {
            let f = Fixture::start(true, move |request| {
                let mut reply = Reply::json(success(request));
                if (request["method"] == "getFeeForMessage") == fee {
                    match case {
                        0 => {
                            reply.status = 503;
                            reply.body = b"PRIVATE_RPC_BODY_SECRET".to_vec();
                        }
                        1 => reply.body = b"PRIVATE_RPC_BODY_SECRET".to_vec(),
                        2 => reply.declared_length = Some(reply.body.len() + 17),
                        3 => {
                            reply.declared_length = Some(ACCOUNTS_RESPONSE_LIMIT + 1);
                            reply.body.clear();
                        }
                        _ => unreachable!(),
                    }
                }
                reply
            })
            .await?;
            let result = NativeFundingRpcClient::new()?
                .collect(
                    &f.endpoint,
                    Duration::from_secs(2),
                    &payload(&budget())?,
                    WALLET,
                    None,
                )
                .await;
            let calls = f.finish().await?;
            assert_eq!(calls.len(), 2);
            let error = format!("{:#}", result.unwrap_err());
            assert!(error.contains(&format!("native_rpc_{reason}")), "{error}");
            assert!(
                !error.contains("PRIVATE_RPC_BODY_SECRET")
                    && !error.contains("private-endpoint-secret")
            );
            assert!(error.len() < 150);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_response_bounds_include_chunked_and_missing_content_length() -> Result<()> {
    for fee in [false, true] {
        for framing in [Framing::Length, Framing::Chunked, Framing::Close] {
            let limit = if fee {
                FEE_RESPONSE_LIMIT
            } else {
                ACCOUNTS_RESPONSE_LIMIT
            };
            let f = Fixture::start(true, move |request| {
                let mut reply = Reply::json(success(request));
                if (request["method"] == "getFeeForMessage") == fee {
                    reply.body.resize(limit + 1, b' ');
                    reply.framing = framing;
                }
                reply
            })
            .await?;
            let result = NativeFundingRpcClient::new()?
                .collect(
                    &f.endpoint,
                    Duration::from_secs(2),
                    &payload(&budget())?,
                    WALLET,
                    None,
                )
                .await;
            let calls = f.finish().await?;
            assert_eq!(calls.len(), 2);
            assert!(format!("{:#}", result.unwrap_err()).contains("native_rpc_response_too_large"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_exact_response_limit_is_accepted_without_prefix_data() -> Result<()> {
    for fee in [false, true] {
        let f = Fixture::start(true, move |request| {
            let mut value = success(request);
            if request["method"] == "getMultipleAccounts" {
                value["result"]["value"][0] = account(17, &vec![42; 65_537]);
            }
            let mut reply = Reply::json(value);
            if (request["method"] == "getFeeForMessage") == fee {
                reply.body.resize(
                    if fee {
                        FEE_RESPONSE_LIMIT
                    } else {
                        ACCOUNTS_RESPONSE_LIMIT
                    },
                    b' ',
                );
                reply.framing = Framing::Chunked;
            }
            reply
        })
        .await?;
        let result = NativeFundingRpcClient::new()?
            .collect(
                &f.endpoint,
                Duration::from_secs(2),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let calls = f.finish().await?;
        let value = result?;
        assert_eq!(calls.len(), 2);
        assert_eq!(value.fee().value, Some(19_000));
        assert_eq!(
            value.accounts().value[0].account,
            crate::execution_native_rpc::types::AccountObservation::Present {
                lamports: 17,
                owner_program: [171; 32],
                executable: false,
                data: vec![42; 65_537],
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_shared_deadline_cancels_both_header_or_body_waits() -> Result<()> {
    for partial_body in [false, true] {
        let f = Fixture::start(true, move |request| {
            let mut reply = Reply::json(success(request));
            reply.wait_for_cancel = true;
            reply.headers_before_cancel = partial_body;
            reply
        })
        .await?;
        let started = Instant::now();
        let result = NativeFundingRpcClient::new()?
            .collect(
                &f.endpoint,
                Duration::from_millis(100),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let elapsed = started.elapsed();
        let calls = f.finish().await?;
        assert!(
            elapsed >= Duration::from_millis(80) && elapsed < Duration::from_secs(1),
            "{elapsed:?}"
        );
        assert_eq!(calls.len(), 2);
        assert!(calls
            .iter()
            .all(|call| call.cancellation_seen && call.completed.is_some()));
        assert!(format!("{:#}", result.unwrap_err()).contains("native_rpc_timeout"));
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_one_error_cancels_other_inflight_without_partial_success() -> Result<()> {
    for fee_error in [false, true] {
        let f = Fixture::start(true, move |request| {
            let mut reply = Reply::json(success(request));
            if (request["method"] == "getFeeForMessage") == fee_error {
                reply.body = b"{".to_vec();
                reply.delay = Duration::from_millis(20);
            } else {
                reply.wait_for_cancel = true;
                reply.headers_before_cancel = true;
            }
            reply
        })
        .await?;
        let result = NativeFundingRpcClient::new()?
            .collect(
                &f.endpoint,
                Duration::from_secs(2),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let calls = f.finish().await?;
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls.iter().filter(|call| call.cancellation_seen).count(),
            1
        );
        assert!(format!("{:#}", result.unwrap_err()).contains("native_rpc_invalid_json"));
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_transport_and_endpoint_errors_never_include_endpoint_key() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    for (endpoint, reason) in [
        (
            format!("http://{address}/PRIVATE_RPC_KEY"),
            "native_rpc_transport",
        ),
        ("PRIVATE_RPC_KEY".to_owned(), "native_rpc_endpoint"),
        ("file:///PRIVATE_RPC_KEY".to_owned(), "native_rpc_endpoint"),
    ] {
        let error = format!(
            "{:#}",
            NativeFundingRpcClient::new()?
                .collect(
                    &endpoint,
                    Duration::from_secs(1),
                    &payload(&budget())?,
                    WALLET,
                    None
                )
                .await
                .unwrap_err()
        );
        assert!(error.contains(reason), "{error}");
        assert!(!error.contains("PRIVATE_RPC_KEY") && !error.contains(&address.to_string()));
    }
    Ok(())
}
