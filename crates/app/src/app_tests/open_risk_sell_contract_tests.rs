use super::open_risk_sell_fixture::{Fixture, SOL, TOKEN};
use super::open_risk_sell_task_fixture::RpcTask;
use anyhow::Result;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?)
}

#[tokio::test]
async fn idle_finish_closes_socket_and_repeated_finish_is_rejected() -> Result<()> {
    let mut f = Fixture::new(600_000).await?;
    let url = reqwest::Url::parse(&f.config.submit_adapter_http_url)?;
    let address = (url.host_str().unwrap(), url.port().unwrap());
    let dir = f.dir.clone();
    f.finish().await?;
    assert!(tokio::time::timeout(
        Duration::from_secs(1),
        tokio::net::TcpStream::connect(address)
    )
    .await?
    .is_err());
    assert!(f
        .finish()
        .await
        .unwrap_err()
        .to_string()
        .contains("already finished"));
    drop(f);
    assert!(!dir.exists());
    Ok(())
}

#[tokio::test]
async fn completed_success_is_observed() -> Result<()> {
    let (notify, done) = tokio::sync::oneshot::channel::<()>();
    let mut task = RpcTask::new(tokio::spawn(async move {
        let _notify = notify;
        Ok(())
    }));
    // Drop of the sender proves the task body exited; no timing sleep/yield guess.
    assert!(done.await.is_err());
    task.finish().await?;
    Ok(())
}

#[tokio::test]
async fn quotes_and_token_supply_use_supported_pairs_and_exact_units() -> Result<()> {
    let mut f = Fixture::new(600_000).await?;
    let http = client()?;
    for mint in [TOKEN.to_owned(), bs58::encode([21u8; 32]).into_string()] {
        for (input, output, amount, out, minimum) in [
            (mint.as_str(), SOL, "7000", "70000000", "66500000"),
            (SOL, mint.as_str(), "200000000", "20000", "19000"),
        ] {
            let response: Value = http
                .get(format!("{}/quote", f.config.quote_canary_base_url))
                .query(&[
                    ("inputMint", input),
                    ("outputMint", output),
                    ("amount", amount),
                    ("slippageBps", "500"),
                ])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(response["inputMint"], input);
            assert_eq!(response["outputMint"], output);
            assert_eq!(response["inAmount"], amount);
            assert_eq!(response["outAmount"], out);
            assert_eq!(response["otherAmountThreshold"], minimum);
        }
        let decimals = crate::execution_quote_canary_rpc::fetch_spl_token_decimals(
            &http,
            &f.config.priority_fee_canary_rpc_url,
            &mint,
            500,
        )
        .await?;
        assert_eq!(decimals, Some(3));
    }
    assert_eq!(f.responses.lock().unwrap().len(), 6);
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn negative_rpc_errors_propagate_through_finish() -> Result<()> {
    for (method, params, expected) in [
        ("unsupportedMethod", json!([]), "unexpected loopback RPC"),
        (
            "getTokenSupply",
            json!(["unknown-mint"]),
            "unsupported getTokenSupply mint",
        ),
        ("getTokenSupply", json!([TOKEN, {}]), "requires one mint"),
        ("getTokenSupply", json!([3]), "mint must be a string"),
        ("getTokenSupply", json!({}), "malformed RPC params"),
    ] {
        let mut f = Fixture::new(600_000).await?;
        assert!(client()?
            .post(&f.config.submit_adapter_http_url)
            .json(&json!({"jsonrpc":"2.0","id":"negative","method":method,"params":params}))
            .send()
            .await
            .is_err());
        let error = format!("{:#}", f.finish().await.unwrap_err());
        assert!(error.contains(expected), "{error}");
        assert!(
            error.contains("RPC fixture server failed"),
            "Result error observed"
        );
        assert!(
            f.finish().await.is_err(),
            "failed finish cannot become success"
        );
        eprintln!("expected negative RPC: {error}");
    }
    Ok(())
}

#[tokio::test]
async fn negative_quote_requests_cannot_receive_generic_success() -> Result<()> {
    for (query, expected) in [
        (
            format!("inputMint={SOL}&outputMint={SOL}&amount=10"),
            "unsupported quote pair",
        ),
        (
            format!("inputMint=unknown&outputMint={SOL}&amount=7000"),
            "unsupported quote pair",
        ),
        (
            format!("inputMint={TOKEN}&outputMint={SOL}&amount=0"),
            "zero quote amount",
        ),
        (
            format!("inputMint={TOKEN}&outputMint={SOL}&amount=-1"),
            "invalid quote amount",
        ),
        (
            format!("inputMint={TOKEN}&outputMint={SOL}&amount=18446744073709551615"),
            "quote amount overflow",
        ),
        (
            format!("inputMint={TOKEN}&outputMint={SOL}&amount=1&amount=2"),
            "requires one amount",
        ),
        (
            format!("inputMint={TOKEN}&amount=7000"),
            "requires one outputMint",
        ),
    ] {
        let mut f = Fixture::new(600_000).await?;
        assert!(client()?
            .get(format!("{}/quote?{query}", f.config.quote_canary_base_url))
            .send()
            .await
            .is_err());
        let error = format!("{:#}", f.finish().await.unwrap_err());
        assert!(error.contains(expected), "{error}");
        eprintln!("expected negative quote: {error}");
    }
    Ok(())
}

#[tokio::test]
async fn negative_malformed_http_and_unknown_endpoint_fail() -> Result<()> {
    for (request, expected) in [
        (
            "POST / HTTP/1.1\r\nContent-Length: 1\r\n\r\n{",
            "malformed HTTP JSON",
        ),
        (
            "POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}",
            "malformed RPC envelope",
        ),
        (
            "GET /unexpected HTTP/1.1\r\n\r\n",
            "unexpected loopback endpoint",
        ),
        (
            "POST / HTTP/1.1\r\nContent-Length: 65536\r\n\r\n",
            "HTTP body exceeds fixture bound",
        ),
    ] {
        let mut f = Fixture::new(600_000).await?;
        let url = reqwest::Url::parse(&f.config.submit_adapter_http_url)?;
        let mut stream =
            tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap())).await?;
        stream.write_all(request.as_bytes()).await?;
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut reply)).await??;
        assert!(reply.is_empty());
        let error = format!("{:#}", f.finish().await.unwrap_err());
        assert!(error.contains(expected), "{error}");
        eprintln!("expected negative HTTP: {error}");
    }
    Ok(())
}

#[tokio::test]
async fn negative_panicking_task_is_not_normal_cancellation() -> Result<()> {
    let (notify, done) = tokio::sync::oneshot::channel::<()>();
    let mut task = RpcTask::new(tokio::spawn(async move {
        let _notify = notify;
        panic!("intentional owned-SELL server panic control");
        #[allow(unreachable_code)]
        Ok(())
    }));
    assert!(done.await.is_err());
    let error = task.finish().await.unwrap_err().to_string();
    assert!(error.contains("RPC fixture task failed"));
    assert!(error.contains("intentional owned-SELL server panic control"));
    assert!(task.finish().await.is_err());
    eprintln!("expected negative JoinError: {error}");
    Ok(())
}
