use super::{
    receipt_reconciliation_fixture::Fixture,
    receipt_rpc_fixture::{Fault, Rpc, ServerPolicy},
};
use anyhow::{ensure, Result};
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub(super) async fn caller_requires_finish(fault: Option<Fault>) -> Result<()> {
    let f = Fixture::new("buy")?;
    let rpc = Rpc::new(json!({"result":null})).await?;
    if let Some(fault) = fault {
        rpc.inject(fault);
    }
    let out = f.reconcile(&rpc, 1).await?;
    // The same valid pending business result can occur even when the child failed.
    ensure!(
        out.confirmation_pending == 1 && f.fills()? == 0,
        "pending control"
    );
    rpc.finish().await?;
    ensure!(rpc.tasks().counts() == (0, 0, 2), "unfinished server tasks");
    Ok(())
}

#[tokio::test]
async fn receipt_fixture_child_panic_and_error_are_finish_errors() -> Result<()> {
    for (fault, reason) in [
        (Fault::Panic, "synthetic receipt child panic"),
        (Fault::Error, "synthetic receipt child error"),
    ] {
        let error = caller_requires_finish(Some(fault)).await.unwrap_err();
        assert!(format!("{error:#}").contains(reason), "{error:#}");
    }
    caller_requires_finish(None).await
}

async fn raw_request(bytes: &[u8]) -> Result<String> {
    let rpc = Rpc::with_policy(
        json!({"result":null}),
        ServerPolicy {
            io: Duration::from_millis(100),
            ..ServerPolicy::default()
        },
    )
    .await?;
    let mut stream = TcpStream::connect(rpc.url.trim_start_matches("http://")).await?;
    stream.write_all(bytes).await?;
    // EOF is an error unless the complete getTransaction request explicitly expects cancellation.
    stream.shutdown().await?;
    rpc.tasks().wait_accepted(1).await?;
    let error = rpc.finish().await.unwrap_err();
    assert_eq!(rpc.tasks().counts(), (0, 0, 1));
    Ok(format!("{error:#}"))
}
#[tokio::test]
async fn receipt_fixture_read_and_parse_failures_are_bounded_and_visible() -> Result<()> {
    let cases = [
        (b"".as_slice(), "EOF before complete request"),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 20\r\n\r\n{}",
            "EOF before complete request",
        ),
        (b"POST / HTTP/1.1\r\n\r\n", "missing content length"),
        (
            b"POST / HTTP/1.1\r\nContent-Length: nope\r\n\r\n",
            "content length",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
            "duplicate content length",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 65537\r\n\r\n",
            "body too large",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 1\r\n\r\n{",
            "request JSON",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}",
            "missing method",
        ),
    ];
    for (bytes, reason) in cases {
        let error = raw_request(bytes).await?;
        assert!(error.contains(reason), "expected {reason}: {error}");
    }
    let oversized = vec![b'x'; 8193];
    assert!(raw_request(&oversized).await?.contains("headers too large"));
    Ok(())
}

#[tokio::test]
async fn receipt_fixture_idle_and_partial_request_finish_cannot_hang() -> Result<()> {
    for request in [
        b"".as_slice(),
        b"POST / HTTP/1.1\r\nContent-Length: 50\r\n\r\n{",
    ] {
        let rpc = Rpc::with_policy(
            json!(null),
            ServerPolicy {
                io: Duration::from_millis(100),
                ..ServerPolicy::default()
            },
        )
        .await?;
        let mut stream = TcpStream::connect(rpc.url.trim_start_matches("http://")).await?;
        stream.write_all(request).await?;
        rpc.tasks().wait_handlers(1).await?;
        let error = rpc.finish().await.unwrap_err();
        assert!(
            format!("{error:#}").contains("request timeout"),
            "{error:#}"
        );
        assert_eq!(rpc.tasks().counts(), (0, 0, 1));
    }
    Ok(())
}

#[tokio::test]
async fn receipt_fixture_finish_deadline_aborts_and_awaits_owned_handlers() -> Result<()> {
    let rpc = Rpc::with_policy(
        json!(null),
        ServerPolicy {
            finish: Duration::from_millis(100),
            ..ServerPolicy::default()
        },
    )
    .await?;
    let _stream = TcpStream::connect(rpc.url.trim_start_matches("http://")).await?;
    rpc.tasks().wait_handlers(1).await?;
    let error = rpc.finish().await.unwrap_err();
    assert!(format!("{error:#}").contains("finish timeout"), "{error:#}");
    assert_eq!(rpc.tasks().counts(), (0, 0, 1));
    Ok(())
}

#[tokio::test]
async fn receipt_fixture_drop_on_early_error_and_panic_aborts_owned_handlers() -> Result<()> {
    for panic in [false, true] {
        let rpc = Rpc::new(json!(null)).await?;
        let tasks = rpc.tasks();
        let _stream = TcpStream::connect(rpc.url.trim_start_matches("http://")).await?;
        tasks.wait_handlers(1).await?;
        let caller = tokio::spawn(async move {
            let _owned = rpc;
            if panic {
                panic!("synthetic early caller panic");
            }
            Err::<(), anyhow::Error>(anyhow::anyhow!("synthetic early caller error"))
        })
        .await;
        if panic {
            assert!(caller.unwrap_err().is_panic());
        } else {
            assert!(caller?.is_err());
        }
        tasks.quiescent().await?;
        assert_eq!(tasks.counts(), (0, 0, 1));
    }
    Ok(())
}

#[tokio::test]
async fn receipt_fixture_unexpected_peer_close_is_not_silently_accepted() -> Result<()> {
    // A response larger than the local socket buffer guarantees a write still in flight
    // when the peer closes; no sleep determines this ordering.
    let rpc = Rpc::new(json!(null)).await?;
    *rpc.response.lock().unwrap() = (200, "x".repeat(8 * 1024 * 1024), 0);
    let mut stream = TcpStream::connect(rpc.url.trim_start_matches("http://")).await?;
    let body = r#"{"method":"getTransaction"}"#;
    stream
        .write_all(
            format!(
                "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    let mut byte = [0];
    tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut byte)).await??;
    drop(stream);
    let error = rpc.finish().await.unwrap_err();
    assert!(format!("{error:#}").contains("response write"), "{error:#}");
    assert_eq!(rpc.tasks().counts(), (0, 0, 1));
    Ok(())
}
