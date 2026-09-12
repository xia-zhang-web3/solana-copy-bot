use super::b58_fixture::{config, Fixture};
use super::b60_http_tests::{GENERIC, PUMP};
use anyhow::{Context, Result};
use chrono::Utc;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn b64_entry_accepts_both_http_before_first_response() -> Result<()> {
    let f = Fixture::new("b64-barrier")?;
    f.seed("TokenB", "buy", Utc::now() - chrono::Duration::seconds(1))?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut cfg = config(format!("http://{}", listener.local_addr()?));
    cfg.quote_canary_pump_fun_parallel_enabled = true;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg);
    let now = Utc::now();
    let server = async {
        let mut sockets = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
                .await
                .context("overlap barrier: second quote did not start before first response")??;
            let (path, _) = tokio::time::timeout(
                Duration::from_secs(1),
                super::rpc_simulation_http_fixture::read(&mut socket),
            )
            .await??;
            sockets.push((socket, path));
        }
        assert!(sockets.iter().any(|(_, p)| p.starts_with("/quote?")));
        assert!(sockets
            .iter()
            .any(|(_, p)| p.starts_with("/pump-fun/quote?")));
        for (mut socket, path) in sockets {
            let body = if path.starts_with("/pump-fun/") {
                PUMP
            } else {
                GENERIC
            };
            socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
            socket.shutdown().await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let (run, served) = tokio::time::timeout(Duration::from_secs(6), async {
        tokio::join!(
            runner.process_tick(
                &f.store,
                "shadow_recorded",
                now,
                now - chrono::Duration::seconds(30),
                10
            ),
            server
        )
    })
    .await?;
    served?;
    assert_eq!(run?.entry_inserted, 1);
    assert_eq!(
        f.event("TokenB", "buy")?.quote_out_amount_raw.as_deref(),
        Some("2000000")
    );
    Ok(())
}
