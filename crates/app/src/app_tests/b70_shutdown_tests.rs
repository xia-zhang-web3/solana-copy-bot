use super::{
    b64_http_fixture as http, b70_event_capture, b70_fixture::Fixture, b70_hooks::Installed,
};
use anyhow::{ensure, Result};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn b70_actual_shutdown_joins_held_http_and_keeps_durable_sell() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let f = Fixture::new(&format!("http://{}", listener.local_addr()?), true).await?;
    let (installed, tx) = Installed::new();
    let h = &installed.0;
    let a = f.buy();
    let b = f.sell();
    let controller = async {
        let result = async {
            tx.try_send(a.clone())?;
            let requests = http::pair(&listener).await?;
            h.mark("both_buy_http_held", &a.signature);
            tx.try_send(b.clone())?;
            tokio::time::timeout(Duration::from_millis(500), async {
                h.wait("ack_inserted", &b.signature, 1).await?;
                h.wait("staging_inserted", &b.signature, 1).await
            })
            .await??;
            h.wait(
                "recorded_pending",
                "shadow:b70-hot-buy-a:leader-a:buy:TokenA",
                1,
            )
            .await?;
            h.mark("stop_while_http_held", &a.signature);
            h.stop();
            for request in requests {
                request.cancelled().await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        h.stop();
        result
    };
    let (joined, logs) =
        b70_event_capture::capture(tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(f.run(), controller)
        }))
        .await;
    let (daemon, control) = joined?;
    daemon?;
    control?;
    ensure!(
        h.count("checked_shutdown", "") == 1 && h.count("hot_quote_completed", &a.signature) == 0
    );
    let cancelled: Vec<_> = logs
        .iter()
        .filter(|e| e["reason"] == "hot_quote_cancelled")
        .collect();
    ensure!(cancelled.len() == 1);
    ensure!(cancelled[0]["signal_id"] == "shadow:b70-hot-buy-a:leader-a:buy:TokenA");
    ensure!(cancelled[0]["active"] == "0" && cancelled[0]["pending"] == "0");
    let quote_rows: i64 = f.f.conn()?.query_row(
        "SELECT count(*) FROM execution_quote_canary_events",
        [],
        |r| r.get(0),
    )?;
    ensure!(quote_rows == 0 && f.snapshot()?["staged"]["position"] == f.position);
    http::no_more(&listener).await?;
    f.save("shutdown-held", json!({"events":h.events.lock().unwrap().clone(), "logs":logs,
        "quote_rows":quote_rows,"state":f.snapshot()?,"boundary":"actual daemon shutdown with two held HTTP responses"}))?;
    Ok(())
}
