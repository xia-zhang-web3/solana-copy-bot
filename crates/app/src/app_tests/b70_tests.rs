use super::{b64_http_fixture as http, b70_fixture::Fixture, b70_hooks::Installed};
use anyhow::{ensure, Result};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

async fn scenario(label: &str, with_a: bool, hot: bool, error: bool) -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let f = Fixture::new(&format!("http://{}", listener.local_addr()?), hot).await?;
    let (installed, tx) = Installed::new();
    let h = &installed.0;
    let a = f.buy();
    let b = f.sell();
    let before = f.snapshot()?;
    let controller = async {
        let result = async {
            let mut requests = Vec::new();
            let mut captures = Vec::new();
            if with_a {
                h.mark("source_ready", &a.signature);
                tx.try_send(a.clone())?;
                h.mark("source_enqueued", &a.signature);
                if hot {
                    requests = http::pair(&listener).await?;
                    h.mark("both_buy_http_held", &a.signature);
                } else {
                    h.wait("next_swap_consumed", &a.signature, 1).await?;
                }
            }
            h.mark("source_ready", &b.signature);
            tx.try_send(b.clone())?;
            h.mark("source_enqueued", &b.signature);
            // The positive deadline is shorter than A's unchanged 2s HTTP timeout.
            tokio::time::timeout(Duration::from_millis(500), async {
                h.wait("ack_inserted", &b.signature, 1).await?;
                h.wait("staging_inserted", &b.signature, 1).await
            }).await??;
            let held = f.snapshot()?;
            ensure!(held["observed_sell_rows"] == 1);
            ensure!(held["handoff"]["position"] == f.position);
            ensure!(held["staged"]["position"] == f.position);
            tx.try_send(b.clone())?;
            h.mark("duplicate_enqueued", &b.signature);
            h.wait("ack_duplicate", &b.signature, 1).await?;
            if with_a && hot {
                // Exercise both entry consumers while the same canonical claim is alive.
                h.wait("recorded_pending", "shadow:b70-hot-buy-a:leader-a:buy:TokenA", 1).await?;
                let next_tick = h.count("execution_tick", "") + 1;
                h.wait("execution_tick", "", next_tick).await?;
                ensure!(h.count("hot_quote_completed", &a.signature) == 0);
                let quotes: i64 = f.f.conn()?.query_row(
                    "SELECT COUNT(*) FROM execution_quote_canary_events", [], |r| r.get(0))?;
                let orders: i64 = f.f.conn()?.query_row(
                    "SELECT COUNT(*) FROM orders WHERE signal_id='shadow:b70-hot-buy-a:leader-a:buy:TokenA'", [], |r| r.get(0))?;
                ensure!(quotes == 0 && orders == 0, "pending entry must not quote or enter state machine");
                http::no_more(&listener).await?;
                h.mark("held_checkpoint", &b.signature);
                h.mark("release_buy_responses", &a.signature);
                for request in requests {
                    let body = if error { json!({"error":"bounded provider error"}) } else { request.quote() };
                    let status = if error {503} else {200};
                    let capture = request.reply(status, body.clone()).await?;
                    captures.push(json!({"path":capture.path,"received":capture.received,
                        "replied":capture.replied,"status":status,"response":body}));
                }
                h.wait("hot_quote_completed", &a.signature, 1).await?;
            }
            http::no_more(&listener).await?;
            Ok::<_, anyhow::Error>((held, captures))
        }.await;
        h.stop();
        result
    };
    let (daemon, control) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(f.run(), controller)
    })
    .await?;
    daemon?;
    if let Err(error) = &control {
        f.save(
            &format!("{label}-failed"),
            json!({"error":format!("{error:#}"),
            "state":f.snapshot()?,"events":h.events.lock().unwrap().clone()}),
        )?;
    }
    let (held, captures) = control?;
    let after = f.snapshot()?;
    ensure!(
        held == after,
        "repeat B must preserve P and the first durable records"
    );
    ensure!(
        h.count("ack_inserted", &b.signature) == 1 && h.count("ack_duplicate", &b.signature) == 1
    );
    ensure!(h.count("staging_inserted", &b.signature) == 1 && h.count("checked_shutdown", "") == 1);
    let events = h.events.lock().unwrap().clone();
    let seq = |stage: &str, signature: &str| {
        events
            .iter()
            .find(|e| e["stage"] == stage && e["signature"] == signature)
            .unwrap()["seq"]
            .as_u64()
            .unwrap()
    };
    ensure!(seq("source_enqueued", &b.signature) < seq("capture_start", &b.signature));
    ensure!(seq("capture_start", &b.signature) < seq("ack_inserted", &b.signature));
    if with_a && hot {
        ensure!(seq("staging_inserted", &b.signature) < seq("release_buy_responses", &a.signature));
    }
    f.save(label, json!({"label":label,"with_a":with_a,"hot_quote":hot,"provider_error":error,
        "original_position":f.position,"before":before,"held":held,"after_duplicate":after,
        "events":events,"http":captures,"boundary":"actual run_app_loop with bounded next_swap seam"}))?;
    Ok(())
}

#[tokio::test]
async fn b70_actual_dispatch_sell_before_hot_buy_success() -> Result<()> {
    scenario("hot-success", true, true, false).await
}
#[tokio::test]
async fn b70_actual_dispatch_sell_before_hot_buy_error() -> Result<()> {
    scenario("hot-error", true, true, true).await
}
#[tokio::test]
async fn b70_actual_dispatch_without_a() -> Result<()> {
    scenario("without-a", false, true, false).await
}
#[tokio::test]
async fn b70_actual_dispatch_hot_disabled() -> Result<()> {
    scenario("hot-disabled", true, false, false).await
}
