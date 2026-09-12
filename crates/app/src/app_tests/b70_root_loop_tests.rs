use super::{b64_http_fixture as http, b70_fixture::Fixture, b70_hooks::Installed};
use anyhow::{ensure, Result};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

async fn check(label: &str, risk_enabled: bool, wakeup: bool) -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut f = Fixture::new(&format!("http://{}", listener.local_addr()?), true).await?;
    f.risk.shadow_killswitch_enabled = risk_enabled;
    let (installed, tx) = Installed::new();
    let h = &installed.0;
    let a = f.buy();
    let b = f.sell();
    let signal = "shadow:b70-hot-buy-a:leader-a:buy:TokenA";
    let orders = || -> Result<i64> {
        Ok(f.f.conn()?.query_row(
            "SELECT COUNT(*) FROM orders WHERE signal_id=?1",
            [signal],
            |r| r.get(0),
        )?)
    };
    let controller = async {
        let result = async {
            tx.try_send(a.clone())?;
            let requests = http::pair(&listener).await?;
            tx.try_send(b.clone())?;
            tokio::time::timeout(Duration::from_millis(500), async {
                h.wait("ack_inserted", &b.signature, 1).await?;
                h.wait("staging_inserted", &b.signature, 1).await
            })
            .await??;
            h.wait("recorded_pending", signal, 1).await?;
            let lots = f.f.store.shadow_risk_open_lot_count_for_token("TokenA")?;
            ensure!(lots == 1, "A must have its own recorded shadow lot");
            let tick = h.count("execution_tick", "") + 1;
            h.wait("execution_tick", "", tick).await?;
            ensure!(orders()? == 0);
            h.mark("root_release_http", &a.signature);
            for request in requests {
                let body = request.quote();
                request.reply(200, body).await?;
            }
            h.wait("hot_quote_completed", &a.signature, 1).await?;
            let event =
                f.f.store
                    .load_latest_execution_quote_canary_entry_event(signal)?
                    .unwrap();
            let mut before = orders()?;
            let mut after = before;
            if wakeup {
                tokio::time::sleep(Duration::from_millis(250)).await;
                ensure!(
                    h.count("execution_tick", "") == tick,
                    "timing arm crossed next tick; inconclusive"
                );
                before = orders()?;
                h.mark("root_before_next_tick", &a.signature);
                h.wait("execution_tick", "", tick + 1).await?;
                after = orders()?;
                ensure!(
                    after == 1,
                    "healthy positive control must execute at next tick"
                );
            }
            http::no_more(&listener).await?;
            Ok::<_, anyhow::Error>(json!({"risk_enabled":risk_enabled,"own_shadow_lots":lots,
                "quote_status":event.quote_status,"quote_error":event.error,
                "orders_before_next_tick":before,"orders_after_next_tick":after,
                "sell":f.snapshot()?,"events":h.events.lock().unwrap().clone()}))
        }
        .await;
        h.stop();
        result
    };
    let (daemon, control) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(f.run(), controller)
    })
    .await?;
    daemon?;
    let observation = control?;
    ensure!(h.count("checked_shutdown", "") == 1);
    f.save(label, observation.clone())?;
    ensure!(
        observation["quote_status"] == "ok",
        "own accepted lot must not reject completion: {observation}"
    );
    if wakeup {
        ensure!(
            observation["orders_before_next_tick"] == 1,
            "ready BUY was deferred until periodic timer: {observation}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn b70_root_healthy_completion_control() -> Result<()> {
    check("root-healthy", false, false).await
}
#[tokio::test]
async fn b70_root_own_lot_must_not_reject_quote() -> Result<()> {
    check("root-own-lot", true, false).await
}
#[tokio::test]
async fn b70_root_ready_buy_must_wake_before_timer() -> Result<()> {
    check("root-wakeup", false, true).await
}
