use super::{b64_http_fixture as http, b70_fixture::Fixture, b70_hooks::Installed};
use anyhow::{ensure, Result};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn b70_r1_quote_before_shadow_resumes_once_without_tick() -> Result<()> {
    quote_before_shadow(None).await
}

#[tokio::test]
async fn b70_r2_rounding_actual_loop_quote_before_shadow_resumes_once() -> Result<()> {
    quote_before_shadow(Some(0.067)).await
}

async fn quote_before_shadow(size: Option<f64>) -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut f = Fixture::new(&format!("http://{}", listener.local_addr()?), true).await?;
    f.risk.shadow_killswitch_enabled = true;
    if let Some(size) = size {
        let mut quality = super::permissive_shadow_quality();
        quality.copy_notional_sol = size;
        f.f.shadow = copybot_shadow::ShadowService::new(quality);
    }
    let (installed, tx) = Installed::new();
    let h = &installed.0;
    let a = f.buy();
    let signal = "shadow:b70-hot-buy-a:leader-a:buy:TokenA";
    let orders = || -> Result<i64> {
        Ok(f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [signal],
            |r| r.get(0),
        )?)
    };
    h.hold_shadow(true);
    let controller = async {
        let result = async {
            h.wait("execution_tick", "", 1).await?;
            tx.try_send(a.clone())?;
            let requests = http::pair(&listener).await?;
            h.wait("shadow_worker_entered", &a.signature, 1).await?;
            for request in requests {
                let body = request.quote();
                request.reply(200, body).await?;
            }
            h.wait("hot_quote_network_ready", &a.signature, 1).await?;
            ensure!(
                orders()? == 0 && f.f.store.shadow_risk_open_lot_count_for_token("TokenA")? == 0
            );
            let tick = h.count("execution_tick", "");
            h.hold_shadow(false);
            tokio::time::timeout(
                Duration::from_millis(250),
                h.wait("hot_buy_resumed", signal, 1),
            )
            .await??;
            ensure!(orders()? == 1 && h.count("execution_tick", "") == tick);
            tx.try_send(a.clone())?;
            h.wait("next_swap_consumed", &a.signature, 2).await?;
            h.wait("execution_tick", "", tick + 1).await?;
            ensure!(orders()? == 1 && h.count("hot_quote_completed", &a.signature) == 1);
            let event =
                f.f.store
                    .load_latest_execution_quote_canary_entry_event(signal)?
                    .unwrap();
            ensure!(event.quote_status == "ok" && event.signal_ts == Some(a.ts_utc));
            http::no_more(&listener).await?;
            if size.is_some() {
                let stored = f.f.store.load_copy_signal_by_signal_id(signal)?.unwrap();
                let lots = f.f.store.list_shadow_lots(&a.wallet, &a.token_out)?;
                ensure!(stored.notional_lamports.unwrap().as_u64() == 67_000_000);
                ensure!(lots.len() == 1 && lots[0].cost_lamports.unwrap().as_u64() == 67_000_001);
            }
            f.save(
                if size.is_some() {
                    "r2-loop-rounding"
                } else {
                    "r1-quote-before-shadow"
                },
                json!({"orders":orders()?, "events":h.events.lock().unwrap().clone()}),
            )?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        h.hold_shadow(false);
        h.stop();
        result
    };
    let (daemon, control) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(f.run(), controller)
    })
    .await?;
    daemon?;
    control?;
    ensure!(h.count("checked_shutdown", "") == 1);
    Ok(())
}
