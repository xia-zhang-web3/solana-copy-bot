use super::{
    b58_fixture::{config, Fixture, SOL},
    b64_http_fixture as http,
};
use anyhow::Result;
use chrono::Utc;
use copybot_core_types::{SwapEvent, TokenQuantity};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn b64_invalid_amounts_reach_no_quote_http() -> Result<()> {
    for kind in ["entry", "hot", "close"] {
        let f = Fixture::new(&format!("b64-invalid-{kind}"))?;
        let now = Utc::now();
        if kind == "entry" {
            f.seed("TokenB", "buy", now - chrono::Duration::seconds(1))?;
        }
        if kind == "close" {
            f.store.insert_shadow_closed_trade_exact(
                "bad-close",
                "leader-b58",
                "TokenB",
                1.0,
                Some(TokenQuantity::new(1_000_000, 6)),
                0.2,
                0.2,
                0.0,
                now - chrono::Duration::seconds(2),
                now,
            )?;
            rusqlite::Connection::open(&f.path)?
                .execute("UPDATE shadow_closed_trades SET qty_raw='1'", [])?;
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut cfg = config(format!("http://{}", listener.local_addr()?));
        cfg.quote_canary_pump_fun_parallel_enabled = true;
        cfg.quote_canary_buy_size_sol = 0.0;
        cfg.priority_fee_canary_rpc_url = cfg.quote_canary_base_url.clone();
        let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg);
        let swap = SwapEvent {
            signature: "invalid-amount".into(),
            wallet: "leader".into(),
            dex: "pumpswap".into(),
            token_in: SOL.into(),
            token_out: "TokenB".into(),
            amount_in: 0.2,
            amount_out: 1.0,
            slot: 42,
            ts_utc: now,
            exact_amounts: None,
        };
        let work = async {
            if kind == "hot" {
                runner
                    .process_hot_observed_buy_swap(&f.store, &swap, now)
                    .await
            } else {
                runner
                    .process_tick(
                        &f.store,
                        "shadow_recorded",
                        now,
                        now - chrono::Duration::seconds(30),
                        10,
                    )
                    .await
            }
        };
        let (out, server) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(work, http::no_more(&listener))
        })
        .await?;
        server?;
        let out = out?;
        assert_eq!(
            out.entry_inserted + out.close_inserted,
            1,
            "{kind}: {out:?}"
        );
        let event = f
            .store
            .load_execution_quote_canary_event_by_id(out.last_event_id.as_deref().unwrap())?
            .unwrap();
        assert!(event.error.is_some());
        assert_eq!(event.http_request_started_ts, None);
        assert_eq!(event.quote_latency_ms, None);
        assert!(f
            .store
            .load_execution_quote_canary_provider_sample(
                &event.event_id,
                copybot_storage_core::PROVIDER_PUMP_FUN_PAID
            )?
            .is_none());
    }
    Ok(())
}
