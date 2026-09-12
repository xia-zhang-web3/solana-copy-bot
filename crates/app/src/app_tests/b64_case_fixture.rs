use super::{
    b58_fixture::{self, Fixture, SOL},
    b64_http_fixture::{self as http, Capture, Replies},
};
use anyhow::Result;
use chrono::Utc;
use copybot_core_types::{ExactSwapAmounts, SwapEvent, TokenQuantity};
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert as Event, ExecutionQuoteCanaryProviderSampleInsert as Sample,
    PROVIDER_GENERIC_METIS, PROVIDER_PUMP_FUN_PAID,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::net::TcpListener;

pub(super) struct Case {
    pub f: Fixture,
    pub event: Event,
    pub generic: Sample,
    pub pump: Option<Sample>,
    pub captures: Vec<Capture>,
}
pub(super) async fn run(kind: &str, replies: Replies, known: bool) -> Result<Case> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let f = Fixture::new(&format!(
        "b64-{kind}-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))?;
    let now = Utc::now();
    let ts = now - chrono::Duration::seconds(1);
    let token = "TokenB";
    let swap = SwapEvent {
        wallet: "leader-b58".into(),
        signature: "sig-b58-TokenB".into(),
        dex: "pumpswap".into(),
        token_in: SOL.into(),
        token_out: token.into(),
        amount_in: 0.2,
        amount_out: 1.0,
        slot: 42,
        ts_utc: ts,
        exact_amounts: known.then(|| ExactSwapAmounts {
            amount_in_raw: "200000000".into(),
            amount_out_raw: "1000000".into(),
            amount_in_decimals: 9,
            amount_out_decimals: 6,
        }),
    };
    match kind {
        "entry" => {
            f.seed(token, "buy", ts)?;
            if !known {
                rusqlite::Connection::open(&f.path)?.execute("DELETE FROM observed_swaps", [])?;
            }
        }
        "owned" => f.seed(token, "sell", ts)?,
        "close" => {
            f.store.insert_shadow_closed_trade_exact(
                "b64-close",
                "leader-b58",
                token,
                1.0,
                Some(TokenQuantity::new(1_000_000, 6)),
                0.2,
                0.2,
                0.0,
                ts - chrono::Duration::seconds(10),
                ts,
            )?;
        }
        "hot" => (),
        _ => anyhow::bail!("unknown builder"),
    }
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut cfg = b58_fixture::config(format!("http://{}", listener.local_addr()?));
    cfg.quote_canary_pump_fun_parallel_enabled = !replies.generic_only;
    cfg.priority_fee_canary_rpc_url = cfg.quote_canary_base_url.clone();
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(cfg);
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
    let (result, captures) = tokio::time::timeout(Duration::from_secs(6), async {
        tokio::join!(work, http::serve(listener, replies))
    })
    .await?;
    let captures = captures?;
    let summary = result?;
    assert_eq!(
        summary.entry_inserted + summary.close_inserted,
        1,
        "{summary:?}"
    );
    let event = f
        .store
        .load_execution_quote_canary_event_by_id(summary.last_event_id.as_deref().unwrap())?
        .unwrap();
    let generic = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_GENERIC_METIS)?
        .unwrap();
    let pump = f
        .store
        .load_execution_quote_canary_provider_sample(&event.event_id, PROVIDER_PUMP_FUN_PAID)?;
    assert_eq!(event.request_ts, now);
    assert_eq!(event.signal_ts, Some(ts));
    assert_eq!(event.wallet_id, "leader-b58");
    assert_eq!(event.token, token);
    assert_eq!(
        event.side,
        if kind == "entry" || kind == "hot" {
            "buy"
        } else {
            "sell"
        }
    );
    let stored = rusqlite::Connection::open(&f.path)?;
    let names = stored.prepare("SELECT provider FROM execution_quote_canary_provider_samples WHERE event_id=?1 ORDER BY rowid")?.query_map([&event.event_id],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(
        names,
        if pump.is_some() {
            vec![PROVIDER_GENERIC_METIS, PROVIDER_PUMP_FUN_PAID]
        } else {
            vec![PROVIDER_GENERIC_METIS]
        }
    );
    Ok(Case {
        f,
        event,
        generic,
        pump,
        captures,
    })
}
impl Case {
    pub fn correlated(&self, selected_pump: bool) -> Result<()> {
        for (sample, is_pump) in [
            Some((&self.generic, false)),
            self.pump.as_ref().map(|p| (p, true)),
        ]
        .into_iter()
        .flatten()
        {
            assert_eq!(sample.event_id, self.event.event_id);
            assert_eq!(sample.request_ts, self.event.request_ts);
            let requests: Vec<_> = self
                .captures
                .iter()
                .filter(|r| r.body.is_null() && r.path.starts_with("/pump-fun/") == is_pump)
                .collect();
            let first = requests.iter().map(|r| r.received).min().unwrap();
            let last = requests.iter().map(|r| r.replied).max().unwrap();
            let start = sample.http_request_started_ts.unwrap();
            assert!(start >= self.event.request_ts && start <= first);
            assert!(sample.quote_latency_ms.unwrap() as i64 >= (last - start).num_milliseconds());
            for r in requests {
                let url = reqwest::Url::parse(&format!("http://localhost{}", r.path))?;
                let query: std::collections::BTreeMap<_, _> =
                    url.query_pairs().into_owned().collect();
                assert_eq!(
                    query["amount"],
                    if self.event.side == "buy" {
                        "200000000"
                    } else {
                        "1000000"
                    }
                );
                if is_pump {
                    assert_eq!(query["mint"], self.event.token);
                    assert_eq!(query["type"], self.event.side.to_ascii_uppercase());
                } else {
                    assert_eq!(query["swapMode"], "ExactIn");
                    assert_eq!(query["instructionVersion"], "V2");
                    assert_eq!(
                        query["slippageBps"],
                        if self.event.side == "buy" {
                            "50"
                        } else {
                            "500"
                        }
                    );
                }
            }
        }
        let selected = if selected_pump {
            self.pump.as_ref().unwrap()
        } else {
            &self.generic
        };
        assert_eq!(self.event.quote_response_json, selected.quote_response_json);
        assert_eq!(
            self.event.quote_out_amount_raw,
            selected.quote_out_amount_raw
        );
        assert_eq!(
            self.event.http_request_started_ts,
            selected.http_request_started_ts
        );
        assert_eq!(self.event.quote_latency_ms, selected.quote_latency_ms);
        let metadata =
            crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
                &self.f.store,
                self.event.clone(),
            )?;
        assert_eq!(metadata.quote_response_json, selected.quote_response_json);
        assert_eq!(
            metadata.http_request_started_ts,
            selected.http_request_started_ts
        );
        Ok(())
    }
}
