#[path = "b105_producer_fixture.rs"]
mod fixture;
#[path = "b105_http_support.rs"]
pub(super) mod http;
use super::b58_fixture;
use crate::execution_quote_canary::ExecutionQuoteCanaryRunner;
use anyhow::{ensure, Result};
use chrono::DateTime;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::oneshot};

async fn run_case(name: &str, hold_ms: u64, status: &'static str, truncate: bool) -> Result<()> {
    let dir = PathBuf::from(std::env::var("B105_EVIDENCE")?).join(name);
    fs::create_dir(&dir)?;
    let f = b58_fixture::Fixture::new(name)?;
    let token = bs58::encode([104u8; 32]).into_string();
    let supplied_now = fixture::initial_time();
    f.seed(&token, "buy", supplied_now)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let config = fixture::config(format!("http://{}", listener.local_addr()?));
    let runner = ExecutionQuoteCanaryRunner::new(config);
    let signal = fixture::signal(&token);
    let origin = Instant::now();
    let (got_tx, got_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let reply = http::Reply {
        status,
        body: if truncate {
            "{".into()
        } else {
            fixture::body(&token)
        },
        advertised_extra: if truncate { 100 } else { 0 },
    };
    let server = http::serve(listener, reply, origin, got_tx, release_rx);
    let controller = async {
        let get = got_rx.await?;
        if hold_ms > 0 {
            tokio::time::sleep(Duration::from_millis(hold_ms)).await;
        }
        let before = http::Stamp::now(origin);
        // Independent DB evidence while the server is still blocked by release_rx.
        ensure!(f
            .store
            .load_execution_quote_canary_event_by_id(&format!("quote:entry:{}", signal.signal_id))?
            .is_none());
        if hold_ms > 0 {
            ensure!(
                DateTime::from_timestamp_millis(before.utc.timestamp_millis()).unwrap() >= get.utc
            );
            http::later_ms(before, origin).await?;
        }
        release_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("server disappeared"))?;
        Ok::<_, anyhow::Error>(before)
    };
    let producer = async {
        let summary = runner
            .process_recorded_shadow_signal(&f.store, &signal, supplied_now)
            .await?;
        let returned = http::Stamp::now(origin);
        let after = http::later_ms(returned, origin).await?;
        Ok::<_, anyhow::Error>((summary, returned, after))
    };
    let (server, before, producer) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(server, controller, producer)
    })
    .await?;
    let server = server?;
    let before = before?;
    let (summary, returned, after) = producer?;
    let row = f.event(&token, "buy")?;
    let start = row.http_request_started_ts.expect("actual HTTP start");
    ensure!(start <= server.get.utc && server.get.utc <= before.utc);
    ensure!(
        before.utc < server.body_release.utc
            && server.body_release.utc <= returned.utc
            && returned.utc <= after.utc
    );
    ensure!(
        server.get.mono_ns <= before.mono_ns
            && before.mono_ns < server.body_release.mono_ns
            && server.body_release.mono_ns <= returned.mono_ns
            && returned.mono_ns <= after.mono_ns
    );
    ensure!(DateTime::from_timestamp_millis(after.utc.timestamp_millis()).unwrap() >= returned.utc);
    if hold_ms > 0 {
        ensure!(
            DateTime::from_timestamp_millis(before.utc.timestamp_millis() + 1).unwrap()
                <= server.body_release.utc
        );
        let actual_hold = server.body_release.mono_ns - server.get.mono_ns;
        ensure!(
            (200_000_000..=500_000_000).contains(&actual_hold),
            "hold outside bounded 200-500ms: {actual_hold}"
        );
    }
    let success = status == "200 OK" && !truncate;
    ensure!(summary.entry_candidates == 1 && summary.entry_inserted == 1);
    ensure!(row.request_ts == supplied_now);
    ensure!(row.quote_status == if success { "ok" } else { "error" });
    if success {
        let available = row
            .quote_response_available_ts
            .expect("actual response ready instant");
        ensure!(
            start <= available && server.body_release.utc <= available && available <= returned.utc
        );
        ensure!(available > before.utc);
        ensure!(row.quote_in_amount_raw.as_deref() == Some("200000000"));
        ensure!(row.quote_out_amount_raw.as_deref() == Some("1000000"));
        ensure!(row.decision_status.as_deref() == Some("would_execute"));
        ensure!(row.quote_response_json.as_deref() == Some(fixture::body(&token).as_str()));
    } else {
        ensure!(row.quote_response_available_ts.is_none());
        ensure!(row.quote_response_json.is_none() && row.quote_out_amount_raw.is_none());
        ensure!(row.decision_status.as_deref() == Some("unknown"));
    }
    let event_id = row.event_id.clone();
    let row_json = fixture::row_json(&row);
    let provider = f
        .store
        .load_execution_quote_canary_provider_sample(
            &event_id,
            copybot_storage_core::PROVIDER_GENERIC_METIS,
        )?
        .unwrap();
    ensure!(
        provider.http_request_started_ts == row.http_request_started_ts
            && provider.quote_response_available_ts == row.quote_response_available_ts
            && provider.quote_status == row.quote_status
    );
    let path = f.path.clone();
    drop(f); // close the actual producer writer before freezing and reopening.
    fixture::freeze(&path, &dir.join("frozen.sqlite"))?;
    let reopened = copybot_storage_core::SqliteStore::open_read_only(&dir.join("frozen.sqlite"))?;
    ensure!(
        fixture::row_json(
            &reopened
                .load_execution_quote_canary_event_by_id(&event_id)?
                .unwrap()
        ) == row_json
    );
    drop(reopened);
    fs::write(dir.join("request.txt"), server.request)?;
    fs::write(
        dir.join("observation.json"),
        serde_json::to_vec_pretty(&json!({
            "case":name,"row":row_json,"get":server.get.json(),"t_before":before.json(),
            "body_release":server.body_release.json(),"body_sent":server.body_sent.json(),
            "producer_return":returned.json(),"t_after":after.json(),
            "hold_requested_ms":hold_ms,"hold_observed_ns":(server.body_release.mono_ns-server.get.mono_ns).to_string(),
            "http_start_ns":start.timestamp_nanos_opt().unwrap().to_string(),
            "row_absent_while_held":true,"writer_closed_before_freeze":true,"reopened_equal":true,
            "network":"sandbox loopback only; one GET; optional calls disabled", "success":success,
            "execution_enabled":false,"canary_tiny_submit_enabled":false,"production_green":false,
            "no_completion_utc_derived_from_latency":true
        }))?,
    )?;
    Ok(())
}

#[tokio::test]
async fn b105_delayed_body_independent_one() -> Result<()> {
    run_case("delayed-one", 250, "200 OK", false).await
}
#[tokio::test]
async fn b105_delayed_body_independent_two() -> Result<()> {
    run_case("delayed-two", 300, "200 OK", false).await
}
#[tokio::test]
async fn b105_immediate_body_control() -> Result<()> {
    run_case("immediate", 0, "200 OK", false).await
}
#[tokio::test]
async fn b105_http_error_control() -> Result<()> {
    run_case("http-error", 0, "503 Unavailable", false).await
}
#[tokio::test]
async fn b105_truncated_body_control() -> Result<()> {
    run_case("truncated", 0, "200 OK", true).await
}
