use anyhow::Result;
use chrono::{TimeZone, Utc};
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, ExecutionQuoteCanaryRecordOutcome, SqliteStore,
};
use rusqlite::{params, Connection};
use std::path::Path;
use tempfile::NamedTempFile;

#[test]
fn stamps_execution_quote_canary_event_with_discovery_rank() -> Result<()> {
    let db = NamedTempFile::new()?;
    let mut store = SqliteStore::open(db.path())?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    seed_wallet_metrics(db.path())?;

    let request_ts = Utc.with_ymd_and_hms(2026, 6, 23, 0, 10, 0).unwrap();
    assert_eq!(
        store.record_execution_quote_canary_event(&quote_event(request_ts))?,
        ExecutionQuoteCanaryRecordOutcome::Inserted
    );
    let stamps = store
        .load_execution_quote_canary_discovery_rank_stamps(request_ts, &["wallet-b".to_string()])?;
    let stamp = stamps
        .get("wallet-b")
        .expect("wallet-b should receive a rank stamp");
    assert_eq!(stamp.rank, 16);
    assert_eq!(stamp.cohort, "rank_16_30");
    assert!(
        store.stamp_execution_quote_canary_discovery_rank("quote:entry-shadow-diag:sig", stamp)?
    );
    drop(store);

    let conn = Connection::open(db.path())?;
    let stored = conn.query_row(
        "SELECT discovery_rank, discovery_rank_cohort, discovery_rank_window_start
         FROM execution_quote_canary_events
         WHERE event_id = 'quote:entry-shadow-diag:sig'",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;
    assert_eq!(stored.0, 16);
    assert_eq!(stored.1, "rank_16_30");
    assert_eq!(stored.2, "2026-06-23T00:00:00+00:00");
    Ok(())
}

fn seed_wallet_metrics(path: &Path) -> Result<()> {
    let conn = Connection::open(path)?;
    for rank in 1..16 {
        conn.execute(
            "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
             VALUES (?1, ?2, ?2, 'active')",
            params![
                format!("wallet-before-{rank:02}"),
                "2026-06-23T00:00:00+00:00"
            ],
        )?;
        conn.execute(
            "INSERT INTO wallet_metrics(wallet_id, window_start, score)
             VALUES (?1, ?2, ?3)",
            params![
                format!("wallet-before-{rank:02}"),
                "2026-06-23T00:00:00+00:00",
                10_000.0 - rank as f64,
            ],
        )?;
    }
    conn.execute(
        "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
         VALUES ('wallet-b', ?1, ?1, 'active')",
        params!["2026-06-23T00:00:00+00:00"],
    )?;
    conn.execute(
        "INSERT INTO wallet_metrics(wallet_id, window_start, score)
         VALUES ('wallet-b', ?1, 1.0)",
        params!["2026-06-23T00:00:00+00:00"],
    )?;
    Ok(())
}

fn quote_event(request_ts: chrono::DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: "quote:entry-shadow-diag:sig".to_string(),
        signal_id: Some("sig".to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "wallet-b".to_string(),
        token: "token-b".to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts,
        signal_ts: Some(Utc.with_ymd_and_hms(2026, 6, 23, 0, 0, 0).unwrap()),
        decision_delay_ms: Some(600_000),
        quote_latency_ms: Some(10),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("1000000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0002),
        shadow_price_sol: Some(0.0002),
        slippage_bps: Some(0.0),
        price_impact_pct: Some(0.0),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}
