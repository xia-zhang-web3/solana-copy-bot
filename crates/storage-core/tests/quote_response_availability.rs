#[path = "common/quote_allocation_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::*;
use fixture::Fixture;
use rusqlite::Connection;

const TABLES: [&str; 3] = [
    "execution_quote_canary_events",
    "execution_quote_canary_provider_samples",
    "execution_canary_build_plan_metadata",
];
fn connection(f: &Fixture) -> Result<Connection> {
    Ok(Connection::open(f.dir.path().join("allocation.db"))?)
}
fn signal(f: &Fixture, side: &str) -> CopySignalRow {
    CopySignalRow {
        signal_id: "timing-signal".into(),
        wallet_id: "wallet".into(),
        side: side.into(),
        token: "Token".into(),
        notional_sol: 0.1,
        notional_lamports: Some(Lamports::new(100_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: f.opened,
        status: "execution_sell_intent".into(),
    }
}
fn provider(e: &ExecutionQuoteCanaryEventInsert) -> ExecutionQuoteCanaryProviderSampleInsert {
    ExecutionQuoteCanaryProviderSampleInsert {
        http_request_started_ts: e.http_request_started_ts,
        quote_response_available_ts: e.quote_response_available_ts,
        event_id: e.event_id.clone(),
        provider: PROVIDER_GENERIC_METIS.into(),
        side: e.side.clone(),
        quote_status: e.quote_status.clone(),
        request_ts: e.request_ts,
        quote_latency_ms: e.quote_latency_ms,
        quote_in_amount_raw: e.quote_in_amount_raw.clone(),
        quote_out_amount_raw: e.quote_out_amount_raw.clone(),
        quote_response_json: e.quote_response_json.clone(),
        quote_price_sol: e.quote_price_sol,
        shadow_price_sol: e.shadow_price_sol,
        slippage_bps: e.slippage_bps,
        price_impact_pct: e.price_impact_pct,
        route_plan_json: e.route_plan_json.clone(),
        decision_status: e.decision_status.clone(),
        decision_reason: e.decision_reason.clone(),
        error: None,
    }
}
fn metadata(f: &Fixture, order_id: &str, client: &str) -> ExecutionCanaryBuildPlanMetadata {
    ExecutionCanaryBuildPlanMetadata {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        order_id: order_id.into(),
        signal_id: "timing-signal".into(),
        client_order_id: client.into(),
        quote_event_id: Some("quote:entry:buy".into()),
        quote_request_ts: Some(f.opened),
        quote_source: Some("execution_quote_canary_event".into()),
        quote_status: Some("ok".into()),
        quote_in_amount_raw: Some("100000000".into()),
        quote_out_amount_raw: Some("10".into()),
        quote_response_json: Some("{}".into()),
        quote_price_sol: Some(0.1),
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_source: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        slippage_bps: None,
        decision_status: Some("would_execute".into()),
        decision_reason: None,
        recorded_ts: f.opened,
    }
}
#[test]
fn availability_sell_retry_replaces_success_error_success_and_preserves_provider_version(
) -> Result<()> {
    let mut f = Fixture::new()?;
    f.store.insert_copy_signal(&signal(&f, "sell"))?;
    let mut e = f.event(false, 0, "10", None);
    e.signal_id = Some("timing-signal".into());
    e.shadow_closed_trade_id = None;
    e.http_request_started_ts = Some(f.opened);
    e.quote_response_available_ts = Some(f.opened + Duration::nanoseconds(123456789));
    f.store.record_execution_quote_canary_event(&e)?;
    let first = provider(&e);
    f.store
        .record_execution_quote_canary_provider_sample(&first)?;
    for success in [false, true] {
        e.request_ts += Duration::seconds(1);
        e.http_request_started_ts = Some(e.request_ts);
        e.quote_status = if success { "ok" } else { "error" }.into();
        // Deliberately keep the old Some on error: writer must clear with replaced status.
        if success {
            e.quote_response_available_ts = Some(e.request_ts + Duration::nanoseconds(987654321));
        }
        e.quote_response_json = success.then(|| "{\"new_version\":3}".into());
        e.quote_out_amount_raw = success.then(|| "200000000".into());
        f.store.record_execution_quote_canary_event(&e)?;
        let current = provider(&e);
        f.store
            .record_execution_quote_canary_provider_sample(&current)?;
        // Earlier provider version must not overwrite the retry pair.
        f.store
            .record_execution_quote_canary_provider_sample(&first)?;
        f.reopen()?;
        let loaded = f
            .store
            .load_execution_quote_canary_event_by_id(&e.event_id)?
            .unwrap();
        let p = f
            .store
            .load_execution_quote_canary_provider_sample(&e.event_id, PROVIDER_GENERIC_METIS)?
            .unwrap();
        assert_eq!(
            loaded.quote_response_available_ts,
            e.quote_response_available_ts.filter(|_| success)
        );
        assert_eq!(
            p.quote_response_available_ts,
            loaded.quote_response_available_ts
        );
        assert_eq!(p.quote_response_json, loaded.quote_response_json);
        assert_eq!(p.quote_out_amount_raw, e.quote_out_amount_raw);
        assert_eq!(p.request_ts, e.request_ts);
    }
    Ok(())
}
#[test]
fn availability_buy_insert_once_does_not_expand_retry_policy() -> Result<()> {
    let mut f = Fixture::new()?;
    let mut e = f.event(true, 0, "10", None);
    e.http_request_started_ts = Some(f.opened);
    e.quote_response_available_ts = Some(f.opened + Duration::nanoseconds(123));
    f.store.record_execution_quote_canary_event(&e)?;
    let first = provider(&e);
    f.store
        .record_execution_quote_canary_provider_sample(&first)?;
    let original = f
        .store
        .load_execution_quote_canary_event_by_id(&e.event_id)?
        .unwrap();
    e.request_ts += Duration::seconds(1);
    e.quote_response_available_ts = Some(e.request_ts);
    e.quote_response_json = Some("replacement".into());
    assert_eq!(
        f.store.record_execution_quote_canary_event(&e)?,
        ExecutionQuoteCanaryRecordOutcome::Existing
    );
    assert_eq!(
        f.store
            .record_execution_quote_canary_provider_sample(&provider(&e))?,
        ExecutionQuoteCanaryRecordOutcome::Existing
    );
    f.reopen()?;
    assert_eq!(
        f.store
            .load_execution_quote_canary_event_by_id(&e.event_id)?
            .unwrap(),
        original
    );
    assert_eq!(
        f.store
            .load_execution_quote_canary_provider_sample(&e.event_id, PROVIDER_GENERIC_METIS)?
            .unwrap(),
        first
    );
    Ok(())
}
#[test]
fn availability_metadata_reopen_replacement_and_old_serde() -> Result<()> {
    let mut f = Fixture::new()?;
    f.store.insert_copy_signal(&signal(&f, "buy"))?;
    let order = f
        .store
        .reserve_execution_canary_order("timing-signal", "metis", f.opened)?
        .order;
    let mut m = metadata(&f, &order.order_id, &order.client_order_id);
    for success in [true, false, true] {
        m.quote_request_ts = Some(m.recorded_ts);
        m.http_request_started_ts = Some(m.recorded_ts);
        m.quote_response_available_ts = Some(m.recorded_ts + Duration::nanoseconds(17));
        m.quote_status = Some(if success { "ok" } else { "error" }.into());
        m.quote_response_json = success.then(|| m.recorded_ts.to_rfc3339());
        f.store.record_execution_canary_build_plan_metadata(&m)?;
        f.reopen()?;
        let r = f
            .store
            .load_execution_canary_build_plan_metadata(&order.order_id)?
            .unwrap();
        assert_eq!(
            r.quote_response_available_ts,
            m.quote_response_available_ts.filter(|_| success)
        );
        assert_eq!(r.quote_response_json, m.quote_response_json);
        m.recorded_ts += Duration::seconds(1);
    }
    let mut v = serde_json::to_value(&m)?;
    v.as_object_mut()
        .unwrap()
        .remove("quote_response_available_ts");
    let legacy: ExecutionCanaryBuildPlanMetadata = serde_json::from_value(v)?;
    assert!(legacy.quote_response_available_ts.is_none());
    Ok(())
}
#[test]
fn availability_legacy_readonly_no_backfill_and_unprepared_writes_refused() -> Result<()> {
    let f = Fixture::new()?;
    let e = f.event(true, 0, "10", None);
    f.store.record_execution_quote_canary_event(&e)?;
    f.store
        .record_execution_quote_canary_provider_sample(&provider(&e))?;
    f.store.insert_copy_signal(&signal(&f, "buy"))?;
    let order = f
        .store
        .reserve_execution_canary_order("timing-signal", "metis", f.opened)?
        .order;
    let mut m = metadata(&f, &order.order_id, &order.client_order_id);
    f.store.record_execution_canary_build_plan_metadata(&m)?;
    let c = connection(&f)?;
    c.execute(
        "DELETE FROM schema_migrations WHERE version=?1",
        [quote_response_availability::MIGRATION],
    )?;
    for t in TABLES {
        c.execute_batch(&format!(
            "ALTER TABLE {t} DROP COLUMN quote_response_available_ts"
        ))?;
    }
    let ro = SqliteStore::open_read_only(&f.dir.path().join("allocation.db"))?;
    assert!(ro
        .load_execution_quote_canary_event_by_id(&e.event_id)?
        .unwrap()
        .quote_response_available_ts
        .is_none());
    assert!(ro
        .load_execution_quote_canary_provider_sample(&e.event_id, PROVIDER_GENERIC_METIS)?
        .unwrap()
        .quote_response_available_ts
        .is_none());
    assert!(ro
        .load_execution_canary_build_plan_metadata(&m.order_id)?
        .unwrap()
        .quote_response_available_ts
        .is_none());
    for t in TABLES {
        assert!(!quote_response_availability::available(&c, t)?);
    }
    let mut known = e.clone();
    known.quote_response_available_ts = Some(f.opened);
    m.quote_response_available_ts = Some(f.opened);
    assert!(f
        .store
        .record_execution_quote_canary_event(&known)
        .unwrap_err()
        .to_string()
        .contains("0076"));
    assert!(f
        .store
        .record_execution_quote_canary_provider_sample(&provider(&known))
        .is_err());
    assert!(f
        .store
        .record_execution_canary_build_plan_metadata(&m)
        .is_err());
    // Upgrade only through normal migration; old rows remain NULL.
    let mut upgraded = SqliteStore::open(&f.dir.path().join("allocation.db"))?;
    upgraded.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    for t in TABLES {
        assert!(quote_response_availability::available(&c, t)?);
        assert_eq!(
            c.query_row(
                &format!("SELECT count(*) FROM {t} WHERE quote_response_available_ts IS NOT NULL"),
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
    }
    Ok(())
}
#[test]
fn availability_applied_missing_or_wrong_column_type_errors_in_all_readers() -> Result<()> {
    for t in TABLES {
        for wrong_type in [false, true] {
            let f = Fixture::new()?;
            let c = connection(&f)?;
            c.execute_batch(&format!(
                "ALTER TABLE {t} DROP COLUMN quote_response_available_ts"
            ))?;
            if wrong_type {
                c.execute_batch(&format!(
                    "ALTER TABLE {t} ADD COLUMN quote_response_available_ts INTEGER"
                ))?;
            }
            let mut ro = SqliteStore::open_read_only(&f.dir.path().join("allocation.db"))?;
            assert!(ro
                .load_execution_quote_canary_event_by_id("missing")
                .is_err());
            assert!(ro
                .load_execution_quote_canary_provider_sample("missing", PROVIDER_GENERIC_METIS)
                .is_err());
            assert!(ro
                .load_execution_canary_build_plan_metadata("missing")
                .is_err());
            assert!(ro
                .run_migrations(std::path::Path::new(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../migrations"
                )))
                .is_err());
        }
    }
    Ok(())
}
