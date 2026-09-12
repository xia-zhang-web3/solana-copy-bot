use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use copybot_shadow::{
    FollowSnapshot, RecordedBuyLot, ShadowDropReason, ShadowProcessOutcome, ShadowService,
};
use copybot_storage_core::SqliteStore;
use tempfile::{tempdir, TempDir};

pub const SOL: &str = "So11111111111111111111111111111111111111112";
pub const TOKEN: &str = "event-time-token";
pub const WALLET: &str = "event-time-leader";

pub fn now() -> DateTime<Utc> {
    "2026-09-08T12:00:00.123456789Z".parse().unwrap()
}

pub fn config(quality: bool) -> ShadowConfig {
    let mut cfg = ShadowConfig::default();
    cfg.enabled = true;
    cfg.quality_gates_enabled = quality;
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.1;
    cfg.max_signal_lag_seconds = 45;
    cfg
}

pub fn setup(quality: bool) -> Result<(TempDir, SqliteStore, ShadowService)> {
    let dir = tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("event-time.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    store.insert_observed_swap(&buy(
        now() - chrono::Duration::seconds(120),
        true,
        "history",
    ))?;
    store.upsert_token_quality_cache(TOKEN, Some(100), Some(100.0), Some(86400), now())?;
    Ok((dir, store, ShadowService::new(config(quality))))
}

pub fn follow() -> FollowSnapshot {
    FollowSnapshot::from_active_wallets([WALLET.to_string()].into())
}

pub fn buy(ts: DateTime<Utc>, exact: bool, signature: &str) -> SwapEvent {
    SwapEvent {
        wallet: WALLET.into(),
        dex: "pumpswap".into(),
        token_in: SOL.into(),
        token_out: TOKEN.into(),
        amount_in: 1.0,
        amount_out: 100.0,
        signature: signature.into(),
        slot: 100,
        ts_utc: ts,
        exact_amounts: exact.then(|| ExactSwapAmounts {
            amount_in_raw: "1000000000".into(),
            amount_in_decimals: 9,
            amount_out_raw: "100000".into(),
            amount_out_decimals: 3,
        }),
    }
}

pub fn process(
    receipt_api: bool,
    service: &ShadowService,
    store: &SqliteStore,
    swap: &SwapEvent,
    followed: &FollowSnapshot,
) -> Result<(ShadowProcessOutcome, Option<RecordedBuyLot>)> {
    if receipt_api {
        service.process_swap_with_buy_receipt(store, swap, followed, now())
    } else {
        Ok((service.process_swap(store, swap, followed, now())?, None))
    }
}

pub fn counts(store: &SqliteStore, signals: usize, lots: usize) -> Result<()> {
    assert_eq!(
        store
            .list_copy_signals_by_status("shadow_recorded", 100)?
            .len(),
        signals
    );
    assert_eq!(store.list_shadow_lots(WALLET, TOKEN)?.len(), lots);
    Ok(())
}

pub fn dropped(outcome: &ShadowProcessOutcome, expected: &str) {
    match outcome {
        ShadowProcessOutcome::Dropped(reason) => assert_eq!(reason.as_str(), expected),
        other => panic!("expected {expected}, got {other:?}"),
    }
}

pub fn recorded(outcome: &ShadowProcessOutcome) -> &copybot_shadow::ShadowSignalResult {
    match outcome {
        ShadowProcessOutcome::Recorded(result) => result,
        other => panic!("expected recorded, got {other:?}"),
    }
}

pub fn reason(outcome: &ShadowProcessOutcome, expected: ShadowDropReason) {
    assert!(matches!(outcome, ShadowProcessOutcome::Dropped(actual) if *actual == expected));
}

use serde_json::{json, Map, Value};
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Metadata, Subscriber,
};
#[derive(Clone, Default)]
pub struct Capture(pub Arc<Mutex<Vec<Value>>>);
struct Fields(Map<String, Value>);
impl Visit for Fields {
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        self.0.insert(f.name().into(), json!(format!("{v:?}")));
    }
    fn record_str(&mut self, f: &Field, v: &str) {
        self.0.insert(f.name().into(), json!(v));
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.0.insert(f.name().into(), json!(v));
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.0.insert(f.name().into(), json!(v));
    }
}
impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, e: &Event<'_>) {
        let mut f = Fields(Map::new());
        e.record(&mut f);
        self.0.lock().unwrap().push(Value::Object(f.0));
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}
