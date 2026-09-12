use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::SwapEvent;
use copybot_shadow::{FollowSnapshot, ShadowProcessOutcome, ShadowService};
use copybot_storage_core::SqliteStore;
use serde_json::{json, Map, Value};
use std::{
    collections::HashSet,
    fmt, fs,
    path::Path,
    sync::{Arc, Mutex},
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Metadata, Subscriber,
};
pub mod rpc;
pub const TOKEN: &str = "Batch65Token";
pub const SOL: &str = "So11111111111111111111111111111111111111112";
pub fn now() -> DateTime<Utc> {
    "2026-05-11T10:00:02Z".parse().unwrap()
}
pub fn buy() -> SwapEvent {
    swap("leader", "buy", 0, 0.5)
}
pub fn swap(wallet: &str, sig: &str, seconds: i64, sol: f64) -> SwapEvent {
    SwapEvent {
        wallet: wallet.into(),
        dex: "pumpswap".into(),
        token_in: SOL.into(),
        token_out: TOKEN.into(),
        amount_in: sol,
        amount_out: sol * 20.0,
        signature: sig.into(),
        slot: (1000 + seconds) as u64,
        ts_utc: now() - Duration::seconds(2) + Duration::seconds(seconds),
        exact_amounts: None,
    }
}
pub fn fixture(proxy_sufficient: bool) -> Result<(tempfile::TempDir, SqliteStore)> {
    let dir = tempfile::tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    for i in 0..5 {
        store.insert_observed_swap(&swap(
            &format!("prefix-{i}"),
            &format!("prefix-{i}"),
            -120 + i,
            if proxy_sufficient { 1.25 } else { 0.2 },
        ))?;
    }
    store.insert_observed_swap(&buy())?;
    Ok((dir, store))
}
pub fn cache(store: &SqliteStore, ts: DateTime<Utc>) -> Result<()> {
    store.upsert_token_quality_cache(TOKEN, Some(5), Some(20.0), Some(122), ts)
}
pub fn read_cache(store: &SqliteStore) -> Result<Value> {
    Ok(serde_json::to_value(store.get_token_quality_cache(TOKEN)?)?)
}
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Value>>>);
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
pub fn consume(
    store: &SqliteStore,
    swap: &SwapEvent,
    at: DateTime<Utc>,
    endpoint: Option<String>,
    config: ShadowConfig,
) -> Result<Value> {
    let follow = FollowSnapshot::from_active_wallets(HashSet::from(["leader".into()]));
    let capture = Capture::default();
    let result = tracing::subscriber::with_default(capture.clone(), || {
        ShadowService::new_with_helius(config, endpoint).process_swap(store, swap, &follow, at)
    })?;
    let events = capture.0.lock().unwrap().clone();
    let quality = events
        .iter()
        .find(|e| e["message"] == "shadow quality metrics evaluated")
        .cloned();
    let outcome = match result {
        ShadowProcessOutcome::Recorded(_) => "recorded".to_owned(),
        ShadowProcessOutcome::Dropped(r) => r.as_str().to_owned(),
    };
    let ids: Vec<_> = store
        .list_copy_signals_by_status("shadow_recorded", 20)?
        .into_iter()
        .map(|s| s.signal_id)
        .collect();
    Ok(
        json!({"outcome":outcome,"signal_count":ids.len(),"signal_ids":ids,
        "quality":quality,"events":events,"signal_time":swap.ts_utc,"evaluation_time":at,
        "cache_after":read_cache(store)?}),
    )
}
pub fn check(name: &str, result: &Value, outcome: &str, count: usize, label: Option<&str>) {
    if let Ok(dir) = std::env::var("B68_OUTPUT") {
        fs::write(
            Path::new(&dir).join(format!("{name}.json")),
            serde_json::to_vec_pretty(result).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(result["outcome"], outcome, "{name}: {result}");
    assert_eq!(result["signal_count"], count, "{name}");
    match label {
        Some(label) => assert_eq!(result["quality"]["quality_source"], label, "{name}"),
        None => assert!(result["quality"].is_null(), "{name}"),
    }
}
