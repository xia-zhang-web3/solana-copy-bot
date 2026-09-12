use super::*;
use copybot_core_types::{CopySignalRow, ExactSwapAmounts, Lamports, TokenQuantity};
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;
use serde_json::{json, Value};

pub(super) const SOL: &str = crate::execution_quote_canary_helpers::SOL_MINT;
pub(super) struct Fixture {
    pub store: SqliteStore,
    pub path: PathBuf,
}
impl Fixture {
    pub fn new(name: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("b58-{name}-{}.sqlite", std::process::id()));
        anyhow::ensure!(!path.exists(), "capture must not overwrite an earlier run");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        Ok(Self { store, path })
    }
    pub fn seed(&self, token: &str, side: &str, ts: chrono::DateTime<Utc>) -> Result<()> {
        let sell = side == "sell";
        let signal = CopySignalRow {
            signal_id: signal_id(token, side),
            wallet_id: "leader-b58".into(),
            side: side.into(),
            token: token.into(),
            notional_sol: 0.2,
            notional_lamports: Some(Lamports::new(200_000_000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts,
            status: "shadow_recorded".into(),
        };
        self.store.insert_copy_signal(&signal)?;
        self.store.insert_observed_swap(&SwapEvent {
            wallet: signal.wallet_id.clone(),
            dex: "pumpswap".into(),
            token_in: if sell { token } else { SOL }.into(),
            token_out: if sell { SOL } else { token }.into(),
            amount_in: if sell { 1.0 } else { 0.2 },
            amount_out: if sell { 0.2 } else { 1.0 },
            signature: format!("sig-b58-{token}"),
            slot: 42,
            ts_utc: ts,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: if sell { "1000000" } else { "200000000" }.into(),
                amount_in_decimals: if sell { 6 } else { 9 },
                amount_out_raw: if sell { "200000000" } else { "1000000" }.into(),
                amount_out_decimals: if sell { 9 } else { 6 },
            }),
        })?;
        if sell {
            self.store.record_execution_canary_open_position(
                "b58-existing-buy",
                token,
                1.0,
                Some(TokenQuantity::new(1_000_000, 6)),
                0.2,
                ts - chrono::Duration::seconds(1),
            )?;
        }
        Ok(())
    }
    pub fn event(&self, token: &str, side: &str) -> Result<ExecutionQuoteCanaryEventInsert> {
        let prefix = if side == "sell" {
            "owned-close"
        } else {
            "entry"
        };
        self.store
            .load_execution_quote_canary_event_by_id(&format!(
                "quote:{prefix}:{}",
                signal_id(token, side)
            ))?
            .ok_or_else(|| anyhow::anyhow!("missing actual runner event"))
    }
    pub fn observe(&self, token: &str, side: &str) -> Result<Value> {
        let event = self.event(token, side)?;
        let provider = self
            .store
            .load_execution_quote_canary_provider_sample(
                &event.event_id,
                copybot_storage_core::PROVIDER_GENERIC_METIS,
            )?
            .expect("actual provider row");
        let metadata =
            crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
                &self.store,
                event.clone(),
            )?;
        let age_before = Utc::now();
        let age = crate::execution_build_plan_age::quote_age_ms_at_build(&metadata);
        let age_after = Utc::now();
        assert_eq!(metadata.quote_request_ts, Some(event.request_ts));
        assert_eq!(provider.request_ts, event.request_ts);
        assert_eq!(provider.quote_latency_ms, event.quote_latency_ms);
        assert_eq!(
            metadata.http_request_started_ts,
            event.http_request_started_ts
        );
        assert_eq!(
            provider.http_request_started_ts,
            event.http_request_started_ts
        );
        if let Some(started) = event.http_request_started_ts {
            assert!(age.unwrap() >= (age_before - started).num_milliseconds());
            assert!(age.unwrap() <= (age_after - started).num_milliseconds());
        } else {
            assert_eq!(age, None);
        }
        Ok(
            json!({"token":token,"side":side,"event_id":event.event_id,"signal_id":event.signal_id,
            "http_request_started_ts":event.http_request_started_ts,"request_ts":event.request_ts,"signal_ts":event.signal_ts,"decision_delay_ms":event.decision_delay_ms,
            "quote_latency_ms":event.quote_latency_ms,"quote_status":event.quote_status,"error":event.error,
            "quote_in_amount_raw":event.quote_in_amount_raw,"quote_out_amount_raw":event.quote_out_amount_raw,
            "route_plan_json":event.route_plan_json,"quote_response_json":event.quote_response_json,
            "decision_status":event.decision_status,"decision_reason":event.decision_reason,
            "provider_request_ts":provider.request_ts,"provider_latency_ms":provider.quote_latency_ms,
            "metadata_source":metadata.quote_source,"metadata_request_ts":metadata.quote_request_ts,
            "age_ms":age,"age_before_utc":age_before,"age_after_utc":age_after}),
        )
    }
    pub fn capture(&self, label: &str, observation: Value) -> Result<()> {
        let Ok(capture_dir) = std::env::var("B58_CAPTURE_DIR") else {
            return Ok(());
        };
        let out = PathBuf::from(capture_dir);
        std::fs::create_dir_all(&out)?;
        let path = out.join(format!("{label}.json"));
        anyhow::ensure!(!path.exists());
        std::fs::write(path, serde_json::to_vec_pretty(&observation)?)?;
        let conn = Connection::open(&self.path)?;
        let snapshot = out.join(format!("{label}.sqlite"));
        conn.execute("VACUUM INTO ?1", [snapshot.to_string_lossy().as_ref()])?;
        println!("B58_OBSERVATION {}", observation);
        Ok(())
    }
}
pub(super) fn signal_id(token: &str, side: &str) -> String {
    format!("shadow:sig-b58-{token}:leader-b58:{side}:{token}")
}
pub(super) fn config(url: String) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = url;
    config.quote_canary_buy_size_sol = 0.2;
    config.quote_canary_buy_slippage_bps = 50;
    config.quote_canary_sell_slippage_bps = 500;
    config.quote_canary_timeout_ms = 2_000;
    // No RPC is required: observed raw quantities supply exact decimals.
    assert!(!config.priority_fee_canary_enabled);
    assert!(!config.quote_canary_public_parallel_enabled);
    assert!(!config.quote_canary_pump_fun_parallel_enabled);
    config
}
