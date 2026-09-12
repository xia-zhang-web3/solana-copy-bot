use super::token2022_ata_inputs_tests::*;
use crate::execution_signing_envelope::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::SqliteStore;
use serde_json::json;
use std::path::{Path, PathBuf};

pub(super) struct Boundary {
    pub store: SqliteStore,
    pub path: PathBuf,
    pub now: DateTime<Utc>,
    pub request: ExecutionSubmitRequest,
    pub envelope: ExecutionSigningEnvelope,
    pub intent: ExecutionSubmitIntent,
    pub gate: crate::execution_canary_submit_contract::ExecutionTinySubmitGate,
}
impl Boundary {
    pub fn new(spec: &Spec, side: &str, dir: &Path) -> Result<Self> {
        let now = Utc::now() - chrono::Duration::seconds(2);
        let path = dir.join("boundary.sqlite");
        assert!(!path.exists());
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let mut config =
            super::initial_sol_rpc_fixture::buy_config(&bs58::encode(spec.wallet).into_string());
        config.canary_entry_submit_enabled = true;
        config.canary_max_open_positions = 10;
        config.canary_max_daily_loss_sol = 0.1;
        config.pretrade_min_sol_reserve = 0.05;
        config.pretrade_max_priority_fee_lamports = 22_000;
        config.submit_timeout_ms = 2000;
        config.canary_kill_switch_path = dir.join("absent-kill-switch").to_string_lossy().into();
        config.canary_route = "synthetic-boundary119".into();
        let signal = copybot_core_types::CopySignalRow {
            signal_id: "synthetic-boundary119".into(),
            wallet_id: "synthetic-leader".into(),
            side: side.into(),
            token: MINT.into(),
            notional_sol: 0.01,
            notional_lamports: Some(copybot_core_types::Lamports::new(10_000_000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "shadow_recorded".into(),
        };
        store.insert_copy_signal(&signal)?;
        let order = store
            .reserve_execution_canary_order(&signal.signal_id, &config.canary_route, now)?
            .order;
        store.mark_execution_canary_built(&order.order_id, now)?;
        store.mark_execution_canary_simulated(&order.order_id, now, "ok", None)?;
        let total = crate::execution_priority_fee_wire::decode_priority_fee(&spec.payload)?.total;
        let request = ExecutionSubmitRequest {
            order_id: order.order_id,
            signal_id: signal.signal_id,
            client_order_id: order.client_order_id,
            attempt: order.attempt,
            route: order.route,
            wallet_id: signal.wallet_id,
            token: signal.token,
            side: signal.side,
            buy_size_sol: 0.01,
            slippage_tolerance_bps: 500,
            wallet_pubkey: config.execution_signer_pubkey.clone(),
            entry_route_plan_json: None,
            metadata: ExecutionBuildPlanMetadata {
                priority_fee_status: Some("ok".into()),
                priority_fee_lamports: Some(total),
                priority_fee_json: Some(super::priority_fee_fixture::total_json(total)),
                ..Default::default()
            },
        };
        let plan = NoSubmitExecutionAdapter.build_transaction_plan(&request)?;
        // This API wraps bytes and does not create a signature. The payload stays all-zero signed.
        let mut envelope = build_signed_transaction_execution_envelope(
            &request,
            &plan,
            ExecutionSignedTransactionPayload {
                signed_transaction_base64: spec.payload.clone(),
                tx_signature_hint: None,
            },
        )?;
        envelope.priority_fee_proof = Some(crate::execution_priority_fee_proof::prove(
            &request,
            &spec.payload,
            config.pretrade_max_priority_fee_lamports,
        )?);
        crate::execution_priority_fee_proof::persist(
            &store,
            &request,
            &plan,
            &envelope,
            config.pretrade_max_priority_fee_lamports,
            now,
        )?;
        let intent = execution_submit_intent_from_signed_envelope(
            &request,
            &envelope,
            request.route.clone(),
        )?;
        let gate =
            crate::execution_canary_submit_contract::ExecutionTinySubmitGate::from_config(&config);
        crate::execution_priority_fee_proof::validate_submit(
            &store,
            &request,
            &envelope,
            &intent,
            gate.pretrade_max_priority_fee_lamports,
        )?;
        crate::execution_native_floor_policy::verify_submit_payload(
            &request,
            &intent.signed_transaction_base64,
            gate.pretrade_min_sol_reserve,
            &gate.execution_wallet_pubkey,
        )?;
        crate::execution_tiny_submit_state::eligible(&store, &request)
            .map_err(anyhow::Error::msg)?;
        let safety =
            crate::execution_canary_safety::live_pre_submit_safety_snapshot(&config, &store, now)?;
        assert_eq!(safety.blocked_reason, None, "{safety:?}");
        save(
            dir,
            "boundary-prerequisites.json",
            &json!({"label":"synthetic typed boundary input; unsigned placeholders; not full submit/daemon E2E",
            "request":format!("{request:#?}"),"envelope":format!("{envelope:#?}"),"intent":format!("{intent:#?}"),
            "safety":format!("{safety:#?}"),"eligible":true,"priority_fee_submit_proof":true,"floor_submit_proof":true,
            "reserve":crate::execution_native_floor_policy::reserve_lamports(0.05)?,"sign_calls":0,"keyloader_calls":0}),
        );
        Ok(Self {
            store,
            path,
            now,
            request,
            envelope,
            intent,
            gate,
        })
    }
    pub fn sql(&self) -> Result<serde_json::Value> {
        let c = rusqlite::Connection::open(&self.path)?;
        let order = self
            .store
            .load_execution_canary_order(&self.request.order_id)?;
        let expenses: i64 = c.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get(0),
        )?;
        let fills: i64 = c.query_row("SELECT COUNT(*) FROM fills", [], |r| r.get(0))?;
        Ok(
            json!({"order":format!("{order:#?}"),"failed_expenses":expenses,"fills":fills,
            "unresolved_buy":self.store.execution_canary_unresolved_buy()?,"accounting_pending":self.store.execution_canary_accounting_pending()?}),
        )
    }
}
