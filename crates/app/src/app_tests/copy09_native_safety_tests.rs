//! Quoted COPY09 boundary only. No historical admission/freshness is reconstructed.
//! External I/O, transaction construction and signing are deliberately unreachable.
use crate::execution_canary_route::test_process_buy;
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, NoSubmitExecutionAdapter};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::SqliteStore;

const SOURCE_BUY: &str =
    "2nvgdzG95xneNMfQYUedbwKAxjFbjpXZVa73Dts6u8ABfPHvveyW94iezU4Njg9dHx5VHeEy3h4bZ6XY5RCx7M5J";

struct Boundary {
    config: ExecutionConfig,
    store: SqliteStore,
    signal: CopySignalRow,
    now: DateTime<Utc>,
    root: super::temporary_output_fixture::OutputRoot,
}

impl Boundary {
    fn saved() -> Result<Self> {
        let root = super::temporary_output_fixture::OutputRoot::new("copy09-native-safety")?;
        let mut config: ExecutionConfig =
            serde_json::from_str(include_str!("fixtures/copy09_native_safety.json"))?;
        config.canary_kill_switch_path = root.path().join("STOP").to_string_lossy().into();
        copybot_config::validate_controlled_receipt_lot(&config)?;
        let mut store = SqliteStore::open(root.path().join("LOCAL.db"))?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        // Preserve the captured tick time. This is a local replay of the safety
        // boundary, not a renewed native decision or a claim that the BUY is fresh.
        let now =
            DateTime::parse_from_rfc3339("2026-10-23T11:32:37.416242553Z")?.with_timezone(&Utc);
        let signal = CopySignalRow {
            signal_id: format!("native-buy-v1:{SOURCE_BUY}"),
            wallet_id: config
                .controlled_receipt_lot
                .as_ref()
                .unwrap()
                .source_wallet
                .clone(),
            side: "buy".into(),
            token: copybot_storage_core::OWNER_EXIT_MINT.into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "native_buy_fenced_v1".into(),
        };
        Ok(Self {
            config,
            store,
            signal,
            now,
            root,
        })
    }

    async fn run(
        &self,
    ) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
        // NativeBuyGuard has already passed in the captured incident. Do not
        // fabricate it from old evidence. The shared process_buy calls the real
        // apply_safety before inspecting the intentionally absent fresh quote.
        super::entry_risk_clock_fixture::at(
            self.now + Duration::milliseconds(1),
            test_process_buy(
                &self.config,
                &self.store,
                &self.signal,
                self.now,
                None,
                &NoSubmitExecutionAdapter,
                Some(ExecutionBuildPlanMetadata::default()),
            ),
        )
        .await
    }
}

#[tokio::test]
async fn copy09_native_safety_saved_quoted_boundary_allows_valid_receipt_zero() -> Result<()> {
    let f = Boundary::saved()?;
    assert_eq!(f.config.pretrade_min_sol_reserve, 0.0);
    assert_eq!(
        crate::execution_controlled_receipt_lot::reserve(&f.config)?,
        0
    );
    let out = f.run().await?;
    assert_eq!(out.safety_blocked, 0, "{out:?}");
    assert_eq!(
        out.skipped_reason,
        Some("native_buy_quote_binding"),
        "{out:?}"
    );
    assert_eq!((out.reserved, out.built, out.simulated), (0, 0, 0));
    assert!(f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .is_none());
    Ok(())
}

#[tokio::test]
async fn copy09_native_safety_unscoped_or_invalid_scope_stays_refused() -> Result<()> {
    for case in [
        "unscoped-zero",
        "unscoped-nan",
        "unscoped-overflow",
        "unknown-policy",
        "wrong-wallet",
        "bad-wsol",
        "principal-cap",
        "priority-cap",
        "loss-cap",
        "invalid-reserve",
    ] {
        let mut f = Boundary::saved()?;
        match case {
            "unscoped-zero" => f.config.controlled_receipt_lot = None,
            "unscoped-nan" => {
                f.config.controlled_receipt_lot = None;
                f.config.pretrade_min_sol_reserve = f64::NAN;
            }
            "unscoped-overflow" => {
                f.config.controlled_receipt_lot = None;
                f.config.pretrade_min_sol_reserve = f64::MAX;
            }
            "unknown-policy" => {
                f.config.controlled_receipt_lot.as_mut().unwrap().policy = "LOCAL-unaccepted".into()
            }
            "wrong-wallet" => {
                f.config.canary_wallet_pubkey = "11111111111111111111111111111111".into()
            }
            "bad-wsol" => {
                f.config
                    .controlled_receipt_lot
                    .as_mut()
                    .unwrap()
                    .wsol_lifecycle = "LOCAL-unaccepted".into()
            }
            "principal-cap" => f.config.canary_buy_size_sol = 0.02,
            "priority-cap" => f.config.pretrade_max_priority_fee_lamports = 50_001,
            "loss-cap" => f.config.canary_max_daily_loss_sol = 0.03,
            "invalid-reserve" => f.config.pretrade_min_sol_reserve = f64::NAN,
            _ => unreachable!(),
        }
        assert!(
            crate::execution_controlled_receipt_lot::reserve(&f.config).is_err(),
            "{case}"
        );
        let out = f.run().await?;
        assert_eq!(out.safety_blocked, 1, "{case}: {out:?}");
        assert_eq!(
            out.skipped_reason,
            Some("native_floor_invalid_policy"),
            "{case}: {out:?}"
        );
        assert_eq!((out.reserved, out.built, out.simulated), (0, 0, 0));
    }
    Ok(())
}

#[tokio::test]
async fn copy09_native_safety_zero_does_not_bypass_stop_or_unknown() -> Result<()> {
    let f = Boundary::saved()?;
    std::fs::write(f.root.path().join("STOP"), b"LOCAL stop")?;
    let stopped = f.run().await?;
    assert_eq!(stopped.skipped_reason, Some("kill_switch_active"));
    std::fs::remove_file(f.root.path().join("STOP"))?;
    let mut unknown = f.signal.clone();
    unknown.signal_id = "native-buy-v1:LOCAL-unknown-obligation".into();
    assert!(f.store.insert_copy_signal(&unknown)?);
    let order = f
        .store
        .reserve_execution_canary_order(&unknown.signal_id, &f.config.canary_route, f.now)?
        .order;
    f.store
        .mark_execution_canary_built(&order.order_id, f.now)?;
    f.store
        .mark_execution_canary_simulated(&order.order_id, f.now, "ok", None)?;
    f.store.mark_execution_canary_submitted_unknown(
        &order.order_id,
        f.now,
        "LOCAL outcome unknown; no send",
    )?;
    let before = f
        .store
        .load_execution_canary_order(&order.order_id)?
        .unwrap();
    let blocked = f.run().await?;
    assert_eq!(blocked.safety_blocked, 1);
    assert_eq!(
        blocked.skipped_reason,
        Some(copybot_storage_core::EXECUTION_UNRESOLVED_BUY_REASON)
    );
    assert_eq!(
        f.store
            .load_execution_canary_order(&order.order_id)?
            .unwrap(),
        before
    );
    assert_eq!(
        (blocked.reserved, blocked.built, blocked.simulated),
        (0, 0, 0)
    );
    Ok(())
}
