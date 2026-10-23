//! One new child only. Real parent bytes preserved; transports/signing are LOCAL fixtures.
use super::{source_child_fixture as f, source_recovery_submit_fixture::Adapter};
use anyhow::{ensure, Result};
use chrono::{Duration, Utc};
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
#[ignore = "Requires the pinned stopped COPY09 failed export; never an eligible store"]
async fn source_child_saved_failure_upgrade_normal_runner_to_quote() -> Result<()> {
    let (c, before, pins) = f::saved().await?;
    assert_eq!(c.intent.amount_raw, 1_037_351);
    let parent = c
        .config
        .owner_exit
        .as_ref()
        .unwrap()
        .source_recovery_parent_intent_id
        .as_ref()
        .unwrap();
    let old = c.store.load_owner_exit_intent(parent)?.unwrap();
    let old_order = c
        .store
        .load_execution_canary_order(&owner_exit_order_id(parent))?
        .unwrap();
    let out = c.runner(c.config.clone()).await?;
    assert_eq!(c.server.count("quote"), 1, "{out:?}");
    assert_eq!(c.server.count("sendTransaction"), 0);
    assert_eq!(c.store.load_owner_exit_intent(parent)?, Some(old));
    assert_eq!(
        c.store.load_execution_canary_order(&old_order.order_id)?,
        Some(old_order)
    );
    let sql = c.sql()?;
    assert_eq!(
        sql.query_row("SELECT count(*) FROM owner_exit_intents", [], |r| r
            .get::<_, u32>(0))?,
        1
    );
    assert_eq!(
        sql.query_row("SELECT count(*) FROM owner_exit_child_attempts", [], |r| {
            r.get::<_, u32>(0)
        })?,
        1
    );
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM execution_order_sources WHERE owner_exit_child_id=?1",
            [&c.intent.intent_id],
            |r| r.get::<_, u32>(0)
        )?,
        1
    );
    assert_eq!(
        c.store.owner_source_recovery_native_remaining(&c.intent)?,
        4_995_000
    );
    assert!(!sql.prepare("PRAGMA foreign_key_check")?.exists([])?);
    if let Ok(root) = std::env::var("SOURCE_RECOVERY_OUTPUT") {
        let p = std::path::Path::new(&root);
        std::fs::create_dir_all(p)?;
        std::fs::write(
            p.join("CHILD_NORMAL_RUNNER.json"),
            serde_json::to_vec_pretty(&json!({
          "kind":"LOCAL_PRESERVED_FAILED_PARENT_NOT_FINANCIAL_ACTIVATION","child":c.intent,
          "parent_before_after_unchanged":true,"preupgrade_financial_projection":before,
          "requests":c.server.state.lock().unwrap().calls,"actual_source_inputs":pins,
          "new_REAL_signs":0,"new_REAL_sends":0,"quote":1,"simulation":"explicit LOCAL refusal before signing"}))?,
        )?;
    }
    f::unchanged(&pins)?;
    Ok(())
}

#[tokio::test]
async fn source_child_synthetic_dispatch_restart_confirmation_and_unknown_no_resend() -> Result<()>
{
    for unknown in [false, true] {
        let c = f::synthetic().await?;
        let parent = c
            .config
            .owner_exit
            .as_ref()
            .unwrap()
            .source_recovery_parent_intent_id
            .clone()
            .unwrap();
        let old = c.store.load_owner_exit_intent(&parent)?.unwrap();
        let old_order = c
            .store
            .load_execution_canary_order(&owner_exit_order_id(&parent))?
            .unwrap();
        c.server.state.lock().unwrap().unknown = unknown;
        let adapter = Adapter {
            config: c.config.clone(),
            server: &c.server,
        };
        let first = crate::execution_owner_exit::tick_with_adapter(
            &c.config,
            &c.store,
            Utc::now(),
            &adapter,
        )
        .await?;
        assert_eq!(c.server.count("sendTransaction"), 1, "{first:?}");
        let order = owner_exit_order_id(&c.intent.intent_id);
        assert_eq!(
            c.store.execution_order_origin(&order)?,
            Some(ExecutionOrderOrigin::OwnerExit {
                intent_id: c.intent.intent_id.clone()
            })
        );
        let reopened = SqliteStore::open(&c.path)?;
        if unknown {
            assert!(reopened
                .list_owner_exit_recovery_intents()?
                .iter()
                .any(|i| i == &c.intent));
        }
        let mut inactive = c.config.clone();
        inactive.owner_exit.as_mut().unwrap().activate = false;
        crate::execution_owner_exit::tick_with_adapter(&inactive, &reopened, Utc::now(), &adapter)
            .await?;
        assert_eq!(c.server.count("sendTransaction"), 1);
        c.server.state.lock().unwrap().unknown = false;
        crate::execution_owner_exit::tick_with_adapter(&inactive, &reopened, Utc::now(), &adapter)
            .await?;
        assert_eq!(c.server.count("sendTransaction"), 1);
        assert!(reopened.execution_canary_fill_exists(&order)?);
        assert!(reopened
            .load_execution_canary_open_position(OWNER_EXIT_MINT)?
            .is_none());
        assert_eq!(reopened.load_owner_exit_intent(&parent)?, Some(old));
        assert_eq!(
            reopened.load_execution_canary_order(&old_order.order_id)?,
            Some(old_order)
        );
        assert_eq!(
            c.sql()?.query_row(
                "SELECT count(*),actual_fee FROM owner_exit_fee_reservations",
                [],
                |r| Ok((r.get::<_, u32>(0)?, r.get::<_, u64>(1)?))
            )?,
            (1, 5000)
        );
        assert!(!reopened.rearm_owner_exit_undispatched(&c.intent.intent_id)?);
        assert!(!reopened.owner_exit_failed_retry_available(&c.intent.intent_id)?);
    }
    Ok(())
}

#[tokio::test]
async fn source_child_dispatched_unknown_or_confirmed_parent_refuses() -> Result<()> {
    for unknown in [true, false] {
        let mut c = super::source_recovery_fixture::Case::synthetic().await?;
        let parent = c.intent.clone();
        c.server.state.lock().unwrap().unknown = unknown;
        let adapter = Adapter {
            config: c.config.clone(),
            server: &c.server,
        };
        crate::execution_owner_exit::tick_with_adapter(&c.config, &c.store, Utc::now(), &adapter)
            .await?;
        assert_eq!(c.server.count("sendTransaction"), 1);
        let order = owner_exit_order_id(&parent.intent_id);
        assert!(c.store.load_execution_canary_dispatch(&order)?.is_some());
        if unknown {
            assert!(!c.store.execution_canary_fill_exists(&order)?);
            assert!(!c.store.list_owner_exit_recovery_intents()?.is_empty());
        } else {
            assert!(c.store.execution_canary_fill_exists(&order)?);
        }
        f::child_config(&mut c)?;
        assert!(c.store.register_owner_exit_intent(&c.intent).is_err());
        assert_eq!(
            c.store.load_owner_exit_intent(&parent.intent_id)?,
            Some(parent)
        );
        assert_eq!(
            c.sql()?
                .query_row("SELECT count(*) FROM owner_exit_child_attempts", [], |r| {
                    r.get::<_, u32>(0)
                })?,
            0
        );
        assert_eq!(c.server.count("sendTransaction"), 1);
    }
    Ok(())
}

#[tokio::test]
async fn source_child_registration_and_order_claim_concurrent_once() -> Result<()> {
    let c = f::synthetic().await?;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut jobs = vec![];
    for _ in 0..2 {
        let path = c.path.clone();
        let i = c.intent.clone();
        let b = barrier.clone();
        jobs.push(std::thread::spawn(move || -> Result<_> {
            let store = SqliteStore::open(&path)?;
            b.wait();
            let outcome = store.register_owner_exit_intent(&i)?;
            let reserve = store.reserve_owner_exit_order(&i.intent_id, || Ok(Utc::now()))?;
            Ok((outcome, reserve.outcome))
        }));
    }
    let results = jobs
        .into_iter()
        .map(|j| j.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        results
            .iter()
            .filter(|(r, _)| *r == OwnerExitIntentRecordOutcome::Inserted)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| *r == ExecutionCanaryRecordOutcome::Inserted)
            .count(),
        1
    );
    assert_eq!(
        c.sql()?
            .query_row("SELECT count(*) FROM owner_exit_child_attempts", [], |r| {
                r.get::<_, u32>(0)
            })?,
        1
    );
    let old = c
        .store
        .load_owner_exit_intent(&c.intent.intent_id)?
        .unwrap();
    let mut renewed = old.clone();
    renewed.expires_at += Duration::seconds(1);
    assert!(c.store.register_owner_exit_intent(&renewed).is_err());
    let mut alias = old;
    alias.intent_id.push_str(":alias");
    assert!(c.store.register_owner_exit_intent(&alias).is_err());
    Ok(())
}

#[tokio::test]
async fn source_child_missing_signed_build_partial_and_overlot_refuse() -> Result<()> {
    for case in 0..6 {
        let c = f::synthetic().await?;
        let parent = c
            .config
            .owner_exit
            .as_ref()
            .unwrap()
            .source_recovery_parent_intent_id
            .clone()
            .unwrap();
        let order = owner_exit_order_id(&parent);
        let sql = c.sql()?;
        match case {
            0 => {
                sql.execute(
                    "UPDATE orders SET tx_signature='LOCAL_SIGNED'WHERE order_id=?1",
                    [&order],
                )?;
            }
            1 => {
                sql.execute(
                    "UPDATE orders SET simulation_error=NULL WHERE order_id=?1",
                    [&order],
                )?;
            }
            2 => {
                sql.execute(
                    "UPDATE orders SET confirm_ts=NULL WHERE order_id=?1",
                    [&order],
                )?;
            }
            3 => {
                sql.execute("UPDATE positions SET qty_raw='1'", [])?;
            }
            4 => {
                sql.execute(
                    "UPDATE orders SET simulation_status='passed'WHERE order_id=?1",
                    [&order],
                )?;
            }
            _ => {
                sql.execute(
                    "UPDATE orders SET network_fee_lamports_hint=5000 WHERE order_id=?1",
                    [&order],
                )?;
            }
        }
        assert!(
            c.store.register_owner_exit_intent(&c.intent).is_err(),
            "negative{case}"
        );
        assert_eq!(
            sql.query_row("SELECT count(*) FROM owner_exit_child_attempts", [], |r| {
                r.get::<_, u32>(0)
            })?,
            0
        );
    }
    let c = f::synthetic().await?;
    let mut over = c.intent.clone();
    over.amount_raw += 1;
    assert!(c.store.register_owner_exit_intent(&over).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "Requires pinned failed snapshot for transactional migration failure proof"]
async fn source_child_upgrade_fault_rolls_back_all_parent_rows_and_schema() -> Result<()> {
    let root = super::temporary_output_fixture::OutputRoot::new("source-child-upgrade-fault")?;
    let path = root.path().join("fault.db");
    let pins = f::copy_input(
        std::path::Path::new(&std::env::var("SOURCE_CHILD_SAVED_DB")?),
        &path,
    )?;
    let before = f::graph(&rusqlite::Connection::open(&path)?)?;
    let migrations = root.path().join("migrations");
    std::fs::create_dir(&migrations)?;
    let original = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    for p in std::fs::read_dir(original)? {
        let p = p?.path();
        if p.extension().is_some_and(|e| e == "sql") {
            std::fs::copy(&p, migrations.join(p.file_name().unwrap()))?;
        }
    }
    let p = migrations.join("0092_owner_exit_child_attempt.sql");
    let mut sql = std::fs::read_to_string(&p)?;
    sql.push_str("\nSELECT force_LOCAL_migration_failure FROM missing_LOCAL_table;\n");
    std::fs::write(p, sql)?;
    let mut store = SqliteStore::open(&path)?;
    ensure!(
        store.run_migrations(&migrations).is_err(),
        "fault must fail"
    );
    let sql = rusqlite::Connection::open(&path)?;
    assert_eq!(f::graph(&sql)?, before);
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='owner_exit_child_attempts'",
            [],
            |r| r.get::<_, u32>(0)
        )?,
        0
    );
    assert!(!f::columns(&sql, "execution_order_sources")?
        .iter()
        .any(|s| s == "owner_exit_child_id"));
    assert!(!sql.prepare("PRAGMA foreign_key_check")?.exists([])?);
    assert!(sql.execute("DELETE FROM owner_exit_intents", []).is_err());
    f::unchanged(&pins)?;
    Ok(())
}
