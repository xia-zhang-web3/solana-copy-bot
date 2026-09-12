use super::{b93_attempt_fixture::Attempt, b93_fixture as f};
use crate::execution_submit_adapter::build_tiny_submit_reconciliation_request;
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
async fn b93_persisted_unsigned_roundtrip_reopen_refuses_partial_and_allows_unchanged() -> Result<()>
{
    for partial in [false, true] {
        let mut a = Attempt::new(&format!("b93-retry-{partial}")).await?;
        let encoded =
            serde_json::to_string(a.request.metadata.owned_sell_amount.as_ref().unwrap())?;
        assert_eq!(
            a.db.store
                .load_execution_canary_sell_amount_proof(&a.request.order_id)?
                .as_deref(),
            Some(encoded.as_str())
        );
        if partial {
            a.partial("retry-partial")?;
        }
        a.reopen()?;
        let order =
            a.db.store
                .load_execution_canary_order(&a.request.order_id)?
                .unwrap();
        let r = build_tiny_submit_reconciliation_request(&a.db.store, &a.config, &order)?;
        assert_eq!(
            r.metadata.owned_sell_amount,
            a.request.metadata.owned_sell_amount
        );
        assert_eq!(r.metadata.quote_in_amount_raw.as_deref(), Some("7000"));
        let simulation_before = a.rpc.count("simulateTransaction");
        let out = a.retry().await?;
        assert_eq!(out.signing_envelope_built, 0);
        assert_eq!(a.rpc.count("sendTransaction"), 0);
        if partial {
            assert_eq!(out.source_sell_refusals.count(), 1, "{out:?}");
            assert_eq!(out.skipped_reason, Some("source_sell_amount_stale"));
            assert_eq!(a.rpc.count("simulateTransaction"), simulation_before);
            assert!(!out
                .last_error
                .as_deref()
                .unwrap()
                .contains("signer keypair"));
            super::b93_r1_retry_fixture::fresh_after_refusal(&mut a).await?;
        } else {
            assert_eq!(out.source_sell_refusals.count(), 0, "{out:?}");
            assert!(
                out.last_error
                    .as_deref()
                    .unwrap()
                    .contains("signer keypair"),
                "{out:?}"
            );
            assert_eq!(a.rpc.count("simulateTransaction"), simulation_before + 1);
        }
        a.rpc.finish().await?;
        f::write(
            &format!("retry-{partial}"),
            json!({"proof":encoded,"summary":format!("{out:?}"),"calls":a.rpc.state.lock().unwrap().calls,"position":f::raw(&a.db,&a.m)?}),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn b93_unsigned_legacy_missing_and_damaged_proof_refuse_after_reopen() -> Result<()> {
    for arm in [
        "missing",
        "malformed",
        "missing-raw",
        "decimals",
        "attempt",
        "quote",
        "wallet",
        "missing-field",
    ] {
        let mut a = Attempt::new(&format!("b93-proof-{arm}")).await?;
        let mut value =
            serde_json::to_value(a.request.metadata.owned_sell_amount.as_ref().unwrap())?;
        match arm {
            "missing-raw" => value["position"]["raw"] = json!(null),
            "decimals" => value["position"]["decimals"] = json!(4),
            "attempt" => value["attempt"] = json!(22),
            "quote" => value["quote_hash"] = json!("conflict"),
            "wallet" => value["wallet_raw"] = json!(1000),
            "missing-field" => {
                value.as_object_mut().unwrap().remove("selected_raw");
            }
            _ => {}
        }
        let proof = match arm {
            "missing" => None,
            "malformed" => Some("{".into()),
            _ => Some(value.to_string()),
        };
        let metadata =
            a.db.store
                .load_execution_canary_build_plan_metadata(&a.request.order_id)?
                .unwrap();
        // Only metadata provenance is adversarial; quantity is never changed by SQL.
        a.db.store
            .record_execution_canary_build_plan_metadata_with_sell_amount(
                &metadata,
                proof.as_deref(),
            )?;
        a.reopen()?;
        let before = a.rpc.count("simulateTransaction");
        let out = a.retry().await?;
        a.rpc.finish().await?;
        assert_eq!(out.source_sell_refusals.count(), 1, "{arm}: {out:?}");
        assert_eq!(out.signing_envelope_built, 0);
        assert_eq!(a.rpc.count("simulateTransaction"), before);
        assert_eq!(a.rpc.count("sendTransaction"), 0);
        assert_eq!(f::raw(&a.db, &a.m)?, 7000);
        assert!(!out
            .last_error
            .as_deref()
            .unwrap()
            .contains("signer keypair"));
    }
    Ok(())
}

#[tokio::test]
async fn b93_direct_signing_seam_checks_stale_amount_and_caller_identity() -> Result<()> {
    for arm in [
        "healthy",
        "partial",
        "side",
        "plan",
        "blueprint",
        "missing",
        "quote",
        "zero",
    ] {
        let mut a = Attempt::new(&format!("b93-sign-{arm}")).await?;
        let mut r = a.request.clone();
        let mut p = a.plan.clone();
        match arm {
            "partial" => {
                a.partial("sign-partial")?;
            }
            "side" => {
                r.side = "buy".into();
                p.side = "buy".into();
            }
            "plan" => {
                p.metadata.quote_in_amount_raw = Some("4000".into());
            }
            "blueprint" => {
                p.swap_blueprint.as_mut().unwrap().input_amount_raw = "4000".into();
            }
            "missing" => {
                r.metadata.owned_sell_amount = None;
                p.metadata.owned_sell_amount = None;
            }
            "quote" => {
                r.metadata.quote_in_amount_raw = Some("4000".into());
                p.metadata = r.metadata.clone();
            }
            "zero" => {
                r.metadata.quote_in_amount_raw = Some("0".into());
                p.metadata = r.metadata.clone();
            }
            _ => {}
        }
        let out = a.sign(&r, &p)?;
        a.rpc.finish().await?;
        assert_eq!(out.built, 0);
        assert_eq!(a.rpc.count("sendTransaction"), 0);
        if arm == "healthy" {
            assert!(out.source_refusal.is_none(), "{out:?}");
            assert!(out.error.as_deref().unwrap().contains("signer keypair"));
        } else {
            assert!(out.source_refusal.is_some(), "{arm}: {out:?}");
            assert!(!out.error.as_deref().unwrap().contains("signer keypair"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn b93_failed_sell_sweep_rebuilds_fresh_attempt_4000() -> Result<()> {
    let mut a = Attempt::new("b93-failed-sweep").await?;
    a.partial("failed-sweep-partial")?;
    a.db.store.mark_execution_canary_failed(
        &a.request.order_id,
        f::at(),
        EXECUTION_ERROR_SIMULATION_FAILED,
        "synthetic simulation failure",
    )?;
    a.rpc.state.lock().unwrap().wallet_raw = 4000;
    a.config.max_submit_attempts = 3;
    let out = crate::execution_canary_route::process_failed_sell_simulation_sweep(
        &a.config,
        &a.db.store,
        f::at(),
    )
    .await?
    .unwrap();
    a.rpc.finish().await?;
    let order =
        a.db.store
            .load_execution_canary_order(&a.request.order_id)?
            .unwrap();
    assert_eq!(order.attempt, a.request.attempt + 1);
    let r = build_tiny_submit_reconciliation_request(&a.db.store, &a.config, &order)?;
    assert_eq!(r.metadata.quote_in_amount_raw.as_deref(), Some("4000"));
    assert_ne!(
        r.metadata.owned_sell_amount,
        a.request.metadata.owned_sell_amount
    );
    assert_eq!(out.source_sell_refusals.count(), 0, "{out:?}");
    assert!(
        out.last_error
            .as_deref()
            .unwrap()
            .contains("signer keypair"),
        "{out:?}"
    );
    assert_eq!(a.rpc.count("sendTransaction"), 0);
    Ok(())
}

#[tokio::test]
async fn b93_unsigned_retry_simulation_await_rechecks_original_proof() -> Result<()> {
    let mut a = Attempt::new("b93-retry-await").await?;
    {
        let path = a.db.path.clone();
        let m = a.m.clone();
        let mut c = a.rpc.state.lock().unwrap();
        c.mutate_at = Some("simulateTransaction");
        c.mutation = Some(Box::new(move || {
            let db = f::open(&path)?;
            f::settle(&db, &f::receipt(&db, &m, "retry-await", 3000)?)?;
            Ok(())
        }));
    }
    a.reopen()?;
    let out = a.retry().await?;
    assert_eq!(out.source_sell_refusals.count(), 1, "{out:?}");
    assert_eq!(out.skipped_reason, Some("source_sell_amount_stale"));
    assert_eq!(out.signing_envelope_built, 0);
    assert!(!out
        .last_error
        .as_deref()
        .unwrap()
        .contains("signer keypair"));
    assert_eq!(f::raw(&a.db, &a.m)?, 4000);
    assert_eq!(a.rpc.count("sendTransaction"), 0);
    super::b93_r1_retry_fixture::fresh_after_refusal(&mut a).await?;
    a.rpc.finish().await?;
    Ok(())
}
