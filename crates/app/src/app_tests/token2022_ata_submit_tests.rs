use super::token2022_ata_boundary_tests::Boundary;
use super::token2022_ata_inputs_tests::*;
use super::token2022_ata_rpc_tests::{Hook, Server};
use super::token2022_ata_transport_tests::rent170;
use anyhow::Result;
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

async fn invoke(
    f: &Boundary,
    spec: &Spec,
    dir: &Path,
    hook: Option<Hook>,
) -> Result<Option<crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome>> {
    let before = f.sql()?;
    let state = crate::execution_tiny_submit_state::eligible(&f.store, &f.request)
        .map_err(anyhow::Error::msg)?;
    let server = Server::start(spec, hook).await?;
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(
        server.rpc.endpoint.clone(),
    );
    let out = crate::execution_initial_sol_submit::before_send(
        &f.store,
        &f.request,
        &f.envelope,
        &f.intent,
        &f.gate,
        &transport,
        &state,
        f.now,
    )
    .await;
    let trace = server.finish(dir).await?;
    if f.request.side == "sell" {
        assert_eq!(trace.len(), 0);
    } else {
        assert!(trace.len() >= 3);
    }
    assert!(trace
        .iter()
        .all(|r| r.request["method"] != "sendTransaction"));
    save(
        dir,
        "before-send.json",
        &json!({"result":format!("{out:#?}"),"before":before,"after":f.sql()?,"collection_requests":trace.len(),"downstream":0,"sign":0}),
    );
    Ok(out)
}
#[tokio::test]
async fn token2022_ata_before_send_healthy_and_unknown_controls() -> Result<()> {
    for arm in [
        "absent",
        "prefunded",
        "existing",
        "missing-mint",
        "165",
        "minus-one",
    ] {
        let mut s = if arm == "missing-mint" {
            Spec::buy(false)?
        } else {
            Spec::sufficient(if arm == "165" {
                "existing"
            } else if arm == "minus-one" {
                "absent"
            } else {
                arm
            })?
        };
        if arm == "165" {
            super::token2022_ata_layout_tests::edit_data(&mut s, ATA, |d| d.truncate(165));
        }
        if arm == "minus-one" {
            s.rows[0] = super::initial_sol_rpc_fixture::system(64_140_360);
        }
        let dir = output(&format!("submit-{arm}"));
        let f = Boundary::new(&s, "buy", &dir)?;
        let out = invoke(&f, &s, &dir, None).await?.unwrap();
        assert_eq!(out.failed, 1);
        if ["absent", "prefunded", "existing"].contains(&arm) {
            assert_eq!(
                out.error.as_deref(),
                Some(super::b123_jupiter_fixture_tests::UNPROVEN)
            );
        } else {
            assert!(out.error.unwrap().starts_with(if arm == "minus-one" {
                "initial_sol_insufficient:"
            } else {
                "initial_sol_unsupported_setup"
            }));
        }
        assert_eq!(f.sql()?["fills"], 0);
        assert_eq!(f.sql()?["failed_expenses"], 0);
        assert!(f
            .store
            .load_execution_canary_dispatch(&f.request.order_id)?
            .is_none());
        assert!(f
            .store
            .load_execution_canary_receipt_proof(&f.request.order_id)?
            .is_none());
    }
    let s = Spec::from_frozen(&frozen(117)?);
    let dir = output("sell117-skip");
    let f = Boundary::new(&s, "sell", &dir)?;
    let before = f.sql()?;
    assert!(invoke(&f, &s, &dir, None).await?.is_none());
    assert_eq!(f.sql()?, before);
    Ok(())
}
#[tokio::test]
async fn token2022_ata_held_rent_preserves_submitted_and_receipt() -> Result<()> {
    for receipt in [false, true] {
        for result in ["success", "error", "timeout"] {
            let s = Spec::sufficient("absent")?;
            let dir = output(&format!("held170-{receipt}-{result}"));
            let mut f = Boundary::new(&s, "buy", &dir)?;
            f.gate.submit_timeout_ms = 600;
            let expected = Arc::new(Mutex::new(None));
            let change = expected.clone();
            let (path, id, now) = (f.path.clone(), f.request.order_id.clone(), f.now);
            let hook: Hook = Box::new(move |r, reply| {
                if rent170(r) {
                    let store = copybot_storage_core::SqliteStore::open(&path).unwrap();
                    let signature = "synthetic-held119-submitted";
                    store
                        .mark_execution_canary_submitted(&id, now, signature)
                        .unwrap();
                    if receipt {
                        let proof = copybot_storage_core::ExecutionCanaryReceiptProof {
                            tx_signature: signature.into(),
                            wallet_pubkey: PAYER.into(),
                            token: MINT.into(),
                            side: "buy".into(),
                            confirmation_status: "confirmed".into(),
                            slot: Some(123),
                            confirmed_at: now,
                            reason: "synthetic-pending119".into(),
                        };
                        store
                            .mark_execution_canary_confirmed_unreconciled(&id, &proof, now)
                            .unwrap();
                    }
                    *change.lock().unwrap() = Some((
                        store.load_execution_canary_order(&id).unwrap().unwrap(),
                        store.load_execution_canary_receipt_proof(&id).unwrap(),
                    ));
                    match result {
                        "error" => reply.body = b"{".to_vec(),
                        "timeout" => reply.wait_for_cancel = true,
                        _ => reply.delay = Duration::from_millis(100),
                    }
                }
            });
            let out = invoke(&f, &s, &dir, Some(hook)).await?.unwrap();
            assert_eq!(out.failed, 0);
            assert_eq!(out.reason.as_deref(), Some("initial_sol_order_changed"));
            let saved = expected
                .lock()
                .unwrap()
                .clone()
                .expect("new170 phase reached");
            assert_eq!(
                f.store
                    .load_execution_canary_order(&f.request.order_id)?
                    .unwrap(),
                saved.0
            );
            assert_eq!(
                f.store
                    .load_execution_canary_receipt_proof(&f.request.order_id)?,
                saved.1
            );
            assert_eq!(f.sql()?["failed_expenses"], 0);
            assert_eq!(f.sql()?["fills"], 0);
            save(
                &dir,
                "durable-preserved.json",
                &json!({"order":format!("{:?}",saved.0),"receipt":format!("{:?}",saved.1),"mutated_during":"rent170 await"}),
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn token2022_ata_postawait_fee_floor_and_safety_still_apply() -> Result<()> {
    for case in ["fee", "floor", "safety"] {
        let s = if case == "safety" {
            Spec::sufficient("absent")?
        } else {
            // Genuine System/ATA route reaches the existing post-funding checks;
            // no Jupiter program ID is replaced to manufacture an admission.
            super::b123_jupiter_fixture_tests::non_jupiter()?
        };
        let dir = output(&format!("postawait-{case}"));
        let mut f = Boundary::new(&s, "buy", &dir)?;
        if case == "floor" {
            f.gate.pretrade_min_sol_reserve = 0.06;
        }
        let path = f.path.clone();
        let id = f.request.order_id.clone();
        let kill = f
            .gate
            .buy_safety_config
            .as_ref()
            .unwrap()
            .canary_kill_switch_path
            .clone();
        let touched = Arc::new(Mutex::new(false));
        let flag = touched.clone();
        let hook: Hook = Box::new(move |r, _| {
            if (case == "safety" && rent170(r))
                || (case != "safety" && r["method"] == "getFeeForMessage")
            {
                *flag.lock().unwrap() = true;
                if case == "fee" {
                    rusqlite::Connection::open(&path).unwrap().execute("UPDATE execution_canary_build_plan_metadata SET priority_fee_json='{}' WHERE order_id=?1",[&id]).unwrap();
                }
                if case == "safety" {
                    std::fs::write(&kill, b"synthetic guard").unwrap();
                }
            }
        });
        let out = invoke(&f, &s, &dir, Some(hook)).await?.unwrap();
        assert!(*touched.lock().unwrap());
        if case == "safety" {
            assert_eq!(out.failed, 0);
            assert_eq!(out.reason.as_deref(), Some("kill_switch_active"));
        } else {
            assert_eq!(out.failed, 1);
            assert_eq!(
                out.error.as_deref(),
                Some(if case == "fee" {
                    "priority_fee_durable_proof_missing"
                } else {
                    "native_floor_reserve_mismatch"
                })
            );
        }
    }
    Ok(())
}
