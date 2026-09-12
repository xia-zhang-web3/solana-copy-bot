use super::b123_jupiter_fixture_tests::*;
use super::token2022_ata_boundary_tests::Boundary;
use super::token2022_ata_inputs_tests::{frozen, output, save, Spec};
use super::token2022_ata_rpc_tests::Hook;
use super::token2022_ata_transport_tests::rent170;
use anyhow::Result;
use copybot_storage_core::{ExecutionCanaryReceiptProof, SqliteStore};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

fn snapshot(store: &SqliteStore, id: &str, pending: Option<&str>) -> Result<Value> {
    Ok(json!({
        "order":format!("{:?}",store.load_execution_canary_order(id)?),
        "dispatch":format!("{:?}",store.load_execution_canary_dispatch(id)?),
        "receipt":format!("{:?}",store.load_execution_canary_receipt_proof(id)?),
        "pending_order":format!("{:?}",pending.map(|p|store.load_execution_canary_order(p)).transpose()?),
        "pending_receipt":format!("{:?}",pending.map(|p|store.load_execution_canary_receipt_proof(p)).transpose()?),
        "unresolved":store.execution_canary_unresolved_buy()?,
        "accounting_pending":store.execution_canary_accounting_pending()?
    }))
}

#[tokio::test]
async fn b123_held_collector_preserves_dispatch_signature_receipt_and_pending() -> Result<()> {
    for change in ["dispatch", "signature", "receipt", "pending"] {
        let spec = Spec::sufficient("absent")?;
        let dir = output(&format!("b123-held-{change}"));
        let boundary = Boundary::new(&spec, "buy", &dir)?;
        let (path, id, now) = (
            boundary.path.clone(),
            boundary.request.order_id.clone(),
            boundary.now,
        );
        let request = boundary.request.clone();
        let binding =
            crate::execution_transaction_wire::decode_message(&spec.payload, |_| Ok(()))?.binding;
        let captured = Arc::new(Mutex::new(None));
        let writer = captured.clone();
        let hook: Hook = Box::new(move |rpc, reply| {
            if !rent170(rpc) {
                return;
            }
            let store = SqliteStore::open(&path).unwrap();
            let signature = "synthetic-b123-authoritative-signature";
            let mut pending = None;
            if change == "dispatch" {
                let order = store.load_execution_canary_order(&id).unwrap().unwrap();
                let signal = store
                    .load_copy_signal_by_signal_id(&request.signal_id)
                    .unwrap()
                    .unwrap();
                // Exercise the ordinary durable writer as a concurrent existing
                // authority. No cryptographic signature or send is generated.
                let identity = copybot_storage_core::ExecutionCanaryDispatch {
                    order_id: id.clone(),
                    signal_id: request.signal_id.clone(),
                    client_order_id: request.client_order_id.clone(),
                    route: request.route.clone(),
                    attempt: request.attempt,
                    wallet: request.wallet_pubkey.clone(),
                    token: request.token.clone(),
                    side: request.side.clone(),
                    tx_signature: signature.into(),
                    transaction_sha256: binding.transaction_sha256.clone(),
                    message_sha256: binding.message_sha256.clone(),
                };
                assert_eq!(
                    store
                        .claim_execution_canary_dispatch(&order, &signal, &identity, now)
                        .unwrap(),
                    copybot_storage_core::ExecutionDispatchClaim::New
                );
                assert_eq!(
                    store
                        .claim_execution_canary_dispatch(&order, &signal, &identity, now)
                        .unwrap(),
                    copybot_storage_core::ExecutionDispatchClaim::Existing
                );
            } else if change != "pending" {
                store
                    .mark_execution_canary_submitted(&id, now, signature)
                    .unwrap();
            }
            if ["receipt", "pending"].contains(&change) {
                let receipt_id = if change == "pending" {
                    let mut signal = store
                        .load_copy_signal_by_signal_id(&request.signal_id)
                        .unwrap()
                        .unwrap();
                    signal.signal_id = "synthetic-b123-other-pending-signal".into();
                    signal.token = "OtherPendingMint123".into();
                    store.insert_copy_signal(&signal).unwrap();
                    let order = store
                        .reserve_execution_canary_order(&signal.signal_id, &request.route, now)
                        .unwrap()
                        .order;
                    store
                        .mark_execution_canary_built(&order.order_id, now)
                        .unwrap();
                    store
                        .mark_execution_canary_simulated(&order.order_id, now, "ok", None)
                        .unwrap();
                    store
                        .mark_execution_canary_submitted(&order.order_id, now, signature)
                        .unwrap();
                    pending = Some(order.order_id.clone());
                    order.order_id
                } else {
                    id.clone()
                };
                store
                    .mark_execution_canary_confirmed_unreconciled(
                        &receipt_id,
                        &ExecutionCanaryReceiptProof {
                            tx_signature: signature.into(),
                            wallet_pubkey: request.wallet_pubkey.clone(),
                            token: if change == "pending" {
                                "OtherPendingMint123".into()
                            } else {
                                request.token.clone()
                            },
                            side: "buy".into(),
                            confirmation_status: "confirmed".into(),
                            slot: Some(123),
                            confirmed_at: now,
                            reason: "synthetic-b123-receipt-still-pending".into(),
                        },
                        now,
                    )
                    .unwrap();
            }
            *writer.lock().unwrap() =
                Some((snapshot(&store, &id, pending.as_deref()).unwrap(), pending));
            reply.delay = Duration::from_millis(100); // Hold the existing rent response after the durable write.
        });
        let outcome = invoke(&boundary, &spec, &dir, Some(hook)).await?.unwrap();
        assert_eq!(outcome.failed, 0, "{change}: {outcome:?}");
        assert_eq!(outcome.reason.as_deref(), Some("initial_sol_order_changed"));
        let (expected, pending) = captured
            .lock()
            .unwrap()
            .clone()
            .expect("existing rent170 collector phase reached");
        assert_eq!(
            snapshot(
                &boundary.store,
                &boundary.request.order_id,
                pending.as_deref()
            )?,
            expected
        );
        assert!(
            crate::execution_tiny_submit_state::eligible(&boundary.store, &boundary.request)
                .is_err()
        );
        assert_eq!(boundary.sql()?["failed_expenses"], 0);
        assert_eq!(boundary.sql()?["fills"], 0);
        let recovery_id = pending.as_deref().unwrap_or(&boundary.request.order_id);
        boundary.store.visit_execution_canary_reconciliation(
            recovery_id,
            &boundary.request.wallet_pubkey,
            boundary.now + chrono::Duration::seconds(1),
        )?;
        assert_eq!(
            snapshot(
                &boundary.store,
                &boundary.request.order_id,
                pending.as_deref()
            )?,
            expected
        );
        save(
            &dir,
            "authority-and-recovery.json",
            &json!({
                "change":change,"preserved":expected,"reconciliation_visit":"ordinary writer; no new permit or send",
                "collector_response":"existing rent170 held after durable write","sign_calls":0,"send_calls":0
            }),
        );
    }
    Ok(())
}

#[tokio::test]
async fn b123_jupiter_sell_funding_bypass_remains_no_request() -> Result<()> {
    let spec = Spec::from_frozen(&frozen(117)?);
    assert!(instructions(&spec)?
        .iter()
        .any(|i| i.program_id == super::token2022_ata_inputs_tests::key(JUPITER)));
    for (ordinal, side) in ["sell", "SELL"].into_iter().enumerate() {
        let dir = output(&format!("b123-sell-bypass-{ordinal}-{side}"));
        let boundary = Boundary::new(&spec, side, &dir)?;
        let before = boundary.sql()?;
        assert!(invoke(&boundary, &spec, &dir, None).await?.is_none());
        assert_eq!(boundary.sql()?, before);
        assert!(boundary
            .store
            .load_execution_canary_dispatch(&boundary.request.order_id)?
            .is_none());
    }
    Ok(())
}

#[tokio::test]
async fn b123_postawait_existing_non_jupiter_fee_floor_and_jupiter_safety_apply() -> Result<()> {
    for case in ["fee", "floor", "jupiter-safety"] {
        let spec = if case == "jupiter-safety" {
            Spec::sufficient("absent")?
        } else {
            non_jupiter()?
        };
        let dir = output(&format!("b123-postawait-{case}"));
        let mut boundary = Boundary::new(&spec, "buy", &dir)?;
        if case == "floor" {
            boundary.gate.pretrade_min_sol_reserve = 0.06;
        }
        let path = boundary.path.clone();
        let id = boundary.request.order_id.clone();
        let kill = boundary
            .gate
            .buy_safety_config
            .as_ref()
            .unwrap()
            .canary_kill_switch_path
            .clone();
        let reached = Arc::new(Mutex::new(false));
        let witness = reached.clone();
        let hook: Hook = Box::new(move |r, _| {
            if r["method"] != "getFeeForMessage" {
                return;
            }
            *witness.lock().unwrap() = true;
            if case == "fee" {
                rusqlite::Connection::open(&path).unwrap().execute(
                    "UPDATE execution_canary_build_plan_metadata SET priority_fee_json='{}' WHERE order_id=?1", [&id]).unwrap();
            }
            if case == "jupiter-safety" {
                std::fs::write(&kill, b"synthetic-b123-kill-switch").unwrap();
            }
        });
        let out = invoke(&boundary, &spec, &dir, Some(hook)).await?.unwrap();
        assert!(*reached.lock().unwrap());
        if case == "jupiter-safety" {
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
        assert_eq!(boundary.sql()?["failed_expenses"], 0);
        assert_eq!(boundary.sql()?["fills"], 0);
    }
    Ok(())
}
