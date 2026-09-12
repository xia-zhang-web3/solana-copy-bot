use super::generic_sell_fixture::*;
use super::generic_sell_loopback::*;
use super::generic_sell_test_support::*;
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

#[tokio::test]
async fn batch116_flag_disabled_paths_and_skipped_helper_remain_separate() -> Result<()> {
    for tiny in [false, true] {
        for ix in [false, true] {
            for tx in [false, true] {
                let r = run(
                    Replies::default(),
                    |c| {
                        c.canary_tiny_submit_enabled = tiny;
                        c.swap_instructions_dry_run_enabled = ix;
                        c.swap_transaction_dry_run_enabled = tx;
                    },
                    |_| {},
                )
                .await?;
                let guarded = tiny && ix && tx;
                let old_v0_refusal = tiny && tx && !ix;
                assert_eq!(
                    r.result.is_err(),
                    old_v0_refusal,
                    "{tiny}/{ix}/{tx}: {:?}",
                    r.result
                );
                assert_eq!(r.server.count("swap-instructions"), usize::from(ix));
                assert_eq!(r.server.count("/swap"), usize::from(tx && !guarded));
                assert_eq!(r.server.simulations().len(), usize::from(tiny && tx));
                assert_eq!(r.server.count_kind("unexpected"), 0);
                assert_eq!(r.payload().is_some(), tx && !old_v0_refusal);
                if let Some(payload) = r.payload() {
                    assert_eq!(payload, if guarded { legacy() } else { old_v0() });
                }
            }
        }
    }
    let server = Server::start(Replies::default());
    let mut cfg = config(&server.base);
    cfg.canary_tiny_submit_enabled = false;
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(cfg.clone()).build_transaction_plan(&request()?)?;
    let http = reqwest::Client::builder().no_proxy().build()?;
    let skipped =
        crate::execution_transaction_rpc_simulation::verify_serialized_transaction_rpc_simulation(
            &http,
            &cfg,
            &legacy(),
            "test-only-skipped-control",
            std::time::Duration::from_millis(1),
        )
        .await?;
    assert_eq!(
        skipped,
        crate::execution_transaction_rpc_simulation::RpcSimulationOutcome::Skipped
    );
    assert!(
        crate::execution_guarded_generic_sell::prepare(&http, &cfg, &plan)
            .await
            .is_err()
    );
    assert!(plan
        .serialized_transaction_payload_slot
        .as_ref()
        .unwrap()
        .load()?
        .is_none());
    assert!(server.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn batch116_only_existing_provider_soft_errors_may_reach_swap() -> Result<()> {
    for (status, body, soft, ix_calls) in [
        (
            200,
            json!({"simulationError":"missing account required"}).to_string(),
            true,
            2,
        ),
        (400, "Missing token program".into(), true, 3),
        (400, "provider unavailable".into(), false, 1),
        (200, json!({"error":"not authorized"}).to_string(), false, 1),
    ] {
        let r = run(
            Replies {
                instructions_status: status,
                instructions: body,
                ..Default::default()
            },
            |_| {},
            |_| {},
        )
        .await?;
        assert!(r.result.is_err());
        assert!(r.payload().is_none());
        assert_eq!(r.server.count("swap-instructions"), ix_calls);
        assert_eq!(r.server.count("/swap"), usize::from(soft));
        assert_eq!(
            r.server.count_kind("old_v0_local_refusal"),
            usize::from(soft)
        );
        assert_eq!(r.server.count_kind("recorded_legacy_simulation"), 0);
        assert_eq!(r.server.count_kind("unexpected"), 0);
    }
    Ok(())
}

#[test]
fn batch116_packet_boundary_and_metadata_do_not_replace_encoded_fee_proof() -> Result<()> {
    let cfg = config("http://127.0.0.1:1");
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(cfg.clone()).build_transaction_plan(&request()?)?;
    // Growing this instruction beyond127 bytes also adds one shortvec byte.
    for (extra, size) in [(149, 1232), (150, 1233)] {
        let mut value: Value = serde_json::from_str(INSTRUCTIONS)?;
        let mut data = STANDARD.decode(value["swapInstruction"]["data"].as_str().unwrap())?;
        data.extend(vec![0; extra]);
        value["swapInstruction"]["data"] = json!(STANDARD.encode(data));
        let bundle = BundleRequest::capture(&plan)?.bind(&value)?;
        let result = crate::execution_guarded_generic_sell::assemble(&cfg, &plan, &bundle);
        if size == 1232 {
            assert_eq!(
                super::generic_sell_decode::verify(
                    &result?.serialized_transaction_base64,
                    &value,
                    PAYER
                ),
                size
            );
        } else {
            assert!(result.unwrap_err().to_string().contains("packet_too_large"));
        }
        assert!(plan
            .serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .load()?
            .is_none());
    }
    let mut value: Value = serde_json::from_str(INSTRUCTIONS)?;
    value["prioritizationFeeLamports"] = json!(0);
    value["computeUnitLimit"] = json!(1);
    value["prioritizationType"] = json!({});
    let bundle = BundleRequest::capture(&plan)?.bind(&value)?;
    assert_eq!(
        crate::execution_guarded_generic_sell::assemble(&cfg, &plan, &bundle)?
            .serialized_transaction_base64,
        legacy()
    );
    let mut low_cap = cfg;
    low_cap.pretrade_max_priority_fee_lamports = 21_999;
    assert!(
        crate::execution_guarded_generic_sell::assemble(&low_cap, &plan, &bundle)
            .unwrap_err()
            .to_string()
            .contains("priority_fee_cap_exceeded")
    );
    Ok(())
}

#[tokio::test]
async fn batch116_loopback_refuses_nonexact_instruction_request_body() -> Result<()> {
    let r = run(
        Replies {
            expected_body: "{}".into(),
            ..Default::default()
        },
        |_| {},
        |_| {},
    )
    .await?;
    assert!(r.result.is_err());
    assert!(r.payload().is_none());
    assert_eq!(r.server.count("/swap"), 0);
    assert!(r.server.simulations().is_empty());
    assert_eq!(r.server.count_kind("unexpected"), 1);
    save(
        "exact-body-refusal.json",
        &json!({"calls":r.server.calls(),"slot":r.payload()}),
    )
}
