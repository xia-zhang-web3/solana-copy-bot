use super::generic_sell_fixture::*;
use super::generic_sell_loopback::*;
use super::generic_sell_test_support::*;
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use serde_json::{json, Value};

#[tokio::test]
async fn batch116_actual_adapter_rejects_quote_blueprint_wallet_mismatch() -> Result<()> {
    for field in [
        "inAmount",
        "outAmount",
        "inputMint",
        "outputMint",
        "slippageBps",
        "routePlan",
        "swapMode",
        "otherAmountThreshold",
    ] {
        let r = run(
            Replies::default(),
            |_| {},
            |plan| {
                let mut quote: Value =
                    serde_json::from_str(plan.metadata.quote_response_json.as_ref().unwrap())
                        .unwrap();
                quote[field] = match field {
                    "slippageBps" => json!(501),
                    "routePlan" => json!([]),
                    "otherAmountThreshold" => json!("0"),
                    _ => json!("wrong"),
                };
                plan.metadata.quote_response_json = Some(quote.to_string());
            },
        )
        .await?;
        r.assert_rejected(field, 0);
        assert_eq!(r.server.count("swap-instructions"), 0);
    }
    for field in ["wallet", "quote-event", "amount", "fee"] {
        let r = run(
            Replies::default(),
            |_| {},
            |plan| match field {
                "wallet" => {
                    plan.swap_blueprint.as_mut().unwrap().wallet_pubkey =
                        Some(bs58::encode([3; 32]).into_string())
                }
                "quote-event" => plan.metadata.quote_event_id = Some("other".into()),
                "amount" => plan.metadata.quote_in_amount_raw = Some("123".into()),
                "fee" => {
                    plan.metadata.priority_fee_json =
                        Some(super::priority_fee_fixture::total_json(1))
                }
                _ => unreachable!(),
            },
        )
        .await?;
        r.assert_rejected(field, 0);
    }
    for which in [0, 1] {
        let r = run(
            Replies::default(),
            |config| match which {
                0 => config.execution_signer_pubkey = bs58::encode([3; 32]).into_string(),
                1 => config.canary_wallet_pubkey = bs58::encode([3; 32]).into_string(),
                _ => unreachable!(),
            },
            |_| {},
        )
        .await?;
        r.assert_rejected(&format!("current-wallet-policy-{which}"), 0);
    }
    Ok(())
}

#[test]
fn batch116_bound_bundle_cannot_cross_plan_attempt_or_quote() -> Result<()> {
    let cfg = config("http://127.0.0.1:1");
    let request = request()?;
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(cfg.clone()).build_transaction_plan(&request)?;
    let bundle = BundleRequest::capture(&plan)?.bind(&serde_json::from_str(INSTRUCTIONS)?)?;
    for change in 0..15 {
        let mut changed = plan.clone();
        match change {
            0 => changed.attempt += 1,
            1 => changed.plan_id.push('x'),
            2 => changed.order_id.push('x'),
            3 => changed.client_order_id.push('x'),
            4 => changed.wallet_pubkey = bs58::encode([3; 32]).into_string(),
            5 => changed.metadata.quote_event_id = Some("other".into()),
            6 => changed.metadata.quote_response_json = Some("{}".into()),
            7 => {
                changed.metadata.http_request_started_ts =
                    Some(chrono::DateTime::from_timestamp(1, 0).unwrap())
            }
            8 => changed.buy_size_sol = 0.02,
            9 => changed.submit_enabled = true,
            10 => changed.route.push('x'),
            11 => changed.metadata.route_plan_json = Some("[]".into()),
            12 => changed.metadata.quote_in_amount_raw = Some("1".into()),
            13 => changed.slippage_tolerance_bps += 1,
            _ => changed.metadata.quote_out_amount_raw = Some("1".into()),
        }
        assert!(
            crate::execution_guarded_generic_sell::assemble(&cfg, &changed, &bundle).is_err(),
            "change{change}"
        );
        assert!(changed
            .serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .load()?
            .is_none());
    }
    let result = crate::execution_guarded_generic_sell::assemble(&cfg, &plan, &bundle)?;
    assert_eq!(result.serialized_transaction_base64, legacy());
    for reserve in [0.0, 0.05, 1.0, f64::NAN] {
        let mut changed_policy = cfg.clone();
        changed_policy.pretrade_min_sol_reserve = reserve;
        assert_eq!(
            crate::execution_guarded_generic_sell::assemble(&changed_policy, &plan, &bundle)?
                .serialized_transaction_base64,
            legacy()
        );
    }
    Ok(())
}
