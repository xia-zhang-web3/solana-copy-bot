use super::{
    b127_causal_fixture::*,
    priority_fee_route_fixture::{Fixture, Route},
};
use crate::execution_native_floor_policy::protected;
use anyhow::Result;
use copybot_config::TinyPolicyMode;
use serde_json::json;

pub(super) async fn prepared(route: Route) -> Result<Fixture> {
    let mut f = fixture(route).await?;
    f.config.tiny_experiment.policy_mode = TinyPolicyMode::ProtectedNativeCapital;
    f.sync_config();
    protected::prepare_request(&f.store, &f.config, &mut f.request, f.now).await?;
    Ok(f)
}
#[tokio::test]
async fn b127_all_buy_builders_send_only_final_guarded_message() -> Result<()> {
    for route in [
        Route::Metis,
        Route::Direct,
        Route::DirectFallback,
        Route::Paid,
        Route::PaidFallback,
    ] {
        let mut f = prepared(route).await?;
        let e = f.store.tiny_native_policy(
            "local-variant-a",
            &f.request.wallet_pubkey,
            chrono::Utc::now(),
        )?;
        assert_eq!(
            (
                e.initial_lamports,
                e.floor_lamports,
                e.allowance,
                e.original_reserve
            ),
            (B, F, 15_000_000, 50_000_001)
        );
        let envelope = f.build().await?.envelope.unwrap();
        let out = f.submit(&envelope).await?;
        assert_eq!(out.submitted, 1, "{route:?}: {out:?}");
        assert_eq!(f.sends(), 1);
        trace(&f, envelope.signed_transaction_base64.as_ref().unwrap(), F)?;
        let row: (Option<u64>,u64,String) = f.conn()?.query_row("SELECT r.buy_lamports,e.requested_lamports,e.floor_lamports FROM execution_tiny_reservations r JOIN execution_tiny_capital_evidence e USING(order_id)",[],|r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        assert_eq!(row, (None, 10_000_000, F.to_string()));
        assert_eq!(
            f.store.tiny_native_policy(
                "local-variant-a",
                &f.request.wallet_pubkey,
                chrono::Utc::now()
            )?,
            e
        );
        println!("B127_VARIANT {route:?} {out:?} decoded=NULL requested=10000000");
        f.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn b127_stricter_weaker_reserve_and_restart_reuse_original_anchor() -> Result<()> {
    for sol in [0.04, 0.09] {
        let mut f = prepared(Route::Metis).await?;
        let old = f.store.tiny_native_policy(
            "local-variant-a",
            &f.request.wallet_pubkey,
            chrono::Utc::now(),
        )?;
        super::b126_r1_fixture::reopen(&mut f)?;
        f.funding.lock().unwrap().balance += 900_000_000; // inflow, not a new allowance
        f.config.pretrade_min_sol_reserve = sol;
        f.sync_config();
        protected::prepare_request(&f.store, &f.config, &mut f.request, f.now).await?;
        assert_eq!(
            f.store.tiny_native_policy(
                "local-variant-a",
                &f.request.wallet_pubkey,
                chrono::Utc::now()
            )?,
            old
        );
        assert_eq!(
            f.calls.lock().unwrap().len(),
            1,
            "reprepare needs no new anchor RPC"
        );
        let envelope = f.build().await?.envelope.unwrap();
        let out = f.submit(&envelope).await?;
        assert_eq!(out.submitted, 1, "{out:?}");
        trace(
            &f,
            envelope.signed_transaction_base64.as_ref().unwrap(),
            F.max(crate::execution_native_floor_policy::reserve_lamports(sol)?),
        )?;
        f.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn b127_requested_amount_plus_one_and_invalid_payer_never_activate() -> Result<()> {
    for case in [
        "plus-one",
        "missing",
        "owner",
        "executable",
        "data",
        "fraction",
        "negative",
        "string",
        "overflow",
        "underflow",
        "insufficient",
    ] {
        let mut f = fixture(Route::Metis).await?;
        f.config.tiny_experiment.policy_mode = TinyPolicyMode::ProtectedNativeCapital;
        f.sync_config();
        let mut payer = super::initial_sol_rpc_fixture::system(B);
        match case {
            "plus-one" => {
                f.request.buy_size_sol = 0.010000001;
                f.request.metadata.quote_in_amount_raw = Some("10000001".into());
                let mut q: serde_json::Value =
                    serde_json::from_str(f.request.metadata.quote_response_json.as_ref().unwrap())?;
                q["inAmount"] = json!("10000001");
                f.request.metadata.quote_response_json = Some(q.to_string());
            }
            "missing" => payer = json!(null),
            "owner" => payer["owner"] = json!(JUP),
            "executable" => payer["executable"] = json!(true),
            "data" => payer["data"] = json!(["AA==", "base64"]),
            "fraction" => payer["lamports"] = json!(1.5),
            "negative" => payer["lamports"] = json!(-1),
            "string" => payer["lamports"] = json!("102324740"),
            "overflow" => payer["lamports"] = serde_json::from_str("18446744073709551616")?,
            "underflow" => payer["lamports"] = json!(14_999_999),
            "insufficient" => payer["lamports"] = json!(50_000_001),
            _ => unreachable!(),
        }
        f.funding
            .lock()
            .unwrap()
            .rows
            .insert(f.request.wallet_pubkey.clone(), payer);
        let error = protected::prepare_request(&f.store, &f.config, &mut f.request, f.now)
            .await
            .unwrap_err();
        assert!(
            f.store.load_tiny_experiment(chrono::Utc::now())?.is_none(),
            "{case}: {error}"
        );
        assert_eq!(f.sends(), 0);
        assert_eq!(f.signatures(), 0);
        println!("B127_ACTIVATION_REFUSAL {case}: {error}");
        f.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn b127_changed_context_config_and_deadline_refuse_prepared_buy() -> Result<()> {
    for case in [
        "missing-context",
        "missing-policy",
        "mode",
        "id",
        "wallet",
        "floor",
        "deadline",
        "request",
    ] {
        let mut f = prepared(Route::Metis).await?;
        let envelope = f.build().await?.envelope.unwrap();
        match case {
            "missing-context" => f.request.metadata.protected_capital = None,
            "missing-policy" => {
                f.conn()?
                    .execute("DELETE FROM execution_tiny_native_policy", [])?;
            }
            "mode" => f.config.tiny_experiment.policy_mode = TinyPolicyMode::DecodedAmount,
            "id" => f.config.tiny_experiment.id = Some("changed".into()),
            "wallet" => f.config.canary_wallet_pubkey = "changed".into(),
            "floor" => f.config.pretrade_min_sol_reserve = 0.1,
            "deadline" => {
                f.now += chrono::Duration::hours(2);
            }
            "request" => f.request.metadata.quote_in_amount_raw = Some("9999999".into()),
            _ => unreachable!(),
        }
        let out = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(1),
            f.submit(&envelope),
        )
        .await?;
        assert_eq!(f.sends(), 0, "{case}: {out:?}");
        assert!(f
            .store
            .load_execution_canary_dispatch(&f.request.order_id)?
            .is_none());
        println!("B127_CURRENT_REFUSAL {case}: {out:?}");
        f.finish().await?;
    }
    Ok(())
}
