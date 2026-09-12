use super::{b93_fixture as f, b93_http_fixture::Server};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
async fn b93_fresh_wallet_cap_and_wallet_await_never_reuse_stale_7000() -> Result<()> {
    for arm in ["cap", "before", "wallet-stale", "wallet-lower"] {
        let (db, m) = f::seeded(&format!("b93-fresh-{arm}")).await?;
        let signal = f::legacy(&db, &m)?;
        if arm == "before" {
            f::settle(&db, &f::receipt(&db, &m, "before", 3000)?)?;
        }
        let mut rpc = Server::new(db.path.clone(), f::at(), [94; 32]).await?;
        {
            let mut control = rpc.state.lock().unwrap();
            control.wallet_raw = if arm == "cap" { 2500 } else { 7000 };
            if arm.starts_with("wallet-") {
                control.mutate_at = Some("getTokenAccountsByOwner");
                control.after_wallet_raw = Some(if arm == "wallet-lower" { 4000 } else { 7000 });
                let path = db.path.clone();
                let m = m.clone();
                control.mutation = Some(Box::new(move || {
                    let db = f::open(&path)?;
                    f::settle(&db, &f::receipt(&db, &m, arm, 3000)?)?;
                    Ok(())
                }));
            }
        }
        let quote = super::source_write_off_fixture::quote(&signal, f::at());
        db.store.record_execution_quote_canary_event(&quote)?;
        let mut config = f::config(&rpc.url);
        config.max_submit_attempts = 3;
        let out = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
            &config,
            &db.store,
            &quote.event_id,
            f::at(),
        )
        .await?
        .unwrap();
        if arm.starts_with("wallet-") {
            assert_eq!(out.source_sell_refusals.count(), 1, "{out:?}");
            assert_eq!(out.skipped_reason, Some("source_sell_amount_stale"));
            assert_eq!(rpc.count("quote"), 0);
            let order = db
                .store
                .load_execution_canary_order_by_signal(&signal.signal_id)?
                .unwrap();
            assert_eq!(order.status, EXECUTION_STATUS_CANARY_FAILED);
            assert_eq!(
                order.err_code.as_deref(),
                Some(EXECUTION_ERROR_BUILD_FAILED)
            );
            assert_eq!(
                order.simulation_error.as_deref(),
                Some("source_sell_amount_stale")
            );
            let reopened = f::open(&db.path)?;
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &config,
                &reopened.store,
                f::at(),
            )
            .await?;
            rpc.state.lock().unwrap().wallet_raw = 4000;
            let fresh =
                crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
                    &config,
                    &reopened.store,
                    &quote.event_id,
                    f::at(),
                )
                .await?
                .unwrap();
            assert_eq!(fresh.source_sell_refusals.count(), 0, "{fresh:?}");
            assert!(fresh
                .last_error
                .as_deref()
                .unwrap()
                .contains("signer keypair"));
        } else {
            assert_eq!(out.source_sell_refusals.count(), 0, "{out:?}");
            assert!(out
                .last_error
                .as_deref()
                .unwrap()
                .contains("signer keypair"));
        }
        rpc.finish().await?;
        let order = db
            .store
            .load_execution_canary_order_by_signal(&signal.signal_id)?
            .unwrap();
        let stored = db
            .store
            .load_execution_canary_build_plan_metadata(&order.order_id)?
            .unwrap();
        assert_eq!(
            stored.quote_in_amount_raw.as_deref(),
            Some(if arm == "cap" { "2500" } else { "4000" })
        );
        assert_eq!(rpc.count("sendTransaction"), 0);
        f::write(
            arm,
            json!({"summary":format!("{out:?}"),"stored":format!("{stored:?}"),"calls":rpc.state.lock().unwrap().calls}),
        )?;
    }
    Ok(())
}
