use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_source_sell_guard as guard;
use anyhow::Result;
use copybot_storage_core::*;

#[tokio::test]
async fn owned_sell_current_proof_reaches_signed_loopback_submit() -> Result<()> {
    let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
    f.make_sell()?;
    assert!(guard::request(&f.store, &f.request, &[EXECUTION_STATUS_CANARY_CANDIDATE])?.is_some());
    let outcome = f.build().await?;
    assert_eq!(outcome.built, 1);
    let envelope = outcome.envelope.unwrap();
    assert!(envelope.signed_transaction_base64.is_some());
    assert_eq!(f.signatures(), 1);
    let out = f.submit(&envelope).await?;
    assert_eq!(out.submitted, 1, "{out:?}");
    assert_eq!(f.sends(), 1);
    let order = f
        .store
        .load_execution_canary_order(&f.request.order_id)?
        .unwrap();
    assert_eq!(order.tx_signature, envelope.tx_signature_hint);
    assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(
        f.request.metadata.quote_in_amount_raw.as_deref(),
        Some("123456")
    );
    assert!(f
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(_, r)| r["method"] == "simulateTransaction"));
    f.finish().await
}

#[tokio::test]
async fn owned_sell_missing_identity_and_conflicting_proof_stop_before_sign() -> Result<()> {
    for (change, expected) in [
        ("missing", "source_sell_amount_proof_missing"),
        ("identity", "source_sell_order_identity_mismatch"),
        ("quote", "source_sell_amount_proof_conflict"),
        ("partial", "source_sell_amount_proof_conflict"),
    ] {
        let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
        f.make_sell()?;
        match change {
            "missing" => f.request.metadata.owned_sell_amount = None,
            "identity" => f.request.wallet_id = "other-source-wallet".into(),
            "quote" => f.request.metadata.quote_out_amount_raw = Some("1".into()),
            "partial" => {
                // Even a typed proof cannot authorize arbitrary partial23456 at both balances123456.
                let position = f
                    .store
                    .load_execution_canary_open_position(&f.request.token)?
                    .unwrap();
                let selection = guard::amount::Selection::new(&position)?;
                assert_eq!(selection.amount(123456, 0)?, 123456);
                let source = guard::order(
                    &f.store,
                    &f.request.order_id,
                    &[EXECUTION_STATUS_CANARY_CANDIDATE],
                )?;
                f.request.metadata.quote_in_amount_raw = Some("23456".into());
                let mut quote: serde_json::Value = serde_json::from_str(
                    f.request.metadata.quote_response_json.as_deref().unwrap(),
                )?;
                quote["inAmount"] = "23456".into();
                f.request.metadata.quote_response_json = Some(quote.to_string());
                f.request.metadata = selection.finish(
                    f.request.metadata.clone(),
                    source.as_ref(),
                    &f.request.wallet_pubkey,
                    123456,
                    23456,
                )?;
            }
            _ => unreachable!(),
        }
        let err = guard::request(&f.store, &f.request, &[EXECUTION_STATUS_CANARY_CANDIDATE])
            .err()
            .unwrap();
        assert_eq!(
            err.downcast_ref::<guard::Refusal>().unwrap().reason,
            expected
        );
        // Actual signing consumer also rejects the same request after simulation.
        let out = f.build().await?;
        assert_eq!(out.source_refusal.unwrap().reason, expected);
        assert!(out.envelope.is_none());
        assert_eq!(f.signatures(), 0);
        assert_eq!(f.sends(), 0);
        assert!(f
            .store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap()
            .tx_signature
            .is_none());
        f.finish().await?;
    }
    Ok(())
}
