use super::receipt_cash_facts_fixture::*;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::{
    ReceiptDecomposition, ReceiptFeeCoverage, ReceiptTokenCoverage, ReceiptWsolCoverage,
};
use serde_json::json;

#[tokio::test]
async fn receipt_cash_facts_signed_sell_survives_completion_and_restart() -> Result<()> {
    for delta in [-400_000, 0, 123_456_789] {
        let mut f = Fixture::new("sell")?;
        let mut value = receipt("sell", delta);
        value["result"]["meta"]["fee"] = json!(5000);
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "receipt_cash_facts_signed_sell_survives_completion_and_restart delta={delta:?}"
        ));
        let outcome = f.reconcile(&rpc, 10).await?;
        assert_eq!(outcome.confirmation_pending, 0);
        assert_eq!(outcome.confirmation_confirmed, 1);
        assert_eq!(
            transaction_calls(&rpc),
            1,
            "one receipt RPC before both writes"
        );
        f.reopen()?;
        let observed = facts(&f)?;
        assert_eq!(observed.order_id, f.order_id);
        assert_eq!(
            (
                &*observed.tx_signature,
                &*observed.wallet_pubkey,
                &*observed.token,
                &*observed.side
            ),
            (SIGNATURE, WALLET, TOKEN, "sell")
        );
        assert_eq!(observed.slot, 42);
        assert_eq!(observed.wallet_native_pre.as_u64(), 2_000_000_000);
        assert_eq!(
            observed.wallet_native_post.as_u64(),
            (2_000_000_000 + delta) as u64
        );
        assert_eq!(observed.wallet_native_delta.as_i128(), i128::from(delta));
        assert_eq!(observed.transaction_fee.unwrap().as_u64(), 5000);
        assert_eq!(observed.fee_coverage, ReceiptFeeCoverage::Known);
        assert_eq!(observed.fee_payer.as_deref(), Some(WALLET));
        assert_eq!(observed.wallet_is_fee_payer(), Some(true));
        assert_eq!(observed.token_delta.unwrap().raw, -7000);
        assert_eq!(observed.token_delta.unwrap().decimals, 3);
        assert_eq!(
            observed.token_coverage,
            ReceiptTokenCoverage::PairedBalances
        );
        assert_eq!(observed.decomposition, ReceiptDecomposition::Unresolved);
        assert_eq!(observed.wsol_coverage, ReceiptWsolCoverage::Unresolved);
        f.reconcile(&rpc, 400).await?;
        f.reopen()?;
        assert_eq!(facts(&f)?, observed);
        assert_eq!(f.fills()?, 1);
        assert_eq!(
            f.conn()?.query_row(
                "SELECT COUNT(*) FROM execution_canary_receipt_facts",
                [],
                |r| r.get::<_, u64>(0)
            )?,
            1
        );
        assert_eq!(transaction_calls(&rpc), 1);
        let stored: i64 = f.conn()?.query_row(
            "SELECT wallet_native_delta_lamports FROM fills WHERE order_id = ?1",
            [&f.order_id],
            |r| r.get(0),
        )?;
        assert_eq!(stored, delta, "no separate fee debit");
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_buy_distinguishes_transaction_fee_and_payer() -> Result<()> {
    for (fee, coverage) in [
        (None, ReceiptFeeCoverage::Missing),
        (Some(json!(null)), ReceiptFeeCoverage::Missing),
        (Some(json!(0)), ReceiptFeeCoverage::Known),
        (Some(json!(5000)), ReceiptFeeCoverage::Known),
        (Some(json!(u64::MAX)), ReceiptFeeCoverage::Known),
        (Some(json!(-1)), ReceiptFeeCoverage::Invalid),
        (Some(json!(1.5)), ReceiptFeeCoverage::Invalid),
        (Some(json!("5000")), ReceiptFeeCoverage::Invalid),
        (
            Some(serde_json::from_str("18446744073709551616")?),
            ReceiptFeeCoverage::Invalid,
        ),
    ] {
        let f = Fixture::new("buy")?;
        let mut value = sponsored(receipt("buy", -900_000_000));
        if let Some(fee) = &fee {
            value["result"]["meta"]["fee"] = fee.clone();
        }
        let rpc = Rpc::new(value).await?;
        rpc.context(format!("receipt_cash_facts_buy_distinguishes_transaction_fee_and_payer fee={fee:?} coverage={coverage:?}"));
        assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
        let observed = facts(&f)?;
        assert_eq!(observed.wallet_native_delta.as_i128(), -900_000_000);
        assert_eq!(observed.fee_payer.as_deref(), Some("Sponsor"));
        assert_eq!(observed.wallet_is_fee_payer(), Some(false));
        assert_eq!(observed.fee_coverage, coverage);
        assert_eq!(
            observed.transaction_fee.map(|v| v.as_u64()),
            fee.as_ref().and_then(|v| v.as_u64())
        );
        assert_eq!(observed.token_delta.unwrap().raw, 7000);
        let notional: i64 = f.conn()?.query_row(
            "SELECT notional_lamports FROM fills WHERE order_id = ?1",
            [&f.order_id],
            |r| r.get(0),
        )?;
        assert_eq!(
            notional, 900_000_000,
            "foreign fee must not adjust wallet cash flow"
        );
        assert_eq!(observed.decomposition, ReceiptDecomposition::Unresolved);
        rpc.finish().await?;
    }
    let f = Fixture::new("buy")?;
    let mut value = sponsored(receipt("buy", -900_000_000));
    value["result"]["transaction"]["message"]["accountKeys"][0]["signer"] = json!(false);
    value["result"]["meta"]["fee"] = json!(5000);
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "receipt_cash_facts_buy_distinguishes_transaction_fee_and_payer"
    ));
    f.reconcile(&rpc, 1).await?;
    let observed = facts(&f)?;
    assert_eq!(observed.fee_payer, None);
    assert_eq!(observed.wallet_is_fee_payer(), None);
    assert_eq!(observed.transaction_fee.unwrap().as_u64(), 5000);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_exact_native_domain_exceeds_f64_and_legacy_sql_integer() -> Result<()> {
    let large = (1_u64 << 53) + 123;
    for (side, pre, post, accounted) in [
        ("sell", large, large + 1, true),
        ("sell", 0, large, true),
        ("buy", u64::MAX, u64::MAX - 123_456_789, true),
        ("sell", 0, u64::MAX, false),
        ("buy", u64::MAX, 0, false),
        ("sell", u64::MAX, 0, false),
    ] {
        let mut f = Fixture::new(side)?;
        let initial = money_snapshot(&f)?;
        let mut value = receipt(side, 0);
        value["result"]["meta"]["preBalances"][0] = json!(pre);
        value["result"]["meta"]["postBalances"][0] = json!(post);
        let rpc = Rpc::new(value).await?;
        rpc.context(format!("receipt_cash_facts_exact_native_domain_exceeds_f64_and_legacy_sql_integer side={side:?} pre={pre:?} post={post:?} accounted={accounted:?}"));
        let out = f.reconcile(&rpc, 1).await;
        if accounted {
            assert_eq!(out?.confirmation_confirmed, 1);
        } else {
            assert!(out.is_err() || out.unwrap().confirmation_pending == 1);
        }
        f.reopen()?;
        let observed = facts(&f)?;
        assert_eq!(observed.wallet_native_pre.as_u64(), pre);
        assert_eq!(observed.wallet_native_post.as_u64(), post);
        assert_eq!(
            observed.wallet_native_delta.as_i128(),
            i128::from(post) - i128::from(pre)
        );
        let stored: (String, String, String) = f.conn()?.query_row(
            "SELECT wallet_native_pre, wallet_native_post, wallet_native_delta FROM execution_canary_receipt_facts",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        assert_eq!(
            stored,
            (
                pre.to_string(),
                post.to_string(),
                observed.wallet_native_delta.as_i128().to_string()
            )
        );
        if !accounted {
            assert_pending(&f)?;
            assert_eq!(money_snapshot(&f)?, initial);
        }
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_cash_facts_exact_token_sum_can_exceed_u64_without_becoming_fill() -> Result<()> {
    let f = Fixture::new("sell")?;
    let initial = money_snapshot(&f)?;
    let mut value = receipt("sell", 123);
    value["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .push(json!({"pubkey":"second-token-account","signer":false,"writable":true}));
    for name in ["preBalances", "postBalances"] {
        value["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .push(json!(1));
    }
    for (name, raw) in [("preTokenBalances", u64::MAX), ("postTokenBalances", 0)] {
        let rows = value["result"]["meta"][name].as_array_mut().unwrap();
        rows[0]["uiTokenAmount"]["amount"] = json!(raw.to_string());
        let mut second = rows[0].clone();
        second["accountIndex"] = json!(2);
        rows.push(second);
    }
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "receipt_cash_facts_exact_token_sum_can_exceed_u64_without_becoming_fill"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_pending, 1);
    assert_eq!(
        facts(&f)?.token_delta.unwrap().raw,
        -2 * i128::from(u64::MAX)
    );
    assert_pending(&f)?;
    assert_eq!(money_snapshot(&f)?, initial);
    rpc.finish().await?;
    Ok(())
}
