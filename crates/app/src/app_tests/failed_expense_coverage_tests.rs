use super::{failed_expense_runtime_tests::*, receipt_reconciliation_fixture::*};
use anyhow::Result;
use copybot_storage_core::FailedExpenseCoverage as Coverage;
use serde_json::json;

#[tokio::test]
async fn failed_expense_runtime_fee_and_payer_matrix_preserves_coverage_and_u64() -> Result<()> {
    for case in [
        "zero",
        "large",
        "u64max",
        "missing",
        "invalid",
        "overflow",
        "foreign",
        "payer_unknown",
        "raw_keys",
        "alt",
        "bad_flags",
        "missing_native",
        "bad_native",
    ] {
        let f = Fixture::new("sell")?;
        let mut value = failed_receipt("sell", 5000);
        match case {
            "zero" => value = failed_receipt("sell", 0),
            "large" => value = failed_receipt("sell", 9_007_199_254_740_993),
            "u64max" => value = failed_receipt("sell", u64::MAX),
            "missing" => {
                value["result"]["meta"]
                    .as_object_mut()
                    .unwrap()
                    .remove("fee");
            }
            "invalid" => value["result"]["meta"]["fee"] = json!(-1),
            "overflow" => {
                value["result"]["meta"]["fee"] = serde_json::from_str("18446744073709551616")?
            }
            "foreign" => {
                value["result"]["transaction"]["signatures"] =
                    json!([SIGNATURE, "wallet-secondary-signature"]);
                value["result"]["transaction"]["message"]["accountKeys"] = json!([
                {"pubkey":"payer","signer":true,"writable":true},{"pubkey":WALLET,"signer":true,"writable":true}]);
                value["result"]["meta"]["preBalances"] = json!([5000, 10]);
                value["result"]["meta"]["postBalances"] = json!([0, 10]);
            }
            "payer_unknown" => {
                value["result"]["transaction"]["message"]["accountKeys"][0]["signer"] = json!(false)
            }
            "raw_keys" => {
                value["result"]["transaction"]["message"]["accountKeys"] =
                    json!([WALLET, "token-account"])
            }
            "alt" => {
                value["result"]["transaction"]["message"]["addressTableLookups"] =
                    json!([{"accountKey":"alt"}])
            }
            "bad_flags" => {
                value["result"]["transaction"]["message"]["accountKeys"][0]["writable"] =
                    json!("true")
            }
            "missing_native" => {
                value["result"]["meta"]
                    .as_object_mut()
                    .unwrap()
                    .remove("preBalances");
            }
            _ => value["result"]["meta"]["preBalances"] = json!(["5000", 2039280]),
        }
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "failed_expense_runtime_fee_and_payer_matrix_preserves_coverage_and_u64 case={case:?}"
        ));
        rpc.context(format!("fee matrix case={case} side=sell"));
        if case == "invalid" {
            rpc.response.lock().unwrap().2 = 120;
        }
        f.reconcile(&rpc, 1).await?;
        rpc.finish().await?;
        let facts = f
            .store
            .load_failed_transaction_facts(&f.order_id)?
            .expect(case);
        let expected = match case {
            "zero" | "foreign" => Some("0".into()),
            "large" => Some("9007199254740993".into()),
            "u64max" => Some(u64::MAX.to_string()),
            "missing_native" | "bad_native" => Some("5000".into()),
            _ => None,
        };
        assert_eq!(
            facts.wallet_fee()?.map(|v| v.as_u64().to_string()),
            expected,
            "{case}"
        );
        assert_eq!(ledger_count(&f)?, u64::from(expected.is_some()), "{case}");
        assert_eq!(f.fills()?, 0);
        if case == "missing" {
            assert_eq!(facts.fee_coverage, Coverage::Missing);
        }
        if ["invalid", "overflow"].contains(&case) {
            assert_eq!(facts.fee_coverage, Coverage::Invalid);
        }
        if case == "missing_native" {
            assert_eq!(facts.native_coverage, Coverage::Missing);
        }
        if case == "bad_native" {
            assert_eq!(facts.native_coverage, Coverage::Invalid);
        }
    }
    Ok(())
}
#[tokio::test]
async fn failed_expense_receipt_identity_conflicts_never_book_fees() -> Result<()> {
    for conflict in ["wallet", "signature", "slot", "duplicate_keys"] {
        let f = Fixture::new("buy")?;
        let mut value = failed_receipt("buy", 5000);
        match conflict {
            "wallet" => {
                value["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
                    json!("foreign")
            }
            "signature" => value["result"]["transaction"]["signatures"][0] = json!("foreign"),
            "slot" => value["result"]["slot"] = json!(43),
            _ => {
                value["result"]["transaction"]["message"]["accountKeys"][1]["pubkey"] =
                    json!(WALLET)
            }
        }
        let rpc = Rpc::new(value).await?;
        rpc.context(format!(
            "failed_expense_receipt_identity_conflicts_never_book_fees conflict={conflict:?}"
        ));
        rpc.status.lock().unwrap()["result"]["value"][0]["err"] = failure();
        f.reconcile(&rpc, 1).await?;
        assert_eq!(ledger_count(&f)?, 0);
        assert_eq!(f.fills()?, 0);
        assert_ne!(
            f.store
                .load_failed_expense_task(&f.order_id)?
                .unwrap()
                .status,
            "complete"
        );
        rpc.finish().await?;
    }
    Ok(())
}
