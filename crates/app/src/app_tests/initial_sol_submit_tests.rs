use super::initial_sol_rpc_fixture::FundingRpc;
use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn initial_sol_ordinary_actual_submit_exact_balance_and_minus_one() -> Result<()> {
    let required = 50_000_001 + 19_000 + 10_000_000 + 2 * 2_039_280;
    for balance in [0, required - 1, required] {
        let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
        let envelope = f.build().await?.envelope.unwrap();
        let rpc = Arc::new(Mutex::new(FundingRpc {
            balance,
            ..Default::default()
        }));
        let server = FundingRpc::server(rpc).await?;
        f.config.submit_adapter_http_url = server.endpoint.clone();
        let out = f.submit(&envelope).await;
        f.finish().await?;
        let trace = server.finish().await?;
        let out = out?;
        let decoded = crate::execution_transaction_wire::decode_message(
            envelope.signed_transaction_base64.as_deref().unwrap(),
            |_| Ok(()),
        )?;
        let fee = trace
            .iter()
            .find(|r| r.request["method"] == "getFeeForMessage")
            .unwrap();
        assert_eq!(
            fee.request["params"][0],
            STANDARD.encode(&decoded.binding.message_bytes)
        );
        assert_eq!(
            fee.request["params"][1],
            serde_json::json!({"commitment":"confirmed"})
        );
        let accounts = trace
            .iter()
            .find(|r| r.request["method"] == "getMultipleAccounts")
            .unwrap();
        let keys: Vec<_> = decoded
            .binding
            .accounts
            .iter()
            .map(|a| bs58::encode(a.pubkey).into_string())
            .collect();
        assert_eq!(accounts.request["params"][0], serde_json::json!(keys));
        assert_eq!(
            trace
                .iter()
                .filter(|r| r.request["method"] == "sendTransaction")
                .count(),
            usize::from(balance == required)
        );
        assert_eq!(trace.len(), if balance == required { 5 } else { 3 });
        assert!(trace.iter().all(|r| r.completed.is_some()));
        assert_eq!(
            trace
                .iter()
                .filter(|r| r.request["method"] == "getFeeForMessage")
                .count(),
            if balance == required { 2 } else { 1 }
        );
        if balance == required {
            assert_eq!(out.submitted, 1, "{out:?}");
            let sent = trace
                .iter()
                .find(|r| r.request["method"] == "sendTransaction")
                .unwrap();
            assert_eq!(
                sent.request["params"][0],
                envelope.signed_transaction_base64.unwrap()
            );
        } else {
            assert_eq!(out.failed, 1, "{out:?}");
            assert_eq!(
                out.error.unwrap(),
                format!(
                    "initial_sol_insufficient:observed={balance}:required={required}:shortfall={}",
                    required - balance
                )
            );
        }
        assert_eq!(
            f.conn()?.query_row(
                "SELECT COUNT(*) FROM execution_failed_expense_ledger",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_extension_token2022_and_missing_fee_do_not_send() -> Result<()> {
    for case in ["extension", "token2022", "fee"] {
        let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
        f.wire.lock().unwrap().extension = case == "extension";
        f.wire.lock().unwrap().token2022 = case == "token2022";
        if case == "fee" {
            f.funding.lock().unwrap().fee = None;
        }
        let envelope = f.build().await?.envelope.unwrap();
        let out = f.submit(&envelope).await;
        f.finish().await?;
        let out = out?;
        assert_eq!(out.failed, 1, "{out:?}");
        assert_eq!(
            out.error.as_deref(),
            Some(if case == "fee" {
                "initial_sol_fee_unavailable"
            } else {
                "initial_sol_unsupported_setup"
            })
        );
        assert_eq!(f.sends(), 0);
    }
    Ok(())
}
