use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_native_funding::{decode_native_funding_requirements as decode, types::*};
use crate::execution_pumpswap_accounts::parse_pubkey;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use anyhow::Result;

#[tokio::test]
async fn native_funding_actual_http_direct_builder_bytes_do_not_add_calls_or_enforcement(
) -> Result<()> {
    for sell in [false, true] {
        let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
        if sell {
            f.make_sell()?;
        }
        let plan = f.adapter.build_transaction_plan(&f.request)?;
        let result =
            crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run(
                &reqwest::Client::new(),
                &f.config,
                &plan,
            )
            .await;
        f.finish().await?;
        let built = result?.unwrap();
        assert!(built.summary.ends_with("rpc_simulation=passed"));
        let calls = f.calls.lock().unwrap().clone();
        let wallet = parse_pubkey(&f.config.canary_wallet_pubkey, "fixture")?;
        let requirements = decode(&built.serialized_transaction_base64, wallet)?;
        assert_eq!(
            requirements.nominal_wallet_source_transfer_operands_lamports,
            if sell { 0 } else { 10_000_000 + 50_000_001 }
        );
        assert_eq!(
            requirements.unavailable_budget,
            UnavailableNativeBudget::default()
        );
        assert_eq!(
            requirements.coverage,
            FundingCoverage::PartialWithStateOrUnsupported
        );
        assert_eq!(requirements.encoded_priority_fee.total, 14_000);
        assert_eq!(
            *f.calls.lock().unwrap(),
            calls,
            "offline reader makes no additional RPC"
        );
        assert_eq!(f.signatures(), 0);
        assert_eq!(f.sends(), 0);
    }
    Ok(())
}
