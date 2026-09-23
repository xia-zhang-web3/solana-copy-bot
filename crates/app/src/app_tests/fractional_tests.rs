use super::{fractional_fixture::Fixture, fractional_synthetic_fixture as synthetic};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::ordered_sell_quote::{
    fractional::inventory::{allocate, Evidence, CONTRACT, TOKEN},
    QuoteClaim,
};
use serde_json::json;
pub(super) fn config(f: &Fixture) -> Result<ExecutionConfig> {
    Ok(serde_json::from_value(
        json!({"enabled":false,"canary_tiny_submit_enabled":false,"canary_enabled":true,"quote_canary_enabled":true,"canary_wallet_pubkey":f.meta["our"]["signer"],"execution_signer_pubkey":f.meta["our"]["signer"],"canary_route":"jupiter_swap_instructions","swap_instructions_dry_run_enabled":true,"swap_transaction_dry_run_enabled":true,"pretrade_max_priority_fee_lamports":22000,"tiny_experiment":{"id":"fractional-test","activate":false},"owned_sell_preparation":{"policy":"rpc_finalized_cross_slot_owned_sell_v1","rpc_url":"http://127.0.0.1:1","genesis_hash":"11111111111111111111111111111111","identity":"fractional-test","fractional_inventory":CONTRACT}}),
    )?)
}
pub(super) fn evidence() -> Result<Evidence> {
    synthetic::evidence(false)
}
pub(super) async fn bind(f: &mut Fixture, claim: QuoteClaim, e: Evidence) -> Result<QuoteClaim> {
    let c = config(f)?;
    let wallet = c.canary_wallet_pubkey.clone();
    let genesis = c
        .owned_sell_preparation
        .as_ref()
        .unwrap()
        .genesis_hash
        .clone();
    crate::execution_owned_sell_rpc::fractional::bind(
        &mut f.db.store,
        &c,
        claim,
        super::association_parent_fixture::limits(),
        |request| {
            let result = match request["method"].as_str().unwrap() {
                "getGenesisHash" => json!(genesis),
                "getBlock" if request["params"][0] == e.slot => e.block.clone(),
                "getBlock" => e.parent.clone(),
                "getTokenAccountsByOwnerAtSlot" => e
                    .pages
                    .iter()
                    .find(|p| request["params"][1]["programId"] == p.program)
                    .map(|p| p.response.clone())
                    .unwrap_or(json!({"context":{"slot":0},"value":[],"pageKey":null})),
                "getTokenAccountsByOwner" => {
                    assert_eq!(request["params"][0], wallet);
                    e.execution_accounts.clone()
                }
                _ => panic!("unexpected RPC method"),
            };
            std::future::ready(Ok(
                json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
            ))
        },
    )
    .await
}
#[tokio::test]
async fn fractional_native_producer_claim_250_matches_accepted_oracle() -> Result<()> {
    let mut f = Fixture::new().await?;
    let original = f.claim()?;
    assert_eq!(original.binding.raw, 1000);
    let claim = bind(&mut f, original, evidence()?).await?;
    assert_eq!(claim.binding.raw, 250);
    assert_eq!(claim.binding.version, 2);
    let d = claim.binding.fractional.as_ref().unwrap();
    // Independent synthetic source inventory: 10,000 + 30,000, target 10,000.
    assert_eq!(d.inventory.denominator, "40000");
    assert_eq!(d.inventory.numerator, 10000);
    assert_eq!(d.owned_raw, 1000);
    f.db.store.recheck_strict_sell_quote(
        &claim,
        super::association_parent_fixture::limits(),
        Utc::now(),
    )?;
    let mut forged = claim.clone();
    forged.binding.raw = 1000;
    assert!(f
        .db
        .store
        .recheck_strict_sell_quote(
            &forged,
            super::association_parent_fixture::limits(),
            Utc::now()
        )
        .is_err());
    let mut forged = claim.clone();
    forged
        .binding
        .fractional
        .as_mut()
        .unwrap()
        .inventory
        .denominator = "10000".into();
    assert!(f
        .db
        .store
        .owned_sell_snapshot(&forged.binding, super::association_parent_fixture::limits())
        .is_err());
    Ok(())
}
#[tokio::test]
async fn fractional_full_control_and_immutable_restart() -> Result<()> {
    let mut f = Fixture::new().await?;
    let claim = f.claim()?;
    let mut e = evidence()?;
    e.pages
        .iter_mut()
        .find(|p| p.program == TOKEN)
        .unwrap()
        .response["value"][1]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"] =
        json!("0");
    let claim = bind(&mut f, claim, e).await?;
    assert_eq!(claim.binding.raw, 1000);
    assert!(claim.binding.fractional.is_some());
    let reopened = copybot_storage_core::SqliteStore::open(&f.db.path)?;
    reopened.recheck_strict_sell_quote(
        &claim,
        super::association_parent_fixture::limits(),
        Utc::now(),
    )?;
    assert!(reopened
        .begin_fractional_sell(
            &claim,
            super::association_parent_fixture::limits(),
            Utc::now(),
            &"a".repeat(64)
        )
        .is_err());
    Ok(())
}
#[test]
fn fractional_exact_per_lot_rounding_and_large_integers() -> Result<()> {
    assert_eq!(allocate(&[3, 3], 1, 2)?, vec![1, 1]);
    assert_eq!(allocate(&[1], 1, 2)?, vec![0]);
    assert_eq!(
        allocate(&[u64::MAX], u64::MAX, u128::from(u64::MAX))?,
        vec![u64::MAX]
    );
    assert_eq!(allocate(&[u64::MAX], 1, 2)?, vec![u64::MAX / 2]);
    assert!(allocate(&[1], 1, 0).is_err());
    Ok(())
}
#[tokio::test]
async fn fractional_raw_proof_refusals_no_quote_no_rearm() -> Result<()> {
    for fault in [
        "missing_program",
        "wrong_parent",
        "wrong_source",
        "missing_cpi",
        "wrong_mint",
        "wrong_wallet",
        "short_wallet",
        "prefix_unknown",
    ] {
        let mut f = Fixture::new().await?;
        let claim = f.claim()?;
        let mut e = evidence()?;
        let index = e.block["transactions"].as_array().unwrap().len() - 1;
        match fault {
            "missing_program" => {
                e.pages.remove(0);
            }
            "wrong_parent" => e.parent["blockhash"] = json!("11111111111111111111111111111111"),
            "wrong_source" => {
                e.block["transactions"][index]["transaction"]["signatures"][0] = json!("unknown")
            }
            "missing_cpi" => {
                e.block["transactions"][index]["meta"]["innerInstructions"] = json!([])
            }
            "wrong_mint" => {
                e.pages[1].response["value"][0]["account"]["data"]["parsed"]["info"]["mint"] =
                    json!("11111111111111111111111111111111")
            }
            "wrong_wallet" => {
                e.pages[1].response["value"][0]["account"]["data"]["parsed"]["info"]["owner"] =
                    json!("11111111111111111111111111111111")
            }
            "short_wallet" => {
                e.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]
                    ["tokenAmount"]["amount"] = json!("249")
            }
            "prefix_unknown" => {
                e.block["transactions"][index]["meta"]["err"] = json!({"failed":true})
            }
            _ => unreachable!(),
        }
        assert!(
            bind(&mut f, claim.clone(), e).await.is_err(),
            "fault={fault}"
        );
        assert!(f
            .db
            .store
            .begin_fractional_sell(
                &claim,
                super::association_parent_fixture::limits(),
                Utc::now(),
                &"a".repeat(64)
            )
            .is_err());
        let count: i64 =
            f.db.sql
                .query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| {
                    r.get(0)
                })?;
        assert_eq!(count, 0);
        let row: Option<String> =
            f.db.sql
                .query_row("SELECT record FROM ordered_sell_quote_results", [], |r| {
                    r.get(0)
                })?;
        ensure!(row.is_none(), "quote was attempted");
    }
    Ok(())
}
