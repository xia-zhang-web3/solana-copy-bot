use super::receipt_cash_facts_fixture::{facts, money_snapshot, transaction_calls};
use super::receipt_lifecycle_fixture::{program_receipt, SPL_TOKEN, TOKEN_2022};
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::{
    NativeAccountObservations, ObservationCoverage as Cov, ObservationSource as Src,
};
use serde_json::{json, Value};
const SYSTEM: &str = "11111111111111111111111111111111";
const AUX: &str = "aux-token-account";
const MINT: &str = "AuxiliaryMint";
const OWNER: &str = "ForeignTokenOwner";
fn saved(f: &Fixture) -> Result<NativeAccountObservations> {
    Ok(f.store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap())
}
fn funding(side: &str, program: &str) -> Value {
    let mut v = program_receipt(side, SPL_TOKEN);
    let index = v["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .len();
    v["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .extend([
            json!({"pubkey":AUX,"signer":false,"writable":true}),
            json!({"pubkey":SYSTEM,"signer":false,"writable":false}),
            json!({"pubkey":"derived-source","signer":false,"writable":true}),
        ]);
    for (name, balance) in [("preBalances", 2039280), ("postBalances", 2039287)] {
        v["result"]["meta"][name].as_array_mut().unwrap().extend([
            json!(balance),
            json!(1),
            json!(500),
        ]);
    }
    let wallet_post = v["result"]["meta"]["postBalances"][0].as_u64().unwrap();
    v["result"]["meta"]["postBalances"][0] = json!(wallet_post - 7);
    for name in ["preTokenBalances", "postTokenBalances"] {
        v["result"]["meta"][name].as_array_mut().unwrap().push(json!({"accountIndex":index,"mint":MINT,"owner":OWNER,"programId":program,"uiTokenAmount":{"amount":"123","decimals":6}}));
    }
    v["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().push(json!({"programId":SYSTEM,"parsed":{"type":"transfer","info":{"source":WALLET,"destination":AUX,"lamports":7}}}));
    v
}
#[tokio::test]
async fn receipt_system_funding_buy_sell_foreign_accounts_keep_exact_cash_and_completed_replay(
) -> Result<()> {
    for side in ["buy", "sell"] {
        for program in [SPL_TOKEN, TOKEN_2022] {
            for mode in ["transfer", "seed_base", "wallet_destination"] {
                let mut f = Fixture::new(side)?;
                let mut v = funding(side, program);
                let index = v["result"]["meta"]["preTokenBalances"][2]["accountIndex"]
                    .as_u64()
                    .unwrap() as usize;
                let adjustment = match mode {
                    "seed_base" => {
                        v["result"]["transaction"]["message"]["instructions"][1]["parsed"] = json!({"type":"transferWithSeed","info":{"source":"derived-source","sourceBase":WALLET,"destination":AUX,"lamports":7,"sourceOwner":SYSTEM,"sourceSeed":"seed"}});
                        v["result"]["meta"]["postBalances"][index + 2] = json!(493);
                        0
                    }
                    "wallet_destination" => {
                        v["result"]["transaction"]["message"]["instructions"][1]["parsed"]
                            ["info"] = json!({"source":AUX,"destination":WALLET,"lamports":7});
                        // Only the final row proves token type: the source may
                        // become a token account later; no CPI ordering is inferred.
                        v["result"]["meta"]["preTokenBalances"]
                            .as_array_mut()
                            .unwrap()
                            .pop();
                        v["result"]["meta"]["preBalances"][index] = json!(2039287);
                        v["result"]["meta"]["postBalances"][index] = json!(2039280);
                        v["result"]["meta"]["innerInstructions"] = json!(null);
                        7
                    }
                    _ => -7,
                };
                let cash = if side == "buy" {
                    -900_000_000_i64
                } else {
                    1_200_000_000
                } + adjustment;
                v["result"]["meta"]["postBalances"][0] = json!(2_000_000_000_i64 + cash);
                let rpc = Rpc::new(v).await?;
                rpc.context(format!("receipt_system_funding_buy_sell_foreign_accounts_keep_exact_cash_and_completed_replay side={side:?} program={program:?} mode={mode:?}"));
                assert_eq!(
                    f.reconcile(&rpc, 1).await?.confirmation_confirmed,
                    1,
                    "{side}/{mode}"
                );
                f.reopen()?;
                let o = saved(&f)?;
                let a = o.accounts.iter().find(|a| a.pubkey == AUX).unwrap();
                assert_eq!(a.account_index, index as u32);
                assert_eq!(
                    a.native_delta.value.as_deref(),
                    Some(if mode == "wallet_destination" {
                        "-7"
                    } else {
                        "7"
                    })
                );
                assert_eq!(a.native_delta.source, Src::RpcNativeBalance);
                assert_eq!(a.post_token.raw.value.as_deref(), Some("123"));
                assert_eq!(a.post_token.raw.source, Src::RpcTokenBalance);
                assert_eq!(a.post_token.mint.value.as_deref(), Some(MINT));
                assert_eq!(a.post_token.token_owner.value.as_deref(), Some(OWNER));
                assert_eq!(a.post_token.token_program.value.as_deref(), Some(program));
                assert_eq!(a.post_token.decimals.value.as_deref(), Some("6"));
                assert_eq!(a.relevance, vec!["wallet_instruction_link"]);
                if mode == "wallet_destination" {
                    assert_eq!(a.pre_token.raw.coverage, Cov::Missing);
                    assert_ne!(o.accounts_coverage, Cov::Known);
                } else {
                    assert_eq!(a.pre_token.raw.value.as_deref(), Some("123"));
                    if mode == "seed_base" {
                        // The derived SOL source has no token identity evidence.
                        assert_eq!(o.accounts_coverage, Cov::Missing);
                    } else {
                        assert_eq!(o.accounts_coverage, Cov::Known);
                    }
                }
                assert_eq!(facts(&f)?.wallet_native_delta.as_i128(), i128::from(cash));
                assert!(f.store.load_execution_canary_open_position(MINT)?.is_none());
                assert_eq!(
                    f.conn()?
                        .query_row("SELECT count(*) FROM positions", [], |r| r.get::<_, u64>(0))?,
                    1
                );
                if side == "sell" {
                    let settled = f
                        .store
                        .load_execution_canary_cash_settlement(&f.order_id)?
                        .unwrap();
                    assert_eq!(settled.wallet_native_cash_delta.as_i128(), i128::from(cash));
                    assert_eq!(
                        settled.cash_result_delta.as_i128(),
                        i128::from(cash) - 560_000_000
                    );
                } else {
                    assert_eq!(
                        f.conn()?
                            .query_row("SELECT cost_lamports FROM positions", [], |r| r
                                .get::<_, i64>(0))?,
                        -cash
                    );
                }
                let money = money_snapshot(&f)?;
                assert_eq!(transaction_calls(&rpc), 1);
                rpc.set(json!({"result":null}));
                f.reopen()?;
                assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_confirmed, 1);
                assert_eq!(transaction_calls(&rpc), 1);
                assert_eq!(saved(&f)?, o);
                assert_eq!(money_snapshot(&f)?, money);
                assert_eq!(f.fills()?, 1);
                rpc.finish().await?;
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn receipt_system_funding_selection_requires_token_evidence_and_wallet_role() -> Result<()> {
    for mode in [
        "plain_sol",
        "wrong_program",
        "owner_only",
        "unlinked",
        "unknown_key",
        "foreign_close",
        "ata",
    ] {
        let mut f = Fixture::new("sell")?;
        let mut v = funding("sell", SPL_TOKEN);
        match mode {
            "plain_sol" => {
                for name in ["preTokenBalances", "postTokenBalances"] {
                    v["result"]["meta"][name].as_array_mut().unwrap().pop();
                }
            }
            "wrong_program" => {
                v["result"]["transaction"]["message"]["instructions"][1]["programId"] =
                    json!("PretendSystem")
            }
            "owner_only" => {
                v["result"]["transaction"]["message"]["instructions"][1]["parsed"] = json!({"type":"transferWithSeed","info":{"source":"derived-source","destination":AUX,"sourceBase":"ForeignBase","sourceOwner":WALLET,"lamports":7}})
            }
            "unlinked" => {
                v["result"]["transaction"]["message"]["instructions"][1]["parsed"]["info"]
                    ["source"] = json!("derived-source")
            }
            "unknown_key" => {
                v["result"]["transaction"]["message"]["instructions"][1]["parsed"]["info"]
                    ["destination"] = json!("missing-key")
            }
            "foreign_close" => {
                v["result"]["transaction"]["message"]["instructions"][1] = json!({"programId":SPL_TOKEN,"parsed":{"type":"closeAccount","info":{"account":"token-account","destination":AUX,"owner":WALLET}}})
            }
            _ => {
                v["result"]["transaction"]["message"]["instructions"][1] = json!({"programId":"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL","parsed":{"type":"createIdempotent","info":{"source":WALLET,"account":AUX,"wallet":OWNER,"mint":MINT,"tokenProgram":SPL_TOKEN}}})
            }
        }
        let rpc = Rpc::new(v).await?;
        rpc.context(format!("receipt_system_funding_selection_requires_token_evidence_and_wallet_role mode={mode:?}"));
        f.reconcile(&rpc, 1).await?;
        f.reopen()?;
        let o = saved(&f)?;
        assert_eq!(
            o.accounts.iter().any(|a| a.pubkey == AUX),
            mode == "ata",
            "{mode}"
        );
        assert!(o.accounts.iter().any(|a| a.pubkey == "token-account"));
        if matches!(mode, "plain_sol" | "unknown_key") {
            assert_ne!(o.accounts_coverage, Cov::Known);
        }
        if mode == "plain_sol" {
            assert!(o
                .reasons
                .iter()
                .any(|r| r == "funding_token_identity_unproven"));
        }
        assert!(f.store.load_execution_canary_open_position(MINT)?.is_none());
        rpc.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn receipt_system_funding_partial_identity_never_guesses_and_keeps_valid_neighbors(
) -> Result<()> {
    for mode in [
        "missing_owner",
        "invalid_mint",
        "unsupported_program",
        "duplicate",
        "missing_array",
        "invalid_array",
        "one_bad_endpoint",
        "invalid_raw",
    ] {
        let mut f = Fixture::new("sell")?;
        let mut v = funding("sell", SPL_TOKEN);
        for name in ["preTokenBalances", "postTokenBalances"] {
            match mode {
                "missing_owner" => v["result"]["meta"][name][2]["owner"] = json!(null),
                "invalid_mint" => v["result"]["meta"][name][2]["mint"] = json!({}),
                "unsupported_program" => {
                    v["result"]["meta"][name][2]["programId"] = json!("UnknownTokenProgram")
                }
                "duplicate" => {
                    let row = v["result"]["meta"][name][2].clone();
                    v["result"]["meta"][name].as_array_mut().unwrap().push(row);
                }
                "invalid_raw" => {
                    v["result"]["meta"][name][2]["uiTokenAmount"]["amount"] = json!(123)
                }
                _ => (),
            }
        }
        match mode {
            "missing_array" => v["result"]["meta"]["preTokenBalances"] = json!(null),
            "invalid_array" => v["result"]["meta"]["preTokenBalances"] = json!({}),
            "one_bad_endpoint" => v["result"]["meta"]["preTokenBalances"][2]["owner"] = json!(null),
            _ => (),
        }
        let rpc = Rpc::new(v).await?;
        rpc.context(format!("receipt_system_funding_partial_identity_never_guesses_and_keeps_valid_neighbors mode={mode:?}"));
        f.reconcile(&rpc, 1).await?;
        f.reopen()?;
        let o = saved(&f)?;
        assert_ne!(o.accounts_coverage, Cov::Known, "{mode}");
        let target = o
            .accounts
            .iter()
            .find(|a| a.pubkey == "token-account")
            .unwrap();
        assert_eq!(target.post_token.raw.value.as_deref(), Some("3000"));
        let aux = o.accounts.iter().find(|a| a.pubkey == AUX);
        if matches!(
            mode,
            "missing_array" | "invalid_array" | "one_bad_endpoint" | "invalid_raw"
        ) {
            let a = aux.unwrap();
            assert_eq!(a.native_delta.value.as_deref(), Some("7"));
            if mode == "invalid_raw" {
                assert_eq!(a.post_token.raw.coverage, Cov::Invalid);
            } else {
                assert_ne!(a.pre_token.token_owner.coverage, Cov::Known);
            }
            assert_ne!(a.pre_token.raw.source, Src::ProvenLifecycle);
        } else {
            assert!(aux.is_none(), "{mode}");
        }
        rpc.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn receipt_system_funding_pending_enrichment_preserves_known_and_sticky_conflict(
) -> Result<()> {
    let mut f = Fixture::new("sell")?;
    f.conn()?
        .execute("UPDATE positions SET pnl_lamports=NULL", [])?;
    let full = funding("sell", SPL_TOKEN);
    let mut partial = full.clone();
    for name in ["preTokenBalances", "postTokenBalances"] {
        partial["result"]["meta"][name][2]["programId"] = json!(null);
    }
    let rpc = Rpc::new(partial.clone()).await?;
    rpc.context(format!(
        "receipt_system_funding_pending_enrichment_preserves_known_and_sticky_conflict"
    ));
    let before = money_snapshot(&f)?;
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_pending, 1);
    assert!(!saved(&f)?.accounts.iter().any(|a| a.pubkey == AUX));
    assert_ne!(saved(&f)?.accounts_coverage, Cov::Known);
    rpc.set(full.clone());
    f.reopen()?;
    assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_pending, 1);
    let known = saved(&f)?;
    assert!(known.accounts.iter().any(|a| a.pubkey == AUX));
    assert_eq!(known.accounts_coverage, Cov::Known);
    rpc.set(partial);
    f.reopen()?;
    f.reconcile(&rpc, 3).await?;
    assert_eq!(saved(&f)?, known);
    let mut conflict = full.clone();
    conflict["result"]["meta"]["postTokenBalances"][2]["uiTokenAmount"]["amount"] = json!("124");
    rpc.set(conflict);
    f.reconcile(&rpc, 4).await?;
    f.reopen()?;
    rpc.set(full);
    f.reconcile(&rpc, 5).await?;
    assert_eq!(
        f.store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap()
            .reason,
        "native_observation_conflict"
    );
    assert_eq!(saved(&f)?, known);
    assert_eq!(money_snapshot(&f)?, before);
    assert_eq!(f.fills()?, 0);
    assert!(f.store.execution_canary_accounting_pending()?);
    rpc.finish().await?;
    Ok(())
}
