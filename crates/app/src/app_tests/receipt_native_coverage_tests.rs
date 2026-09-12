use super::receipt_lifecycle_fixture::*;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::ObservationCoverage as Cov;
use serde_json::json;
#[tokio::test]
async fn receipt_native_partial_rows_instruction_identity_domain_and_bounds() -> Result<()> {
    for case in [
        "zero",
        "u64max",
        "bad_index",
        "duplicate",
        "raw_number",
        "overflow",
        "missing_inner",
        "invalid_inner",
        "duplicate_inner",
        "raw_instruction",
        "wrong_program",
        "many_instructions",
        "many_accounts",
        "authority",
    ] {
        let mut f = Fixture::new("sell")?;
        let mut v = program_receipt("sell", SPL_TOKEN);
        match case {
            "zero" => {
                v["result"]["meta"]["preBalances"][1] = json!(0);
                v["result"]["meta"]["postBalances"][1] = json!(0);
            }
            "u64max" => {
                v["result"]["meta"]["preBalances"][1] = json!(0);
                v["result"]["meta"]["postBalances"][1] = json!(u64::MAX);
            }
            "bad_index" => {
                v["result"]["meta"]["preTokenBalances"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"accountIndex":900,"mint":TOKEN}));
            }
            "duplicate" => {
                let row = v["result"]["meta"]["preTokenBalances"][1].clone();
                v["result"]["meta"]["preTokenBalances"]
                    .as_array_mut()
                    .unwrap()
                    .push(row);
            }
            "raw_number" => {
                v["result"]["meta"]["preTokenBalances"][1]["uiTokenAmount"]["amount"] = json!(5)
            }
            "overflow" => {
                v["result"]["meta"]["preTokenBalances"][1]["uiTokenAmount"]["amount"] =
                    json!("18446744073709551616")
            }
            "missing_inner" => v["result"]["meta"]["innerInstructions"] = json!(null),
            "invalid_inner" => v["result"]["meta"]["innerInstructions"] = json!({}),
            "duplicate_inner" => {
                v["result"]["meta"]["innerInstructions"] =
                    json!([{"index":0,"instructions":[]},{"index":0,"instructions":[]}])
            }
            "raw_instruction" => {
                v["result"]["transaction"]["message"]["instructions"] =
                    json!([{"programId":SPL_TOKEN,"data":"1"}])
            }
            "wrong_program" => {
                v["result"]["transaction"]["message"]["instructions"][0]["programId"] =
                    json!("PretendTokenProgram")
            }
            "many_instructions" => {
                let ix = v["result"]["transaction"]["message"]["instructions"][0].clone();
                v["result"]["transaction"]["message"]["instructions"] = json!(vec![ix; 65]);
            }
            "many_accounts" => {
                for n in 0..70 {
                    let i = v["result"]["transaction"]["message"]["accountKeys"]
                        .as_array()
                        .unwrap()
                        .len();
                    v["result"]["transaction"]["message"]["accountKeys"]
                        .as_array_mut()
                        .unwrap()
                        .push(
                            json!({"pubkey":format!("account-{n}"),"signer":false,"writable":true}),
                        );
                    for name in ["preBalances", "postBalances"] {
                        v["result"]["meta"][name]
                            .as_array_mut()
                            .unwrap()
                            .push(json!(0));
                    }
                    for name in ["preTokenBalances", "postTokenBalances"] {
                        v["result"]["meta"][name].as_array_mut().unwrap().push(json!({"accountIndex":i,"owner":"foreign","mint":TOKEN,"programId":SPL_TOKEN,"uiTokenAmount":{"amount":"0","decimals":3}}));
                    }
                }
            }
            _ => {
                v["result"]["transaction"]["message"]["instructions"] = json!([{"programId":SPL_TOKEN,"parsed":{"type":"setAuthority","info":{"account":"token-account","authority":WALLET,"authorityType":"closeAccount","newAuthority":null}}}])
            }
        }
        let rpc = Rpc::new(v).await?;
        rpc.context(format!(
            "receipt_native_partial_rows_instruction_identity_domain_and_bounds case={case:?}"
        ));
        f.reconcile(&rpc, 1).await?;
        f.reopen()?;
        let o = f
            .store
            .load_receipt_native_observations(&f.order_id)?
            .expect(case);
        let a = &o.accounts[0];
        assert_eq!(a.pubkey, "token-account", "{case}");
        assert_eq!(
            a.pre_token.raw.value.as_deref(),
            Some("10000"),
            "valid neighbor survives {case}"
        );
        match case {
            "zero" => assert_eq!(a.native_delta.value.as_deref(), Some("0")),
            "u64max" => assert_eq!(
                a.native_delta.value.as_deref(),
                Some("18446744073709551615")
            ),
            "bad_index" | "duplicate" | "raw_number" | "overflow" => {
                assert_eq!(f.fills()?, 0);
                assert_ne!(o.accounts_coverage, Cov::Known);
            }
            "missing_inner" => assert_eq!(o.instructions_coverage, Cov::Missing),
            "invalid_inner" | "duplicate_inner" => {
                assert_eq!(o.instructions_coverage, Cov::Invalid)
            }
            "raw_instruction" | "wrong_program" => {
                assert_eq!(o.instructions[0].coverage, Cov::Unsupported);
                assert!(o.instructions[0].fields.is_empty());
            }
            "many_instructions" => {
                assert_eq!(o.instructions.len(), 64);
                assert_eq!(o.instructions_coverage, Cov::Truncated);
            }
            "many_accounts" => {
                assert_eq!(o.accounts.len(), 64);
                assert_eq!(o.accounts_coverage, Cov::Truncated);
            }
            _ => assert_eq!(
                o.instructions[0].fields["newAuthority"].value.as_deref(),
                Some("null")
            ),
        }
        rpc.finish().await?;
    }
    Ok(())
}
