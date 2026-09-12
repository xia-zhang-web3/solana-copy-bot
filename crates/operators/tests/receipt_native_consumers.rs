#[path = "../../storage-core/tests/common/receipt_facts_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::Db;
use serde_json::{json, Value};
use std::process::Command;
#[test]
fn primary_and_economics_report_native_buy_sell_without_changing_cash_meaning() -> Result<()> {
    for side in ["buy", "sell"] {
        let db = Db::new(Some(42))?;
        db.store.execution_canary_quote_pnl_summary(
            db.now,
            db.now - chrono::Duration::hours(1),
            10,
        )?;
        if side == "sell" {
            db.conn()?.execute_batch("UPDATE copy_signals SET side='sell'; UPDATE execution_canary_receipt_proofs SET side='sell';")?;
        }
        let mut facts = db.facts();
        facts.side = side.into();
        let mut native = NativeAccountObservations::empty(&facts);
        let v = |s: &str| json!({"value":s,"coverage":"known","source":"rpc_native_balance"});
        let end = json!({"mint":{"value":"mint","coverage":"known","source":"rpc_token_balance"},"token_owner":{"value":"wallet","coverage":"known","source":"rpc_token_balance"},"token_program":{"value":null,"coverage":"missing","source":"unavailable"},"decimals":{"value":"3","coverage":"known","source":"rpc_token_balance"},"raw":{"value":"0","coverage":"known","source":"rpc_token_balance"}});
        native.accounts.push(serde_json::from_value(json!({"account_index":1,"pubkey":"account","native_pre":v("0"),"native_post":v("9007199254740993"),"native_delta":v("9007199254740993"),"pre_token":end,"post_token":end,"relevance":["target_mint"]}))?);
        let mut auxiliary = native.accounts[0].clone();
        auxiliary.account_index = 2;
        auxiliary.pubkey = "funded-aux-token-account".into();
        auxiliary.relevance = vec!["wallet_instruction_link".into()];
        auxiliary.native_pre = serde_json::from_value(v("2039280"))?;
        auxiliary.native_post = serde_json::from_value(v("2039287"))?;
        auxiliary.native_delta = serde_json::from_value(v("7"))?;
        for end in [&mut auxiliary.pre_token, &mut auxiliary.post_token] {
            let known = |s: &str| NativeObservation::known(s, ObservationSource::RpcTokenBalance);
            end.mint = known("AuxiliaryMint");
            end.token_owner = known("ForeignTokenOwner");
            end.token_program = known("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
            end.decimals = known("6");
            end.raw = known("123");
        }
        native.accounts.push(auxiliary);
        native.accounts_coverage = ObservationCoverage::Missing;
        db.store.record_receipt_observation_bundle(
            &ReceiptObservationBundle { facts, native },
            db.now,
        )?;
        for (bin, pointer) in [
            (
                env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
                "/tiny_execution_proof/native_observations",
            ),
            (
                env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
                "/tiny/native_observations",
            ),
        ] {
            let out = Command::new(bin)
                .args([
                    "--db-path",
                    db.path.to_str().unwrap(),
                    "--json",
                    "--limit",
                    "1",
                ])
                .output()?;
            assert!(
                out.status.success(),
                "stdout={} stderr={}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            let data: Value = serde_json::from_slice(&out.stdout)?;
            let r = data.pointer(pointer).unwrap();
            assert_eq!(r["total_orders"], "1");
            assert_eq!(r["partial_orders"], "1");
            assert_eq!(r["account_rows"], "2");
            assert_eq!(r["coverage"], "partial_unresolved");
            assert_eq!(r["decomposition"], "unresolved");
            assert_eq!(r["rows"][0]["side"], side);
            assert_eq!(
                r["rows"][0]["observations"]["accounts"][0]["native_post"]["value"],
                "9007199254740993"
            );
            let a = &r["rows"][0]["observations"]["accounts"][1];
            assert_eq!(a["pubkey"], "funded-aux-token-account");
            assert_eq!(a["native_pre"]["value"], "2039280");
            assert_eq!(a["native_post"]["value"], "2039287");
            assert_eq!(a["native_delta"]["value"], "7");
            assert_eq!(a["post_token"]["raw"]["value"], "123");
            assert_eq!(a["post_token"]["token_owner"]["value"], "ForeignTokenOwner");
            assert_eq!(a["post_token"]["mint"]["value"], "AuxiliaryMint");
            assert_eq!(a["relevance"], json!(["wallet_instruction_link"]));
            if pointer.starts_with("/tiny_execution") {
                assert_eq!(data["tiny_execution_quality"]["economic_green"], false);
            }
        }
    }
    Ok(())
}
