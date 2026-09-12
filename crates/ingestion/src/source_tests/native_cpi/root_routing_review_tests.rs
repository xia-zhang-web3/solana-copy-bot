use super::harness::{capture, fixture, known};
use anyhow::Result;
use serde_json::json;

#[test]
fn root_malformed_seed_account_lists_must_not_prove_ata_exemption() -> Result<()> {
    let healthy = fixture("temporary", "buy");
    let mut mixed = healthy.clone();
    let r = mixed["roles"].clone();
    mixed["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().insert(0, json!({
        "programId":r["associated_token_program"],
        "accounts":[r["user"],r["user_base"],r["user"],r["base_mint"],r["system_program"],r["token_program"]],
        "data":"2"
    }));
    mixed["result"]["meta"]["innerInstructions"][0]["index"] = json!(5);
    let mut damaged = mixed.clone();
    // The same contradictory seed create/init remain in the wire. Missing account
    // lists cannot prove that they ceased to touch the parent temporary account.
    damaged["result"]["transaction"]["message"]["instructions"][1]["accounts"] = json!([]);
    damaged["result"]["transaction"]["message"]["instructions"][2]["accounts"] = json!([]);
    let mut failures = Vec::new();
    for provider in ["rpc_backfill", "helius_fetch", "yellowstone"] {
        let good = capture("root-ata-known", &healthy, provider)?;
        let denied = capture("root-ata-mixed-control", &mixed, provider)?;
        let result = capture("root-ata-damaged-seed-lists", &damaged, provider)?;
        let restored = capture("root-ata-restored-mixed", &mixed, provider)?;
        known(&good, true);
        assert!(denied.is_null(), "intact mixed lifecycle must be Unknown");
        assert_eq!(restored, denied);
        if !result.is_null() {
            failures.push(json!({"provider":provider,"event":result}));
        }
    }
    println!("ROOT_B55_ATA_DAMAGED {}", json!(failures));
    assert!(
        failures.is_empty(),
        "damaged seed metadata restored native fallback: {failures:?}"
    );
    Ok(())
}

#[test]
fn root_parsed_missing_seed_identity_must_not_prove_ata_exemption() -> Result<()> {
    let healthy = super::parsed::converted(&fixture("temporary", "buy"));
    let mut mixed = healthy.clone();
    let r = mixed["roles"].clone();
    mixed["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().insert(0, json!({
        "programId":r["associated_token_program"],
        "accounts":[r["user"],r["user_base"],r["user"],r["base_mint"],r["system_program"],r["token_program"]],"data":"2"}));
    mixed["result"]["meta"]["innerInstructions"][0]["index"] = json!(5);
    let mut damaged = mixed.clone();
    damaged["result"]["transaction"]["message"]["instructions"][1]["parsed"]["info"]
        .as_object_mut()
        .unwrap()
        .remove("newAccount");
    damaged["result"]["transaction"]["message"]["instructions"][2]["parsed"]["info"]
        .as_object_mut()
        .unwrap()
        .remove("account");
    let mut failures = Vec::new();
    for provider in ["rpc_backfill", "helius_fetch"] {
        let good = capture("root-ata-parsed-known", &healthy, provider)?;
        let denied = capture("root-ata-parsed-mixed", &mixed, provider)?;
        let result = capture("root-ata-parsed-missing-identity", &damaged, provider)?;
        let restored = capture("root-ata-parsed-restored", &mixed, provider)?;
        known(&good, true);
        assert!(denied.is_null());
        assert_eq!(restored, denied);
        if !result.is_null() {
            failures.push(json!({"provider":provider,"event":result}));
        }
    }
    println!("ROOT_B55_PARSED_DAMAGED {}", json!(failures));
    assert!(
        failures.is_empty(),
        "missing parsed account identity restored fallback: {failures:?}"
    );
    Ok(())
}
