//! Historical31 layout only. Mutations are mocks; neither proves a live quote.
use anyhow::Result;
use copybot_storage_core::ordered_sell_quote::fractional::inventory::prove_jupiter_source_sell as prove;
use serde_json::{json, Value};
const WALLET: &str = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
const MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
fn raw() -> Value {
    serde_json::from_str(include_str!("fixtures/canary31-source-sell-raw.json")).unwrap()
}
fn accepts(t: &Value) -> bool {
    prove(t, WALLET, MINT, 6, 1_091_636).is_ok()
}
#[test]
fn historical31_raw_and_parsed_sell_have_exact_cpi_amounts() -> Result<()> {
    for t in [
        raw(),
        serde_json::from_str(include_str!("fixtures/canary31-source-sell-parsed.json"))?,
    ] {
        let p = prove(&t, WALLET, MINT, 6, 1_091_636)?;
        assert_eq!(p.sol_raw, 9_948_805);
        assert_eq!(p.target_raw, 1_091_636);
        assert_eq!(p.route_index, 3);
        assert_eq!(p.setup_index, 2);
    }
    Ok(())
}
#[test]
fn changed_route_cpi_vault_or_partial_amount_never_proves_sell() {
    let original = raw();
    assert!(accepts(&original));
    let mut t = original.clone();
    t["transaction"]["message"]["instructions"][3]["accounts"][2] = json!(5);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["innerInstructions"][1]["instructions"][0]["accounts"][5] = json!(5);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["postTokenBalances"][2]["uiTokenAmount"]["amount"] = json!("12850721458773");
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("1");
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["preTokenBalances"][1]["owner"] = json!(WALLET);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["innerInstructions"][1]["instructions"]
        .as_array_mut()
        .unwrap()
        .remove(2);
    assert!(!accepts(&t));
    assert!(prove(&original, WALLET, MINT, 6, 1_091_635).is_err());
}
#[test]
fn missing_lifecycle_foreign_source_or_extra_native_effect_refuses() {
    let original = raw();
    let mut t = original.clone();
    t["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .remove(4);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["transaction"]["message"]["instructions"][4]["accounts"][1] = json!(2);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["innerInstructions"][0]["instructions"]
        .as_array_mut()
        .unwrap()
        .remove(3);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["preBalances"][1] = json!(1488440);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["postBalances"][0] = json!(t["meta"]["postBalances"][0].as_u64().unwrap() + 1);
    assert!(!accepts(&t));
    let mut t = original.clone();
    t["meta"]["err"] = json!("unknown");
    assert!(!accepts(&t));
    assert!(prove(
        &original,
        "7wDVE9mGs3rmvDqbEUAZRDDwGD3uR9WxVzC23ZZgA4hr",
        MINT,
        6,
        1_091_636
    )
    .is_err());
    let mut t = original;
    t["transaction"]["message"]["header"]["numRequiredSignatures"] = json!(2);
    assert!(!accepts(&t));
}
#[test]
fn duplicate_route_cpi_or_unknown_event_refuses() {
    let original = raw();
    let mut t = original.clone();
    let i = t["transaction"]["message"]["instructions"][3].clone();
    t["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .push(i);
    assert!(!accepts(&t));
    let mut t = original.clone();
    let i = t["meta"]["innerInstructions"][1].clone();
    t["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
        .push(i);
    assert!(!accepts(&t));
    let mut t = original;
    t["meta"]["innerInstructions"][1]["instructions"][3]["accounts"][0] = json!(0);
    assert!(!accepts(&t));
}
