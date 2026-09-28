//! Saved HTTP corpus is read locally; no provider or source fixture substitution.
use super::{rows, QuoteBinding};
use serde_json::Value;

fn saved() -> anyhow::Result<(Value, QuoteBinding)> {
    let root = std::env::var("COPYBOT_FRACTIONAL_CORPUS")?;
    let record: Value = serde_json::from_slice(&std::fs::read(
        std::path::Path::new(&root).join("response-725.json"),
    )?)?;
    let block = record["result"].clone();
    let binding = QuoteBinding {
        version: 1,
        intent_id: "modeled-follower".into(),
        policy: "test".into(),
        position_id: "modeled-position".into(),
        position_opened_ts: "test".into(),
        source_signature: block["transactions"][775]["transaction"]["signatures"][0]
            .as_str()
            .unwrap()
            .into(),
        source_wallet: "7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF".into(),
        mint: "DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX".into(),
        output_mint: "So11111111111111111111111111111111111111112".into(),
        side: "sell".into(),
        provider: "offline".into(),
        endpoint: "offline".into(),
        raw: 163956354,
        decimals: 9,
        fractional: None,
        snapshot_version: "modeled".into(),
    };
    Ok((block, binding))
}

#[test]
#[ignore = "requires local saved source-selection HTTP corpus"]
fn saved_raydium_mixed_prefix_inventory() -> anyhow::Result<()> {
    use super::{verify, Evidence, Page, TOKEN, TOKEN22};
    use serde_json::json;
    let (block, binding) = saved()?;
    let txs = block["transactions"].as_array().unwrap();
    assert_eq!(txs.len(), 794);
    let config_count = txs[..775]
        .iter()
        .filter(|t| {
            t["transaction"]["message"]
                .get("transactionConfig")
                .is_some()
        })
        .count();
    assert_eq!(config_count, 133);
    let target = &txs[775];
    let (keys, pre, _) = rows::balances(target, &binding)?;
    let source = rows::source(target, &keys, &binding, 264731434)?;
    assert_eq!(pre[&source], 6770149697);
    let mut accounts = std::collections::BTreeMap::from([(source.clone(), 6770149697)]);
    for transaction in &txs[..775] {
        rows::advance(transaction, &binding, &mut accounts)?;
    }
    assert_eq!(accounts[&source], 6770149697);
    assert_eq!(accounts.len(), 1);
    let parent = block["parentSlot"].as_u64().unwrap();
    // Only the block/target are saved RPC facts. The complete parent-program pages
    // and follower custody below are explicit model operands, not historical RPC proof.
    let page = |program: &str, values: Vec<Value>| Page {
        program: program.into(),
        cursor: None,
        response: json!({"context":{"slot":parent},"value":values,"pageKey":null}),
    };
    let token_account = |address: &str, owner: &str, amount: u64| {
        json!({
        "pubkey":address,"account":{"owner":TOKEN,"data":{"parsed":{
            "type":"account","info":{"owner":owner,"mint":binding.mint,
                "tokenAmount":{"amount":amount.to_string(),"decimals":9}}}}}})
    };
    let follower = "11111111111111111111111111111111";
    let evidence = Evidence {
        version: 1,
        slot: 451313058,
        parent: json!({"blockhash":block["previousBlockhash"]}),
        block,
        pages: vec![
            page(TOKEN22, vec![]),
            page(
                TOKEN,
                vec![token_account(&source, &binding.source_wallet, 6770149697)],
            ),
        ],
        execution_accounts: json!({"context":{"slot":451313058},"value":[
            token_account("So11111111111111111111111111111111111111112", follower, binding.raw)]}),
    };
    let proof = verify(
        &evidence,
        &binding,
        &binding.source_signature,
        451313058,
        264731434,
        follower,
    )?;
    assert_eq!(proof.target_index, 775);
    assert_eq!(proof.numerator, 264731434);
    assert_eq!(proof.denominator, "6770149697");
    let selected = super::allocate(&[binding.raw], proof.numerator, proof.denominator.parse()?)?[0];
    assert_eq!(selected, 6411143);
    assert!(selected < binding.raw);
    Ok(())
}

#[test]
fn real_direct_raydium_source_rejects_wrong_owner_program_and_amount() -> anyhow::Result<()> {
    let template: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/run15-source-sell-amm-v4.json"
    ))?;
    let binding = QuoteBinding {
        version: 1,
        intent_id: "test".into(),
        policy: "test".into(),
        position_id: "test".into(),
        position_opened_ts: "test".into(),
        source_signature: template["transaction"]["signatures"][0]
            .as_str()
            .unwrap()
            .into(),
        source_wallet: "7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF".into(),
        mint: "DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX".into(),
        output_mint: "So11111111111111111111111111111111111111112".into(),
        side: "sell".into(),
        provider: "offline".into(),
        endpoint: "offline".into(),
        raw: 163956354,
        decimals: 9,
        fractional: None,
        snapshot_version: "test".into(),
    };
    let (keys, _, _) = rows::balances(&template, &binding)?;
    assert_eq!(
        rows::source(&template, &keys, &binding, 264731434)?,
        "eNQee2A5Bsx56JJMs3LBezwkBNeqvt8P98fesn43yLD"
    );
    for fault in [
        "owner",
        "token_program",
        "cpi_authority",
        "cpi_amount",
        "extra_cpi",
        "pool_owner",
        "target_mint",
        "wrong_n",
        "version",
        "config",
    ] {
        let mut t = template.clone();
        let mut n = 264731434;
        match fault {
            "owner" => {
                t["transaction"]["message"]["instructions"][2]["accounts"][17] =
                    serde_json::json!(14)
            }
            "token_program" => {
                t["transaction"]["message"]["accountKeys"][19] = serde_json::json!(super::TOKEN22)
            }
            "cpi_authority" => {
                t["meta"]["innerInstructions"][0]["instructions"][0]["accounts"][2] =
                    serde_json::json!(14)
            }
            "cpi_amount" => {
                t["meta"]["innerInstructions"][0]["instructions"][0]["data"] =
                    serde_json::json!("3ZCbePdvQwJf")
            }
            "extra_cpi" => {
                let extra = t["meta"]["innerInstructions"][0]["instructions"][0].clone();
                t["meta"]["innerInstructions"][0]["instructions"]
                    .as_array_mut()
                    .unwrap()
                    .push(extra);
            }
            "pool_owner" => {
                t["meta"]["preTokenBalances"][2]["owner"] = serde_json::json!(binding.source_wallet)
            }
            "target_mint" => {
                t["meta"]["postTokenBalances"][3]["mint"] = serde_json::json!(binding.output_mint)
            }
            "wrong_n" => n += 1,
            "version" => t["version"] = serde_json::json!(1),
            "config" => t["transaction"]["message"]["transactionConfig"] = serde_json::Value::Null,
            _ => unreachable!(),
        }
        let result = rows::balances(&t, &binding)
            .and_then(|(keys, _, _)| rows::source(&t, &keys, &binding, n));
        assert!(result.is_err(), "{fault}");
    }
    Ok(())
}
