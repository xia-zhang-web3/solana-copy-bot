use super::super::rpc_backfill::{raw_observation_from_transaction_result, rpc_http_urls};
use anyhow::Result;
use copybot_config::IngestionConfig;
use serde_json::{json, Value};
use std::collections::HashSet;

fn token_balance(owner: &str, mint: &str, amount: &str) -> Value {
    let raw_amount = amount
        .parse::<u64>()
        .expect("token balance fixture must be an integer ui amount")
        .saturating_mul(1_000_000)
        .to_string();
    json!({
        "owner": owner,
        "mint": mint,
        "uiTokenAmount": {
            "uiAmountString": amount,
            "amount": raw_amount,
            "decimals": 6
        }
    })
}

#[test]
fn raw_backfill_parser_recovers_sell_swap_from_get_transaction_result() -> Result<()> {
    let signer = "Leader111111111111111111111111111111111";
    let result = json!({
        "slot": 42,
        "blockTime": 1_700_000_000i64,
        "transaction": {
            "message": {
                "accountKeys": [
                    {"pubkey": signer, "signer": true}
                ],
                "instructions": [
                    {"programId": "raydium-program"}
                ]
            }
        },
        "meta": {
            "err": null,
            "preBalances": [1_000_000_000u64],
            "postBalances": [2_000_000_000u64],
            "preTokenBalances": [token_balance(signer, "TokenMintA", "100")],
            "postTokenBalances": [token_balance(signer, "TokenMintA", "0")],
            "logMessages": []
        }
    });
    let interested = HashSet::from([String::from("raydium-program")]);
    let raydium = HashSet::from([String::from("raydium-program")]);
    let raw = raw_observation_from_transaction_result(
        "sig-recovered-sell",
        0,
        &result,
        &interested,
        &raydium,
        &HashSet::new(),
    )?
    .expect("sell swap should parse");

    assert_eq!(raw.signature, "sig-recovered-sell");
    assert_eq!(raw.slot, 42);
    assert_eq!(raw.signer, signer);
    assert_eq!(raw.token_in, "TokenMintA");
    assert_eq!(raw.token_out, "So11111111111111111111111111111111111111112");
    assert!((raw.amount_in - 100.0).abs() < 1e-9);
    assert!((raw.amount_out - 1.0).abs() < 1e-9);
    assert_eq!(raw.dex_hint, "raydium");
    Ok(())
}

#[test]
fn rpc_http_urls_drop_placeholders_and_non_http_urls() {
    let mut config = IngestionConfig::default();
    config.helius_http_url = "https://rpc.example.com/?api-key=secret".to_string();
    config.helius_http_urls = vec![
        "wss://not-http.example.com".to_string(),
        "https://YOUR_QUICKNODE_HOST.solana-mainnet.quiknode.pro/REPLACE_ON_SERVER/".to_string(),
    ];

    assert_eq!(
        rpc_http_urls(&config),
        vec!["https://rpc.example.com/?api-key=secret".to_string()]
    );
}
