use super::*;
use serde_json::json;

#[test]
fn confirmed_transaction_meta_replaces_quote_buy_fill() -> Result<()> {
    let expected = crate::execution_submit_adapter::ExecutionConfirmedFill::Buy(
        crate::execution_submit_adapter::ExecutionConfirmedBuyFill {
            order_id: "order-buy".to_string(),
            token: "TokenMint".to_string(),
            qty: 216_298.048456,
            qty_exact: Some(copybot_core_types::TokenQuantity::new(216_298_048_456, 6)),
            cost_sol: 0.01,
            fill_ts: Utc::now(),
        },
    );
    let actual = crate::execution_submit_adapter::confirmed_fill_from_transaction_json(
        "Wallet1111111111111111111111111111111111",
        &expected,
        json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction": {
                    "message": {
                        "accountKeys": [
                            {"pubkey": "Wallet1111111111111111111111111111111111", "signer": true, "writable": true}
                        ]
                    }
                },
                "meta": {
                    "preBalances": [1500000000_i64],
                    "postBalances": [1489000000_i64],
                    "preTokenBalances": [],
                    "postTokenBalances": [{
                        "owner": "Wallet1111111111111111111111111111111111",
                        "mint": "TokenMint",
                        "uiTokenAmount": {"amount": "210353432436", "decimals": 6}
                    }]
                }
            }
        }),
    )?
    .expect("actual buy fill");

    match actual {
        crate::execution_submit_adapter::ExecutionConfirmedFill::Buy(fill) => {
            assert_eq!(
                fill.qty_exact,
                Some(copybot_core_types::TokenQuantity::new(210_353_432_436, 6))
            );
            assert!((fill.qty - 210_353.432436).abs() < 1e-9);
            assert!((fill.cost_sol - 0.011).abs() < 1e-12);
        }
        _ => panic!("expected buy fill"),
    }
    Ok(())
}

#[test]
fn confirmed_transaction_meta_replaces_quote_sell_fill() -> Result<()> {
    let expected = crate::execution_submit_adapter::ExecutionConfirmedFill::Sell(
        crate::execution_submit_adapter::ExecutionConfirmedSellFill {
            order_id: "order-sell".to_string(),
            token: "TokenMint".to_string(),
            target_qty: 216_298.048456,
            target_qty_exact: Some(copybot_core_types::TokenQuantity::new(216_298_048_456, 6)),
            exit_price_sol: 0.00000005,
            dust_qty_epsilon: 1e-9,
            fill_ts: Utc::now(),
        },
    );
    let actual = crate::execution_submit_adapter::confirmed_fill_from_transaction_json(
        "Wallet1111111111111111111111111111111111",
        &expected,
        json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction": {
                    "message": {
                        "accountKeys": [
                            {"pubkey": "Wallet1111111111111111111111111111111111", "signer": true, "writable": true}
                        ]
                    }
                },
                "meta": {
                    "preBalances": [1000000000_i64],
                    "postBalances": [1009000000_i64],
                    "preTokenBalances": [{
                        "owner": "Wallet1111111111111111111111111111111111",
                        "mint": "TokenMint",
                        "uiTokenAmount": {"amount": "210353432436", "decimals": 6}
                    }],
                    "postTokenBalances": [{
                        "owner": "Wallet1111111111111111111111111111111111",
                        "mint": "TokenMint",
                        "uiTokenAmount": {"amount": "0", "decimals": 6}
                    }]
                }
            }
        }),
    )?
    .expect("actual sell fill");

    match actual {
        crate::execution_submit_adapter::ExecutionConfirmedFill::Sell(fill) => {
            assert_eq!(
                fill.target_qty_exact,
                Some(copybot_core_types::TokenQuantity::new(210_353_432_436, 6))
            );
            assert!((fill.target_qty - 210_353.432436).abs() < 1e-9);
            assert!((fill.exit_price_sol - (0.009 / 210_353.432436)).abs() < 1e-18);
        }
        _ => panic!("expected sell fill"),
    }
    Ok(())
}
