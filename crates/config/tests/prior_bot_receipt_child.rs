//! A completed append-only child may witness closure, never a new exit authority.
use copybot_config::{validate_prior_bot_receipt_exit, ExecutionConfig, PriorBotReceiptExitConfig};

fn config(id: &str) -> ExecutionConfig {
    let buy = "exec-canary:native-buy-v1:LOCAL-source-buy";
    let mut c = ExecutionConfig::default();
    c.prior_bot_receipt_exit = Some(PriorBotReceiptExitConfig {
        policy: copybot_config::PRIOR_BOT_RECEIPT_EXIT_V1.into(),
        state_db_path: "/prior-state/live_runtime.db".into(),
        exit_intent_id: id.into(),
        buy_order_id: buy.into(),
        buy_receipt_signature: "LOCAL-confirmed-buy".into(),
        position_id: format!("exec-canary-pos:{buy}"),
        wallet_pubkey: "LOCAL-wallet".into(),
        mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
        amount_raw: 914598,
        decimals: 6,
    });
    c
}

#[test]
fn prior_exit_accepts_same_native_buy_parent_or_child_identity() {
    for prefix in ["bot-receipt-exit:", "bot-receipt-child:"] {
        validate_prior_bot_receipt_exit(&config(&format!("{prefix}LOCAL-source-buy"))).unwrap();
    }
}

#[test]
fn prior_exit_refuses_foreign_or_malformed_child_identity() {
    for id in ["bot-receipt-child:foreign", "bot-receipt-child:",
        "bot-receipt-child:LOCAL-source-buy:second", "source-child:LOCAL-source-buy"] {
        assert!(validate_prior_bot_receipt_exit(&config(id)).is_err(), "{id}");
    }
}
