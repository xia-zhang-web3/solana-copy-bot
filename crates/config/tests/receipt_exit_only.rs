//! Selector contract for normal and malformed receipt policies.
use copybot_config::{execution_only_mode, ExecutionConfig, OwnerExitConfig};

#[test]
fn ordinary_config_does_not_select_execution_only() {
    assert_eq!(execution_only_mode(&ExecutionConfig::default()).unwrap(), None);
}

#[test]
fn malformed_receipt_policy_never_selects_a_mode() {
    let mut c = ExecutionConfig::default();
    c.owner_exit = Some(OwnerExitConfig {
        policy: copybot_config::OWNER_BOT_RECEIPT_EXIT_V1.into(),
        activate: true, source_recovery_parent_intent_id: None,
        run_id: "INVALID".into(), intent_id: "INVALID".into(),
        buy_order_id: "INVALID".into(), buy_receipt_signature: "INVALID".into(),
        position_id: "INVALID".into(), wallet_pubkey: "INVALID".into(),
        signer_pubkey: "INVALID".into(), genesis_hash: "INVALID".into(), mint: "INVALID".into(),
        amount_raw: 988620, decimals: 6, route: "jupiter_swap_instructions".into(),
        activated_at: "2026-10-27T00:00:00Z".into(), expires_at: "2026-10-27T00:01:00Z".into(),
        max_priority_fee_lamports: 22000, min_reserve_lamports: 0,
        max_slippage_bps: 50, max_daily_loss_lamports: 20000000,
    });
    assert!(execution_only_mode(&c).is_err()); // Missing genuine controlled scope/cohort.
}
