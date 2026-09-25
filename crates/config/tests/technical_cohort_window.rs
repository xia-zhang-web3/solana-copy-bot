use copybot_config::{
    validate_technical_cohort, ExecutionConfig, NativeFreshBuyConfig, OwnedSellPreparationConfig,
    TechnicalCohortConfig, TinyPolicyMode, CLASSIC_SPL_MINT_V1,
    PROCESSED_SLOT_FENCE_AVAILABILITY_V1, RPC_FINALIZED_OWNED_SELL_V1, TECHNICAL_COHORT_V1,
};

fn cohort() -> ExecutionConfig {
    let mut e = ExecutionConfig::default();
    e.canary_wallet_pubkey = "BotWallet111111111111111111111111111111111".into();
    e.canary_route = "jupiter_swap_instructions".into();
    e.canary_enabled = true;
    e.canary_tiny_submit_enabled = true;
    e.quote_canary_enabled = true;
    e.swap_instructions_dry_run_enabled = true;
    e.swap_transaction_dry_run_enabled = true;
    e.tiny_experiment.id = Some("bounded-cohort".into());
    e.tiny_experiment.activate = true;
    e.tiny_experiment.policy_mode = TinyPolicyMode::ProtectedNativeCapital;
    e.native_fresh_buy = Some(NativeFreshBuyConfig {
        policy: PROCESSED_SLOT_FENCE_AVAILABILITY_V1.into(),
    });
    e.owned_sell_preparation = Some(OwnedSellPreparationConfig {
        policy: RPC_FINALIZED_OWNED_SELL_V1.into(),
        tiny_dispatch: true,
        fractional_inventory: Some("whole_wallet_parent_program_fraction_v1".into()),
        rpc_url: "http://127.0.0.1:9".into(),
        genesis_hash: "genesis".into(),
        identity: "bounded-cohort".into(),
    });
    e.canary_max_signal_age_seconds = 120;
    e.canary_buy_size_sol = 0.01;
    e.quote_canary_buy_size_sol = 0.01;
    e.canary_max_open_positions = 1;
    e.canary_max_daily_loss_sol = 0.02;
    e.pretrade_min_sol_reserve = 0.160_200_031;
    e.pretrade_max_priority_fee_lamports = 50_000;
    e.quote_canary_slippage_bps = 50;
    e.quote_canary_buy_slippage_bps = 50;
    e.quote_canary_sell_slippage_bps = 50;
    e.technical_cohort = Some(TechnicalCohortConfig {
        policy: TECHNICAL_COHORT_V1.into(),
        activate: true,
        run_id: "bounded-cohort".into(),
        wallet_ids: vec!["LeaderWallet111111111111111111111111111111".into()],
        mint_policy: CLASSIC_SPL_MINT_V1.into(),
        route: e.canary_route.clone(),
        activated_at: "2026-09-25T00:00:00Z".into(),
        deadline: "2026-09-25T04:00:00Z".into(),
        max_wait_seconds: 14_400,
        max_buy_count: 1,
        max_source_sell_count: 1,
    });
    e
}

#[test]
fn four_hour_cohort_keeps_trade_limits() {
    let accepted = cohort();
    assert!(validate_technical_cohort(&accepted).is_ok());
    for fault in [
        "window_plus_one",
        "window_too_short",
        "second_buy",
        "second_sell",
        "larger_buy",
        "older_signal",
        "more_positions",
        "higher_loss",
        "lower_floor",
        "higher_priority",
        "higher_slippage",
        "higher_buy_slippage",
        "higher_sell_slippage",
        "second_submit",
    ] {
        let mut e = accepted.clone();
        match fault {
            "window_plus_one" => e.technical_cohort.as_mut().unwrap().max_wait_seconds = 14_401,
            "window_too_short" => e.technical_cohort.as_mut().unwrap().max_wait_seconds = 120,
            "second_buy" => e.technical_cohort.as_mut().unwrap().max_buy_count = 2,
            "second_sell" => e.technical_cohort.as_mut().unwrap().max_source_sell_count = 2,
            "larger_buy" => e.canary_buy_size_sol = 0.02,
            "older_signal" => e.canary_max_signal_age_seconds = 121,
            "more_positions" => e.canary_max_open_positions = 2,
            "higher_loss" => e.canary_max_daily_loss_sol = 0.03,
            "lower_floor" => e.pretrade_min_sol_reserve = 0.16,
            "higher_priority" => e.pretrade_max_priority_fee_lamports = 50_001,
            "higher_slippage" => e.quote_canary_slippage_bps = 51,
            "higher_buy_slippage" => e.quote_canary_buy_slippage_bps = 51,
            "higher_sell_slippage" => e.quote_canary_sell_slippage_bps = 51,
            "second_submit" => e.max_submit_attempts = 2,
            _ => unreachable!(),
        }
        assert!(validate_technical_cohort(&e).is_err(), "{fault}");
    }
}
