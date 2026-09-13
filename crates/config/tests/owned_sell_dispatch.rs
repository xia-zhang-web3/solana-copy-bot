use copybot_config::{owned_sell_dispatch, validate_owned_sell_preparation, AppConfig};
fn config() -> AppConfig {
    toml::from_str(
        r#"
[ingestion]
source="yellowstone_grpc"
yellowstone_delivery_mode="durable_association_v1"
[execution]
enabled=false
canary_tiny_submit_enabled=true
canary_enabled=true
quote_canary_enabled=true
swap_instructions_dry_run_enabled=true
swap_transaction_dry_run_enabled=true
execution_signer_pubkey="test"
canary_wallet_pubkey="test"
execution_signer_keypair_path="synthetic-key.json"
pretrade_max_priority_fee_lamports=22000
submit_adapter_http_url="http://127.0.0.1:1"
[execution.tiny_experiment]
id="b136"
activate=false
[execution.owned_sell_preparation]
policy="rpc_finalized_cross_slot_owned_sell_v1"
tiny_dispatch=true
rpc_url="http://127.0.0.1:1"
genesis_hash="pinned"
identity="test"
"#,
    )
    .unwrap()
}
#[test]
fn b136_explicit_dispatch_flags_default_off_and_invalid_combinations() {
    assert!(!owned_sell_dispatch(&AppConfig::default().execution));
    for broad in [false, true] {
        for tiny in [false, true] {
            for owned in [false, true] {
                let mut c = config();
                c.execution.enabled = broad;
                c.execution.canary_tiny_submit_enabled = tiny;
                c.execution
                    .owned_sell_preparation
                    .as_mut()
                    .unwrap()
                    .tiny_dispatch = owned;
                assert_eq!(
                    validate_owned_sell_preparation(&c.execution, &c.ingestion).is_ok(),
                    !broad && tiny == owned
                );
            }
        }
    }
    for field in 0..8 {
        let mut c = config();
        match field {
            0 => c.ingestion.yellowstone_delivery_mode = "legacy".into(),
            1 => c.execution.submit_adapter_http_url = "http://127.0.0.1:2".into(),
            2 => c.execution.execution_signer_keypair_path.clear(),
            3 => c.execution.canary_wallet_pubkey = "other".into(),
            4 => c.execution.tiny_experiment.activate = true,
            5 => c.execution.submit_timeout_ms = 0,
            6 => c.execution.owned_sell_preparation.as_mut().unwrap().policy = "arbitrary".into(),
            _ => c.execution.quote_canary_enabled = false,
        };
        assert!(
            validate_owned_sell_preparation(&c.execution, &c.ingestion).is_err(),
            "{field}"
        );
    }
}
