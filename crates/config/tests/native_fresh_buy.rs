use copybot_config::{
    validate_association_delivery, AppConfig, NativeFreshBuyConfig,
    PROCESSED_SLOT_FENCE_AVAILABILITY_V1,
};

fn configured() -> AppConfig {
    toml::from_str(
        r#"
[ingestion]
source="yellowstone_grpc"
yellowstone_delivery_mode="durable_association_v1"
[ingestion.yellowstone_association]
pending={count=10,bytes=100000}
blocks={count=10,bytes=100000}
history={count=10,bytes=100000}
outputs={count=10,bytes=100000}
queue={count=10,bytes=100000}
inbox={count=100,bytes=1000000}
input_bytes=100000
metadata_bytes=100000
pending_ttl_ms=1000
block_ttl_ms=1000
history_ttl_ms=1000
tick_ms=100
sqlite_busy_ms=10
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
id="existing"
activate=false
[execution.owned_sell_preparation]
policy="rpc_finalized_cross_slot_owned_sell_v1"
tiny_dispatch=true
fractional_inventory="whole_wallet_parent_program_fraction_v1"
rpc_url="http://127.0.0.1:1"
genesis_hash="pinned"
identity="test"
[execution.native_fresh_buy]
policy="processed_slot_fence_availability_v1"
"#,
    )
    .unwrap()
}

#[test]
fn native_fresh_buy_defaults_off_and_accepts_only_explicit_policy() {
    assert!(AppConfig::default().execution.native_fresh_buy.is_none());
    let c = configured();
    assert_eq!(
        c.execution.native_fresh_buy.as_ref().unwrap().policy,
        PROCESSED_SLOT_FENCE_AVAILABILITY_V1
    );
    validate_association_delivery(&c).unwrap();

    let mut unsupported = c;
    unsupported.execution.native_fresh_buy = Some(NativeFreshBuyConfig {
        policy: "arbitrary".into(),
    });
    assert!(validate_association_delivery(&unsupported).is_err());
}

#[test]
fn native_fresh_buy_requires_durable_owned_sell_and_existing_experiment() {
    for case in 0..8 {
        let mut c = configured();
        match case {
            0 => c.ingestion.yellowstone_delivery_mode = "legacy".into(),
            1 => c.ingestion.capture_scope_db = Some("capture.db".into()),
            2 => c.execution.canary_tiny_submit_enabled = false,
            3 => c.execution.owned_sell_preparation.as_mut().unwrap().tiny_dispatch = false,
            4 => c.execution.owned_sell_preparation = None,
            5 => c.execution.tiny_experiment.id = None,
            6 => c.execution.tiny_experiment.activate = true,
            _ => c.execution.canary_max_signal_age_seconds = 0,
        }
        assert!(validate_association_delivery(&c).is_err(), "case {case}");
    }
}

#[test]
fn native_fresh_buy_rejects_unknown_policy_fields() {
    assert!(toml::from_str::<NativeFreshBuyConfig>(
        "policy='processed_slot_fence_availability_v1'\nsource_utc='invented'"
    )
    .is_err());
}
