use super::b136_fixture::Fixture;
use anyhow::Result;
use copybot_config::AppConfig;
use serde_json::json;
pub(super) fn load(f: &Fixture, url: &str, dispatch: bool) -> Result<AppConfig> {
    let path = f.root.path().join("synthetic-config.toml");
    let signer = f.root.path().join("synthetic-test-key.json");
    if dispatch {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        assert_eq!(
            bs58::encode(key.verifying_key().to_bytes()).into_string(),
            f.meta["our"]["signer"]
        );
        std::fs::write(
            &signer,
            serde_json::to_vec(&key.to_keypair_bytes().as_slice())?,
        )?;
    }
    let mode: String = f.db.sql.query_row(
        "SELECT policy_mode FROM execution_tiny_experiment",
        [],
        |r| r.get(0),
    )?;
    let value = json!({
      "ingestion":{"source":"yellowstone_grpc","yellowstone_delivery_mode":"durable_association_v1","yellowstone_grpc_url":"http://127.0.0.1:1","yellowstone_x_token":"fixture","yellowstone_program_ids":f.meta["programs"],"pumpswap_program_ids":f.meta["pumpswap"],"raydium_program_ids":[],"yellowstone_association":{
        "pending":{"count":1024,"bytes":16777216},"blocks":{"count":32,"bytes":67108864},"history":{"count":2048,"bytes":33554432},"outputs":{"count":17,"bytes":4194304},"queue":{"count":4,"bytes":8388608},"inbox":{"count":20000,"bytes":134217728},"input_bytes":8388608,"metadata_bytes":33554432,"pending_ttl_ms":60000,"block_ttl_ms":60000,"history_ttl_ms":120000,"tick_ms":1000,"sqlite_busy_ms":5000}},
      "execution":{"enabled":false,"canary_tiny_submit_enabled":dispatch,"canary_enabled":true,"canary_route":"jupiter_swap_instructions","canary_wallet_pubkey":f.meta["our"]["signer"],"execution_signer_pubkey":f.meta["our"]["signer"],"execution_signer_keypair_path":signer,"canary_kill_switch_path":f.root.path().join("kill"),"submit_adapter_http_url":url,"max_confirm_seconds":1,"submit_timeout_ms":200,"quote_canary_enabled":true,"quote_canary_base_url":url,"quote_canary_timeout_ms":4000,"priority_fee_canary_rpc_url":url,"priority_fee_canary_enabled":false,"quote_canary_pump_fun_parallel_enabled":false,"quote_canary_public_parallel_enabled":false,"swap_instructions_dry_run_enabled":true,"swap_transaction_dry_run_enabled":true,"canary_batch_limit":1,"pretrade_max_priority_fee_lamports":22000,"tiny_experiment":{"id":"b136","activate":false,"policy_mode":mode},"owned_sell_preparation":{"policy":"rpc_finalized_cross_slot_owned_sell_v1","tiny_dispatch":dispatch,"rpc_url":url,"genesis_hash":"11111111111111111111111111111111","identity":"b136-loopback"}}
    });
    std::fs::write(&path, toml::to_string(&value)?)?;
    copybot_config::load_from_path(path.to_str().unwrap())
}
