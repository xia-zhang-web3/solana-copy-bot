//! Exact stopped32 config proof. No provider, signer, daemon or financial action.
use anyhow::{Context, Result};
use chrono::{Duration, Utc};

fn startup(c: &copybot_config::AppConfig) -> Result<()> {
    crate::config_contract::validate_execution_runtime_contract(&c.execution, &c.system.env)?;
    crate::config_contract::validate_live_ingestion_source_contract(
        &c.ingestion.source,
        &c.system.env,
    )?;
    crate::risk_contract::validate_execution_risk_contract(&c.risk)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(
        &c.execution,
        &c.risk,
        &c.system.env,
    )
}

#[test]
fn owner_source_gate_exact_package_configs_and_native_identity() -> Result<()> {
    let Ok(source_path) = std::env::var("COPYBOT_SOURCE_COPY_GATE_SOURCE_CONFIG") else {
        return Ok(());
    };
    let bot_path = std::env::var("COPYBOT_SOURCE_COPY_GATE_BOT_CONFIG")?;
    let original_source = copybot_config::load_from_path(&source_path)?;
    let original_bot = copybot_config::load_from_path(&bot_path)?;
    assert!(
        !original_source
            .execution
            .owner_technical_buy
            .as_ref()
            .unwrap()
            .activate
    );
    assert!(
        !original_bot
            .execution
            .technical_cohort
            .as_ref()
            .unwrap()
            .activate
    );
    startup(&original_source)?;
    startup(&original_bot)?;
    let start = match std::env::var("COPYBOT_SOURCE_COPY_GATE_CLOCK") {
        Ok(v) => chrono::DateTime::parse_from_rfc3339(&v)?.with_timezone(&Utc),
        Err(_) => Utc::now(),
    };
    let end = start + Duration::seconds(14_400);
    let output = super::temporary_output_fixture::OutputRoot::new("source-gate-config-proof")?;
    let mut bot: toml::Value = toml::from_str(&std::fs::read_to_string(&bot_path)?)?;
    bot["execution"]["technical_cohort"]["activate"] = true.into();
    bot["execution"]["technical_cohort"]["activated_at"] = start.to_rfc3339().into();
    bot["execution"]["technical_cohort"]["deadline"] = end.to_rfc3339().into();
    bot["execution"]["tiny_experiment"]["activate"] = true.into();
    bot["execution"]["canary_entry_submit_enabled"] = true.into();
    let active_bot = output.path().join("bot.toml");
    std::fs::write(&active_bot, toml::to_string(&bot)?)?;
    let bot = copybot_config::load_from_path(&active_bot)?;
    startup(&bot)?;
    let identity = crate::execution_native_buy_rpc::policy_identity(&bot.execution)?;
    let mut source: toml::Value = toml::from_str(&std::fs::read_to_string(&source_path)?)?;
    source["execution"]["owner_technical_buy"]["activate"] = true.into();
    source["execution"]["owner_technical_buy"]["activated_at"] = start.to_rfc3339().into();
    source["execution"]["owner_technical_buy"]["expires_at"] = end.to_rfc3339().into();
    source["execution"]["canary_entry_submit_enabled"] = true.into();
    source["execution"]["owner_technical_buy"]["source_copy_exit_gate"]["bot_policy_identity"] =
        identity.clone().into();
    let active_source = output.path().join("source.toml");
    std::fs::write(&active_source, toml::to_string(&source)?)?;
    let source = copybot_config::load_from_path(&active_source)?;
    startup(&source)?;
    let p = source.execution.owner_technical_buy.as_ref().unwrap();
    let g = p.source_copy_exit_gate.as_ref().context("source gate")?;
    let cohort =
        crate::execution_technical_cohort::authority(&bot.execution)?.context("bot cohort")?;
    assert_eq!(g.bot_policy_identity, identity);
    assert_eq!(g.bot_run_id, cohort.run_id);
    assert_eq!(g.bot_wallet_pubkey, bot.execution.canary_wallet_pubkey);
    assert_eq!(cohort.wallet_ids, [p.wallet_pubkey.clone()]);
    assert_eq!(
        p.genesis_hash,
        bot.execution
            .owned_sell_preparation
            .as_ref()
            .unwrap()
            .genesis_hash
    );
    assert!(!source.execution.enabled);
    assert!(!bot.execution.enabled);
    if let Ok(path) = std::env::var("COPYBOT_SOURCE_COPY_GATE_IDENTITY_OUTPUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proof":"exact_stopped32_materialized_native_config_no_io",
                "activated_at":start.to_rfc3339(),"deadline":end.to_rfc3339(),
                "bot_policy_identity":identity,
                "source_template_sha256":crate::execution_owned_sell_rpc::digest(std::fs::read(source_path)?),
                "bot_template_sha256":crate::execution_owned_sell_rpc::digest(std::fs::read(bot_path)?),
            }))?,
        )?;
    }
    Ok(())
}
