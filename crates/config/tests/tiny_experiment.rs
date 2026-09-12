use anyhow::Result;
use copybot_config::{ExecutionConfig, TinyExperimentConfig};
#[test]
fn tiny_experiment_defaults_disabled_and_activation_validates() -> Result<()> {
    let c = ExecutionConfig::default();
    assert!(!c.canary_tiny_submit_enabled);
    assert!(!c.enabled);
    assert_eq!(c.tiny_experiment, TinyExperimentConfig::default());
    for id in ["", " white", "new/../id"] {
        assert!(TinyExperimentConfig {
            id: Some(id.into()),
            activate: true,
            ..Default::default()
        }
        .validate("wallet")
        .is_err());
    }
    assert!(TinyExperimentConfig {
        id: None,
        activate: true,
        ..Default::default()
    }
    .validate("wallet")
    .is_err());
    assert!(TinyExperimentConfig {
        id: Some("one".into()),
        activate: true,
        ..Default::default()
    }
    .validate("")
    .is_err());
    TinyExperimentConfig {
        id: Some("one".into()),
        activate: true,
        ..Default::default()
    }
    .validate("wallet")?;
    Ok(())
}
#[test]
fn tiny_experiment_toml_disallows_adjustable_limits() -> Result<()> {
    let path = std::env::temp_dir().join(format!("tiny-budget-{}.toml", std::process::id()));
    for bad in ["fee_limit=999999", "activate=true", "id=' '"] {
        std::fs::write(
            &path,
            format!("[execution.tiny_experiment]\n{bad}\n").replace(
                "\n", "
",
            ),
        )?;
        assert!(copybot_config::load_from_path(&path).is_err());
    }
    std::fs::remove_file(path)?;
    Ok(())
}

#[test]
fn tiny_capital_is_explicit_fixed_mode_with_old_default() -> Result<()> {
    use copybot_config::TinyPolicyMode;
    assert_eq!(
        TinyExperimentConfig::default().policy_mode,
        TinyPolicyMode::DecodedAmount
    );
    let d = std::env::temp_dir().join(format!("tiny-capital-{}.toml", std::process::id()));
    for (mode, valid) in [
        ("protected_native_capital", true),
        ("decoded_amount", true),
        ("jupiter", false),
    ] {
        std::fs::write(&d,format!("[execution]\ncanary_wallet_pubkey='wallet'\n[execution.tiny_experiment]\nid='one'\nactivate=true\npolicy_mode='{mode}'\n"))?;
        assert_eq!(copybot_config::load_from_path(&d).is_ok(), valid);
    }
    std::fs::remove_file(d)?;
    Ok(())
}
