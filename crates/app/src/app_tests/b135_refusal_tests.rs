use super::{b135_fixture::Fixture, b135_server::Server};
use anyhow::Result;
#[tokio::test]
async fn b135_bound_rpc_semantic_refusals() -> Result<()> {
    for (case, reason) in [
        ("genesis", "genesis_mismatch"),
        ("endpoint", "endpoint_status"),
        ("request", "request_binding"),
        ("jsonrpc", "request_binding"),
        ("null", "result_missing"),
        ("meta", "meta_error"),
        ("err", "meta_error"),
        ("version", "transaction_version"),
        ("signature", "signature_binding"),
        ("buy_receipt", "buy_native_receipt"),
        ("buy_wallet", "wallet_binding"),
        ("same", "cross_slot_order"),
        ("earlier", "cross_slot_order"),
        ("sell_wallet", "wallet_binding"),
        ("sell_no_instruction", "decoded_swap_missing"),
        ("sell_wrong_instruction", "decoded_swap_side"),
        ("sell_amount", "source_decoded_amount"),
        ("sell_mint", "instruction_accounts"),
        ("conflict", "conflicting_token_delta"),
    ] {
        let f = Fixture::new().await?;
        let server = Server::new().await?;
        *server.fault.lock().unwrap() = case.into();
        let c = f.config(&server.url)?;
        f.ingress(&c).await?;
        let before = f.rows("positions")?;
        let error = f.drive(&f.runner(&c)?).await.unwrap_err().to_string();
        assert!(error.contains(reason), "{case}: {error}");
        assert_eq!(f.handoffs()?, 0, "{case}");
        assert!(
            f.rows("rpc_owned_sell_handoffs")?.is_empty(),
            "no authority no financial reservation: {case}"
        );
        assert_eq!(f.rows("positions")?, before);
        server.healthy();
        println!("B135_REFUSAL {case}: {error}");
    }
    Ok(())
}
#[tokio::test]
async fn b135_default_off_old_strict_and_invalid_configuration() -> Result<()> {
    let f = Fixture::new().await?;
    let server = Server::new().await?;
    let mut c = f.config(&server.url)?;
    c.execution.owned_sell_preparation = None;
    f.ingress(&c).await?;
    let r = f.runner(&c)?;
    super::strict_quote_fixture::tick(&r, &f.db).await?;
    let q = super::strict_quote_fixture::result(&f.db, &f.meta).await?;
    assert_eq!(
        q.outcome,
        copybot_storage_core::ordered_sell_quote::QuoteOutcome::Current
    );
    assert_eq!(f.handoffs()?, 0);
    assert_eq!(server.calls.lock().unwrap().len(), 1);
    assert!(copybot_config::ExecutionConfig::default()
        .owned_sell_preparation
        .is_none());
    for case in [
        "enabled",
        "tiny",
        "legacy",
        "quote_off",
        "prepare_off",
        "unknown_policy",
        "activate",
    ] {
        let mut bad = f.config(&server.url)?;
        match case {
            "enabled" => bad.execution.enabled = true,
            "tiny" => bad.execution.canary_tiny_submit_enabled = true,
            "legacy" => bad.ingestion.yellowstone_delivery_mode = "legacy".into(),
            "quote_off" => bad.execution.quote_canary_enabled = false,
            "prepare_off" => bad.execution.swap_instructions_dry_run_enabled = false,
            "unknown_policy" => {
                bad.execution
                    .owned_sell_preparation
                    .as_mut()
                    .unwrap()
                    .policy = "untrusted".into()
            }
            _ => bad.execution.tiny_experiment.activate = true,
        }
        assert!(
            copybot_config::validate_association_delivery(&bad).is_err(),
            "{case}"
        );
        assert!(f.runner(&bad).is_err(), "{case}");
        let (_, rx) = tokio::sync::mpsc::channel(1);
        assert!(
            copybot_ingestion::IngestionService::with_replay(&bad, rx, "invalid".into()).is_err()
        );
    }
    assert!(serde_json::from_value::<copybot_config::OwnedSellPreparationConfig>(serde_json::json!({"policy":"rpc_finalized_cross_slot_owned_sell_v1","rpc_url":server.url,"genesis_hash":"11111111111111111111111111111111","identity":"fixture","commitment":"confirmed"})).is_err());
    server.healthy();
    Ok(())
}
