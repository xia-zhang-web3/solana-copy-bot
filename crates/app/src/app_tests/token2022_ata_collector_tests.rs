use super::token2022_ata_inputs_tests::*;
use super::token2022_ata_rpc_tests::Server;
use crate::execution_native_ata_funding::{plan_supported_ata_funding as plan, types::*};
use crate::execution_native_rpc::{rent_types::ClassicAtaFundingFacts, NativeFundingRpcClient};
use anyhow::Result;
use serde_json::json;
use std::{path::Path, time::Duration};

pub(super) async fn facts(spec: &Spec, dir: &Path) -> Result<(ClassicAtaFundingFacts, usize)> {
    std::fs::create_dir_all(dir)?;
    let server = Server::start(spec, None).await?;
    let out = NativeFundingRpcClient::new()?
        .collect_with_supported_ata_rent(
            &server.rpc.endpoint,
            Duration::from_secs(2),
            &spec.payload,
            spec.wallet,
            None,
        )
        .await;
    let trace = server.finish(dir).await?;
    Ok((out?, trace.len()))
}
pub(super) async fn unknown(spec: &Spec, label: &str) -> Result<()> {
    let dir = output(label);
    let (native, count) = facts(spec, &dir).await?;
    let p = plan(&spec.payload, spec.wallet, &native)?;
    assert_eq!(
        p.explicit_ata_coverage,
        ExplicitAtaCoverage::Partial,
        "{label}"
    );
    assert!(p
        .rows
        .iter()
        .any(|row| matches!(row.amount, AtaFundingAmount::Unresolved(_))));
    assert_eq!(count, 3, "invalid size must not request170: {label}");
    let error = crate::execution_initial_sol::check(&spec.payload, spec.wallet, RESERVE, &native)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "initial_sol_unsupported_setup",
        "{label}"
    );
    save(
        &dir,
        "unknown.json",
        &json!({"reason":error.to_string(),"rows":format!("{:?}",p.rows),"requests":count}),
    );
    Ok(())
}
#[tokio::test]
async fn token2022_ata_sufficient_exact_thresholds() -> Result<()> {
    for (arm, required, t22) in [
        ("absent", 64_140_361, 2_074_080),
        ("prefunded", 63_440_361, 1_374_080),
        ("existing", 62_066_281, 0),
    ] {
        for short in [0, 1] {
            let mut spec = Spec::sufficient(arm)?;
            spec.rows[0] = super::initial_sol_rpc_fixture::system(required - short);
            let dir = output(&format!("threshold-{arm}-{short}"));
            let (native, count) = facts(&spec, &dir.join("native")).await?;
            assert_eq!(count, if arm == "existing" { 3 } else { 4 });
            let p = plan(&spec.payload, spec.wallet, &native)?;
            assert_eq!(p.explicit_ata_coverage, ExplicitAtaCoverage::Complete);
            assert_eq!(p.rows.len(), 2);
            assert_eq!(
                (p.rows[0].requirement_index, p.rows[0].amount),
                (2, AtaFundingAmount::Known(2_039_280))
            );
            assert_eq!(
                (p.rows[1].requirement_index, p.rows[1].amount),
                (5, AtaFundingAmount::Known(t22))
            );
            assert_eq!(p.known_wallet_token2022_payer_lamports, u128::from(t22));
            assert_eq!(p.known_wallet_payer_lamports, 2_039_280 + u128::from(t22));
            assert_eq!(native.native().fee().value, Some(27_000));
            assert!(matches!(
                &native.native().requirements().requirements[3].operation,
                crate::execution_native_funding::types::FundingOperation::SystemTransfer {
                    from, to, lamports: 10_000_000, ..
                } if *from == spec.wallet && *to != spec.wallet
            ));
            assert_eq!(
                native
                    .native()
                    .requirements()
                    .nominal_wallet_source_transfer_operands_lamports,
                u128::from(10_000_000 + RESERVE)
            );
            assert_eq!(
                native.native().observed_payer_lamports(),
                Some(required - short)
            );
            // This fixed boundary accounts only for selected explicit costs.
            // The collector/planner does not prove full Jupiter funding.
            assert_eq!(
                u128::from(required),
                u128::from(RESERVE) + 27_000 + 10_000_000 + p.known_wallet_payer_lamports
            );
            let server = Server::start(&spec, None).await?;
            let out = crate::execution_initial_sol::collect_and_check(
                &server.rpc.endpoint,
                2000,
                &spec.payload,
                spec.wallet,
                RESERVE,
            )
            .await;
            assert_eq!(server.finish(&dir).await?.len(), count);
            save(
                &dir,
                "result.json",
                &json!({"result":format!("{out:#?}"),"rows":format!("{:?}",p.rows),
                    "selected_required":required,"full_jupiter_funding":"unproven"}),
            );
            if short == 0 {
                assert_eq!(
                    out.unwrap_err().to_string(),
                    super::b123_jupiter_fixture_tests::UNPROVEN
                );
            } else {
                assert_eq!(
                    out.unwrap_err().to_string(),
                    format!(
                        "initial_sol_insufficient:observed={}:required={required}:shortfall=1",
                        required - 1
                    )
                );
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn token2022_ata_rent_minimum_saturates_without_balance_credit() -> Result<()> {
    for (rent, prefund, want) in [
        (0, 0, 1),
        (0, 1, 0),
        (1, 9_999_999, 0),
        (2_074_080, 1, 2_074_079),
        (u64::MAX, 0, u64::MAX),
    ] {
        let mut spec = Spec::sufficient("absent")?;
        spec.rent170 = rent;
        if prefund > 0 {
            spec.set(ATA, super::initial_sol_rpc_fixture::system(prefund));
        }
        let dir = output(&format!("rent-{rent}-{prefund}"));
        let (native, count) = facts(&spec, &dir).await?;
        assert_eq!(count, 4);
        let p = plan(&spec.payload, spec.wallet, &native)?;
        assert_eq!(p.explicit_ata_coverage, ExplicitAtaCoverage::Complete);
        assert_eq!(p.rows.len(), 2);
        assert_eq!(p.rows[0].amount, AtaFundingAmount::Known(2_039_280));
        assert_eq!(p.rows[1].amount, AtaFundingAmount::Known(want));
        assert_eq!(p.known_wallet_token2022_payer_lamports, u128::from(want));
        assert_eq!(p.known_wallet_payer_lamports, 2_039_280 + u128::from(want));
        let out = crate::execution_initial_sol::check(&spec.payload, spec.wallet, RESERVE, &native);
        let required = 62_066_281 + u128::from(want);
        save(
            &dir,
            "result.json",
            &json!({"rent170":rent,"prefund":prefund,
            "known_token2022_ata":want,"known_all_ata":p.known_wallet_payer_lamports.to_string(),
            "selected_required":required.to_string(),"full_jupiter_funding":"unproven",
            "result":format!("{out:#?}"),"collection_requests":count}),
        );
        if required > 1_000_000_000 {
            assert_eq!(
                out.unwrap_err().to_string(),
                format!(
                    "initial_sol_insufficient:observed=1000000000:required={required}:shortfall={}",
                    required - 1_000_000_000
                )
            );
        } else {
            assert_eq!(
                out.unwrap_err().to_string(),
                super::b123_jupiter_fixture_tests::UNPROVEN
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn token2022_ata_original118_and_classic_api_do_not_upgrade() -> Result<()> {
    for existing in [false, true] {
        unknown(&Spec::buy(existing)?, &format!("original118-{existing}")).await?;
    }
    for arm in ["absent", "existing"] {
        let spec = Spec::sufficient(arm)?;
        let dir = output(&format!("old-api-{arm}"));
        let server = Server::start(&spec, None).await?;
        let native = NativeFundingRpcClient::new()?
            .collect_with_classic_ata_rent(
                &server.rpc.endpoint,
                Duration::from_secs(2),
                &spec.payload,
                spec.wallet,
                None,
            )
            .await?;
        assert_eq!(server.finish(&dir).await?.len(), 3);
        assert!(!native.token2022_collected());
        assert_eq!(
            plan(&spec.payload, spec.wallet, &native)?.explicit_ata_coverage,
            ExplicitAtaCoverage::Partial
        );
        let (extended, _) = facts(&spec, &dir.join("native")).await?;
        assert_eq!(
            crate::execution_native_ata_funding::plan_classic_ata_funding(
                &spec.payload,
                spec.wallet,
                &extended
            )?
            .explicit_ata_coverage,
            ExplicitAtaCoverage::Partial
        );
        assert!(plan(&spec.payload, [7; 32], &extended).is_err());
        let mut raw =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &spec.payload)?;
        raw[1] = 1; // even same message with a different signature cannot reuse facts
        let other = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw);
        assert!(plan(&other, spec.wallet, &extended).is_err());
    }
    Ok(())
}
