use super::super::source_sell_ingress_fixture::Ingress;
use crate::*;
use copybot_ingestion::transaction_membership::{self as membership, Disposition};
use serde_json::{json, Value};
use std::process::Command;
use yellowstone_grpc_proto::prelude::*;

fn event() -> SwapEvent {
    let signature = bs58::encode([55; 64]).into_string();
    let slot = (1..100_000)
        .find(|slot| membership::selected(&signature, *slot))
        .unwrap();
    SwapEvent {
        signature,
        slot,
        wallet: "unfollowed-fixture-wallet".into(),
        dex: "raydium".into(),
        token_in: "So11111111111111111111111111111111111111112".into(),
        token_out: "mint".into(),
        amount_in: 1.,
        amount_out: 2.,
        exact_amounts: None,
        ts_utc: Utc::now(),
    }
}

fn decoded(row: &SwapEvent) {
    membership::decoded_update(&SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot: row.slot,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: bs58::decode(&row.signature).into_vec().unwrap(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    });
}

#[test]
fn membership_disposition_actual_app_dedup_on_off() -> Result<()> {
    let folder =
        std::env::temp_dir().join(format!("membership-ingress-report-{}", std::process::id()));
    std::fs::create_dir(&folder)?;
    let child = format!("{}::child", module_path!().split_once("::").unwrap().1);
    let mut reports = Vec::new();
    for enabled in [false, true] {
        let path = folder.join(format!("{enabled}.json"));
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--exact", &child, "--nocapture"])
            .env("COPYBOT_MEMBERSHIP_INGRESS_REPORT", &path)
            .env_remove(membership::ENV);
        if enabled {
            command.env(membership::ENV, "1");
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        reports.push(serde_json::from_slice::<Value>(&std::fs::read(path)?)?);
    }
    std::fs::remove_dir_all(folder)?;
    assert_eq!(reports[0]["business"], reports[1]["business"]);
    assert_eq!(reports[1]["business"]["rows"], 1);
    let row = &reports[1]["snapshot"]["records"][0];
    assert_eq!(row["decoded"]["count"], 2);
    assert_eq!(row["outcomes"][Disposition::AppDedup.code().to_string()], 1);
    assert_eq!(
        row["outcomes"][Disposition::RawInserted.code().to_string()],
        1
    );
    assert_eq!(row["pre_raw_resolution"], 2);
    Ok(())
}

#[test]
fn child() -> Result<()> {
    let Ok(report) = std::env::var("COPYBOT_MEMBERSHIP_INGRESS_REPORT") else {
        return Ok(());
    };
    membership::initialize_from_env();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let business = rt.block_on(async {
        let mut f = Ingress::new()?;
        let row = event();
        let before = f.money()?;
        decoded(&row);
        f.send(&row, false).await?;
        decoded(&row);
        f.send(&row, false).await?;
        f.finish().await?;
        assert_eq!(f.money()?, before);
        let rows: u64 = f
            .conn()?
            .query_row("SELECT count(*) FROM observed_swaps", [], |r| r.get(0))?;
        Ok::<_, anyhow::Error>(
            json!({"rows":rows,"signature":row.signature,"slot":row.slot,"money_unchanged":true}),
        )
    })?;
    membership::finish();
    let snapshot: Value = serde_json::from_str(&membership::snapshot_json())?;
    std::fs::write(
        report,
        json!({"business":business,"snapshot":snapshot}).to_string(),
    )?;
    Ok(())
}
