use super::*;
use crate::*;
use copybot_ingestion::transaction_membership::{self as membership, Disposition};
use serde_json::{json, Value};
use std::process::Command;

#[path = "tests_pre_raw_fixture.rs"]
mod fixture;
use fixture::*;
#[path = "tests_pre_raw_commit.rs"]
mod commit;

#[test]
fn membership_disposition_actual_app_paths_on_off() -> Result<()> {
    let folder = std::env::temp_dir().join(format!("membership-app-report-{}", std::process::id()));
    std::fs::create_dir(&folder)?;
    let child = format!("{}::child", module_path!().split_once("::").unwrap().1);
    let mut reports = Vec::new();
    for enabled in [false, true] {
        let path = folder.join(format!("{enabled}.json"));
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--exact", &child, "--nocapture"])
            .env("COPYBOT_MEMBERSHIP_APP_REPORT", &path)
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
    let v = &reports[1]["snapshot"];
    for (n, outcome) in [
        (20, Disposition::BestEffortBudget),
        (21, Disposition::BestEffortOutputPressure),
        (22, Disposition::BestEffortWriterPressure),
        (23, Disposition::CriticalBacklogFull),
        (24, Disposition::Buffered),
        (24, Disposition::RawInserted),
        (30, Disposition::RawInserted),
        (30, Disposition::RawDuplicate),
        (31, Disposition::RawInserted),
        (32, Disposition::RawUnknown),
    ] {
        assert_eq!(count(v, n, outcome), 1, "case {n}, {outcome:?}");
    }
    assert_eq!(count(v, 24, Disposition::BestEffortWriterPressure), 0);
    assert_eq!(count(v, 24, Disposition::CriticalBacklogFull), 0);
    assert_eq!(count(v, 31, Disposition::RawUnknown), 0);
    assert_eq!(count(v, 32, Disposition::RawInserted), 0);
    assert_eq!(count(v, 20, Disposition::ContextBestEffort), 1);
    assert!(count(v, 23, Disposition::ContextDiscoveryCritical) > 0);
    assert_eq!(v["complete"], false); // actual nonretryable result is conservatively unknown
    Ok(())
}

async fn refusal_cases() -> Result<Value> {
    let mut outcomes = Vec::new();
    for n in 20..=22 {
        let mut c = Case::new(n)?;
        let (writer, _receiver) = c.paused_writer();
        if n == 20 {
            c.budget.mark_exhausted(StdInstant::now());
        }
        if n == 22 {
            assert!(writer.try_enqueue(&chosen(90))?);
        }
        let row = chosen(n);
        note(&row);
        c.handle(&writer, row, n == 21).await?;
        assert!(c.pending.is_empty());
        assert!(c.rows()?.is_empty());
        outcomes.push(json!({"case":n,"rows":0,"pending":0}));
        writer.shutdown()?;
    }
    let mut c = Case::new(23)?;
    let (writer, _receiver) = c.paused_writer();
    assert!(writer.try_enqueue(&chosen(91))?);
    let filler = chosen(92);
    for _ in 0..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY {
        c.pending.push_back(PendingIrrelevantObservedSwap {
            swap: filler.clone(),
            discovery_critical: true,
            processing_started_at: StdInstant::now(),
            backpressure_started_at: StdInstant::now(),
            last_backpressure_log_at: None,
        });
    }
    c.critical();
    let row = chosen(23);
    note(&row);
    c.handle(&writer, row, false).await?;
    assert_eq!(
        c.pending.len(),
        DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
    );
    outcomes.push(json!({"case":23,"pending":c.pending.len(),"rows":c.rows()?.len()}));
    writer.shutdown()?;

    let mut c = Case::new(24)?;
    let (mut writer, receiver) = c.paused_writer();
    assert!(writer.try_enqueue(&chosen(93))?);
    c.critical();
    let row = chosen(24);
    note(&row);
    c.handle(&writer, row, false).await?;
    assert_eq!(c.pending.len(), 1);
    let before: Value = serde_json::from_str(&membership::snapshot_json())?;
    assert_eq!(count(&before, 24, Disposition::RawInserted), 0);
    assert_eq!(count(&before, 24, Disposition::BestEffortWriterPressure), 0);
    c.start_receiver(&mut writer, receiver);
    c.retry_buffer(&writer).await?;
    writer.shutdown()?;
    outcomes.push(json!({"case":24,"pending":c.pending.len(),"rows_in_order":c.rows()?}));
    Ok(json!(outcomes))
}

#[test]
fn child() -> Result<()> {
    let Ok(report) = std::env::var("COPYBOT_MEMBERSHIP_APP_REPORT") else {
        return Ok(());
    };
    membership::initialize_from_env();
    let rt = Builder::new_current_thread().enable_all().build()?;
    let business = rt.block_on(async {
        Ok::<_, anyhow::Error>(json!({"refusals":refusal_cases().await?,
        "commits":commit::commit_cases().await?}))
    })?;
    membership::finish();
    let snapshot: Value = serde_json::from_str(&membership::snapshot_json())?;
    std::fs::write(
        report,
        json!({"business":business,"snapshot":snapshot}).to_string(),
    )?;
    Ok(())
}
