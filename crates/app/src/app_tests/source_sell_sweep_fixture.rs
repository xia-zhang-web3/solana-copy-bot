#![allow(dead_code)]
pub(super) use super::source_write_off_fixture::*;
use anyhow::Result;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionCanaryOrder, ExecutionSourceSellPromotionOutcome};

pub(super) fn add_signal(f: &Fixture, name: &str) -> Result<CopySignalRow> {
    let intent = staged(&f.store, name, f.now)?;
    let ExecutionSourceSellPromotionOutcome::Inserted(binding) = f
        .store
        .promote_execution_source_sell_intent(&intent.intent_id)?
    else {
        anyhow::bail!("test promotion refused");
    };
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&binding.signal_id)?
        .unwrap();
    let mut event = quote(&signal, f.now);
    event.event_id = format!("quote:{name}");
    f.store.record_execution_quote_canary_event(&event)?;
    Ok(signal)
}

pub(super) struct Prefix {
    pub f: Fixture,
    pub old: Vec<CopySignalRow>,
    pub b: ExecutionCanaryOrder,
}
impl Prefix {
    pub fn new(count: usize, simulation: bool, mixed: bool) -> Result<Self> {
        let f = Fixture::new(7000)?;
        let mut old = vec![f.signal.clone()];
        // Spare durable old intents permit genuinely later failed attempts between ticks.
        for n in 1..count + 8 {
            old.push(add_signal(&f, &format!("old-{n:03}"))?);
        }
        f.replace(7000)?;
        let signal = add_signal(&f, "valid-b")?;
        let b = fail_order(
            &f.store,
            &signal.signal_id,
            ROUTE,
            f.now + chrono::Duration::seconds(2),
            simulation || mixed,
            1,
        )?;
        for signal in &old[..count] {
            fail_order(
                &f.store,
                &signal.signal_id,
                ROUTE,
                f.now + chrono::Duration::seconds(2),
                simulation,
                1,
            )?;
        }
        Ok(Self { f, old, b })
    }
    pub fn later_a(&self, index: usize, simulation: bool) -> Result<ExecutionCanaryOrder> {
        fail_order(
            &self.f.store,
            &self.old[index].signal_id,
            ROUTE,
            self.f.now + chrono::Duration::seconds(100 + index as i64),
            simulation,
            1,
        )
    }
}

pub(super) fn no_business_change(
    f: &Fixture,
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    snapshot(&f.conn()?, &["execution_failed_sell_sweep_cursors"])
}
