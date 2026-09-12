use super::{
    buy_retry_queue_fixture::*, buy_retry_queue_http_fixture::QueueRpc,
    buy_retry_safety_fixture::reopen,
};
use crate::execution_canary::ExecutionCanaryTickSummary;
use crate::telemetry::record_execution_canary_tick;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::{field::Visit, Event, Subscriber};
use tracing_subscriber::{layer::Context, prelude::*, Layer};
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}
impl<S: Subscriber> Layer<S> for Events {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        fields
            .0
            .insert("target".into(), event.metadata().target().into());
        fields
            .0
            .insert("level".into(), event.metadata().level().to_string());
        self.0.lock().unwrap().push(fields.0);
    }
}

pub(super) fn capture_tick(summary: &ExecutionCanaryTickSummary) -> BTreeMap<String, String> {
    let events = Events::default();
    let subscriber = tracing_subscriber::registry().with(events.clone());
    tracing::subscriber::with_default(subscriber, || record_execution_canary_tick(summary));
    let mut captured = events.0.lock().unwrap();
    assert_eq!(captured.len(), 1, "one existing periodic event per tick");
    let event = captured.pop().unwrap();
    assert_eq!(event["target"], "copybot_app::app_loop");
    assert_eq!(event["level"], "INFO");
    assert_eq!(event["message"], "execution canary dry-run tick");
    event
}

#[tokio::test]
async fn root_b53_pending_a_tick_event_keeps_blocking_a_after_sell_b() -> Result<()> {
    let mut f = queue_fixture("root-b53-event", false).await?;
    f.config.canary_entry_submit_enabled = true;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 1;
    let buy = buy_order(&f)?;
    let pending = add_pending(&f, true)?;
    let sell = add_sell(&f, false)?;
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    *rpc.ordinary_pending.lock().unwrap() = true;
    let mut evidence = Vec::new();
    let mut after_b = None;
    for n in 0..4 {
        reopen(&mut f)?;
        let before = rpc
            .trace()
            .iter()
            .filter(|r| *r == "sendTransaction:sell")
            .count();
        let summary = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(8 + n),
            crate::execution_canary::ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n)),
        )
        .await?;
        assert!(
            summary.has_status_change(),
            "actual app loop emission condition"
        );
        let event = capture_tick(&summary);
        let after = rpc
            .trace()
            .iter()
            .filter(|r| *r == "sendTransaction:sell")
            .count();
        if after > before {
            after_b = Some(event.clone());
        }
        evidence.push(event);
    }
    let durable_a = f.store.load_execution_canary_order(&pending)?.unwrap();
    let unchanged_buy = buy_order(&f)?;
    let sell_confirmed = confirmed(&f, &sell);
    rpc.finish().await?;
    let trace = rpc.trace();
    println!(
        "ROOT_B53_EVENT {}",
        serde_json::json!({
            "pending_a":pending,"sell_b":sell,"events":evidence,"trace":trace,
            "durable_a_status":durable_a.status,"durable_a_reason":durable_a.simulation_error,
        })
    );
    sell_confirmed?;
    assert_eq!(unchanged_buy, buy);
    assert_eq!(
        trace
            .iter()
            .filter(|r| *r == "sendTransaction:sell")
            .count(),
        1
    );
    assert!(!trace.iter().any(|r| r == "sendTransaction:buy"));
    assert_eq!(
        durable_a.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    let event = after_b.expect("one actual tick dispatches allowed SELL B");
    assert_eq!(
        event["state_machine_skipped_reason"],
        "unresolved_buy_dispatch"
    );
    assert_eq!(event["last_state_machine_order_id"], sell);
    assert_eq!(event["buy_blocker_reason"], "unresolved_buy_dispatch");
    assert_eq!(
        event["buy_blocker_order_id"], pending,
        "unresolved BUY reason must identify A, not successful SELL B; actual event={event:?}"
    );
    Ok(())
}
