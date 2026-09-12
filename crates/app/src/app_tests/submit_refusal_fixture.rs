use super::priority_fee_route_fixture::Fixture;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::{field::Visit, Event, Subscriber};
use tracing_subscriber::{layer::Context, prelude::*, Layer};

pub(super) async fn seed_hot(f: &Fixture) -> Result<copybot_core_types::CopySignalRow> {
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&f.request.signal_id)?
        .unwrap();
    f.conn()?.execute(
        "DELETE FROM orders WHERE order_id=?1",
        [&f.request.order_id],
    )?;
    super::execution_state_machine_tiny_submit_route::record_tiny_route_quote(
        &f.store, &signal, f.now,
    )?;
    f.conn()?.execute(
        "UPDATE execution_quote_canary_events SET quote_price_sol=?1, quote_response_json=?2,
         quote_in_amount_raw=?3, quote_out_amount_raw=?4, route_plan_json=?5 WHERE signal_id=?6",
        rusqlite::params![
            f.request.metadata.quote_price_sol,
            f.request.metadata.quote_response_json,
            f.request.metadata.quote_in_amount_raw,
            f.request.metadata.quote_out_amount_raw,
            f.request.metadata.route_plan_json,
            f.request.signal_id
        ],
    )?;
    Ok(signal)
}

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
        self.0.lock().unwrap().push(fields.0);
    }
}
pub(super) fn capture(emit: impl FnOnce()) -> BTreeMap<String, String> {
    let events = Events::default();
    let subscriber = tracing_subscriber::registry().with(events.clone());
    tracing::subscriber::with_default(subscriber, emit);
    let mut captured = events.0.lock().unwrap();
    assert_eq!(captured.len(), 1, "exactly one existing production event");
    let event = captured.pop().unwrap();
    assert_eq!(event["target"], "copybot_app::app_loop");
    event
}
pub(super) fn check_event(
    event: &BTreeMap<String, String>,
    order_id: &str,
    reason: &str,
    count: usize,
) {
    assert_eq!(event["pre_submit_refusals"], count.to_string());
    assert_eq!(event["pre_submit_refusal_order_id"], order_id);
    assert_eq!(event["pre_submit_refusal_reason"], reason);
    assert!(event
        .values()
        .all(|v| !v.contains("SYNTHETIC_PRIVATE_PAYLOAD")));
    assert!(event["pre_submit_refusal_reason"].len() <= 64);
    eprintln!("B26_R1_EVENT {}", serde_json::to_string(event).unwrap());
}
