use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tracing::{field::Visit, instrument::WithSubscriber, Event, Subscriber};
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
        if event.metadata().target() != "copybot_app::source_sell_staging" {
            return;
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}
pub(super) async fn capture<F: Future>(future: F) -> (F::Output, Vec<BTreeMap<String, String>>) {
    let events = Events::default();
    let subscriber = tracing_subscriber::registry().with(events.clone());
    let result = future.with_subscriber(subscriber).await;
    let captured = events.0.lock().unwrap().clone();
    (result, captured)
}
