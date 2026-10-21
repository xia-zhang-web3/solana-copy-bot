//! Captures the actual tracing field emitted by changed Rust ACK observation.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tracing::{
    span::{Attributes, Id, Record},
    Event, Metadata, Subscriber,
};
#[derive(Clone, Default)]
pub(super) struct Trace(pub Arc<Mutex<Vec<String>>>, pub Arc<AtomicBool>);
struct Visit<'a>(&'a mut Vec<String>);
impl tracing::field::Visit for Visit<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "ack_observation_json" {
            self.0.push(format!("{value:?}"));
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "ack_observation_json" {
            self.0.push(value.into());
        }
    }
}
impl Subscriber for Trace {
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::TRACE)
    }
    fn register_callsite(&self, _: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::always()
    }
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut found = Vec::new();
        event.record(&mut Visit(&mut found));
        if !found.is_empty() && self.1.load(Ordering::Acquire) {
            panic!("LOCAL diagnostic ACK subscriber failure");
        }
        let mut values = self.0.lock().unwrap();
        values.extend(found);
        assert!(values.len() <= 8, "finite local fixture tracing only");
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}
