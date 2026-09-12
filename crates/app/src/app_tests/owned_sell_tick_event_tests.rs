use super::open_risk_sell_fixture::TOKEN;
use super::owned_sell_queue_fixture::Queue;
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

fn capture_tick(summary: &ExecutionCanaryTickSummary) -> BTreeMap<String, String> {
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
async fn owned_sell_tick_event_reports_failed_a_while_b_submits_once() -> Result<()> {
    for (old_count, suffix) in [(1, String::new()), (2, "д".repeat(1000))] {
        let mut q = Queue::new(old_count, old_count as u32 + 1).await?;
        q.intake.conn()?.execute_batch(&format!(
            "CREATE TRIGGER fail_A_quote BEFORE INSERT ON execution_quote_canary_events
             WHEN NEW.token = '{TOKEN}' BEGIN
             SELECT RAISE(ABORT, 'synthetic_A_quote_failure{suffix}'); END;"
        ))?;
        q.intake.f.reopen()?;
        let summary = q.intake.tick().await?;
        assert!(summary.has_status_change(), "main loop must emit this tick");
        q.assert_a_pending()?;
        q.assert_b_submitted()?;
        for a in &q.a {
            assert!(q
                .intake
                .f
                .store
                .load_execution_quote_canary_event_by_id(&Queue::quote_id(a))?
                .is_none());
        }
        let event = capture_tick(&summary);
        assert_eq!(event["quote_close_errors"], old_count.to_string());
        assert_eq!(event["quote_decision_unknown"], old_count.to_string());
        assert_eq!(event["last_quote_event_id"], Queue::quote_id(&q.b));
        assert_eq!(
            event.get("owned_sell_recovery_signal_id"),
            Some(&q.a.last().unwrap().signal_id),
            "a successful B must not replace the failing A identity"
        );
        let error = &event["owned_sell_recovery_error"];
        assert!(error.contains("synthetic_A_quote_failure"));
        assert!(error.contains(&q.a.last().unwrap().signal_id));
        assert!(!error.contains(&q.b.signal_id));
        assert!(error.chars().count() <= 503, "bounded existing reason");
        if !suffix.is_empty() {
            assert!(error.ends_with("..."));
            assert!(!error.contains(&suffix));
        }
        // A later clean tick must not repeat the previous recovery error or submit B again.
        q.intake
            .conn()?
            .execute_batch("DROP TRIGGER fail_A_quote")?;
        q.intake.f.reopen()?;
        let clean = q.intake.tick().await?;
        assert!(clean.has_status_change());
        let event = capture_tick(&clean);
        assert_eq!(event["owned_sell_recovery_signal_id"], "none");
        assert_eq!(event["owned_sell_recovery_error"], "none");
        q.assert_a_pending()?;
        q.assert_b_submitted()?;
        q.finish().await?;
    }
    Ok(())
}

#[test]
fn owned_sell_tick_event_does_not_attribute_unrelated_error_payload_to_last_signal() {
    let summary = ExecutionCanaryTickSummary {
        last_signal_id: Some("successful-B".into()),
        last_quote_event_id: Some("quote:successful-B".into()),
        last_error: Some("unrelated SYNTHETIC_PRIVATE_PAYLOAD".into()),
        ..Default::default()
    };
    let event = capture_tick(&summary);
    assert_eq!(event["last_signal_id"], "successful-B");
    assert_eq!(event["last_quote_event_id"], "quote:successful-B");
    assert_eq!(event["owned_sell_recovery_signal_id"], "none");
    assert_eq!(event["owned_sell_recovery_error"], "none");
    assert!(event
        .values()
        .all(|value| !value.contains("SYNTHETIC_PRIVATE_PAYLOAD")));
}

#[tokio::test]
async fn owned_sell_tick_event_preserves_accounting_a_identity_while_b_submits() -> Result<()> {
    use super::receipt_accounting_isolation_tests::ready_queue;
    use tracing::instrument::WithSubscriber;
    for failure in ["fill", "proof"] {
        let mut q = ready_queue(failure).await?;
        let events = Events::default();
        for _ in 0..3 {
            q.intake.f.reopen()?;
            let subscriber = tracing_subscriber::registry().with(events.clone());
            q.intake.tick().with_subscriber(subscriber).await?;
            q.assert_a_pending()?;
            q.assert_b_submitted()?;
        }
        let captured = events.0.lock().unwrap();
        let failures: Vec<_> = captured
            .iter()
            .filter(|e| {
                e.get("reason").map(String::as_str) == Some("receipt_accounting_write_failed")
            })
            .collect();
        assert_eq!(
            failures.len(),
            3,
            "one bounded event per failed reconciliation"
        );
        for event in failures {
            assert_eq!(event["order_id"], q.blocker);
            assert_eq!(event["accounting_status"], "pending");
            assert_eq!(event["level"], "WARN");
            assert_eq!(event["target"], "copybot_app::app_loop");
            assert!(event.values().all(|v| !v.contains(&q.b.signal_id)));
        }
        drop(captured);
        q.finish().await?;
    }
    Ok(())
}
