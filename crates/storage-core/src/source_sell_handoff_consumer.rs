use crate::{
    source_sell_event as event, source_sell_handoff_rows as rows,
    ExecutionSourceSellOutcome as Outcome, ExecutionSourceSellReject as Reject,
    SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};

impl SqliteDiscoveryStore {
    pub fn process_source_sell_handoff(&self, signature: &str) -> Result<Outcome> {
        self.with_immediate_transaction_retry("consume source SELL handoff", |conn| {
            crate::source_sell_handoff_schema::required(conn)?;
            let job =
                rows::load(conn, signature)?.context("pending source SELL handoff missing")?;
            let Some(position) = job.original_position_id.as_deref() else {
                return Ok(Outcome::Rejected(Reject::SourceNotProven));
            };
            if job.disposition == "refused" {
                return Ok(Outcome::Rejected(
                    serde_json::from_str(&job.reason).context("invalid terminal handoff reason")?,
                ));
            }
            // Durable staging already owns this exact immutable identity. This is
            // delivery idempotence only; promotion and signing still revalidate.
            let existing =
                crate::source_sell_intent_rows::load(conn, &event::intent_id(signature))?;
            let outcome = if let Some(staged) = existing {
                ensure!(
                    event::same(&job.event, &staged.event) && staged.position_id == position,
                    "source SELL handoff/staging identity conflict"
                );
                Outcome::Existing(staged)
            } else {
                ensure!(
                    job.disposition == "pending",
                    "completed handoff staging disappeared"
                );
                crate::execution_source_sell_intent::stage_on_conn(
                    self, conn, &job.event, position,
                )?
            };
            if job.disposition == "pending" {
                match &outcome {
                    Outcome::Inserted(_) | Outcome::Existing(_) => {
                        rows::finish(conn, signature, "staged", "staged")?
                    }
                    Outcome::Rejected(reason) => {
                        rows::finish(conn, signature, "refused", &serde_json::to_string(reason)?)?
                    }
                }
            }
            Ok(outcome)
        })
        .context("process durable source SELL handoff")
    }
}
