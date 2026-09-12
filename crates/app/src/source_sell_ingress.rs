use crate::source_sell_staging::{record, SourceSellAdmission, SourceSellStaging, StageNotice};
use crate::{observed_swap_writer::ObservedSwapWriter, RelevantObservedSwapPersistence};
use anyhow::{Context, Result};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{is_fatal_sqlite_anyhow_error, SqliteStore};
use std::collections::{HashSet, VecDeque};

// Freshness comes only from ingress dedupe and stays bound to this event.
pub(crate) struct RecentSwapDelivery<'a> {
    swap: &'a SwapEvent,
    fresh: bool,
}

impl<'a> RecentSwapDelivery<'a> {
    pub(crate) fn note(
        recent: &mut HashSet<String>,
        recent_order: &mut VecDeque<String>,
        swap: &'a SwapEvent,
    ) -> Self {
        Self {
            swap,
            fresh: crate::note_recent_swap_signature(recent, recent_order, &swap.signature),
        }
    }

    pub(crate) fn swap(&self) -> &SwapEvent {
        self.swap
    }

    pub(crate) fn is_fresh(&self) -> bool {
        self.fresh
    }
}

pub(crate) enum SourceSellIngress {
    NotCandidate,
    Refused,
    Durable(RelevantObservedSwapPersistence),
}

pub(crate) async fn persist_and_schedule(
    store: &SqliteStore,
    writer: &ObservedSwapWriter,
    sqlite_path: &str,
    staging: &mut SourceSellStaging,
    delivery: &RecentSwapDelivery<'_>,
    recent: &mut HashSet<String>,
    recent_order: &mut VecDeque<String>,
) -> Result<SourceSellIngress> {
    staging.reap_ready()?;
    let swap = delivery.swap();
    // Snapshot generation before the first await, independently of follow/relevance.
    #[cfg(test)]
    crate::app_tests::b70_hooks::mark("capture_start", &swap.signature);
    let captured = match staging.capture(store, delivery) {
        Ok(SourceSellAdmission::NotOwnedSell) => return Ok(SourceSellIngress::NotCandidate),
        Ok(SourceSellAdmission::Refused(notice)) => {
            record(&swap.signature, notice, None);
            return Ok(SourceSellIngress::Refused);
        }
        Ok(SourceSellAdmission::Captured(captured)) => captured,
        Err(error) => {
            record(&swap.signature, StageNotice::HintFailed, Some(&error));
            if is_fatal_sqlite_anyhow_error(&error) {
                return Err(error).context("owned SELL hint failed with fatal sqlite I/O");
            }
            return Ok(SourceSellIngress::NotCandidate);
        }
    };
    #[cfg(test)]
    crate::app_tests::b70_hooks::mark("capture_proven", &swap.signature);
    let inserted =
        match crate::irrelevant_persistence::persist_relevant_observed_swap_with_candidate(
            writer,
            recent,
            recent_order,
            swap,
            Some(captured.candidate()),
        )
        .await
        {
            Ok(inserted) => inserted,
            Err(error) => {
                record(
                    &swap.signature,
                    StageNotice::PersistenceFailed,
                    Some(&error),
                );
                if crate::observed_swap_writer_error_requires_restart(&error)
                    || is_fatal_sqlite_anyhow_error(&error)
                {
                    return Err(error)
                        .context("owned SELL observed writer failure requires restart");
                }
                return Ok(SourceSellIngress::Refused);
            }
        };
    #[cfg(test)]
    crate::app_tests::b70_hooks::mark(
        if inserted {
            "ack_inserted"
        } else {
            "ack_duplicate"
        },
        &swap.signature,
    );
    let notice = staging.acknowledge_and_schedule(captured, inserted, store, sqlite_path)?;
    record(&swap.signature, notice, None);
    Ok(SourceSellIngress::Durable(
        crate::relevant_observed_swap_persistence(inserted),
    ))
}
