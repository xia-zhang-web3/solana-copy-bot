//! Immutable HTTP capabilities; results return to the sole execution owner.
use super::{hot_observed::*, QuoteEventBundle};
use crate::execution_quote_canary_helpers::*;
use crate::execution_quote_canary_priority_fee::PriorityFeeSampler;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::SwapEvent;
use copybot_storage_core::{ExecutionCanaryObservedLeg, ExecutionQuoteCanaryEventInsert};

pub(crate) struct HotQuoteOrigin {
    pub(crate) swap: SwapEvent,
    pub(crate) requested_at: DateTime<Utc>,
    pub(crate) observed: ExecutionCanaryObservedLeg,
    pub(crate) existing: Option<ExecutionQuoteCanaryEventInsert>,
    pub(crate) wait_for_shadow: bool,
    pub(crate) saved_entry: Option<ExecutionQuoteCanaryEventInsert>,
    pub(crate) shadow_finished: bool,
    pub(crate) shadow_recorded: Option<copybot_shadow::ShadowSignalResult>,
    pub(crate) buy_receipt: Option<copybot_shadow::RecordedBuyLot>,
    pub(crate) _claim: super::claims::EntryClaim,
}
impl HotQuoteOrigin {
    pub(crate) fn signal_id(&self) -> String {
        observed_buy_signal_id(&self.swap)
    }
    pub(crate) fn event_id(&self) -> String {
        entry_quote_event_id(&self.signal_id())
    }
}
pub(crate) struct HotQuoteJob {
    http: reqwest::Client,
    config: ExecutionConfig,
    priority: PriorityFeeSampler,
    swap: SwapEvent,
    requested_at: DateTime<Utc>,
    existing: bool,
}
pub(crate) struct HotQuoteAdmission {
    pub(crate) origin: HotQuoteOrigin,
    pub(crate) job: HotQuoteJob,
}
pub(crate) enum HotQuoteOutput {
    Fresh(QuoteEventBundle),
    ExistingPriority(Option<PriorityFeeSample>),
}
impl HotQuoteJob {
    pub(super) fn new(
        http: reqwest::Client,
        config: ExecutionConfig,
        priority: PriorityFeeSampler,
        swap: SwapEvent,
        requested_at: DateTime<Utc>,
        existing: bool,
    ) -> Self {
        Self {
            http,
            config,
            priority,
            swap,
            requested_at,
            existing,
        }
    }
    pub(crate) async fn run(self) -> HotQuoteOutput {
        if self.existing {
            return HotQuoteOutput::ExistingPriority(self.priority.sample_if_enabled().await);
        }
        let signal_id = observed_buy_signal_id(&self.swap);
        let mut bundle = match super::hot_network::build_hot_observed_buy_quote_event(
            &self.http,
            &self.config,
            &signal_id,
            &self.swap,
            self.requested_at,
            None,
        )
        .await
        {
            Ok(bundle) => bundle,
            Err(error) => QuoteEventBundle::event_only(hot_observed_buy_error_event(
                &signal_id,
                &self.swap,
                self.requested_at,
                &error,
            )),
        };
        let priority = self.priority.sample_if_enabled().await;
        attach_priority_fee(&mut bundle.event, priority.as_ref());
        HotQuoteOutput::Fresh(bundle)
    }
}
