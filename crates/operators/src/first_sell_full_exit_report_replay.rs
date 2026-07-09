use crate::first_sell_full_exit_report_db::{
    AuditEvidence, CloseEvidence, EntryEvidence, SellQuoteEvidence, SellSignalEvidence,
};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayPolicy {
    DaemonTokenFirstSell,
    OriginWalletFirstSell,
}

impl ReplayPolicy {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::DaemonTokenFirstSell => "daemon_token_first_sell",
            Self::OriginWalletFirstSell => "origin_wallet_first_sell",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ExitTrigger {
    SellSignal {
        signal: SellSignalEvidence,
        quote: Option<SellQuoteEvidence>,
    },
    ShadowClose {
        close: CloseEvidence,
        quote: Option<SellQuoteEvidence>,
    },
}

impl ExitTrigger {
    pub(crate) fn ts(&self) -> DateTime<Utc> {
        match self {
            Self::SellSignal { signal, .. } => signal.ts,
            Self::ShadowClose { close, .. } => close.closed_ts,
        }
    }

    pub(crate) fn wallet_id(&self) -> &str {
        match self {
            Self::SellSignal { signal, .. } => &signal.wallet_id,
            Self::ShadowClose { close, .. } => &close.wallet_id,
        }
    }

    pub(crate) fn quote(&self) -> Option<&SellQuoteEvidence> {
        match self {
            Self::SellSignal { quote, .. } | Self::ShadowClose { quote, .. } => quote.as_ref(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayRow {
    pub(crate) entry: EntryEvidence,
    pub(crate) trigger: Option<ExitTrigger>,
}

pub(crate) fn replay_policy(evidence: &AuditEvidence, policy: ReplayPolicy) -> Vec<ReplayRow> {
    let index = EvidenceIndex::new(evidence);
    evidence
        .entries
        .iter()
        .cloned()
        .map(|entry| {
            let signal = index.first_signal(&entry, policy).cloned();
            let close = index.first_close(&entry, policy).cloned();
            let trigger = choose_trigger(signal, close, &index.quotes_by_signal);
            ReplayRow { entry, trigger }
        })
        .collect()
}

struct EvidenceIndex<'a> {
    signals_by_token: HashMap<&'a str, Vec<&'a SellSignalEvidence>>,
    signals_by_origin: HashMap<(&'a str, &'a str), Vec<&'a SellSignalEvidence>>,
    closes_by_entry: HashMap<(&'a str, &'a str, i64), Vec<&'a CloseEvidence>>,
    stale_closes_by_token: HashMap<&'a str, Vec<&'a CloseEvidence>>,
    quotes_by_signal: HashMap<&'a str, &'a SellQuoteEvidence>,
}

impl<'a> EvidenceIndex<'a> {
    fn new(evidence: &'a AuditEvidence) -> Self {
        let mut signals_by_token: HashMap<&str, Vec<&SellSignalEvidence>> = HashMap::new();
        let mut signals_by_origin: HashMap<(&str, &str), Vec<&SellSignalEvidence>> = HashMap::new();
        for signal in &evidence.sell_signals {
            signals_by_token
                .entry(&signal.token)
                .or_default()
                .push(signal);
            signals_by_origin
                .entry((&signal.wallet_id, &signal.token))
                .or_default()
                .push(signal);
        }
        let mut closes_by_entry: HashMap<(&str, &str, i64), Vec<&CloseEvidence>> = HashMap::new();
        let mut stale_closes_by_token: HashMap<&str, Vec<&CloseEvidence>> = HashMap::new();
        for close in &evidence.closes {
            closes_by_entry
                .entry((
                    close.wallet_id.as_str(),
                    close.token.as_str(),
                    ts_key(close.opened_ts),
                ))
                .or_default()
                .push(close);
            if close.signal_id.starts_with("stale-close-")
                && daemon_stale_context(&close.close_context)
            {
                stale_closes_by_token
                    .entry(close.token.as_str())
                    .or_default()
                    .push(close);
            }
        }
        let mut quotes_by_signal: HashMap<&str, &SellQuoteEvidence> = HashMap::new();
        for quote in &evidence.sell_quotes {
            quotes_by_signal
                .entry(quote.signal_id.as_str())
                .and_modify(|current| {
                    if quote_is_better(quote, current) {
                        *current = quote;
                    }
                })
                .or_insert(quote);
        }
        Self {
            signals_by_token,
            signals_by_origin,
            closes_by_entry,
            stale_closes_by_token,
            quotes_by_signal,
        }
    }

    fn first_signal(
        &self,
        entry: &EntryEvidence,
        policy: ReplayPolicy,
    ) -> Option<&'a SellSignalEvidence> {
        let candidates = match policy {
            ReplayPolicy::DaemonTokenFirstSell => self.signals_by_token.get(entry.token.as_str()),
            ReplayPolicy::OriginWalletFirstSell => self
                .signals_by_origin
                .get(&(entry.wallet_id.as_str(), entry.token.as_str())),
        }?;
        let ready = entry.entry_ready_ts?;
        let index = candidates.partition_point(|signal| signal.ts < ready);
        candidates.get(index).copied()
    }

    fn first_close(
        &self,
        entry: &EntryEvidence,
        policy: ReplayPolicy,
    ) -> Option<&'a CloseEvidence> {
        let ready = entry.entry_ready_ts?;
        let origin = self
            .closes_by_entry
            .get(&(
                entry.wallet_id.as_str(),
                entry.token.as_str(),
                ts_key(entry.signal_ts),
            ))
            .and_then(|closes| first_close_at_or_after(closes, ready));
        if policy == ReplayPolicy::OriginWalletFirstSell {
            return origin;
        }
        let stale = self
            .stale_closes_by_token
            .get(entry.token.as_str())
            .and_then(|closes| first_close_at_or_after(closes, ready));
        [origin, stale]
            .into_iter()
            .flatten()
            .min_by_key(|close| close.closed_ts)
    }
}

fn daemon_stale_context(context: &str) -> bool {
    matches!(
        context,
        "market"
            | "stale_market_price"
            | "stale_quote_price"
            | "stale_terminal_zero_price"
            | "recovery_terminal_zero_price"
    )
}

fn first_close_at_or_after<'a>(
    closes: &[&'a CloseEvidence],
    ready: DateTime<Utc>,
) -> Option<&'a CloseEvidence> {
    let index = closes.partition_point(|close| close.closed_ts < ready);
    closes.get(index).copied()
}

fn choose_trigger(
    signal: Option<SellSignalEvidence>,
    close: Option<CloseEvidence>,
    quotes: &HashMap<&str, &SellQuoteEvidence>,
) -> Option<ExitTrigger> {
    match (signal, close) {
        (Some(signal), Some(close)) if close.closed_ts < signal.ts => {
            let quote = quotes
                .get(close.signal_id.as_str())
                .map(|value| (*value).clone());
            Some(ExitTrigger::ShadowClose { close, quote })
        }
        (Some(signal), _) => {
            let quote = quotes
                .get(signal.signal_id.as_str())
                .map(|value| (*value).clone());
            Some(ExitTrigger::SellSignal { signal, quote })
        }
        (None, Some(close)) => {
            let quote = quotes
                .get(close.signal_id.as_str())
                .map(|value| (*value).clone());
            Some(ExitTrigger::ShadowClose { close, quote })
        }
        (None, None) => None,
    }
}

fn quote_is_better(candidate: &SellQuoteEvidence, current: &SellQuoteEvidence) -> bool {
    let candidate_owned = candidate.event_id.starts_with("quote:owned-");
    let current_owned = current.event_id.starts_with("quote:owned-");
    (candidate_owned && !current_owned)
        || (candidate_owned == current_owned && candidate.request_ts < current.request_ts)
}

fn ts_key(ts: DateTime<Utc>) -> i64 {
    ts.timestamp_nanos_opt()
        .unwrap_or_else(|| ts.timestamp_micros().saturating_mul(1_000))
}
