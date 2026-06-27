use crate::track_b_entry_quote_report_summary::{
    summarize_bucket, summarize_close_buckets, summarize_exit_executability, CleanEvent,
};
use crate::track_b_entry_quote_report_types::{BucketSummary, CohortSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RankCohort {
    Rank1To15,
    Rank16To30,
    RankGt30,
    Unranked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceCohort {
    Baseline,
    SlowHold,
    Other,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldTimeBucket {
    Under5Min,
    Min5To15,
    Min15To60,
    Hour1To6,
    Over6Hours,
}

impl SourceCohort {
    pub(crate) fn from_source(source: Option<&str>) -> Self {
        match source {
            Some("baseline") => Self::Baseline,
            Some("slow_hold") => Self::SlowHold,
            Some(_) => Self::Other,
            None => Self::Unknown,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::SlowHold => "slow_hold",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }
}

impl RankCohort {
    pub(crate) fn from_rank(rank: Option<u64>) -> Self {
        match rank {
            Some(1..=15) => Self::Rank1To15,
            Some(16..=30) => Self::Rank16To30,
            Some(_) => Self::RankGt30,
            None => Self::Unranked,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Rank1To15 => "rank_1_15",
            Self::Rank16To30 => "rank_16_30",
            Self::RankGt30 => "rank_gt_30",
            Self::Unranked => "unranked",
        }
    }

    fn rank_bounds(self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Rank1To15 => (Some(1), Some(15)),
            Self::Rank16To30 => (Some(16), Some(30)),
            Self::RankGt30 | Self::Unranked => (None, None),
        }
    }
}

impl HoldTimeBucket {
    pub(crate) fn from_seconds(seconds: i64) -> Self {
        match seconds {
            ..=299 => Self::Under5Min,
            300..=899 => Self::Min5To15,
            900..=3_599 => Self::Min15To60,
            3_600..=21_599 => Self::Hour1To6,
            _ => Self::Over6Hours,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Under5Min => "under_5m",
            Self::Min5To15 => "5m_to_15m",
            Self::Min15To60 => "15m_to_60m",
            Self::Hour1To6 => "1h_to_6h",
            Self::Over6Hours => "over_6h",
        }
    }
}

pub(crate) fn summarize_hold_time_buckets(events: &[CleanEvent]) -> Vec<BucketSummary> {
    [
        HoldTimeBucket::Under5Min,
        HoldTimeBucket::Min5To15,
        HoldTimeBucket::Min15To60,
        HoldTimeBucket::Hour1To6,
        HoldTimeBucket::Over6Hours,
    ]
    .into_iter()
    .map(|bucket| {
        summarize_bucket(
            bucket.label(),
            events
                .iter()
                .filter(move |event| event.hold_time_bucket == bucket),
        )
    })
    .collect()
}

pub(crate) fn summarize_rank_cohorts(events: &[CleanEvent]) -> Vec<CohortSummary> {
    [
        RankCohort::Rank1To15,
        RankCohort::Rank16To30,
        RankCohort::RankGt30,
        RankCohort::Unranked,
    ]
    .into_iter()
    .map(|cohort| {
        let cohort_events = events
            .iter()
            .filter(|event| event.cohort == cohort)
            .cloned()
            .collect::<Vec<_>>();
        let (rank_min, rank_max) = cohort.rank_bounds();
        CohortSummary {
            cohort: cohort.label().to_string(),
            rank_min,
            rank_max,
            events: cohort_events.len() as u64,
            by_close_bucket: summarize_close_buckets(&cohort_events),
            by_exit_executability: summarize_exit_executability(&cohort_events),
        }
    })
    .collect()
}

pub(crate) fn summarize_source_cohorts(events: &[CleanEvent]) -> Vec<CohortSummary> {
    [
        SourceCohort::Baseline,
        SourceCohort::SlowHold,
        SourceCohort::Other,
        SourceCohort::Unknown,
    ]
    .into_iter()
    .map(|cohort| {
        let cohort_events = events
            .iter()
            .filter(|event| event.source_cohort == cohort)
            .cloned()
            .collect::<Vec<_>>();
        CohortSummary {
            cohort: cohort.label().to_string(),
            rank_min: None,
            rank_max: None,
            events: cohort_events.len() as u64,
            by_close_bucket: summarize_close_buckets(&cohort_events),
            by_exit_executability: summarize_exit_executability(&cohort_events),
        }
    })
    .collect()
}
