use super::*;

impl DiscoveryService {
    pub(super) fn recent_raw_nonnegative_row_lag(
        source_row_count: Option<usize>,
        reference_row_count: Option<usize>,
    ) -> Option<u64> {
        Some(source_row_count?.saturating_sub(reference_row_count?) as u64)
    }

    pub(super) fn recent_raw_nonnegative_cursor_time_lag_seconds(
        source_cursor: Option<&DiscoveryRuntimeCursor>,
        reference_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Option<u64> {
        let seconds = source_cursor?
            .ts_utc
            .signed_duration_since(reference_cursor?.ts_utc)
            .num_seconds();
        Some(seconds.max(0) as u64)
    }

    pub(super) fn recent_raw_signed_row_delta(
        candidate_row_count: Option<usize>,
        reference_row_count: Option<usize>,
    ) -> Option<i64> {
        let candidate = i128::try_from(candidate_row_count?).ok()?;
        let reference = i128::try_from(reference_row_count?).ok()?;
        let delta = candidate - reference;
        Some(delta.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
    }

    pub(super) fn recent_raw_signed_cursor_time_delta_seconds(
        candidate_cursor: Option<&DiscoveryRuntimeCursor>,
        reference_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Option<i64> {
        Some(
            candidate_cursor?
                .ts_utc
                .signed_duration_since(reference_cursor?.ts_utc)
                .num_seconds(),
        )
    }

    pub(super) fn recent_raw_optional_ts_newer_than(
        candidate: Option<&DateTime<Utc>>,
        reference: Option<&DateTime<Utc>>,
    ) -> Option<bool> {
        Some(candidate? > reference?)
    }

    pub(super) fn recent_raw_lineage_relation_from_covered_since(
        candidate: Option<&DateTime<Utc>>,
        reference: Option<&DateTime<Utc>>,
        same_source: Option<bool>,
    ) -> RecentRawLineageRelation {
        match same_source {
            Some(false) => RecentRawLineageRelation::DifferentSource,
            None => RecentRawLineageRelation::Unproven,
            Some(true) => match (candidate, reference) {
                (Some(candidate), Some(reference)) => match candidate.cmp(reference) {
                    Ordering::Less => RecentRawLineageRelation::Ahead,
                    Ordering::Equal => RecentRawLineageRelation::Equal,
                    Ordering::Greater => RecentRawLineageRelation::Behind,
                },
                _ => RecentRawLineageRelation::Unproven,
            },
        }
    }

    pub(super) fn recent_raw_lineage_relation_from_ord<T: Ord>(
        candidate: Option<T>,
        reference: Option<T>,
        same_source: Option<bool>,
    ) -> RecentRawLineageRelation {
        match same_source {
            Some(false) => RecentRawLineageRelation::DifferentSource,
            None => RecentRawLineageRelation::Unproven,
            Some(true) => match (candidate, reference) {
                (Some(candidate), Some(reference)) => match candidate.cmp(&reference) {
                    Ordering::Greater => RecentRawLineageRelation::Ahead,
                    Ordering::Equal => RecentRawLineageRelation::Equal,
                    Ordering::Less => RecentRawLineageRelation::Behind,
                },
                _ => RecentRawLineageRelation::Unproven,
            },
        }
    }

    pub(super) fn recent_raw_lineage_relation_from_cursor(
        candidate: Option<&DiscoveryRuntimeCursor>,
        reference: Option<&DiscoveryRuntimeCursor>,
        same_source: Option<bool>,
    ) -> RecentRawLineageRelation {
        match same_source {
            Some(false) => RecentRawLineageRelation::DifferentSource,
            None => RecentRawLineageRelation::Unproven,
            Some(true) => match (candidate, reference) {
                (Some(candidate), Some(reference)) => {
                    match Self::runtime_cursor_cmp(candidate, reference) {
                        Ordering::Greater => RecentRawLineageRelation::Ahead,
                        Ordering::Equal => RecentRawLineageRelation::Equal,
                        Ordering::Less => RecentRawLineageRelation::Behind,
                    }
                }
                _ => RecentRawLineageRelation::Unproven,
            },
        }
    }

    pub(super) fn recent_raw_cursor_relation_explanation(
        same_source: Option<bool>,
        cursor_relation: RecentRawLineageRelation,
        ts_relation: RecentRawLineageRelation,
        slot_relation: RecentRawLineageRelation,
        signature_equal: Option<bool>,
    ) -> String {
        match same_source {
            Some(false) => "recent_raw staged cursor relation is derived from direct covered-through cursor comparison, but the staged and promoted manifests point at different source_db_path values, so the cursor relation is reported as different_source instead of comparing unrelated lineages".to_string(),
            None => "recent_raw staged cursor relation is derived from direct covered-through cursor comparison, but one or both manifests are missing the source_db_path or covered-through cursor needed to prove the relation".to_string(),
            Some(true) => format!(
                "recent_raw staged cursor relation is derived from direct covered-through cursor comparison using ts_utc, then slot, then signature; result={}, ts_relation={}, slot_relation={}, signature_equal={}",
                serde_json::to_string(&cursor_relation)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                serde_json::to_string(&ts_relation)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                serde_json::to_string(&slot_relation)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                signature_equal
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string())
            ),
        }
    }

    pub(super) fn recent_raw_lineage_relation_from_optional_row_count(
        candidate: Option<usize>,
        reference: Option<usize>,
        same_source: Option<bool>,
    ) -> RecentRawLineageRelation {
        match same_source {
            Some(false) => RecentRawLineageRelation::DifferentSource,
            None => RecentRawLineageRelation::Unproven,
            Some(true) => match (candidate, reference) {
                (Some(candidate), Some(reference)) => match candidate.cmp(&reference) {
                    Ordering::Greater => RecentRawLineageRelation::Ahead,
                    Ordering::Equal => RecentRawLineageRelation::Equal,
                    Ordering::Less => RecentRawLineageRelation::Behind,
                },
                _ => RecentRawLineageRelation::Unproven,
            },
        }
    }

    pub(super) fn recent_raw_candidate_closer_to_source_than_reference(
        relation: Option<RecentRawManifestProgressRelation>,
        source_outruns_candidate: Option<bool>,
        source_outruns_reference: Option<bool>,
    ) -> Option<bool> {
        if source_outruns_candidate == Some(false) && source_outruns_reference == Some(true) {
            return Some(true);
        }
        if source_outruns_candidate == Some(true) && source_outruns_reference == Some(false) {
            return Some(false);
        }
        match relation {
            Some(RecentRawManifestProgressRelation::CandidateAhead) => Some(true),
            Some(
                RecentRawManifestProgressRelation::Equivalent
                | RecentRawManifestProgressRelation::ReferenceAhead,
            ) => Some(false),
            None => None,
        }
    }
}
