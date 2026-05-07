use super::*;

impl ShadowRiskGuard {
    #[cfg(test)]
    pub(crate) fn compute_infra_block_reason(&self, now: DateTime<Utc>) -> Option<String> {
        self.compute_infra_block_signal(now)
            .map(|signal| signal.reason)
    }

    pub(crate) fn yellowstone_output_queue_reason_context(
        &self,
        snapshot: &IngestionRuntimeSnapshot,
    ) -> Option<String> {
        if !self.uses_yellowstone_ingestion() || snapshot.yellowstone_output_queue_capacity == 0 {
            return None;
        }
        Some(format!(
            "yellowstone_output_queue_depth={} yellowstone_output_queue_capacity={} yellowstone_output_queue_fill_ratio={:.4} yellowstone_output_oldest_age_ms={}",
            snapshot.yellowstone_output_queue_depth,
            snapshot.yellowstone_output_queue_capacity,
            snapshot.yellowstone_output_queue_fill_ratio,
            snapshot.yellowstone_output_oldest_age_ms
        ))
    }

    pub(crate) fn enrich_infra_reason_with_yellowstone_queue_context(
        &self,
        snapshot: &IngestionRuntimeSnapshot,
        reason: String,
    ) -> String {
        match self.yellowstone_output_queue_reason_context(snapshot) {
            Some(context) => format!("{reason} {context}"),
            None => reason,
        }
    }

    pub(crate) fn build_infra_stop_details_json(&self, reason: &str) -> String {
        let mut details = serde_json::Map::new();
        details.insert(
            "reason".to_string(),
            serde_json::Value::String(reason.to_string()),
        );
        if self.uses_yellowstone_ingestion() {
            if let Some(snapshot) = self.infra_samples.back() {
                if snapshot.yellowstone_output_queue_capacity > 0 {
                    details.insert(
                        "yellowstone_output_queue_depth".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_queue_depth),
                    );
                    details.insert(
                        "yellowstone_output_queue_capacity".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_queue_capacity),
                    );
                    details.insert(
                        "yellowstone_output_queue_fill_ratio".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_queue_fill_ratio),
                    );
                    details.insert(
                        "yellowstone_output_oldest_age_ms".to_string(),
                        serde_json::Value::from(snapshot.yellowstone_output_oldest_age_ms),
                    );
                }
            }
        }
        serde_json::Value::Object(details).to_string()
    }
}
