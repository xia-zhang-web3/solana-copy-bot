use super::*;

impl Ingress {
    pub async fn reopen_without_delivery_memory(&mut self) -> Result<()> {
        self.finish().await?; // Joins all old workers and writer; no orphan spawn_blocking.
        self.scheduler = ShadowScheduler::new();
        self.recent.clear();
        self.recent_order.clear();
        self.lots.clear();
        self.reopen()?;
        self.writer = Some(ObservedSwapWriter::start_for_test(
            self.path.to_string_lossy().into(),
            8,
            8,
        )?);
        self.finished = false;
        Ok(())
    }
}
