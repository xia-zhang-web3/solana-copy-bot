use super::*;

impl ObservedSwapWriter {
    pub(crate) fn shutdown(mut self) -> Result<()> {
        drop(self.sender);
        let raw_result = if let Some(raw_worker) = self.raw_worker.take() {
            Some(
                raw_worker
                    .join()
                    .map_err(|payload| anyhow!(panic_payload_to_string(payload.as_ref())))
                    .context("observed swap writer thread panicked")?,
            )
        } else {
            None
        };
        let journal_result = if let Some(journal_worker) = self.journal_worker.take() {
            Some(
                journal_worker
                    .join()
                    .map_err(|payload| anyhow!(panic_payload_to_string(payload.as_ref())))
                    .context("recent raw journal writer thread panicked")?,
            )
        } else {
            None
        };
        if let Some(result) = raw_result {
            result.context("observed swap writer thread failed")?;
        }
        if let Some(result) = journal_result {
            result.context("recent raw journal writer thread failed")?;
        }
        Ok(())
    }
}
