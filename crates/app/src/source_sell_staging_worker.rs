use super::*;
use anyhow::{anyhow, Context};
use copybot_storage_core::{
    is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error,
    ExecutionSourceSellOutcome as Outcome,
};
use tokio::task::JoinError;

pub(super) struct WorkOutput {
    result: Result<Outcome>,
    panicked: bool,
}

#[derive(Debug)]
pub(crate) struct StageCompletion {
    pub(crate) signature: String,
    pub(crate) notice: StageNotice,
}

impl SourceSellStaging {
    pub(super) fn spawn(&mut self, captured: CapturedSourceSell, sqlite_path: &str) {
        let task = captured.clone();
        let path = sqlite_path.to_owned();
        #[cfg(test)]
        let before_proof = self.before_proof.take();
        let handle = self.workers.spawn_blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(test)]
                if let Some(hook) = before_proof {
                    hook();
                }
                let store = SqliteStore::open(&path).context("open source SELL staging store")?;
                // Uses the unchanged storage transaction/retry policy; no retry daemon.
                let staged = store.process_source_sell_handoff(&task.swap.signature);
                #[cfg(test)]
                match &staged {
                    Ok(Outcome::Inserted(_)) => {
                        crate::app_tests::b70_hooks::mark("staging_inserted", &task.swap.signature)
                    }
                    Ok(Outcome::Existing(_)) => {
                        crate::app_tests::b70_hooks::mark("staging_existing", &task.swap.signature)
                    }
                    _ => crate::app_tests::b70_hooks::mark("staging_failed", &task.swap.signature),
                }
                staged
            }));
            match result {
                Ok(result) => WorkOutput {
                    result,
                    panicked: false,
                },
                Err(_) => WorkOutput {
                    result: Err(anyhow!("source SELL worker panicked")),
                    panicked: true,
                },
            }
        });
        self.active.insert(handle.id(), captured);
    }

    pub(super) fn complete(
        &mut self,
        joined: std::result::Result<(Id, WorkOutput), JoinError>,
    ) -> Result<StageCompletion> {
        let (id, result, failure) = match joined {
            Ok((id, work)) => (
                id,
                work.result,
                if work.panicked {
                    StageNotice::WorkerPanic
                } else {
                    StageNotice::WorkerFailed
                },
            ),
            Err(error) => (
                error.id(),
                Err(anyhow!(error)),
                StageNotice::WorkerJoinFailed,
            ),
        };
        let captured = self
            .active
            .remove(&id)
            .context("source SELL completion lost task identity")?;
        let notice = match &result {
            Ok(Outcome::Inserted(_)) => StageNotice::Staged,
            Ok(Outcome::Existing(_)) => StageNotice::Existing,
            Ok(Outcome::Rejected(reason)) => StageNotice::Rejected(*reason),
            Err(error) if is_fatal_sqlite_anyhow_error(error) => StageNotice::FatalSqlite,
            Err(error) if is_retryable_sqlite_anyhow_error(error) => StageNotice::RetryableSqlite,
            Err(_) => failure,
        };
        let completion = StageCompletion {
            signature: captured.swap.signature,
            notice,
        };
        record(
            &completion.signature,
            completion.notice,
            result.as_ref().err(),
        );
        if notice == StageNotice::FatalSqlite {
            return Err(result.unwrap_err())
                .context("source SELL staging failed with fatal sqlite I/O");
        }
        Ok(completion)
    }
}
