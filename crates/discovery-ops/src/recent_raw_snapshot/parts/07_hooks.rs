#[cfg(test)]
thread_local! {
    static POST_SNAPSHOT_PUBLISH_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(&Path) -> Result<()>>>> =
        std::cell::RefCell::new(None);
    static PRE_ARCHIVE_PROMOTION_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(&Path) -> Result<()>>>> =
        std::cell::RefCell::new(None);
    static RESUMABLE_SNAPSHOT_PROGRESS_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(usize, usize) -> bool>>> =
        std::cell::RefCell::new(None);
    static STAGED_WRITE_FAILURE_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(usize) -> Option<StagedWriteHookFailure>>>> =
        std::cell::RefCell::new(None);
}

struct StagedWriteHookFailure {
    error: anyhow::Error,
    force_budget_context: bool,
}

#[cfg(test)]
struct SnapshotPublishHookGuard;

#[cfg(test)]
impl Drop for SnapshotPublishHookGuard {
    fn drop(&mut self) {
        POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        PRE_ARCHIVE_PROMOTION_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        RESUMABLE_SNAPSHOT_PROGRESS_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        STAGED_WRITE_FAILURE_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
fn install_post_snapshot_publish_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(&Path) -> Result<()> + 'static,
{
    POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "snapshot publish hook already installed");
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn install_pre_archive_promotion_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(&Path) -> Result<()> + 'static,
{
    PRE_ARCHIVE_PROMOTION_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "pre-archive promotion hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn install_resumable_snapshot_progress_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(usize, usize) -> bool + 'static,
{
    RESUMABLE_SNAPSHOT_PROGRESS_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "resumable snapshot progress hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn install_staged_write_failure_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(usize) -> Option<StagedWriteHookFailure> + 'static,
{
    STAGED_WRITE_FAILURE_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "staged write failure hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn invoke_post_snapshot_publish_hook(snapshot_path: &Path) -> Result<()> {
    let hook = POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(mut hook) = hook {
        hook(snapshot_path)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn invoke_post_snapshot_publish_hook(_snapshot_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
fn invoke_pre_archive_promotion_hook(snapshot_path: &Path) -> Result<()> {
    let hook = PRE_ARCHIVE_PROMOTION_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(mut hook) = hook {
        hook(snapshot_path)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn invoke_pre_archive_promotion_hook(_snapshot_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
fn resumable_snapshot_progress_hook_requests_budget_exhaustion(
    completed_batches: usize,
    staged_row_count: usize,
) -> bool {
    RESUMABLE_SNAPSHOT_PROGRESS_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        slot.as_mut()
            .is_some_and(|hook| hook(completed_batches, staged_row_count))
    })
}

#[cfg(not(test))]
fn resumable_snapshot_progress_hook_requests_budget_exhaustion(
    _completed_batches: usize,
    _staged_row_count: usize,
) -> bool {
    false
}

#[cfg(test)]
fn invoke_staged_write_failure_hook(completed_batches: usize) -> Option<StagedWriteHookFailure> {
    STAGED_WRITE_FAILURE_HOOK.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .and_then(|hook| hook(completed_batches))
    })
}

#[cfg(not(test))]
fn invoke_staged_write_failure_hook(_completed_batches: usize) -> Option<StagedWriteHookFailure> {
    None
}

fn snapshot_attempt_deadline(max_attempt_duration: Option<StdDuration>) -> Option<Instant> {
    max_attempt_duration.map(|budget| Instant::now() + budget)
}

fn recent_raw_staged_write_error_is_interruption(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(|sqlite_error| {
                sqlite_error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted)
            })
            || cause
                .to_string()
                .to_ascii_lowercase()
                .contains("interrupted")
    })
}

fn recent_raw_staged_write_error_is_generic_bulk_wrapper(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .to_ascii_lowercase()
            .contains("failed to run recent raw journal bulk batch write")
    })
}

fn recent_raw_staged_write_error_is_fatal_storage(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
        || error.chain().any(|cause| {
            let lowered = cause.to_string().to_ascii_lowercase();
            lowered.contains("database disk image is malformed")
                || lowered.contains("database is corrupt")
                || lowered.contains("malformed")
                || lowered.contains("corrupt")
                || lowered.contains("no such table")
                || lowered.contains("no such column")
                || lowered.contains("schema")
                || lowered.contains("syntax error")
                || lowered.contains("readonly")
                || lowered.contains("permission denied")
        })
}

fn recent_raw_staged_write_error_is_budget_exhaustion(
    error: &anyhow::Error,
    deadline_exhausted: bool,
) -> bool {
    recent_raw_staged_write_error_is_interruption(error)
        || (deadline_exhausted
            && recent_raw_staged_write_error_is_generic_bulk_wrapper(error)
            && !recent_raw_staged_write_error_is_fatal_storage(error))
}

// Preserve the staged snapshot across deferred runs so the bounded scheduled path keeps advancing
// on live-size journals instead of restarting from zero on every timer tick.
