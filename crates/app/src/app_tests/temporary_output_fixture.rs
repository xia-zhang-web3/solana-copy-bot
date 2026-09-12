//! Owned local output directory; no developer environment or retained audit path.
use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
pub(super) struct OutputRoot(PathBuf);
impl OutputRoot {
    pub(super) fn new(name: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "copybot-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path)?; // fail on collision; never reuse old evidence
        Ok(Self(path))
    }
    pub(super) fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for OutputRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
