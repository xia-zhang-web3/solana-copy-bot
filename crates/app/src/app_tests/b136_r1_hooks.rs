//! Passive per-fixture lifecycle counts; no changes to job or receipt outcomes.
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Counts {
    pub scheduled: usize,
    pub running: usize,
    pub max_running: usize,
    pub finished: usize,
}
fn counts() -> &'static Mutex<HashMap<PathBuf, Counts>> {
    static MAP: OnceLock<Mutex<HashMap<PathBuf, Counts>>> = OnceLock::new();
    MAP.get_or_init(Default::default)
}
pub(crate) fn scheduled(path: &Path) {
    counts()
        .lock()
        .unwrap()
        .entry(path.into())
        .or_default()
        .scheduled += 1;
}
pub(super) fn read(path: &Path) -> Counts {
    counts()
        .lock()
        .unwrap()
        .get(path)
        .copied()
        .unwrap_or_default()
}
pub(crate) struct Running(PathBuf);
impl Running {
    pub(crate) fn new(path: &Path) -> Self {
        let mut all = counts().lock().unwrap();
        let c = all.entry(path.into()).or_default();
        c.running += 1;
        c.max_running = c.max_running.max(c.running);
        Self(path.into())
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let mut all = counts().lock().unwrap();
        let c = all.get_mut(&self.0).unwrap();
        c.running -= 1;
        c.finished += 1;
    }
}
