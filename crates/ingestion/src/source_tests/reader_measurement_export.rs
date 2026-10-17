//! Local fixture exporter only: bounded file append+sync_data off the Reader path.
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};
static ERRORS: AtomicU64 = AtomicU64::new(0);
pub(in crate::source) fn errors() -> u64 {
    ERRORS.load(Relaxed)
}
pub(super) fn enabled() -> bool {
    std::env::var_os("COPY49_MEASUREMENT_OUT").is_some()
}
fn append_file(path: &Path, encoded: &str, cap: u64) -> std::io::Result<()> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut file = opts.open(path)?;
    if file
        .metadata()?
        .len()
        .saturating_add(encoded.len() as u64)
        .saturating_add(1)
        > cap
    {
        return Err(std::io::Error::other("diagnostic file cap"));
    }
    file.write_all(encoded.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_data()
}
pub(super) fn export(encoded: &str) -> bool {
    let result = (|| -> std::io::Result<()> {
        let root = std::env::var("COPY49_MEASUREMENT_OUT")
            .map_err(|_| std::io::Error::other("missing fixture output"))?;
        let cap = std::env::var("COPY49_TERMINAL_FILE_CAP")
            .map(|s| s.parse().unwrap_or(0))
            .unwrap_or(1 << 20);
        append_file(
            &Path::new(&root).join("reader-terminal-events.jsonl"),
            encoded,
            cap,
        )
    })();
    if result.is_err() {
        ERRORS.fetch_add(1, Relaxed);
    }
    result.is_ok()
}
#[test]
fn reader_measurement_exporter_cap_or_disk_error_is_explicit() {
    let t = tempfile::tempdir().unwrap();
    let p = t.path().join("bounded.jsonl");
    append_file(&p, "{\"sequence\":1}", 100).unwrap();
    let before = std::fs::read(&p).unwrap();
    assert!(append_file(&p, "{\"sequence\":2}", before.len() as u64).is_err());
    assert_eq!(std::fs::read(&p).unwrap(), before);
    assert!(append_file(t.path(), "{}", 100).is_err());
    let reopened = File::open(&p).unwrap();
    assert!(reopened.metadata().unwrap().len() <= 100);
}
