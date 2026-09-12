#[path = "common/association.rs"]
mod f;
use copybot_storage_core::association_inbox::AssociationInbox;
use f::*;
#[test]
fn b89_crash_child() {
    let Ok(path) = std::env::var("B89_CRASH_DB") else {
        return;
    };
    let stage = std::env::var("B89_CRASH_STAGE").unwrap();
    if stage == "before_commit" {
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute_batch("BEGIN IMMEDIATE; INSERT INTO association_inbox_events VALUES('aborted',0,'uncommitted');").unwrap();
        std::process::exit(17);
    }
    let mut i = AssociationInbox::open(&path, limits()).unwrap();
    i.persist_at(
        &admission(0),
        &candidate("A"),
        chrono::DateTime::from_timestamp(10, 0).unwrap(),
    )
    .unwrap();
    if stage == "after_commit_before_ack" {
        std::process::exit(17);
    }
    i.persist(&terminal(1, result()), &candidate("B")).unwrap();
    std::process::exit(17);
}
#[test]
fn b89_crash_before_commit_after_commit_and_before_terminal() {
    for stage in ["before_commit", "after_commit_before_ack", "after_terminal"] {
        let (_d, p) = db();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "b89_crash_child", "--nocapture"])
            .env("B89_CRASH_DB", &p)
            .env("B89_CRASH_STAGE", stage)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(17));
        let mut i = AssociationInbox::open(&p, limits()).unwrap();
        if stage == "before_commit" {
            assert_eq!(i.usage().unwrap(), (1, 512));
            continue;
        }
        assert_eq!(row(&i).candidate, candidate("A"));
        i.persist(&admission(0), &candidate("B")).unwrap();
        assert_eq!(row(&i).candidate, candidate("A"));
        assert_eq!(row(&i).recovery, stage == "after_commit_before_ack");
    }
}
