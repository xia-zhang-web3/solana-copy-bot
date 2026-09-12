#![cfg(test)]
use crate::association_inbox as subject;
use copybot_core_types::association_delivery::*;
use rusqlite::Connection;
use subject::*;
fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("full.sqlite");
    let c = Connection::open(&p).unwrap();
    drop(c);
    let mut store = crate::SqliteStore::open(&p).unwrap();
    store
        .run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))
        .unwrap();
    (d, p)
}
fn admission() -> Delivery {
    Delivery {
        session: "full".into(),
        sequence: 0,
        arrival_offset_ns: 0,
        event: DeliveryEvent::Admission(AdmissionFacts {
            facts: CheckedFacts {
                signature: "full".into(),
                slot: 1,
                wallet: "w".into(),
                token_in: "in".into(),
                token_out: "out".into(),
                amount_in_bits: 1.0f64.to_bits(),
                amount_out_bits: 1.0f64.to_bits(),
                exact_amounts: None,
                programs: vec![],
                dex: "test".into(),
                program_fallback: false,
            },
            info: InfoIdentity {
                encoded: vec![1],
                float_bits: vec![],
            },
            message_time: MessageTime::Missing,
        }),
    }
}
#[test]
fn b89_real_sqlite_full_and_read_only_never_ack() {
    for read_only in [false, true] {
        let (_d, p) = fixture();
        let mut i = AssociationInbox::open(
            &p,
            InboxLimits {
                count: 1000,
                bytes: 8 << 20,
                busy_ms: 10,
            },
        )
        .unwrap();
        let mut d = admission();
        if read_only {
            i.conn.pragma_update(None, "query_only", true).unwrap();
        } else {
            let pages: i64 = i
                .conn
                .query_row("PRAGMA page_count", [], |r| r.get(0))
                .unwrap();
            i.conn.pragma_update(None, "max_page_count", pages).unwrap();
            if let DeliveryEvent::Admission(a) = &mut d.event {
                a.info.encoded = vec![89; 100_000];
            }
        }
        let error = i.persist(&d, &CandidateGeneration::Unknown).unwrap_err();
        let code = error
            .downcast_ref::<rusqlite::Error>()
            .and_then(|e| e.sqlite_error_code());
        assert_eq!(
            code,
            Some(if read_only {
                rusqlite::ErrorCode::ReadOnly
            } else {
                rusqlite::ErrorCode::DiskFull
            })
        );
        assert_eq!(i.usage().unwrap(), (1, 512));
    }
}
