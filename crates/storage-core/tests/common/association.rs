#![allow(dead_code)]
use copybot_core_types::{association_delivery::*, ExactSwapAmounts};
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
use rusqlite::Connection;
pub fn limits() -> InboxLimits {
    InboxLimits {
        count: 1000,
        bytes: 8 * 1024 * 1024,
        busy_ms: 10,
    }
}
pub fn db() -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("inbox.sqlite");
    init(&p);
    (d, p)
}
pub fn init(p: &std::path::Path) {
    let c = Connection::open(p).unwrap();
    c.pragma_update(None, "journal_mode", "WAL").unwrap();
    drop(c);
    let mut store = copybot_storage_core::SqliteStore::open(p).unwrap();
    store
        .run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))
        .unwrap();
}
pub fn facts() -> AdmissionFacts {
    AdmissionFacts {
        facts: CheckedFacts {
            signature: "checked-sell".into(),
            slot: 7,
            wallet: "leader".into(),
            token_in: "token".into(),
            token_out: "So11111111111111111111111111111111111111112".into(),
            amount_in_bits: 1.0f64.to_bits(),
            amount_out_bits: 2.0f64.to_bits(),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: u64::MAX.to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "123".into(),
                amount_out_decimals: 9,
            }),
            programs: vec!["program".into()],
            dex: "fixture".into(),
            program_fallback: false,
        },
        info: InfoIdentity {
            encoded: vec![1, 2, 3],
            float_bits: vec![Some((-0.0f64).to_bits()), None],
        },
        message_time: MessageTime::Missing,
    }
}
pub fn candidate(id: &str) -> CandidateGeneration {
    CandidateGeneration::AppObserved {
        position_id: id.into(),
        opened_ts: "2026-09-09T00:00:00+00:00".into(),
        token: "token".into(),
    }
}
pub fn admission(seq: u64) -> Delivery {
    event(seq, DeliveryEvent::Admission(facts()))
}
pub fn event(sequence: u64, event: DeliveryEvent) -> Delivery {
    Delivery {
        session: "session-A".into(),
        sequence,
        arrival_offset_ns: sequence,
        event,
    }
}
pub fn terminal(seq: u64, result: Terminal) -> Delivery {
    event(
        seq,
        DeliveryEvent::Terminal {
            signature: facts().facts.signature,
            expected: facts(),
            result,
        },
    )
}
pub fn result() -> Terminal {
    Terminal::ProviderAsserted(ProviderAssertion {
        slot: 7,
        blockhash: "provider-hash".into(),
        signature: facts().facts.signature,
        transaction_index: 9,
        block_time: BlockTime::Missing,
    })
}
pub fn row(i: &AssociationInbox) -> copybot_storage_core::association_inbox::InboxIdentity {
    i.identity(&facts().facts.signature).unwrap().unwrap()
}
