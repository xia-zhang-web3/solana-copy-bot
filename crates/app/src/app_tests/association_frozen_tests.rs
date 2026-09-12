use super::association_fixture as f;
use anyhow::{ensure, Result};
use copybot_core_types::association_delivery::*;
use copybot_ingestion::ReplayInput;
use std::io::Read;
#[tokio::test]
#[ignore = "requires unchanged reader validated capture03 frames and explicit local DB directory"]
async fn b89_frozen_capture03_actual_service_app_sqlite_three_clocks() -> Result<()> {
    let m = f::metadata("frozen.json");
    assert_eq!(m["capture_complete"], false);
    let mut results = vec![];
    for variant in ["original", "missing", "invalid"] {
        let c = f::config(&m);
        let db = f::Db::new(&format!("capture03-{variant}"))?;
        let before = db.financial_counts()?;
        let (mut consumer, tx) = f::start(&db, &c, &format!("capture03-{variant}")).await?;
        let path = f::fixtures().join(format!("frozen-{variant}.frames"));
        // Sequential read with a single bounded source queue. No future block index.
        let producer = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut file = std::fs::File::open(path)?;
            let mut last = 0;
            let mut count = 0;
            loop {
                let mut header = [0u8; 12];
                match file.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
                let ns = u64::from_le_bytes(header[..8].try_into()?);
                ensure!(ns >= last);
                last = ns;
                let n = u32::from_le_bytes(header[8..].try_into()?) as usize;
                ensure!(n <= 8 << 20);
                let mut payload = vec![0; n];
                file.read_exact(&mut payload)?;
                tx.blocking_send(ReplayInput::Update {
                    offset_ns: ns,
                    payload,
                })?;
                count += 1;
            }
            ensure!(count == 2048);
            tx.blocking_send(ReplayInput::End(last))?;
            Ok(())
        });
        f::drain(&mut consumer, &db).await?;
        producer.await??;
        assert_eq!(db.identities()?, 587);
        assert_eq!(db.financial_counts()?, before);
        let admitted:i64=db.sql.query_row("SELECT count(*) FROM association_inbox_events WHERE json_type(delivery,'$.event.Admission') IS NOT NULL",[],|r|r.get(0))?;
        let duplicates:i64=db.sql.query_row("SELECT count(*) FROM association_inbox_events WHERE json_type(delivery,'$.event.Duplicate') IS NOT NULL",[],|r|r.get(0))?;
        assert_eq!(admitted, 587);
        assert_eq!(duplicates, 0);
        assert_eq!(m["raw"].as_i64().unwrap() - admitted, 1446);
        let mut q=db.sql.prepare("SELECT admission,terminal,candidate,conflict FROM association_inbox_identities ORDER BY signature")?;
        let rows = q
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, bool>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut matched = 0;
        let mut unresolved = 0;
        let mut cohort = 0;
        let mut facts = vec![];
        for (a, t, b, conflict) in rows {
            assert!(!conflict);
            assert_eq!(
                serde_json::from_str::<CandidateGeneration>(&b)?,
                CandidateGeneration::Unknown
            );
            let a: AdmissionFacts = serde_json::from_str(&a)?;
            let t: Terminal =
                serde_json::from_str(&t.expect("every admitted identity durable terminal"))?;
            match t {
                Terminal::ProviderAsserted(_) => {
                    matched += 1;
                    if m["cohort"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|v| v.as_str() == Some(&a.facts.signature))
                    {
                        cohort += 1;
                    }
                }
                Terminal::Unresolved(Unresolved::EndOfStream) => unresolved += 1,
                _ => panic!("unexpected terminal"),
            }
            if variant == "missing" {
                assert_eq!(a.message_time, MessageTime::Missing);
            } else if variant == "invalid" {
                assert_eq!(a.message_time, MessageTime::InvalidNanos(-1));
            }
            facts.push(a.facts);
        }
        assert_eq!((matched, unresolved, cohort), (583, 4, 26));
        if let Some((_, prior)) = results.first() {
            assert_eq!(&facts, prior);
        }
        results.push((variant, facts));
        let out = serde_json::json!({"variant":variant,"checked":587,"matched":matched,"unresolved":unresolved,"not_checked":1446,"cohort_matched":cohort,"capture_complete":false,"financial_delta":0,"db":db.path});
        std::fs::write(
            std::path::Path::new(&std::env::var("B89_DB_DIR")?)
                .join(format!("capture03-{variant}.json")),
            serde_json::to_vec_pretty(&out)?,
        )?;
    }
    Ok(())
}
