use super::association_fixture as f;
use crate::association_consumer::AssociationConsumer;
use anyhow::Result;
use copybot_ingestion::IngestionService;
#[tokio::test]
#[ignore = "coordinated local gRPC transport fixture"]
async fn b89_loopback_actual_subscription_service_app_sqlite() -> Result<()> {
    let m = f::metadata("fixture.json");
    let mut c = f::config(&m);
    c.ingestion.yellowstone_grpc_url =
        std::fs::read_to_string(f::fixtures().join("loopback-url.txt"))?;
    let db = f::Db::new("actual-loopback")?;
    let before = db.financial_counts()?;
    let mut service = IngestionService::build_for_app(&c)?;
    let mut consumer =
        AssociationConsumer::start(&mut service, &c.ingestion, &db.path.to_string_lossy())
            .await?
            .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for _ in 0..10 {
            consumer.poll(&db.store).await?;
            let n: i64 = db.sql.query_row(
                "SELECT count(*) FROM association_inbox_identities WHERE terminal IS NOT NULL",
                [],
                |r| r.get(0),
            )?;
            if n == 1 {
                return Ok::<_, anyhow::Error>(());
            }
        }
        anyhow::bail!("missing durable terminal");
    })
    .await??;
    let (a, _, t, _) = f::row(&db)?;
    let a: copybot_core_types::association_delivery::AdmissionFacts = serde_json::from_str(&a)?;
    assert_eq!(
        a.message_time,
        copybot_core_types::association_delivery::MessageTime::Missing
    );
    assert_eq!(a.facts.signature, m["signature"].as_str().unwrap());
    assert!(t.is_some());
    assert_eq!(db.financial_counts()?, before);
    drop(consumer);
    std::fs::write(
        f::fixtures().join("loopback-done"),
        b"service app sqlite success",
    )?;
    Ok(())
}
#[tokio::test]
#[ignore = "explicit fixture config, SQLite startup refusal before provider"]
async fn b89_startup_schema_failure_precedes_connection() -> Result<()> {
    let m = f::metadata("fixture.json");
    let mut c = f::config(&m);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    c.ingestion.yellowstone_grpc_url = format!("http://{}", listener.local_addr()?);
    let db = f::Db::new("startup-schema")?;
    db.sql
        .execute_batch("DROP TABLE association_inbox_events")?;
    let mut service = IngestionService::build_for_app(&c)?;
    assert!(
        AssociationConsumer::start(&mut service, &c.ingestion, &db.path.to_string_lossy())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}
