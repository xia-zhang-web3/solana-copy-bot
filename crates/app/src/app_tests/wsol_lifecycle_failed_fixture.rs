//! Actual signature-status → failed-expense recovery on a copy of the real LOCAL dispatch.
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::Result;
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
async fn confirm(store: &SqliteStore, r: &ExecutionSubmitRequest, receipt: Value) -> Result<()> {
    let payer =
        crate::execution_pumpswap_accounts::parse_pubkey(&r.wallet_pubkey, "LOCAL failed payer")?;
    let instructions = super::priority_fee_fixture::budget(200000, 0);
    let wire = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            payer,
            [9; 32],
            &instructions,
        )?,
    );
    let io = crate::execution_canary_route::NativeBuyMockIo {
        runner: None,
        initial_sol: crate::execution_native_rpc::synthetic_classic_funding(
            &wire, payer, 1000000000, 5000, 151,
        )?,
        fee_lamports: 5000,
        fee_slot: 151,
        expected_message_sha256: "LOCAL confirmation only".into(),
        submit_signature: None,
        confirmation: json!({"result":{"value":[{"err":receipt["result"]["meta"]["err"],"slot":151,"confirmationStatus":"confirmed"}]}}),
        receipt,
        counts: Arc::new(Mutex::new(Default::default())),
    };
    crate::execution_submit_adapter::test_record_execution_rpc_confirmation_boundary_inner(
        store,
        &reqwest::Client::new(),
        "http://127.0.0.1:1",
        &r.order_id,
        &r.wallet_pubkey,
        chrono::Utc::now(),
        1000,
        Some(&io),
    )
    .await?;
    Ok(())
}
pub(super) async fn exercise(
    sql: &rusqlite::Connection,
    r: &ExecutionSubmitRequest,
    receipt: &Value,
) -> Result<()> {
    let root = super::temporary_output_fixture::OutputRoot::new("wsol-failed-expense")?;
    // Atomically occupied temp: all instructions roll back, only the payer fee remains.
    // Both balances are derived from the very same modeled prepared-message pre-state.
    let mut failed = receipt.clone();
    let m = &mut failed["result"]["meta"];
    m["err"] = json!({"InstructionError":[2,{"Custom":0}]});
    m["postBalances"] = m["preBalances"].clone();
    m["postBalances"][0] =
        json!(m["preBalances"][0].as_u64().unwrap() - super::wsol_lifecycle_oracle::FEE);
    m["postTokenBalances"] = m["preTokenBalances"].clone();
    m["innerInstructions"] = json!([]);
    let temp = bs58::encode(crate::execution_wsol_lifecycle::address(r)?).into_string();
    let n = failed["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .position(|k| k["pubkey"] == temp)
        .unwrap();
    failed["result"]["meta"]["preBalances"][n] = json!(1);
    failed["result"]["meta"]["postBalances"][n] = json!(1);
    for foreign in [false, true] {
        let path = root
            .path()
            .join(if foreign { "foreign.db" } else { "known.db" });
        sql.execute("VACUUM INTO ?1", [path.to_string_lossy().as_ref()])?;
        let store = SqliteStore::open(&path)?;
        let mut value = failed.clone();
        if foreign {
            let create = value["result"]["transaction"]["message"]["instructions"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|i| i["parsed"]["type"] == "createAccountWithSeed")
                .unwrap();
            create["parsed"]["info"]["seed"] = json!("f".repeat(32));
        }
        confirm(&store, r, value.clone()).await?;
        let task = store.load_failed_expense_task(&r.order_id)?.unwrap();
        assert_eq!(task.status, if foreign { "pending" } else { "complete" });
        if foreign {
            assert!(task.reason.contains("message_mismatch"));
        }
        let db = rusqlite::Connection::open(&path)?;
        let (count,fee):(i64,u64)=db.query_row("SELECT count(*),COALESCE(SUM(CAST(wallet_fee_lamports AS INTEGER)),0) FROM execution_failed_expense_ledger",[],|x|Ok((x.get(0)?,x.get(1)?)))?;
        assert_eq!(
            (count, fee),
            if foreign {
                (0, 0)
            } else {
                (1, super::wsol_lifecycle_oracle::FEE)
            }
        );
        assert!(!store.execution_canary_fill_exists(&r.order_id)?);
        assert_eq!(
            store
                .load_execution_canary_open_position(&r.token)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            super::protected_usdc_fixture::H
        );
        let dispatched: i64 = db.query_row(
            "SELECT count(*) FROM execution_canary_dispatch WHERE order_id=?1",
            [&r.order_id],
            |x| x.get(0),
        )?;
        assert_eq!(dispatched, 1);
        if !foreign {
            assert!(confirm(&store, r, value).await.is_err());
            let again: i64 = db.query_row(
                "SELECT count(*) FROM execution_failed_expense_ledger",
                [],
                |x| x.get(0),
            )?;
            assert_eq!(again, 1);
        }
    }
    Ok(())
}
