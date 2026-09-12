use super::*;
use fixtures::{Damage, Operand};
use yellowstone_grpc_proto::prelude::{subscribe_update, SubscribeUpdate, TokenBalance};

fn rows(update: &mut SubscribeUpdate, post: bool) -> &mut Vec<TokenBalance> {
    let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof else {
        panic!("expected transaction")
    };
    let meta = tx.transaction.as_mut().unwrap().meta.as_mut().unwrap();
    if post {
        &mut meta.post_token_balances
    } else {
        &mut meta.pre_token_balances
    }
}

pub(super) fn check(
    f: &Value,
    provider: &str,
    operand: Option<Operand>,
    expected: bool,
) -> Result<Value> {
    let input = operand.map(|op| op.json(f)).unwrap_or_else(|| f.clone());
    let mut evidence = Value::Null;
    let mut wire = None;
    let (raw, request) = if provider == "yellowstone" {
        // Convert healthy input first: the older JSON adapter cannot represent
        // ui_token_amount=None. Mutate protobuf itself BEFORE wire encoding.
        let original = super::super::super::proto::update(f);
        let mut mutated = original.clone();
        if let Some(op) = operand {
            let row = &mut rows(&mut mutated, op.post)[op.row_index(f)];
            assert_eq!(row.owner, op.owner(f));
            assert!(yellowstone::parse_proto_ui_amount(row.ui_token_amount.as_ref()).is_some());
            match op.damage {
                Damage::Absent => row.ui_token_amount = None,
                damage => {
                    row.ui_token_amount.as_mut().unwrap().ui_amount_string = damage.text().into()
                }
            }
            assert!(yellowstone::parse_proto_ui_amount(row.ui_token_amount.as_ref()).is_none());
        }
        let encoded = mutated.encode_to_vec();
        let mut decoded = SubscribeUpdate::decode(encoded.as_slice())?;
        assert_eq!(decoded, mutated);
        if let Some(op) = operand {
            let row = &rows(&mut decoded, op.post)[op.row_index(f)];
            assert!(yellowstone::parse_proto_ui_amount(row.ui_token_amount.as_ref()).is_none());
            evidence = json!({"field":op.field(),"row_index":op.row_index(f),"owner":row.owner,"mint":row.mint,
                "ui_token_amount_present":row.ui_token_amount.is_some(),
                "ui_amount_string":row.ui_token_amount.as_ref().map(|a|a.ui_amount_string.as_str()),
                "existing_parser_returned_none_before_and_after_decode":true});
            assert_eq!(
                row.ui_token_amount.is_none(),
                matches!(op.damage, Damage::Absent)
            );
            let mut restored = decoded.clone();
            rows(&mut restored, op.post)[op.row_index(f)].ui_token_amount =
                rows(&mut original.clone(), op.post)[op.row_index(f)]
                    .ui_token_amount
                    .clone();
            assert_eq!(restored, original);
        }
        let source = YellowstoneGrpcSource::new(&config())?;
        let raw = match yellowstone::parse_yellowstone_update(decoded, &source.runtime_config)? {
            Some(YellowstoneParsedUpdate::Observation(raw)) => Some(raw),
            None => None,
            _ => panic!("unexpected ping"),
        };
        wire = Some(encoded);
        (raw, None)
    } else if provider == "helius_fetch" {
        let (raw, request) = super::super::super::http::fetch(&input, &config())?;
        (raw, Some(request)) // fetch returns only after server.join().
    } else {
        (parse(&input, provider)?, None)
    };
    let present = raw.is_some();
    let event = raw.and_then(|r| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(r));
    let record = json!({"case":f["case"],"provider":provider,"healthy_input":f,"input":input,"raw_present":present,"event":event,
        "protobuf_operand_after_decode":evidence,"http_request":request,"http_handler_joined":provider=="helius_fetch"});
    if let Ok(dir) = std::env::var("B48_CAPTURE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir)?;
        let case = f["case"].as_str().unwrap();
        std::fs::write(
            dir.join(format!("{case}-{provider}.json")),
            serde_json::to_vec_pretty(&record)?,
        )?;
        if let Some(bytes) = wire {
            std::fs::write(dir.join(format!("{case}.pb")), bytes)?;
        }
    }
    assert_eq!(
        present, expected,
        "{} {provider}: {}",
        f["case"], record["event"]
    );
    assert_eq!(record["event"].is_null(), !expected);
    Ok(record)
}
