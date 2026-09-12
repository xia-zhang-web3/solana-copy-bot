use super::*;
use yellowstone_grpc_proto::prelude::SubscribeUpdatePing;
use yellowstone_grpc_proto::prost_types::Timestamp;

pub(super) struct Case {
    pub name: String,
    pub update: SubscribeUpdate,
    pub refused: bool,
    pub empty_interest: bool,
}

pub(super) fn base(sell: bool, native: bool) -> SubscribeUpdate {
    let f = fixtures::base(sell, native, &format!("b79-{sell}-{native}"));
    super::super::super::super::super::proto::update(&f)
}

pub(super) fn times() -> Vec<(&'static str, Option<Timestamp>)> {
    vec![
        (
            "healthy",
            Some(Timestamp {
                seconds: 1788868800,
                nanos: 123456789,
            }),
        ),
        (
            "old",
            Some(Timestamp {
                seconds: 1600000000,
                nanos: 123456789,
            }),
        ),
        (
            "epoch",
            Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
        ),
        (
            "negative",
            Some(Timestamp {
                seconds: -1,
                nanos: 999999999,
            }),
        ),
        (
            "future",
            Some(Timestamp {
                seconds: 1788868860,
                nanos: 123456789,
            }),
        ),
        (
            "future1ns",
            Some(Timestamp {
                seconds: 1788868800,
                nanos: 123456790,
            }),
        ),
        ("missing", None),
        (
            "negative_nanos",
            Some(Timestamp {
                seconds: 1788868800,
                nanos: -1,
            }),
        ),
        (
            "overflow_nanos",
            Some(Timestamp {
                seconds: 1788868800,
                nanos: 1000000000,
            }),
        ),
        (
            "overflow_seconds",
            Some(Timestamp {
                seconds: i64::MAX,
                nanos: 0,
            }),
        ),
        (
            "underflow_seconds",
            Some(Timestamp {
                seconds: i64::MIN,
                nanos: 0,
            }),
        ),
        (
            "restored",
            Some(Timestamp {
                seconds: 1788868800,
                nanos: 123456789,
            }),
        ),
    ]
}

pub(super) fn corpus() -> Vec<Case> {
    let mut cases = Vec::new();
    for sell in [false, true] {
        for missing_signature in [false, true] {
            for missing_time in [false, true] {
                let f = inventory::healthy(sell, false, true, true);
                let mut update = super::super::super::super::super::proto::update(&f);
                damage::apply(&mut update, "missing_program_error");
                if missing_signature {
                    damage::apply(&mut update, "missing_signature");
                }
                if missing_time {
                    update.created_at = None;
                }
                cases.push(Case {
                    name: format!("fallback-{sell}-{missing_signature}-{missing_time}"),
                    update,
                    refused: true,
                    empty_interest: false,
                });
            }
        }
    }
    for sell in [false, true] {
        for native in [false, true] {
            for (clock, ts) in times() {
                let mut update = base(sell, native);
                update.created_at = ts;
                cases.push(Case {
                    name: format!("{sell}-{native}-{clock}"),
                    update,
                    refused: matches!(
                        clock,
                        "missing"
                            | "negative_nanos"
                            | "overflow_nanos"
                            | "overflow_seconds"
                            | "underflow_seconds"
                    ),
                    empty_interest: false,
                });
            }
            for damage in damage::NAMES {
                for clock in ["healthy", "missing", "overflow_nanos"] {
                    let mut update = base(sell, native);
                    update.created_at = times().into_iter().find(|(n, _)| *n == clock).unwrap().1;
                    damage::apply(&mut update, damage);
                    cases.push(Case {
                        name: format!("{sell}-{native}-{damage}-{clock}"),
                        update,
                        refused: true,
                        empty_interest: *damage == "missing_program_error",
                    });
                }
            }
        }
    }
    // Reuse the established corpus for balance ambiguity, native attribution,
    // unsupported maintenance and valid exact/fallback controls.
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/source_tests/pumpswap/fixtures");
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    paths.sort();
    for path in paths {
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let f: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        for missing in [false, true] {
            let mut update = super::super::super::super::super::proto::update(&f);
            if missing {
                update.created_at = None;
            }
            cases.push(Case {
                name: format!(
                    "fixture-{}-{missing}",
                    path.file_stem().unwrap().to_str().unwrap()
                ),
                update,
                refused: missing,
                empty_interest: false,
            });
        }
    }
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/source_tests/native_cpi/fixtures");
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    paths.sort();
    for path in paths {
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let mut f: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let absent = f["result"]["meta"].get("innerInstructions").is_none();
        if absent {
            f["result"]["meta"]["innerInstructions"] = json!([]);
        }
        for missing in [false, true] {
            let mut update = super::super::super::super::super::proto::update(&f);
            if let Some(subscribe_update::UpdateOneof::Transaction(tx)) =
                update.update_oneof.as_mut()
            {
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .meta
                    .as_mut()
                    .unwrap()
                    .inner_instructions_none = absent;
            }
            if missing {
                update.created_at = None;
            }
            cases.push(Case {
                name: format!(
                    "native-{}-{missing}",
                    path.file_stem().unwrap().to_str().unwrap()
                ),
                update,
                refused: missing,
                empty_interest: false,
            });
        }
    }
    cases.push(Case {
        name: "ping".into(),
        update: SubscribeUpdate {
            update_oneof: Some(subscribe_update::UpdateOneof::Ping(SubscribeUpdatePing {})),
            ..Default::default()
        },
        refused: true,
        empty_interest: false,
    });
    cases.push(Case {
        name: "no_update".into(),
        update: SubscribeUpdate::default(),
        refused: true,
        empty_interest: false,
    });
    cases
}
