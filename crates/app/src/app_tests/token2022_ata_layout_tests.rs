use super::token2022_ata_collector_tests::unknown;
use super::token2022_ata_inputs_tests::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
pub(super) fn edit_data(spec: &mut Spec, key: &str, edit: impl FnOnce(&mut Vec<u8>)) {
    let mut row = spec.row(key).clone();
    let mut data = STANDARD.decode(row["data"][0].as_str().unwrap()).unwrap();
    edit(&mut data);
    row["data"][0] = json!(STANDARD.encode(&data));
    if row.get("space").is_some() {
        row["space"] = json!(data.len());
    }
    spec.set(key, row);
}
#[tokio::test]
async fn token2022_ata_mint_layout_negative_matrix() -> Result<()> {
    for case in [
        "missing",
        "program",
        "executable",
        "base-short",
        "plain165",
        "uninitialized",
        "state2",
        "decimals",
        "mint-option",
        "freeze-option",
        "padding",
        "type",
        "tlv-truncated",
        "tlv-tail",
        "duplicate18",
        "duplicate19",
        "pointer-length",
        "pointer-key",
        "metadata-mint",
        "utf8",
        "string-overflow",
        "metadata-truncated",
        "metadata-trailing",
        "vector-overflow",
        "duplicate-metadata-keys",
        "oversized",
    ] {
        let mut s = Spec::sufficient("absent")?;
        match case {
            "missing" => s.set(MINT, Value::Null),
            "program" | "executable" => {
                let mut r = s.row(MINT).clone();
                if case == "program" {
                    r["owner"] = json!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
                } else {
                    r["executable"] = json!(true);
                }
                s.set(MINT, r);
            }
            _ => edit_data(&mut s, MINT, |d| match case {
                "base-short" => d.truncate(81),
                "plain165" => d.truncate(165),
                "uninitialized" => d[45] = 0,
                "state2" => d[45] = 2,
                "decimals" => d[44] = 5,
                "mint-option" => d[0] = 2,
                "freeze-option" => d[46] = 2,
                "padding" => d[82] = 1,
                "type" => d[165] = 2,
                "tlv-truncated" => d.truncate(167),
                "tlv-tail" => d.push(0),
                "duplicate18" => d.extend_from_within(166..234),
                "duplicate19" => d.extend_from_within(234..416),
                "pointer-length" => d[168] = 65,
                "pointer-key" => d[202] ^= 1,
                "metadata-mint" => d[270] ^= 1,
                "utf8" => d[306] = 255,
                "string-overflow" => d[302..306].copy_from_slice(&u32::MAX.to_le_bytes()),
                "metadata-truncated" => {
                    d.pop();
                }
                "metadata-trailing" => {
                    d.push(0);
                    d[236..238].copy_from_slice(&179u16.to_le_bytes());
                }
                "vector-overflow" => d[412..416].copy_from_slice(&u32::MAX.to_le_bytes()),
                "duplicate-metadata-keys" => {
                    d[412..416].copy_from_slice(&2u32.to_le_bytes());
                    for value in [b'b', b'c'] {
                        d.extend_from_slice(&1u32.to_le_bytes());
                        d.push(b'a');
                        d.extend_from_slice(&1u32.to_le_bytes());
                        d.push(value);
                    }
                    let len = (d.len() - 238) as u16;
                    d[236..238].copy_from_slice(&len.to_le_bytes());
                }
                "oversized" => d.resize(4097, 0),
                _ => unreachable!(),
            }),
        }
        unknown(&s, &format!("mint-{case}")).await?;
    }
    for kind in [0u16, 1, 4, 6, 9, 12, 14, 16, 20, u16::MAX] {
        let mut s = Spec::sufficient("absent")?;
        edit_data(&mut s, MINT, |d| {
            d[166..168].copy_from_slice(&kind.to_le_bytes())
        });
        unknown(&s, &format!("unsupported-extension-{kind}")).await?;
    }
    Ok(())
}
#[tokio::test]
async fn token2022_ata_existing_identity_state_and_layout_matrix() -> Result<()> {
    for case in [
        "foreign-owner",
        "foreign-mint",
        "state0",
        "state2",
        "state3",
        "delegate-tag",
        "native-some",
        "native-tag",
        "close-tag",
        "type",
        "extension",
        "immutable-length",
        "165",
        "169",
        "171",
        "no-extension",
        "duplicate",
        "program",
        "executable",
        "system-with-data",
        "wrong-row",
    ] {
        let mut s = Spec::sufficient("existing")?;
        match case {
            "program" | "executable" | "system-with-data" => {
                let mut r = s.row(ATA).clone();
                if case == "executable" {
                    r["executable"] = json!(true);
                } else {
                    r["owner"] = json!(if case == "program" {
                        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    } else {
                        "11111111111111111111111111111111"
                    });
                }
                s.set(ATA, r);
            }
            "wrong-row" => {
                let a = s.row(ATA).clone();
                let m = s.row(MINT).clone();
                s.set(MINT, a);
                s.set(ATA, m);
            }
            _ => edit_data(&mut s, ATA, |d| match case {
                "foreign-owner" => d[32] ^= 1,
                "foreign-mint" => d[0] ^= 1,
                "state0" => d[108] = 0,
                "state2" => d[108] = 2,
                "state3" => d[108] = 3,
                "delegate-tag" => d[72] = 2,
                "native-some" => d[109] = 1,
                "native-tag" => d[109] = 2,
                "close-tag" => d[129] = 2,
                "type" => d[165] = 1,
                "extension" => d[166] = 8,
                "immutable-length" => d[168] = 1,
                "165" => d.truncate(165),
                "169" => d.truncate(169),
                "171" => d.push(0),
                "no-extension" => d.truncate(166),
                "duplicate" => d.extend_from_slice(&[7, 0, 0, 0]),
                _ => unreachable!(),
            }),
        }
        unknown(&s, &format!("ata-{case}")).await?;
    }
    Ok(())
}
