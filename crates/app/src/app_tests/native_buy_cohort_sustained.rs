//! Full-block synthetic protobuf envelope without added app dependencies.
use super::*;
use std::path::Path;
use tokio::sync::mpsc;

const BLOCK_BYTES: usize = 3 * 1024 * 1024;
const FIRST_FILLER: u64 = 121;
pub(crate) const FILLER_COUNT: u64 = 36_000;
const STEP_NS: u64 = 400_000_000;
pub(crate) const SELL_SLOT: u64 = FIRST_FILLER + FILLER_COUNT;

struct Field<'a> {
    tag: u64,
    wire: u64,
    raw: &'a [u8],
    body: &'a [u8],
}

fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 128 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}
fn read_varint(data: &[u8], at: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = *data.get(*at).context("truncated protobuf varint")?;
        *at += 1;
        anyhow::ensure!(shift != 63 || byte <= 1, "protobuf varint overflow");
        result |= u64::from(byte & 0x7f) << shift;
        if byte < 128 {
            return Ok(result);
        }
    }
    anyhow::bail!("protobuf varint too long")
}
fn fields(data: &[u8]) -> Result<Vec<Field<'_>>> {
    let mut result = Vec::new();
    let mut at = 0;
    while at < data.len() {
        let start = at;
        let key = read_varint(data, &mut at)?;
        let tag = key >> 3;
        let wire = key & 7;
        anyhow::ensure!(tag > 0, "protobuf zero field");
        let body = match wire {
            0 => {
                let start = at;
                read_varint(data, &mut at)?;
                &data[start..at]
            }
            1 | 5 => {
                let start = at;
                at = at
                    .checked_add(if wire == 1 { 8 } else { 4 })
                    .context("protobuf fixed overflow")?;
                data.get(start..at).context("protobuf fixed truncation")?
            }
            2 => {
                let len = usize::try_from(read_varint(data, &mut at)?)?;
                let start = at;
                at = at.checked_add(len).context("protobuf length overflow")?;
                data.get(start..at).context("protobuf bytes truncation")?
            }
            _ => anyhow::bail!("unsupported protobuf wire type"),
        };
        result.push(Field {
            tag,
            wire,
            raw: &data[start..at],
            body,
        });
    }
    Ok(result)
}
fn only_bytes(data: &[u8], tag: u64) -> Result<&[u8]> {
    let matches = fields(data)?
        .into_iter()
        .filter(|f| f.tag == tag)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matches.len() == 1 && matches[0].wire == 2,
        "protobuf fixture field missing or duplicate: {tag}"
    );
    Ok(matches[0].body)
}
fn number(tag: u64, value: u64) -> Vec<u8> {
    let mut wire = varint(tag << 3);
    wire.extend(varint(value));
    wire
}
fn bytes(tag: u64, value: &[u8]) -> Vec<u8> {
    let mut wire = varint((tag << 3) | 2);
    wire.extend(varint(value.len() as u64));
    wire.extend(value);
    wire
}
fn replace(data: &[u8], tag: u64, new_field: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() + new_field.len());
    let mut found = 0;
    for field in fields(data)? {
        if field.tag == tag {
            out.extend(new_field);
            found += 1;
        } else {
            out.extend(field.raw);
        }
    }
    anyhow::ensure!(found == 1, "protobuf replacement ambiguity: {tag}");
    Ok(out)
}
pub(crate) fn hash(slot: u64) -> String {
    let mut bytes = [0xA5; 32];
    bytes[..8].copy_from_slice(&slot.to_le_bytes());
    bs58::encode(bytes).into_string()
}
async fn update(
    sender: &mpsc::Sender<ReplayInput>,
    offset_ns: u64,
    payload: Vec<u8>,
) -> Result<()> {
    sender
        .send(ReplayInput::Update { offset_ns, payload })
        .await?;
    Ok(())
}
fn padded_transaction(block: &[u8]) -> Result<Vec<u8>> {
    let info = only_bytes(block, 6)?;
    let meta = only_bytes(info, 4)?;
    let mut padded_meta = meta.to_vec();
    padded_meta.extend(bytes(
        6,
        "synthetic full-block sizing ".repeat(32).as_bytes(),
    ));
    let info = replace(info, 4, &bytes(4, &padded_meta))?;
    replace(&info, 1, &bytes(1, &[0x5A; 64]))
}
fn block_frame(
    template: &[u8],
    slot: u64,
    parent_hash: &str,
    transactions: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    for field in fields(template)? {
        match field.tag {
            1 => body.extend(number(1, slot)),
            2 if transactions.is_some() => body.extend(bytes(2, hash(slot).as_bytes())),
            7 => body.extend(number(7, slot - 1)),
            8 => body.extend(bytes(8, parent_hash.as_bytes())),
            6 if transactions.is_some() => {}
            _ => body.extend(field.raw),
        }
    }
    if let Some(transactions) = transactions {
        body.extend(transactions);
    }
    Ok(bytes(5, &body))
}
pub(super) async fn send(
    sender: &mpsc::Sender<ReplayInput>,
    input: &Path,
    old_wallet: &str,
    old_signature: &str,
    bot_wallet: &str,
    bot_signature: &str,
    mut settled: tokio::sync::oneshot::Receiver<()>,
) -> Result<usize> {
    let source = std::fs::read(input.join("source.pb"))?;
    let our = super::rewrite_identity(
        std::fs::read(input.join("cohort-our.pb"))?,
        old_wallet,
        bot_wallet,
        old_signature,
        bot_signature,
    )?;
    let source_block = std::fs::read(input.join("block-100.pb"))?;
    let our_block = super::rewrite_identity(
        std::fs::read(input.join("cohort-block-120.pb"))?,
        old_wallet,
        bot_wallet,
        old_signature,
        bot_signature,
    )?;
    let our_hash = std::str::from_utf8(only_bytes(only_bytes(&our_block, 5)?, 2)?)?.to_owned();
    update(sender, 1_000_000_000, source).await?;
    update(sender, 1_200_000_000, source_block.clone()).await?;
    update(sender, 1_400_000_000, our).await?;
    update(sender, 1_600_000_000, our_block).await?;

    // Run08 did not persist full payloads; this known-field synthetic envelope
    // exceeds its observed 2.65 MiB/slot stream-byte proxy.
    let template = only_bytes(&source_block, 5)?;
    let filler = bytes(6, &padded_transaction(template)?);
    let mut transactions = Vec::new();
    let mut count = 0;
    while transactions.len() + template.len() < BLOCK_BYTES {
        anyhow::ensure!(count < 4_096, "synthetic info too small");
        transactions.extend(&filler);
        count += 1;
    }
    let envelope_bytes = block_frame(template, FIRST_FILLER, &our_hash, Some(&transactions))?.len();
    anyhow::ensure!(
        envelope_bytes <= BLOCK_BYTES + 4_096 && envelope_bytes < 8 << 20,
        "synthetic block sizing or transport bound"
    );
    eprintln!(
        "sustained block envelope_bytes={envelope_bytes} tx_per_block={count} \
        projected_live_metadata_bytes={}",
        152 * (512 + 128 * count)
    );
    let mut parent_hash = our_hash;
    for index in 0..FILLER_COUNT {
        if index > 0 && index % 5_000 == 0 {
            eprintln!("long parent benchmark produced {index} blocks");
        }
        let slot = FIRST_FILLER + index;
        update(
            sender,
            2_000_000_000 + index * STEP_NS,
            block_frame(
                template,
                slot,
                &parent_hash,
                Some(if index < 450 || index >= FILLER_COUNT - 150 {
                    &transactions
                } else {
                    &[]
                }),
            )?,
        )
        .await?;
        parent_hash = hash(slot);
        if index >= FILLER_COUNT - 150 {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        }
    }
    let sell_raw = std::fs::read(input.join("sell.pb"))?;
    let sell = replace(
        &sell_raw,
        4,
        &bytes(
            4,
            &replace(only_bytes(&sell_raw, 4)?, 2, &number(2, SELL_SLOT))?,
        ),
    )?;
    let sell_block_raw = std::fs::read(input.join("block-150.pb"))?;
    let sell_block = block_frame(
        only_bytes(&sell_block_raw, 5)?,
        SELL_SLOT,
        &parent_hash,
        None,
    )?;
    let sell_ns = 2_000_000_000 + FILLER_COUNT * STEP_NS;
    let sell_hash = std::str::from_utf8(only_bytes(only_bytes(&sell_block, 5)?, 2)?)?.to_owned();
    update(sender, sell_ns, sell).await?;
    update(sender, sell_ns + STEP_NS, sell_block).await?;
    parent_hash = sell_hash;
    // The daemon must prepare and settle with parent commits still arriving.
    // Keep the ordinary 400 ms cadence until its receipt is durable.
    let mut tail = 0usize;
    while tail < 225 {
        tokio::select! {
            result = &mut settled => { result?; break; }
            _ = tokio::time::sleep(std::time::Duration::from_millis(400)) => {
                let slot = SELL_SLOT + 1 + tail as u64;
                update(sender, sell_ns + (tail as u64 + 2) * STEP_NS,
                    block_frame(template, slot, &parent_hash, Some(&[]))?).await?;
                parent_hash = hash(slot);
                tail += 1;
            }
        }
    }
    anyhow::ensure!(tail < 225, "SELL did not settle under continuing parent ingress");
    sender.send(ReplayInput::End(sell_ns + (tail as u64 + 3) * STEP_NS)).await?;
    Ok(tail)
}
