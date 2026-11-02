//! Bind finalized legacy wire to the actual observed full transaction message.
use anyhow::{ensure,Context,Result};
use copybot_core_types::association_delivery::InfoIdentity;
use prost::Message;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;
fn short(out:&mut Vec<u8>,mut n:usize)->Result<()>{
    ensure!(n<=65535,"source_wire_count");
    loop {let v=(n&127) as u8;n>>=7;out.push(v|if n>0 {128}else{0});if n==0 {return Ok(())}}
}
pub fn verify_source_legacy_wire(identity:&InfoIdentity,wire:&[u8])->Result<()>{
    ensure!(wire.len()<=1232 && identity.encoded.len()<=1<<20,"source_wire_bound");
    let info=SubscribeUpdateTransactionInfo::decode(identity.encoded.as_slice())?;
    let tx=info.transaction.context("source_wire_transaction")?;
    let m=tx.message.context("source_wire_message")?;
    let h=m.header.context("source_wire_header")?;
    ensure!(!m.versioned && m.address_table_lookups.is_empty() && m.config.is_none()
        && tx.signatures.len()==1 && h.num_required_signatures==1
        && tx.signatures[0]==info.signature && info.signature.len()==64,
        "source_wire_legacy_profile");
    let mut expected=vec![1];expected.extend_from_slice(&info.signature);
    for n in [h.num_required_signatures,h.num_readonly_signed_accounts,h.num_readonly_unsigned_accounts] {
        expected.push(u8::try_from(n)?);
    }
    short(&mut expected,m.account_keys.len())?;
    for key in m.account_keys {ensure!(key.len()==32,"source_wire_key");expected.extend(key)}
    ensure!(m.recent_blockhash.len()==32,"source_wire_blockhash");expected.extend(m.recent_blockhash);
    short(&mut expected,m.instructions.len())?;
    for ix in m.instructions {
        expected.push(u8::try_from(ix.program_id_index)?);
        short(&mut expected,ix.accounts.len())?;expected.extend(ix.accounts);
        short(&mut expected,ix.data.len())?;expected.extend(ix.data);
    }
    ensure!(wire==expected,"source_wire_observed_message_mismatch");Ok(())
}
