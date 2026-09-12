use super::native_ata_fixture::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

#[tokio::test]
async fn native_ata_closed_bundle_rejects_same_keys_other_message_signature_and_wallet(
) -> Result<()> {
    let a = payload(&direct(true, false, 17)?)?;
    let b = payload(&direct(true, false, 19)?)?;
    let mut ai = Inputs::new(&a, 7)?;
    ai.fee = Some(111);
    ai.set(WALLET, present(100, system_program_id(), vec![]));
    let mut bi = Inputs::new(&b, 9)?;
    bi.fee = Some(222);
    bi.set(WALLET, present(900, system_program_id(), vec![]));
    let (a_result, b_result) = tokio::join!(ai.collect(&a), bi.collect(&b));
    let af = a_result?;
    let bf = b_result?;
    assert_eq!(af.native().requested_keys(), bf.native().requested_keys());
    assert_ne!(
        af.native().requirements().binding.message_bytes,
        bf.native().requirements().binding.message_bytes
    );
    assert_eq!(plan(&a, WALLET, &af)?.known_wallet_payer_lamports, 14);
    assert_eq!(plan(&b, WALLET, &bf)?.known_wallet_payer_lamports, 18);
    for (p, facts) in [(&a, &bf), (&b, &af)] {
        assert!(format!("{:#}", plan(p, WALLET, facts).unwrap_err())
            .contains("native_setup_requirements_mismatch"));
    }
    assert!(plan(&a, PEER, &af).is_err());
    let mut wire = STANDARD.decode(&a)?;
    wire[1..65].fill(127);
    assert!(plan(&STANDARD.encode(wire), WALLET, &af).is_err());
    assert_eq!(
        plan(&a, WALLET, &af.clone())?.known_wallet_payer_lamports,
        14
    );
    assert_eq!(af.rent().lamports(), 7);
    assert_eq!(bf.rent().lamports(), 9);
    assert_eq!(af.native().fee().value, Some(111));
    assert_eq!(bf.native().fee().value, Some(222));
    Ok(())
}
