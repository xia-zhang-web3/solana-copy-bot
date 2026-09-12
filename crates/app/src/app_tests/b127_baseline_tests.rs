use super::{b127_causal_fixture::*, priority_fee_route_fixture::Route};
use anyhow::Result;
#[path = "../execution_tiny_budget_amount.rs"]
mod decoded_amount;
#[tokio::test]
async fn b127_jupiter_without_new_mode_keeps_both_refusals() -> Result<()> {
    let mut f = fixture(Route::Metis).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let signed = envelope.signed_transaction_base64.as_ref().unwrap();
    let message = crate::execution_transaction_wire::decode_message(signed, |_| Ok(()))?;
    let amount = decoded_amount::buy(&message, &f.request.wallet_pubkey).unwrap_err();
    assert_eq!(amount.to_string(), "tiny_budget_buy_amount_unproven");
    let out = f.submit(&envelope).await?;
    assert_eq!(f.sends(), 0);
    assert!(
        out.error
            .as_deref()
            .unwrap()
            .contains("initial_sol_jupiter_funding_unproven"),
        "{out:?}"
    );
    println!("B127_BASELINE funding={out:?} amount={amount}");
    f.finish().await
}
