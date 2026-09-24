use super::owner_buy_wire_fixture as fixture;
use crate::execution_owner_buy_wire::verify;
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{SolanaAccountMeta as A, SolanaInstruction};
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::Result;
use serde_json::json;
const WALLET: [u8;32] = [11;32];

fn request() -> ExecutionSubmitRequest {
    let quote = fixture::quote();
    let mut metadata = super::priority_fee_fixture::metadata();
    metadata.quote_in_amount_raw = Some("10000000".into());
    metadata.quote_out_amount_raw = Some(fixture::OUT.to_string());
    metadata.quote_response_json = Some(quote.to_string());
    metadata.route_plan_json = Some(quote["routePlan"].to_string());
    ExecutionSubmitRequest {
        order_id:"exec-canary:owner-buy:wire-test".into(),signal_id:"owner-buy:wire-test".into(),
        client_order_id:"copybot:owner-buy:wire-test".into(),attempt:1,
        route:"jupiter_swap_instructions".into(),wallet_id:format_pubkey(&WALLET),
        token:fixture::USDC.into(),side:"buy".into(),buy_size_sol:0.01,
        slippage_tolerance_bps:50,wallet_pubkey:format_pubkey(&WALLET),
        entry_route_plan_json:None,metadata,
    }
}
fn route(instructions: &mut [SolanaInstruction]) -> &mut SolanaInstruction {
    instructions.iter_mut().find(|i| i.program_id == parse_pubkey(
        crate::execution_owner_buy_wire::JUPITER,"jupiter").unwrap()).unwrap()
}
fn rejected(instructions: &[SolanaInstruction], reason: &str) -> Result<()> {
    let payload = fixture::payload(WALLET,instructions)?;
    let error = verify(&request(), &payload).err().expect("must reject");
    assert!(error.to_string().contains(reason), "expected {reason}, got {error:#}");
    Ok(())
}
#[test]
fn owner_buy_wire_real_v1_raydium_exact_input_and_message_binding() -> Result<()> {
    let instructions = fixture::instructions(WALLET)?;
    let payload = fixture::payload(WALLET,&instructions)?;
    let proof = verify(&request(),&payload)?;
    assert_eq!(proof.decoded_amount_lamports(),10_000_000);
    proof.verify_same_payload(&payload)?;
    let altered = crate::execution_native_floor::prepare_final_native_floor(
        WALLET,[8;32],&instructions,50_000_001)?;
    assert!(proof.verify_same_payload(altered.payload()).is_err());
    Ok(())
}
#[test]
fn owner_buy_wire_amount_output_slippage_and_platform_fee_are_exact() -> Result<()> {
    for (index, value, reason) in [(16,1,"exact_input"),(24,1,"exact_output"),
        (32,51,"slippage_or_fee"),(34,1,"slippage_or_fee")] {
        let mut instructions = fixture::instructions(WALLET)?;
        let data = &mut route(&mut instructions).data;
        if index < 32 { data[index] = data[index].wrapping_add(value); }
        else { data[index] = value; }
        rejected(&instructions,reason)?;
    }
    Ok(())
}
#[test]
fn owner_buy_wire_parses_full_vec_variant_topology_and_exact_end() -> Result<()> {
    for (index,value,reason) in [(8,2,"route_layout"),(12,17,"swap_variant"),
        (12,99,"swap_variant"),(13,99,"route_topology"),(14,1,"route_topology"),
        (15,2,"route_topology")] {
        let mut instructions = fixture::instructions(WALLET)?;
        route(&mut instructions).data[index] = value;
        rejected(&instructions,reason)?;
    }
    let mut trailing = fixture::instructions(WALLET)?;
    route(&mut trailing).data.push(0);
    rejected(&trailing,"trailing_bytes")?;
    let mut truncated = fixture::instructions(WALLET)?;
    route(&mut truncated).data.pop();
    rejected(&truncated,"truncated")?;
    Ok(())
}
#[test]
fn owner_buy_wire_rejects_foreign_wallet_mint_ata_and_optional_accounts() -> Result<()> {
    for index in 0..9 {
        let mut instructions = fixture::instructions(WALLET)?;
        let account = &mut route(&mut instructions).accounts[index];
        account.pubkey = [87;32];
        account.is_signer = false;
        rejected(&instructions,"route_accounts")?;
    }
    let mut changed_request = request(); changed_request.token = format_pubkey(&[7;32]);
    assert!(verify(&changed_request,&fixture::payload(WALLET,&fixture::instructions(WALLET)?)?).is_err());
    Ok(())
}
#[test]
fn owner_buy_wire_rejects_extra_transfers_approvals_and_foreign_close() -> Result<()> {
    let token = token_program_id();
    let source = associated_token_address(&WALLET,&wsol_mint(),&token);
    let mut extra = fixture::instructions(WALLET)?;
    extra.insert(4,super::native_funding_fixture::transfer(WALLET,[87;32],1));
    rejected(&extra,"wrap_amount_or_destination")?;
    let mut approve = fixture::instructions(WALLET)?;
    approve.insert(4,SolanaInstruction{program_id:token,
        accounts:vec![A::writable(source),A::readonly([87;32]),A::signer_writable(WALLET)],
        data:[vec![4],1_u64.to_le_bytes().to_vec()].concat()});
    rejected(&approve,"unsupported_token_instruction")?;
    let mut close = fixture::instructions(WALLET)?;
    close.last_mut().unwrap().accounts[1] = A::writable([87;32]);
    rejected(&close,"wire_close")?;
    let mut wrong_wrap = fixture::instructions(WALLET)?;
    wrong_wrap[4].data[4] = wrong_wrap[4].data[4].wrapping_add(1);
    rejected(&wrong_wrap,"wrap_amount_or_destination")?;
    let mut duplicate = fixture::instructions(WALLET)?;
    let copied = route(&mut duplicate).clone(); duplicate.insert(7,copied);
    rejected(&duplicate,"multiple_routes")?;
    Ok(())
}
#[test]
fn owner_buy_wire_rejects_quote_amount_mint_route_and_slippage_change() -> Result<()> {
    let payload = fixture::payload(WALLET,&fixture::instructions(WALLET)?)?;
    for (key,value) in [("inAmount",json!("10000001")),("outputMint",json!(format_pubkey(&[7;32]))),
        ("slippageBps",json!(51)),("instructionVersion",json!("V2"))] {
        let mut quote=fixture::quote(); quote[key]=value;
        let mut r=request(); r.metadata.quote_response_json=Some(quote.to_string());
        assert!(verify(&r,&payload).is_err(),"changed {key}");
    }
    let mut quote=fixture::quote();quote["routePlan"][0]["swapInfo"]["label"]=json!("Whirlpool");
    let mut r=request();r.metadata.quote_response_json=Some(quote.to_string());
    assert!(verify(&r,&payload).is_err());
    Ok(())
}
#[test]
fn owner_buy_wire_quote_request_is_closed_v1_direct_raydium_only() -> Result<()> {
    let config = super::owner_buy_fixture::config(&format_pubkey(&WALLET),"http://127.0.0.1:9","/unused");
    let request=crate::execution_quote_http::build_owner_quote_request(
        &reqwest::Client::new(),&config,fixture::USDC,"10000000",50)?;
    let pairs:std::collections::BTreeMap<_,_>=request.url().query_pairs().into_owned().collect();
    assert_eq!(pairs["instructionVersion"],"V1");assert_eq!(pairs["onlyDirectRoutes"],"true");
    assert_eq!(pairs["dexes"],"Raydium");assert_eq!(pairs["amount"],"10000000");
    assert_eq!(pairs["slippageBps"],"50");assert_eq!(pairs["swapMode"],"ExactIn");
    Ok(())
}
#[test]
fn owner_buy_wire_quote_threshold_integer_rounding_preserves_approved_bps() -> Result<()> {
    let mut config=super::owner_buy_fixture::config(&format_pubkey(&WALLET),"http://127.0.0.1:9","/unused");
    let policy=config.owner_technical_buy.as_mut().unwrap();
    policy.mint=fixture::USDC.into();policy.max_slippage_bps=50;
    config.quote_canary_buy_slippage_bps=50;
    let intent=crate::execution_owner_buy_authority::configured(&config)?.unwrap();
    let mut quote=crate::execution_quote_http::quote_sample_from_json(fixture::quote())?;
    quote.quote_response_available_ts=Some(chrono::Utc::now());
    let metadata=crate::execution_owner_buy_quote::metadata(&intent,6,quote.clone())?;
    assert_eq!(metadata.slippage_bps,Some(50.0));
    let mut value=fixture::quote();value["otherAmountThreshold"]=json!("1492499");
    quote.response_json=value.to_string();
    assert!(crate::execution_owner_buy_quote::metadata(&intent,6,quote).is_err());
    Ok(())
}

#[test]
fn owner_buy_wire_remaining_accounts_bind_dex_pool_and_user_ends() -> Result<()> {
    for index in [9,10,11,24,25,26] {
        let mut instructions=fixture::instructions(WALLET)?;
        let account=&mut route(&mut instructions).accounts[index];
        account.pubkey=[87;32]; account.is_signer=false;
        rejected(&instructions,"dex_accounts")?;
    }
    for index in [12,13,16,23] {
        let mut instructions=fixture::instructions(WALLET)?;
        let account=&mut route(&mut instructions).accounts[index];
        account.is_writable=!account.is_writable;
        rejected(&instructions,"dex_account_role")?;
    }
    let mut extra=fixture::instructions(WALLET)?;
    route(&mut extra).accounts.push(A::readonly([87;32]));
    rejected(&extra,"route_accounts")?;
    let mut unknown_compute=fixture::instructions(WALLET)?;
    unknown_compute[0].data[0]=99;
    rejected(&unknown_compute,"unsupported_budget_instruction")?;
    Ok(())
}
