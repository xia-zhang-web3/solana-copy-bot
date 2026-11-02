//! Closed classic token initialization proof for parsed and compiled RPC forms.
use anyhow::{ensure,Context,Result};
use serde_json::Value;
fn program<'a>(i:&'a Value,keys:&'a [&str])->Result<&'a str>{
    if let Some(value)=i.get("programIdIndex"){
        let index=value.as_u64().context("owned_sell_init_program")?;
        let p=keys.get(index as usize).copied().context("owned_sell_init_program")?;
        ensure!(i.get("programId").is_none_or(|v|v.as_str()==Some(p)),"owned_sell_init_program_binding");
        return Ok(p);
    }
    i["programId"].as_str().context("owned_sell_init_program")
}
pub(crate) fn initialized(v:&Value,keys:&[&str],account:&str,mint:&str,wallet:&str)->Result<bool>{
    let outer=v["transaction"]["message"]["instructions"].as_array().context("owned_sell_amm_instructions")?;
    let inner=v["meta"]["innerInstructions"].as_array().context("owned_sell_amm_inner")?.iter()
        .map(|g|g["instructions"].as_array().context("owned_sell_amm_inner")).collect::<Result<Vec<_>>>()?;
    for group in std::iter::once(outer).chain(inner){
        for i in group {
            if program(i,keys)?!=super::TOKEN {continue}
            ensure!(i.get("programIdIndex").is_none() || i.get("parsed").is_none(),"owned_sell_init_representation");
            if i.get("parsed").is_some(){
                if i["parsed"]["type"].as_str().is_some_and(|t|["initializeAccount","initializeAccount2","initializeAccount3"].contains(&t)){
                    let p=&i["parsed"]["info"];
                    if p["account"]==account && p["mint"]==mint && p["owner"]==wallet{return Ok(true)}
                }
            }else{
                let d=bs58::decode(i["data"].as_str().context("owned_sell_init_data")?).into_vec()?;
                if d.first()!=Some(&18){continue}
                ensure!(d.len()==33,"owned_sell_init_data");
                let a=i["accounts"].as_array().context("owned_sell_init_accounts")?;
                ensure!(a.len()==2,"owned_sell_init_accounts");
                let key=|v:&Value|->Result<&str>{
                    if let Some(k)=v.as_str(){return keys.iter().copied().find(|a|*a==k).context("owned_sell_init_key")}
                    keys.get(v.as_u64().context("owned_sell_init_index")? as usize).copied().context("owned_sell_init_index")
                };
                if key(&a[0])?==account && key(&a[1])?==mint && bs58::encode(&d[1..]).into_string()==wallet{return Ok(true)}
            }
        }
    }
    Ok(false)
}
