//! Preserve owner EXIT identity while admitting the exact receipt quantity of a fresh BUY.
use anyhow::{ensure,Context,Result};
use rusqlite::{Connection,OptionalExtension};
pub const VERSION: &str = "0090_owner_exit_receipt_amount.sql";
pub fn required(c: &Connection,files: &[std::path::PathBuf]) -> Result<bool> {
    if !files.iter().any(|p|p.file_name().and_then(|s|s.to_str()) == Some(VERSION)) {return Ok(false)}
    Ok(c.query_row("SELECT 1 FROM schema_migrations WHERE version=?1",[VERSION],|r|r.get::<_,i32>(0)).optional()?.is_none())
}
pub fn apply(c: &Connection,sql: &str) -> Result<()> {
    ensure!(!c.pragma_query_value(None,"foreign_keys",|r|r.get::<_,bool>(0))?,"owner EXIT amount requires protected rebuild");
    let old:String = c.query_row("SELECT sql FROM sqlite_master WHERE type='table' AND name='owner_exit_intents'",[],|r|r.get(0))?;
    ensure!(old.starts_with("CREATE TABLE owner_exit_intents (")
        && old.matches("CHECK(amount_raw=1167085)").count() == 1,"owner EXIT amount schema unsupported");
    let objects=c.prepare("SELECT sql FROM sqlite_master WHERE tbl_name='owner_exit_intents' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name")?
        .query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let columns=c.prepare("PRAGMA table_info(owner_exit_intents)")?.query_map([],|r|r.get::<_,String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?.into_iter().map(|s|format!("\"{}\"",s.replace('"',"\"\""))).collect::<Vec<_>>().join(",");
    let ddl=old.replacen("CREATE TABLE owner_exit_intents (","CREATE TABLE owner_exit_intents_0090 (",1)
        .replacen("CHECK(amount_raw=1167085)","CHECK(amount_raw>0)",1);
    let legacy:bool=c.pragma_query_value(None,"legacy_alter_table",|r|r.get(0))?;
    c.pragma_update(None,"legacy_alter_table",true)?;
    let result=(|| {
        c.execute_batch("CREATE TEMP TABLE owner_exit_amount_rebuild_guard(enabled INTEGER);INSERT INTO owner_exit_amount_rebuild_guard VALUES(1);")?;
        c.execute_batch(sql)?;c.execute_batch(&ddl)?;
        c.execute_batch(&format!("INSERT INTO owner_exit_intents_0090({columns}) SELECT {columns} FROM owner_exit_intents;"))?;
        ensure!(!c.prepare(&format!("SELECT {columns} FROM owner_exit_intents EXCEPT SELECT {columns} FROM owner_exit_intents_0090"))?.exists([])?
            && !c.prepare(&format!("SELECT {columns} FROM owner_exit_intents_0090 EXCEPT SELECT {columns} FROM owner_exit_intents"))?.exists([])?,"owner EXIT migration rows changed");
        c.execute_batch("DROP TABLE owner_exit_intents;ALTER TABLE owner_exit_intents_0090 RENAME TO owner_exit_intents;")?;
        for object in objects {c.execute_batch(&object).context("restore owner EXIT identity trigger")?;}
        c.execute_batch("DROP TABLE owner_exit_amount_rebuild_guard")?;Ok(())
    })();
    c.pragma_update(None,"legacy_alter_table",legacy)?;result
}
