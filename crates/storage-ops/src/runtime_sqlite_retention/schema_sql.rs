use anyhow::{bail, Context, Result};
use rusqlite::Connection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SchemaObjectKind {
    Table,
    Index,
    Trigger,
    View,
}

#[derive(Debug, Clone)]
pub(super) struct SchemaObject {
    pub(super) kind: SchemaObjectKind,
    pub(super) name: String,
    pub(super) sql: String,
}

pub(super) fn load_schema_objects(conn: &Connection) -> Result<Vec<SchemaObject>> {
    let mut stmt = conn
        .prepare(
            "SELECT type, name, sql
             FROM main.sqlite_schema
             WHERE sql IS NOT NULL
               AND name NOT LIKE 'sqlite_%'
             ORDER BY
               CASE type
                 WHEN 'table' THEN 0
                 WHEN 'index' THEN 1
                 WHEN 'trigger' THEN 2
                 WHEN 'view' THEN 3
                 ELSE 4
               END,
               name",
        )
        .context("failed preparing sqlite schema read")?;
    let mut rows = stmt.query([]).context("failed querying sqlite schema")?;
    let mut objects = Vec::new();
    while let Some(row) = rows.next().context("failed reading sqlite schema row")? {
        let raw_kind: String = row.get(0)?;
        let Some(kind) = schema_kind(&raw_kind) else {
            continue;
        };
        objects.push(SchemaObject {
            kind,
            name: row.get(1)?,
            sql: row.get(2)?,
        });
    }
    Ok(objects)
}

pub(super) fn qualify_for_schema(object: &SchemaObject, schema: &str) -> Result<String> {
    let mut cursor = Cursor::new(&object.sql);
    cursor.consume_keyword("CREATE")?;
    cursor.skip_ws();
    cursor.consume_optional_keyword("TEMP")?;
    cursor.consume_optional_keyword("TEMPORARY")?;
    cursor.skip_ws();
    if object.kind == SchemaObjectKind::Index {
        cursor.consume_optional_keyword("UNIQUE")?;
        cursor.skip_ws();
    }
    cursor.consume_keyword(object.kind.create_keyword())?;
    cursor.skip_ws();
    if cursor.consume_optional_keyword("IF")? {
        cursor.skip_ws();
        cursor.consume_keyword("NOT")?;
        cursor.skip_ws();
        cursor.consume_keyword("EXISTS")?;
        cursor.skip_ws();
    }
    let (start, end) = cursor.identifier_span()?;
    let qualified = format!("{}.{}", quote_ident(schema), quote_ident(&object.name));
    Ok(format!(
        "{}{}{}",
        &object.sql[..start],
        qualified,
        &object.sql[end..]
    ))
}

pub(super) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn schema_kind(raw: &str) -> Option<SchemaObjectKind> {
    match raw {
        "table" => Some(SchemaObjectKind::Table),
        "index" => Some(SchemaObjectKind::Index),
        "trigger" => Some(SchemaObjectKind::Trigger),
        "view" => Some(SchemaObjectKind::View),
        _ => None,
    }
}

impl SchemaObjectKind {
    fn create_keyword(self) -> &'static str {
        match self {
            SchemaObjectKind::Table => "TABLE",
            SchemaObjectKind::Index => "INDEX",
            SchemaObjectKind::Trigger => "TRIGGER",
            SchemaObjectKind::View => "VIEW",
        }
    }
}

struct Cursor<'a> {
    sql: &'a str,
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, pos: 0 }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.sql[self.pos..].chars().next() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.consume_optional_keyword(keyword)? {
            Ok(())
        } else {
            bail!(
                "expected CREATE SQL keyword {keyword} near: {}",
                &self.sql[self.pos..].chars().take(64).collect::<String>()
            )
        }
    }

    fn consume_optional_keyword(&mut self, keyword: &str) -> Result<bool> {
        let end = self.pos.saturating_add(keyword.len());
        let Some(candidate) = self.sql.get(self.pos..end) else {
            return Ok(false);
        };
        if !candidate.eq_ignore_ascii_case(keyword) {
            return Ok(false);
        }
        if let Some(next) = self.sql[end..].chars().next() {
            if is_ident_char(next) {
                return Ok(false);
            }
        }
        self.pos = end;
        Ok(true)
    }

    fn identifier_span(&mut self) -> Result<(usize, usize)> {
        let Some(ch) = self.sql[self.pos..].chars().next() else {
            bail!("missing CREATE object identifier");
        };
        let start = self.pos;
        match ch {
            '"' | '\'' | '`' => self.quoted_identifier_span(start, ch),
            '[' => self.bracket_identifier_span(start),
            _ => self.bare_identifier_span(start),
        }
    }

    fn quoted_identifier_span(&self, start: usize, quote: char) -> Result<(usize, usize)> {
        let body_start = start + quote.len_utf8();
        let mut iter = self.sql[body_start..].char_indices().peekable();
        while let Some((offset, ch)) = iter.next() {
            if ch == quote {
                if matches!(iter.peek(), Some((_, next)) if *next == quote) {
                    iter.next();
                    continue;
                }
                return Ok((start, body_start + offset + quote.len_utf8()));
            }
        }
        bail!("unterminated quoted CREATE object identifier")
    }

    fn bracket_identifier_span(&self, start: usize) -> Result<(usize, usize)> {
        for (offset, ch) in self.sql[start + 1..].char_indices() {
            if ch == ']' {
                return Ok((start, start + 1 + offset + 1));
            }
        }
        bail!("unterminated bracket CREATE object identifier")
    }

    fn bare_identifier_span(&self, start: usize) -> Result<(usize, usize)> {
        let mut end = start;
        for ch in self.sql[start..].chars() {
            if ch.is_whitespace() || ch == '(' {
                break;
            }
            end += ch.len_utf8();
        }
        if end == start {
            bail!("empty CREATE object identifier");
        }
        Ok((start, end))
    }
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}
