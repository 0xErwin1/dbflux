//! Repository for `snp_snippets`: code snippets scoped to one query language.
//!
//! Fields are validated and trimmed here (the body keeps its inner
//! whitespace; it only has to contain something). A prefix already used in
//! the same language surfaces as `SnippetError::DuplicatePrefix`, never as a
//! raw SQLite constraint error.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

const COLUMNS: &str = "id, name, prefix, language, body, builtin_key, created_at, updated_at";

/// A stored snippet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnippetRecord {
    pub id: String,
    pub name: String,
    pub prefix: String,
    /// Stable lowercase language id, `"sql"` for SQL.
    pub language: String,
    pub body: String,
    /// Stable key of a shipped snippet (`"sql.select"`), `None` for user ones.
    pub builtin_key: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The editable fields of a snippet, for insert and update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnippetDraft {
    pub name: String,
    pub prefix: String,
    pub language: String,
    pub body: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SnippetError {
    #[error("a snippet needs a name")]
    EmptyName,

    #[error("a snippet needs a prefix")]
    EmptyPrefix,

    #[error("a snippet needs a language")]
    EmptyLanguage,

    #[error("a snippet needs a body")]
    EmptyBody,

    #[error("another {language} snippet already uses the prefix {prefix}")]
    DuplicatePrefix { language: String, prefix: String },

    #[error("snippet {0} does not exist")]
    NotFound(String),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Repository for the `snp_snippets` table.
#[derive(Clone)]
pub struct SnippetRepo {
    conn: Arc<Mutex<Connection>>,
}

impl SnippetRepo {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Every snippet, ordered by language then prefix.
    pub fn list_all(&self) -> Result<Vec<SnippetRecord>, SnippetError> {
        self.query(
            &format!("SELECT {COLUMNS} FROM snp_snippets ORDER BY language, prefix"),
            [],
        )
    }

    /// The snippets of one language, ordered by prefix.
    pub fn list_by_language(&self, language: &str) -> Result<Vec<SnippetRecord>, SnippetError> {
        self.query(
            &format!("SELECT {COLUMNS} FROM snp_snippets WHERE language = ?1 ORDER BY prefix"),
            [normalize_language(language)],
        )
    }

    pub fn get(&self, id: &str) -> Result<Option<SnippetRecord>, SnippetError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let record = conn
            .query_row(
                &format!("SELECT {COLUMNS} FROM snp_snippets WHERE id = ?1"),
                [id],
                map_row,
            )
            .optional()
            .map_err(sqlite_err)?;

        Ok(record)
    }

    /// Stores a new user snippet and returns it.
    pub fn insert(&self, draft: &SnippetDraft) -> Result<SnippetRecord, SnippetError> {
        let draft = validate(draft)?;
        let id = Uuid::now_v7().to_string();
        let now_ms = now_millis();

        let conn = self.conn.lock().map_err(lock_err)?;

        conn.execute(
            "INSERT INTO snp_snippets
                 (id, name, prefix, language, body, builtin_key, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?6)",
            rusqlite::params![
                id,
                draft.name,
                draft.prefix,
                draft.language,
                draft.body,
                now_ms
            ],
        )
        .map_err(|error| map_write_error(error, &draft))?;

        Ok(SnippetRecord {
            id,
            name: draft.name,
            prefix: draft.prefix,
            language: draft.language,
            body: draft.body,
            builtin_key: None,
            created_at: now_ms,
            updated_at: now_ms,
        })
    }

    /// Replaces the editable fields of snippet `id`. A built-in keeps its
    /// `builtin_key`, so editing it does not make it a user snippet.
    pub fn update(&self, id: &str, draft: &SnippetDraft) -> Result<SnippetRecord, SnippetError> {
        let draft = validate(draft)?;
        let now_ms = now_millis();

        let conn = self.conn.lock().map_err(lock_err)?;

        let changed = conn
            .execute(
                "UPDATE snp_snippets
                 SET name = ?2, prefix = ?3, language = ?4, body = ?5, updated_at = ?6
                 WHERE id = ?1",
                rusqlite::params![
                    id,
                    draft.name,
                    draft.prefix,
                    draft.language,
                    draft.body,
                    now_ms
                ],
            )
            .map_err(|error| map_write_error(error, &draft))?;

        if changed == 0 {
            return Err(SnippetError::NotFound(id.to_string()));
        }

        conn.query_row(
            &format!("SELECT {COLUMNS} FROM snp_snippets WHERE id = ?1"),
            [id],
            map_row,
        )
        .map_err(|error| sqlite_err(error).into())
    }

    /// Deletes snippet `id`. Returns whether a row was deleted.
    pub fn delete(&self, id: &str) -> Result<bool, SnippetError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let deleted = conn
            .execute("DELETE FROM snp_snippets WHERE id = ?1", [id])
            .map_err(sqlite_err)?;

        Ok(deleted > 0)
    }

    fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<Vec<SnippetRecord>, SnippetError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut statement = conn.prepare(sql).map_err(sqlite_err)?;
        let rows = statement
            .query_map(params, map_row)
            .map_err(sqlite_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_err)?;

        Ok(rows)
    }
}

fn normalize_language(language: &str) -> String {
    language.trim().to_lowercase()
}

fn validate(draft: &SnippetDraft) -> Result<SnippetDraft, SnippetError> {
    let name = draft.name.trim();
    if name.is_empty() {
        return Err(SnippetError::EmptyName);
    }

    let prefix = draft.prefix.trim();
    if prefix.is_empty() {
        return Err(SnippetError::EmptyPrefix);
    }

    let language = normalize_language(&draft.language);
    if language.is_empty() {
        return Err(SnippetError::EmptyLanguage);
    }

    if draft.body.trim().is_empty() {
        return Err(SnippetError::EmptyBody);
    }

    Ok(SnippetDraft {
        name: name.to_string(),
        prefix: prefix.to_string(),
        language,
        body: draft.body.clone(),
    })
}

fn map_write_error(error: rusqlite::Error, draft: &SnippetDraft) -> SnippetError {
    if let rusqlite::Error::SqliteFailure(failure, _) = &error
        && failure.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
    {
        return SnippetError::DuplicatePrefix {
            language: draft.language.clone(),
            prefix: draft.prefix.clone(),
        };
    }

    sqlite_err(error).into()
}

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SnippetRecord> {
    Ok(SnippetRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        prefix: row.get(2)?,
        language: row.get(3)?,
        body: row.get(4)?,
        builtin_key: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn sqlite_err(source: rusqlite::Error) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source,
    }
}

fn lock_err<T>(error: std::sync::PoisonError<T>) -> StorageError {
    StorageError::Sqlite {
        path: DB_PATH.into(),
        source: rusqlite::Error::InvalidParameterName(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
