//! Repository for `qry_saved_document_queries`: queries composed in the
//! document query builder, scoped to one collection.
//!
//! The whole `DocumentQuerySpec` is stored as JSON next to the version of
//! that stored form. Reading tolerates both older rows (fields added to the
//! spec later take their serde default) and newer ones (unknown fields are
//! skipped), so the table needs no migration when the spec grows.

use std::sync::{Arc, Mutex};

use dbflux_core::{DocumentQueryMode, DocumentQuerySpec};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::error::StorageError;

const DB_PATH: &str = "dbflux.db";

/// Version of the stored spec JSON written by this build.
pub const DOCUMENT_QUERY_FORMAT_VERSION: i64 = 1;

/// The collection a saved document query belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocumentQueryScope {
    pub profile_id: String,
    pub database: String,
    pub collection: String,
}

impl DocumentQueryScope {
    pub fn new(
        profile_id: impl Into<String>,
        database: impl Into<String>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            profile_id: profile_id.into(),
            database: database.into(),
            collection: collection.into(),
        }
    }
}

/// A saved document query without its spec, for lists.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedDocumentQuerySummary {
    pub id: String,
    pub scope: DocumentQueryScope,
    pub name: String,
    pub mode: DocumentQueryMode,
    pub updated_at: i64,
}

/// A saved document query with its spec.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedDocumentQuery {
    pub summary: SavedDocumentQuerySummary,
    pub spec: DocumentQuerySpec,
}

/// Repository for the `qry_saved_document_queries` table.
#[derive(Clone)]
pub struct DocumentQueryRepo {
    conn: Arc<Mutex<Connection>>,
}

impl DocumentQueryRepo {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Saved queries of one collection, most recently saved first.
    pub fn list_for_scope(
        &self,
        scope: &DocumentQueryScope,
    ) -> Result<Vec<SavedDocumentQuerySummary>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let mut statement = conn
            .prepare(
                "SELECT id, profile_id, database_name, collection_name, name, mode, updated_at
                 FROM qry_saved_document_queries
                 WHERE profile_id = ?1 AND database_name = ?2 AND collection_name = ?3
                 ORDER BY updated_at DESC, name ASC",
            )
            .map_err(sqlite_err)?;

        let rows = statement
            .query_map(
                rusqlite::params![scope.profile_id, scope.database, scope.collection],
                map_summary_row,
            )
            .map_err(sqlite_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_err)?;

        Ok(rows)
    }

    /// The saved query `id` of `scope`, or `None` when that collection has
    /// no such row.
    pub fn get(
        &self,
        id: &str,
        scope: &DocumentQueryScope,
    ) -> Result<Option<SavedDocumentQuery>, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let row = conn
            .query_row(
                "SELECT id, profile_id, database_name, collection_name, name, mode, updated_at,
                        spec_json
                 FROM qry_saved_document_queries
                 WHERE id = ?1 AND profile_id = ?2 AND database_name = ?3
                   AND collection_name = ?4",
                rusqlite::params![id, scope.profile_id, scope.database, scope.collection],
                |row| Ok((map_summary_row(row)?, row.get::<_, String>(7)?)),
            )
            .optional()
            .map_err(sqlite_err)?;

        let Some((summary, spec_json)) = row else {
            return Ok(None);
        };

        let spec = serde_json::from_str::<DocumentQuerySpec>(&spec_json).map_err(|error| {
            StorageError::Data(format!(
                "saved document query {id} could not be read: {error}"
            ))
        })?;

        Ok(Some(SavedDocumentQuery { summary, spec }))
    }

    /// Saves `spec` as `name` in `scope`. A query with that name in the same
    /// scope is replaced and keeps its id; otherwise a new row is created.
    pub fn upsert_by_name(
        &self,
        scope: &DocumentQueryScope,
        name: &str,
        spec: &DocumentQuerySpec,
    ) -> Result<SavedDocumentQuerySummary, StorageError> {
        let spec_json = serde_json::to_string(spec).map_err(|error| {
            StorageError::Data(format!("document query could not be serialized: {error}"))
        })?;
        let now_ms = now_millis();

        let conn = self.conn.lock().map_err(lock_err)?;

        let existing_id: Option<String> = conn
            .query_row(
                "SELECT id FROM qry_saved_document_queries
                 WHERE profile_id = ?1 AND database_name = ?2 AND collection_name = ?3
                   AND name = ?4",
                rusqlite::params![scope.profile_id, scope.database, scope.collection, name],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;

        let id = existing_id.unwrap_or_else(|| Uuid::now_v7().to_string());

        conn.execute(
            "INSERT INTO qry_saved_document_queries
                 (id, profile_id, database_name, collection_name, name, mode, format_version,
                  spec_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
             ON CONFLICT (id) DO UPDATE SET
                 mode = excluded.mode,
                 format_version = excluded.format_version,
                 spec_json = excluded.spec_json,
                 updated_at = excluded.updated_at",
            rusqlite::params![
                id,
                scope.profile_id,
                scope.database,
                scope.collection,
                name,
                mode_text(spec.mode),
                DOCUMENT_QUERY_FORMAT_VERSION,
                spec_json,
                now_ms,
            ],
        )
        .map_err(sqlite_err)?;

        Ok(SavedDocumentQuerySummary {
            id,
            scope: scope.clone(),
            name: name.to_string(),
            mode: spec.mode,
            updated_at: now_ms,
        })
    }

    /// Deletes the saved query `id` of `scope`. Returns whether a row was
    /// deleted.
    pub fn delete(&self, id: &str, scope: &DocumentQueryScope) -> Result<bool, StorageError> {
        let conn = self.conn.lock().map_err(lock_err)?;

        let deleted = conn
            .execute(
                "DELETE FROM qry_saved_document_queries
                 WHERE id = ?1 AND profile_id = ?2 AND database_name = ?3
                   AND collection_name = ?4",
                rusqlite::params![id, scope.profile_id, scope.database, scope.collection],
            )
            .map_err(sqlite_err)?;

        Ok(deleted > 0)
    }
}

fn mode_text(mode: DocumentQueryMode) -> &'static str {
    match mode {
        DocumentQueryMode::Find => "find",
        DocumentQueryMode::Aggregate => "aggregate",
    }
}

fn parse_mode(text: &str) -> DocumentQueryMode {
    match text {
        "aggregate" => DocumentQueryMode::Aggregate,
        _ => DocumentQueryMode::Find,
    }
}

fn map_summary_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SavedDocumentQuerySummary> {
    Ok(SavedDocumentQuerySummary {
        id: row.get(0)?,
        scope: DocumentQueryScope {
            profile_id: row.get(1)?,
            database: row.get(2)?,
            collection: row.get(3)?,
        },
        name: row.get(4)?,
        mode: parse_mode(&row.get::<_, String>(5)?),
        updated_at: row.get(6)?,
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
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use super::*;
    use crate::bootstrap::StorageRuntime;
    use dbflux_core::{
        DocumentAccumulator, DocumentAccumulatorKind, DocumentCombinator, DocumentCondition,
        DocumentFilterGroup, DocumentFilterNode, DocumentGroupStage, DocumentOperator,
        DocumentValue,
    };

    fn repo() -> (DocumentQueryRepo, Arc<Mutex<Connection>>) {
        let runtime = StorageRuntime::in_memory().expect("runtime");
        let conn = runtime.viz_connection().expect("connection");
        (DocumentQueryRepo::new(Arc::clone(&conn)), conn)
    }

    fn scope(collection: &str) -> DocumentQueryScope {
        DocumentQueryScope::new("profile-1", "shop", collection)
    }

    fn find_spec(age: i64) -> DocumentQuerySpec {
        DocumentQuerySpec {
            filter: DocumentFilterGroup::new(
                DocumentCombinator::And,
                vec![DocumentFilterNode::Condition(DocumentCondition::new(
                    "age",
                    DocumentOperator::Gt,
                    DocumentValue::Integer(age),
                ))],
            ),
            limit: Some(20),
            ..DocumentQuerySpec::default()
        }
    }

    fn aggregate_spec() -> DocumentQuerySpec {
        DocumentQuerySpec {
            mode: DocumentQueryMode::Aggregate,
            group: Some(DocumentGroupStage {
                keys: vec!["status".to_string()],
                accumulators: vec![DocumentAccumulator {
                    name: "count".to_string(),
                    kind: DocumentAccumulatorKind::Count,
                }],
            }),
            ..DocumentQuerySpec::default()
        }
    }

    #[test]
    fn a_saved_query_reads_back_with_its_spec_and_mode() {
        let (repo, _) = repo();

        let summary = repo
            .upsert_by_name(&scope("orders"), "by status", &aggregate_spec())
            .expect("save");
        assert_eq!(summary.mode, DocumentQueryMode::Aggregate);

        let saved = repo
            .get(&summary.id, &scope("orders"))
            .expect("get")
            .expect("exists");
        assert_eq!(saved.spec, aggregate_spec());
        assert_eq!(saved.summary.name, "by status");
        assert_eq!(saved.summary.scope, scope("orders"));
        assert_eq!(saved.summary.mode, DocumentQueryMode::Aggregate);

        assert_eq!(repo.get("missing", &scope("orders")).expect("get"), None);
    }

    #[test]
    fn saving_under_an_existing_name_replaces_it_and_keeps_its_id() {
        let (repo, _) = repo();

        let first = repo
            .upsert_by_name(&scope("orders"), "adults", &find_spec(18))
            .expect("first save");
        let second = repo
            .upsert_by_name(&scope("orders"), "adults", &find_spec(21))
            .expect("second save");

        assert_eq!(first.id, second.id);
        assert_eq!(repo.list_for_scope(&scope("orders")).unwrap().len(), 1);
        assert_eq!(
            repo.get(&first.id, &scope("orders"))
                .unwrap()
                .expect("exists")
                .spec,
            find_spec(21)
        );
    }

    #[test]
    fn a_list_holds_only_the_queries_of_its_collection() {
        let (repo, _) = repo();

        repo.upsert_by_name(&scope("orders"), "adults", &find_spec(18))
            .unwrap();
        repo.upsert_by_name(&scope("customers"), "adults", &find_spec(18))
            .unwrap();
        repo.upsert_by_name(
            &DocumentQueryScope::new("profile-2", "shop", "orders"),
            "other profile",
            &find_spec(18),
        )
        .unwrap();
        repo.upsert_by_name(
            &DocumentQueryScope::new("profile-1", "archive", "orders"),
            "other database",
            &find_spec(18),
        )
        .unwrap();

        let names: Vec<String> = repo
            .list_for_scope(&scope("orders"))
            .unwrap()
            .into_iter()
            .map(|summary| summary.name)
            .collect();

        assert_eq!(names, vec!["adults".to_string()]);
    }

    #[test]
    fn deleting_removes_the_row() {
        let (repo, _) = repo();

        let summary = repo
            .upsert_by_name(&scope("orders"), "adults", &find_spec(18))
            .unwrap();

        assert!(repo.delete(&summary.id, &scope("orders")).unwrap());
        assert!(
            !repo.delete(&summary.id, &scope("orders")).unwrap(),
            "already gone"
        );
        assert!(repo.list_for_scope(&scope("orders")).unwrap().is_empty());
    }

    #[test]
    fn get_and_delete_stay_inside_their_scope() {
        let (repo, _) = repo();

        let summary = repo
            .upsert_by_name(&scope("orders"), "adults", &find_spec(18))
            .unwrap();

        assert_eq!(repo.get(&summary.id, &scope("customers")).unwrap(), None);
        assert!(!repo.delete(&summary.id, &scope("customers")).unwrap());
        assert!(repo.get(&summary.id, &scope("orders")).unwrap().is_some());
    }

    #[test]
    fn a_row_written_by_another_format_version_still_reads() {
        let (repo, conn) = repo();

        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO qry_saved_document_queries
                     (id, profile_id, database_name, collection_name, name, mode,
                      format_version, spec_json, created_at, updated_at)
                 VALUES ('old', 'profile-1', 'shop', 'orders', 'legacy', 'find', 1,
                         '{\"mode\":\"Find\",\"filter\":{\"combinator\":\"And\",\"children\":[]},\"limit\":5,\"added_later\":true}',
                         1, 1)",
                [],
            )
            .unwrap();

        let saved = repo.get("old", &scope("orders")).unwrap().expect("exists");
        assert_eq!(saved.spec.limit, Some(5));
        assert!(saved.spec.sort.is_empty());
    }

    #[test]
    fn an_unreadable_spec_is_an_error_not_an_empty_query() {
        let (repo, conn) = repo();

        conn.lock()
            .unwrap()
            .execute(
                "INSERT INTO qry_saved_document_queries
                     (id, profile_id, database_name, collection_name, name, mode,
                      format_version, spec_json, created_at, updated_at)
                 VALUES ('broken', 'profile-1', 'shop', 'orders', 'broken', 'find', 1,
                         'not json', 1, 1)",
                [],
            )
            .unwrap();

        assert!(matches!(
            repo.get("broken", &scope("orders")),
            Err(StorageError::Data(_))
        ));
    }
}
