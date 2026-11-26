#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use super::*;
use crate::bootstrap::StorageRuntime;
use crate::migrations::MigrationRegistry;

fn repo() -> (SnippetRepo, Arc<Mutex<Connection>>) {
    let runtime = StorageRuntime::in_memory().expect("runtime");
    let conn = runtime.viz_connection().expect("connection");
    (SnippetRepo::new(Arc::clone(&conn)), conn)
}

fn draft(name: &str, prefix: &str, language: &str, body: &str) -> SnippetDraft {
    SnippetDraft {
        name: name.to_string(),
        prefix: prefix.to_string(),
        language: language.to_string(),
        body: body.to_string(),
    }
}

fn row_count(conn: &Arc<Mutex<Connection>>) -> i64 {
    conn.lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM snp_snippets", [], |row| row.get(0))
        .unwrap()
}

const BUILT_INS: [(&str, &str, &str, &str); 6] = [
    (
        "sql.select",
        "sel",
        "Select rows",
        "SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0",
    ),
    (
        "sql.insert",
        "ins",
        "Insert a row",
        "INSERT INTO ${1:table} (${2:columns})\nVALUES (${3:values});$0",
    ),
    (
        "sql.update",
        "upd",
        "Update rows",
        "UPDATE ${1:table}\nSET ${2:column} = ${3:value}\nWHERE ${4:condition};$0",
    ),
    (
        "sql.delete",
        "del",
        "Delete rows",
        "DELETE FROM ${1:table}\nWHERE ${2:condition};$0",
    ),
    (
        "sql.cte",
        "cte",
        "Common table expression",
        "WITH ${1:name} AS (\n  ${2:SELECT 1}\n)\nSELECT *\nFROM $1;$0",
    ),
    (
        "sql.count",
        "cnt",
        "Count rows",
        "SELECT COUNT(*)\nFROM ${1:table};$0",
    ),
];

#[test]
fn the_migration_seeds_exactly_the_six_sql_built_ins() {
    let (repo, _) = repo();

    let records = repo.list_all().unwrap();
    assert_eq!(records.len(), 6);

    let mut expected: Vec<_> = BUILT_INS.to_vec();
    expected.sort_by_key(|(_, prefix, _, _)| *prefix);

    for (record, (key, prefix, name, body)) in records.iter().zip(expected) {
        assert_eq!(record.builtin_key.as_deref(), Some(key));
        assert_eq!(record.prefix, prefix);
        assert_eq!(record.name, name);
        assert_eq!(record.body, body);
        assert_eq!(record.language, "sql");
    }
}

#[test]
fn a_snippet_round_trips_through_insert_list_update_and_delete() {
    let (repo, _) = repo();

    let inserted = repo
        .insert(&draft("  Top ten ", " top ", "sql", "SELECT $0 LIMIT 10"))
        .unwrap();
    assert_eq!(inserted.name, "Top ten");
    assert_eq!(inserted.prefix, "top");
    assert_eq!(inserted.builtin_key, None);

    assert_eq!(repo.get(&inserted.id).unwrap(), Some(inserted.clone()));
    assert!(
        repo.list_by_language("sql")
            .unwrap()
            .iter()
            .any(|record| record.id == inserted.id)
    );
    assert!(repo.list_by_language("mongodb").unwrap().is_empty());

    let updated = repo
        .update(&inserted.id, &draft("Top N", "topn", "sql", "SELECT $0"))
        .unwrap();
    assert_eq!(updated.id, inserted.id);
    assert_eq!(updated.prefix, "topn");
    assert_eq!(updated.created_at, inserted.created_at);
    assert_eq!(repo.get(&inserted.id).unwrap(), Some(updated));

    assert!(repo.delete(&inserted.id).unwrap());
    assert!(!repo.delete(&inserted.id).unwrap());
    assert_eq!(repo.get(&inserted.id).unwrap(), None);
}

#[test]
fn list_all_orders_by_language_then_prefix() {
    let (repo, _) = repo();
    repo.insert(&draft("A", "aaa", "redis", "GET $0")).unwrap();
    repo.insert(&draft("B", "zzz", "sql", "SELECT $0")).unwrap();

    let order: Vec<(String, String)> = repo
        .list_all()
        .unwrap()
        .into_iter()
        .map(|record| (record.language, record.prefix))
        .collect();

    let mut sorted = order.clone();
    sorted.sort();
    assert_eq!(order, sorted);
    assert_eq!(order.first().map(|pair| pair.0.as_str()), Some("redis"));
}

#[test]
fn a_duplicate_prefix_in_the_same_language_is_a_typed_error_and_changes_nothing() {
    let (repo, conn) = repo();
    let before = repo.list_all().unwrap();

    let error = repo
        .insert(&draft("Mine", "sel", "sql", "SELECT 1"))
        .unwrap_err();
    assert!(matches!(
        &error,
        SnippetError::DuplicatePrefix { language, prefix } if language == "sql" && prefix == "sel"
    ));

    assert_eq!(repo.list_all().unwrap(), before);
    assert_eq!(row_count(&conn), 6);

    let other = repo.insert(&draft("Other", "other", "sql", "x")).unwrap();
    let error = repo
        .update(&other.id, &draft("Other", "sel", "sql", "x"))
        .unwrap_err();
    assert!(matches!(error, SnippetError::DuplicatePrefix { .. }));
    assert_eq!(repo.get(&other.id).unwrap(), Some(other));
}

#[test]
fn the_same_prefix_in_another_language_is_allowed() {
    let (repo, _) = repo();

    let record = repo
        .insert(&draft("Find", "sel", "mongodb", "db.find($0)"))
        .unwrap();

    assert_eq!(record.language, "mongodb");
    assert_eq!(repo.list_by_language("mongodb").unwrap().len(), 1);
}

#[test]
fn empty_fields_are_typed_errors_and_nothing_is_stored() {
    let (repo, conn) = repo();

    assert!(matches!(
        repo.insert(&draft("  ", "p", "sql", "b")),
        Err(SnippetError::EmptyName)
    ));
    assert!(matches!(
        repo.insert(&draft("n", " ", "sql", "b")),
        Err(SnippetError::EmptyPrefix)
    ));
    assert!(matches!(
        repo.insert(&draft("n", "p", "sql", "\n  ")),
        Err(SnippetError::EmptyBody)
    ));
    assert!(matches!(
        repo.insert(&draft("n", "p", " ", "b")),
        Err(SnippetError::EmptyLanguage)
    ));
    assert_eq!(row_count(&conn), 6);

    let record = repo.insert(&draft("n", "p", "sql", "b")).unwrap();
    assert!(matches!(
        repo.update(&record.id, &draft("", "p", "sql", "b")),
        Err(SnippetError::EmptyName)
    ));
    assert_eq!(repo.get(&record.id).unwrap(), Some(record));
}

#[test]
fn updating_a_missing_snippet_is_not_found() {
    let (repo, _) = repo();

    assert!(matches!(
        repo.update("missing", &draft("n", "p", "sql", "b")),
        Err(SnippetError::NotFound(id)) if id == "missing"
    ));
}

#[test]
fn a_deleted_built_in_does_not_come_back_when_migrations_run_again() {
    let (repo, conn) = repo();
    let select = repo
        .list_by_language("sql")
        .unwrap()
        .into_iter()
        .find(|record| record.builtin_key.as_deref() == Some("sql.select"))
        .unwrap();

    assert!(repo.delete(&select.id).unwrap());

    {
        let guard = conn.lock().unwrap();
        MigrationRegistry::new().run_all(&guard).unwrap();
    }

    assert_eq!(repo.list_all().unwrap().len(), 5);
    assert!(
        repo.list_all()
            .unwrap()
            .iter()
            .all(|record| record.builtin_key.as_deref() != Some("sql.select"))
    );
}

fn schema_without_snippets(conn: &Connection) -> Vec<(String, String)> {
    let mut statement = conn
        .prepare(
            "SELECT name, COALESCE(sql, '') FROM sqlite_master \
             WHERE name NOT LIKE '%snp_%' ORDER BY name",
        )
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn upgrading_an_existing_file_db_seeds_once_and_leaves_other_tables() {
    let dir = std::env::temp_dir().join(format!("snp-upgrade-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dbflux.db");
    {
        let runtime = StorageRuntime::for_path(path.clone()).unwrap();
        let conn = runtime.open_dbflux_db().unwrap();
        conn.execute_batch(
            "DROP TABLE snp_snippets; DELETE FROM sys_migrations WHERE name = '053_snp_snippets';",
        )
        .unwrap();
    }
    let schema_before = schema_without_snippets(&Connection::open(&path).unwrap());

    let runtime = StorageRuntime::for_path(path.clone()).unwrap();
    let conn = runtime.open_dbflux_db().unwrap();
    let rows: Vec<(String, String, String, Option<String>)> = conn
        .prepare("SELECT prefix, language, body, builtin_key FROM snp_snippets ORDER BY prefix")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|row| row.1 == "sql" && row.3.is_some()));
    let select = rows.iter().find(|row| row.0 == "sel").unwrap();
    assert_eq!(
        select.2.as_bytes(),
        b"SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0"
    );
    assert_eq!(schema_without_snippets(&conn), schema_before);

    conn.execute("DELETE FROM snp_snippets WHERE builtin_key = 'sql.cte'", [])
        .unwrap();
    drop(conn);
    drop(runtime);
    let runtime = StorageRuntime::for_path(path.clone()).unwrap();
    let conn = runtime.open_dbflux_db().unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM snp_snippets", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 5);
    std::fs::remove_dir_all(&dir).unwrap();
}
