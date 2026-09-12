#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

//! Server-backed TursoDB tests. Each test talks to a libSQL server (`sqld`)
//! started through testcontainers, or to the server named by `TURSO_TEST_URL`
//! (with an optional `TURSO_TEST_TOKEN`). Run them with:
//!
//! ```text
//! cargo test -p dbflux_driver_turso --test live_integration -- --ignored
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    ColumnKind, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, DescribeRequest,
    ExecutionSessionScope, ExplainRequest, OrderByColumn, Pagination, QueryRequest, RecordIdentity,
    RowDelete, RowInsert, RowPatch, SchemaLoadingStrategy, TableBrowseRequest, TableCountRequest,
    TableRef, Value,
};
use dbflux_driver_turso::TursoDriver;
use dbflux_test_support::containers;

static TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Unique table name per test so suites can share one server.
fn unique_table(prefix: &str) -> String {
    let n = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{}_{n}", std::process::id())
}

fn connect(url: &str) -> Result<Box<dyn Connection>, DbError> {
    let profile = ConnectionProfile::new(
        "live-turso",
        DbConfig::Turso {
            url: url.to_string(),
        },
    );
    let token = std::env::var("TURSO_TEST_TOKEN")
        .ok()
        .map(SecretString::from);

    containers::retry_db_operation(Duration::from_secs(30), || {
        TursoDriver::new().connect_with_secrets(&profile, token.as_ref(), None)
    })
}

struct TableCleanup<'a> {
    connection: &'a dyn Connection,
    tables: Vec<String>,
}

impl Drop for TableCleanup<'_> {
    fn drop(&mut self) {
        for table in self.tables.iter().rev() {
            if let Err(error) = self.connection.execute(&QueryRequest::new(format!(
                "DROP TABLE IF EXISTS \"{table}\""
            ))) {
                eprintln!("failed to clean up Turso table {table}: {error}");
            }
        }
    }
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let table = unique_table("users");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![table.clone()],
        };

        connection.ping()?;
        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::SingleDatabase
        );

        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{table}\" (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)"
        )))?;
        let inserted = connection.execute(&QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (name, score) VALUES ('alice', 1.5)"
        )))?;
        assert_eq!(inserted.affected_rows, Some(1));

        let result = connection.execute(&QueryRequest::new(format!(
            "SELECT id, name, score, 1 + 1 AS expr FROM \"{table}\""
        )))?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.affected_rows, None);
        assert_eq!(result.columns[0].kind, ColumnKind::Integer);
        assert_eq!(result.columns[1].kind, ColumnKind::Text);
        assert_eq!(result.columns[2].kind, ColumnKind::Float);
        // Expression column: no declared type, inferred from values.
        assert_eq!(result.columns[3].kind, ColumnKind::Integer);
        assert_eq!(result.rows[0][1], Value::Text("alice".into()));
        assert_eq!(result.rows[0][2], Value::Float(1.5));
        assert_eq!(result.rows[0][3], Value::Int(2));

        let schema = connection.schema()?;
        assert!(schema.is_relational());
        let relational = schema.as_relational().expect("relational schema");
        assert!(
            relational
                .schemas
                .iter()
                .flat_map(|s| s.tables.iter())
                .any(|t| t.name == table)
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_bound_parameters_and_limit() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let table = unique_table("params");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![table.clone()],
        };

        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{table}\" (id INTEGER PRIMARY KEY, name TEXT, payload BLOB, flag INTEGER)"
        )))?;

        let mut insert = QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (name, payload, flag) VALUES (?, ?, ?)"
        ));
        insert.params = vec![
            Value::Text("bob".into()),
            Value::Bytes(vec![1, 2, 3]),
            Value::Bool(true),
        ];
        assert_eq!(connection.execute(&insert)?.affected_rows, Some(1));

        for i in 0..5 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO \"{table}\" (name) VALUES ('row{i}')"
            )))?;
        }

        let mut select = QueryRequest::new(format!(
            "SELECT name, payload, flag FROM \"{table}\" WHERE name = ?"
        ));
        select.params = vec![Value::Text("bob".into())];
        let result = connection.execute(&select)?;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Bytes(vec![1, 2, 3]));
        assert_eq!(result.rows[0][2], Value::Int(1));

        let limited = connection
            .execute(&QueryRequest::new(format!("SELECT id FROM \"{table}\"")).with_limit(3))?;
        assert_eq!(limited.rows.len(), 3);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_multi_statement_script_runs_as_one_batch() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let table = unique_table("script");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![table.clone()],
        };

        let result = connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{table}\" (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO \"{table}\" (v) VALUES ('a');
             INSERT INTO \"{table}\" (v) VALUES ('b');
             SELECT COUNT(*) FROM \"{table}\";"
        )))?;

        assert_eq!(result.additional_results.len(), 3);
        let last = result.additional_results.last().expect("select result");
        assert_eq!(last.rows[0][0], Value::Int(2));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_schema_introspection() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let users = unique_table("intro_users");
        let orders = unique_table("intro_orders");
        let view = unique_table("intro_view");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![users.clone(), orders.clone()],
        };

        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{users}\" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER DEFAULT 0 CHECK (age >= 0)
            )"
        )))?;
        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{orders}\" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL REFERENCES \"{users}\"(id) ON DELETE CASCADE,
                amount REAL NOT NULL
            )"
        )))?;
        connection.execute(&QueryRequest::new(format!(
            "CREATE INDEX \"idx_{orders}_user\" ON \"{orders}\"(user_id)"
        )))?;
        connection.execute(&QueryRequest::new(format!(
            "CREATE VIEW \"{view}\" AS SELECT id, name FROM \"{users}\""
        )))?;

        let table = connection.table_details("main", None, &users)?;
        let columns = table.columns.as_ref().expect("columns");
        assert_eq!(columns.len(), 4);
        let id_col = columns.iter().find(|c| c.name == "id").unwrap();
        assert!(id_col.is_primary_key);
        assert!(!id_col.nullable);
        let name_col = columns.iter().find(|c| c.name == "name").unwrap();
        assert!(!name_col.nullable);
        let age_col = columns.iter().find(|c| c.name == "age").unwrap();
        assert_eq!(age_col.default_value.as_deref(), Some("0"));

        let constraints = table.constraints.as_ref().expect("constraints");
        assert!(
            constraints
                .iter()
                .any(|c| c.check_clause.as_deref() == Some("age >= 0"))
        );
        assert!(
            constraints
                .iter()
                .any(|c| c.columns == vec!["email".to_string()])
        );

        let orders_table = connection.table_details("main", None, &orders)?;
        let indexes = match orders_table.indexes.as_ref().expect("indexes") {
            dbflux_core::IndexData::Relational(v) => v,
            _ => panic!("expected relational index data"),
        };
        assert!(
            indexes
                .iter()
                .any(|i| i.name == format!("idx_{orders}_user") && i.columns == vec!["user_id"])
        );
        let foreign_keys = orders_table.foreign_keys.as_ref().expect("foreign keys");
        assert_eq!(foreign_keys.len(), 1);
        assert_eq!(foreign_keys[0].referenced_table, users);
        assert_eq!(foreign_keys[0].columns, vec!["user_id".to_string()]);
        assert_eq!(foreign_keys[0].on_delete.as_deref(), Some("CASCADE"));

        let schema = connection.schema()?;
        let relational = schema.as_relational().expect("relational");
        assert!(
            relational
                .schemas
                .iter()
                .flat_map(|s| s.views.iter())
                .any(|v| v.name == view)
        );

        let dependents = connection.fetch_dependents("main", None, &users)?;
        assert!(dependents.iter().any(|d| d.qualified_name == view));
        assert!(dependents.iter().any(|d| d.qualified_name == orders));

        let all_fks = connection.schema_foreign_keys("main", None)?;
        assert!(all_fks.iter().any(|fk| fk.table_name == orders));
        let all_indexes = connection.schema_indexes("main", None)?;
        assert!(all_indexes.iter().any(|i| i.table_name == orders));

        let missing = connection.table_details("main", None, "definitely_missing_table");
        assert!(matches!(missing, Err(DbError::ObjectNotFound(_))));

        connection.execute(&QueryRequest::new(format!("DROP VIEW \"{view}\"")))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_crud_operations() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let table = unique_table("crud");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![table.clone()],
        };

        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{table}\" (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, value INTEGER DEFAULT 0)"
        )))?;

        let inserted = connection.insert_row(&RowInsert::new(
            table.clone(),
            None,
            vec!["name".to_string(), "value".to_string()],
            vec![Value::Text("alice".into()), Value::Int(42)],
        ))?;
        assert_eq!(inserted.affected_rows, 1);
        let returned = inserted.returning_row.expect("re-read inserted row");
        assert_eq!(returned[1], Value::Text("alice".into()));
        assert_eq!(returned[2], Value::Int(42));

        let updated = connection.update_row(&RowPatch::new(
            RecordIdentity::composite(vec!["name".into()], vec![Value::Text("alice".into())]),
            table.clone(),
            None,
            vec![("value".to_string(), Value::Int(99))],
        ))?;
        assert_eq!(updated.affected_rows, 1);
        assert_eq!(updated.returning_row.expect("re-read")[2], Value::Int(99));

        let by_pk = connection.fetch_row_by_pk(
            "main",
            "main",
            &table,
            "name",
            &Value::Text("alice".into()),
        )?;
        assert_eq!(by_pk.expect("row")["value"], Value::Int(99));

        let stale = connection.update_row(&RowPatch::new(
            RecordIdentity::composite(vec!["name".into()], vec![Value::Text("nobody".into())]),
            table.clone(),
            None,
            vec![("value".to_string(), Value::Int(1))],
        ))?;
        assert_eq!(stale.affected_rows, 0);

        let deleted = connection.delete_row(&RowDelete::new(
            RecordIdentity::composite(vec!["name".into()], vec![Value::Text("alice".into())]),
            table.clone(),
            None,
        ))?;
        assert_eq!(deleted.affected_rows, 1);
        assert!(deleted.returning_row.is_some());

        let rows = connection
            .execute(&QueryRequest::new(format!("SELECT * FROM \"{table}\"")))?
            .rows;
        assert!(rows.is_empty());

        let duplicate = connection.execute(&QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (id, name) VALUES (1, 'x'), (1, 'y')"
        )));
        assert!(matches!(duplicate, Err(DbError::ConstraintViolation(_))));

        let missing = connection.execute(&QueryRequest::new("SELECT * FROM no_such_table_here"));
        assert!(matches!(missing, Err(DbError::ObjectNotFound(_))));

        let syntax = connection.execute(&QueryRequest::new("SELEC 1"));
        assert!(matches!(syntax, Err(DbError::SyntaxError(_))));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_browse_count_explain_describe() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let table = unique_table("browse");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![table.clone()],
        };

        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{table}\" (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
        )))?;
        for i in 1..=25 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO \"{table}\" (name) VALUES ('item_{i}')"
            )))?;
        }

        let table_ref = TableRef::new(&table);
        assert_eq!(
            connection.count_table(&TableCountRequest::new(table_ref.clone()))?,
            25
        );

        let page1 = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 0,
                })
                .with_order_by(vec![OrderByColumn::asc("id")]),
        )?;
        let page2 = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 10,
                })
                .with_order_by(vec![OrderByColumn::asc("id")]),
        )?;
        assert_eq!(page1.rows.len(), 10);
        assert_eq!(page2.rows.len(), 10);
        assert_ne!(page1.rows[0], page2.rows[0]);

        let filtered = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_filter("name = 'item_5'")
                .with_pagination(Pagination::Offset {
                    limit: 100,
                    offset: 0,
                }),
        )?;
        assert_eq!(filtered.rows.len(), 1);

        let plan = connection.explain(&ExplainRequest::new(table_ref.clone()))?;
        assert!(!plan.rows.is_empty());

        let described = connection.describe_table(&DescribeRequest::new(table_ref))?;
        assert_eq!(described.rows.len(), 2);

        let details = connection.table_details("main", None, &table)?;
        for generator in connection.code_generators() {
            assert!(
                !connection
                    .generate_code(&generator.id, &details)?
                    .is_empty()
            );
        }

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_execution_sessions_isolate_transactions() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let connection = connect(&url)?;
        let table = unique_table("tx");
        let _cleanup = TableCleanup {
            connection: connection.as_ref(),
            tables: vec![table.clone()],
        };
        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE \"{table}\" (id INTEGER PRIMARY KEY, v TEXT)"
        )))?;

        let factory = connection
            .execution_session_factory()
            .expect("root connection exposes a session factory");

        // An interactive transaction on session A stays invisible to the root
        // stream until it commits.
        let session_a = factory.open()?;
        let a = session_a.connection();
        assert!(
            a.execution_session_factory().is_none(),
            "children never nest"
        );
        a.execute(&QueryRequest::new("BEGIN"))?;
        a.execute(&QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (v) VALUES ('pending')"
        )))?;
        let inside = a.execute(&QueryRequest::new(format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))?;
        assert_eq!(inside.rows[0][0], Value::Int(1));
        let outside = connection.execute(&QueryRequest::new(format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))?;
        assert_eq!(outside.rows[0][0], Value::Int(0));
        a.execute(&QueryRequest::new("COMMIT"))?;
        let after = connection.execute(&QueryRequest::new(format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))?;
        assert_eq!(after.rows[0][0], Value::Int(1));
        session_a.close()?;
        assert!(session_a.is_closed());
        session_a.close()?; // idempotent

        // Closing a session with an open transaction rolls it back.
        let session_b = factory.open()?;
        let b = session_b.connection();
        b.execute(&QueryRequest::new("BEGIN"))?;
        b.execute(&QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (v) VALUES ('discarded')"
        )))?;
        session_b.close()?;
        let count = connection.execute(&QueryRequest::new(format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))?;
        assert_eq!(count.rows[0][0], Value::Int(1));

        // A scoped operation that leaks a transaction is reported and rolled back.
        let session_c = factory.open()?;
        session_c
            .connection()
            .execute(&QueryRequest::new("BEGIN"))?;
        session_c.connection().execute(&QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (v) VALUES ('leaked')"
        )))?;
        assert!(session_c.finish_operation().is_err());
        assert!(session_c.is_closed());
        let count = connection.execute(&QueryRequest::new(format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))?;
        assert_eq!(count.rows[0][0], Value::Int(1));

        // A clean scoped operation finishes without error.
        let root: std::sync::Arc<dyn Connection> = std::sync::Arc::from(connect(&url)?);
        let mut scope = ExecutionSessionScope::new(root.clone())?;
        let scoped = scope.connection();
        let result = scoped.execute(&QueryRequest::new(format!(
            "INSERT INTO \"{table}\" (v) VALUES ('scoped')"
        )));
        scope.finish(result)?;

        // Shutdown closes retained children and refuses new ones.
        let retained = factory.open()?;
        factory.shutdown()?;
        assert!(retained.is_closed());
        assert!(matches!(factory.open(), Err(DbError::ConnectionFailed(_))));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon or TURSO_TEST_URL"]
fn turso_test_connection_and_close() -> Result<(), DbError> {
    containers::with_libsql_server(|url| {
        let driver = TursoDriver::new();
        let profile = ConnectionProfile::new(
            "live-turso",
            DbConfig::Turso {
                url: url.to_string(),
            },
        );
        driver.test_connection(&profile)?;

        let mut connection = connect(&url)?;
        connection.close()?;

        let bad_profile = ConnectionProfile::new(
            "bad-turso",
            DbConfig::Turso {
                url: "http://127.0.0.1:9".to_string(),
            },
        );
        assert!(driver.test_connection(&bad_profile).is_err());
        Ok(())
    })
}
