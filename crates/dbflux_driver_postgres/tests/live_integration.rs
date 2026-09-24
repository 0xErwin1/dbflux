#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    CollectionRef, ColumnAssignment, ConnectionProfile, DbConfig, DbDriver, DbError,
    DescribeRequest, ExplainRequest, MutationRequest, OrderByColumn, Pagination, QueryRequest,
    RecordIdentity, RowDelete, RowInsert, RowPatch, SchemaLoadingStrategy, SemanticFilter,
    SemanticRequest, SqlUpdateRequest, SqlUpsertRequest, TableBrowseRequest, TableCountRequest,
    TableRef, TransactionStateNote, Value, WhereOperator,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect_postgres(
    uri: String,
) -> Result<(Box<dyn dbflux_core::Connection>, PostgresDriver), dbflux_core::DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "live-postgres",
        DbConfig::Postgres {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: "postgres".to_string(),
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection =
        containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
            let connection = driver.connect(&profile)?;
            connection.ping()?;
            Ok(connection)
        })?;

    Ok((connection, driver))
}

fn error_hint(error: &DbError) -> Option<&str> {
    error
        .formatted()
        .and_then(|formatted| formatted.hint.as_deref())
}

fn error_code(error: &DbError) -> Option<&str> {
    error
        .formatted()
        .and_then(|formatted| formatted.code.as_deref())
}

fn assert_text_array_matches_server_text(decoded: &Value, server_text: &Value) {
    let Value::Array(values) = decoded else {
        panic!("expected decoded text array, got {decoded:?}");
    };
    let Value::Text(server_text) = server_text else {
        panic!("expected server canonical array text, got {server_text:?}");
    };

    let decoded_text: Vec<Option<String>> = values
        .iter()
        .map(|value| match value {
            Value::Text(text) => Some(text.clone()),
            Value::Null => None,
            other => panic!("expected array text or NULL, got {other:?}"),
        })
        .collect();
    let canonical_text: Vec<Option<String>> =
        serde_json::from_str(server_text).expect("server array text must be JSON");

    assert_eq!(decoded_text, canonical_text);
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_live_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let result = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::ConnectionPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let schema = connection.schema()?;
        assert!(schema.is_relational());
        let _ = schema.databases();

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_live_keyword_mode_connects_without_password() -> Result<(), DbError> {
    containers::with_trust_postgres_port(|port| {
        // Regression: the keyword-mode connect string used to be built with a
        // trailing empty `password=`, which libpq parses by consuming the next
        // pair as the password value — `dbname=testdb` was lost and the server
        // fell back to the default database (the user name), rejecting the
        // connection with `database "testuser" does not exist`.
        let driver = PostgresDriver::new();
        let profile = ConnectionProfile::new(
            "live-postgres-keyword-no-password",
            DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "127.0.0.1".to_string(),
                port,
                user: "testuser".to_string(),
                database: "testdb".to_string(),
                ssl_mode: Some("prefer".to_string()),
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let connection =
            containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
                let connection = driver.connect(&profile)?;
                connection.ping()?;
                Ok(connection)
            })?;

        let databases = connection.list_databases()?;
        assert!(
            databases
                .iter()
                .any(|db| db.name == "testdb" && db.is_current),
            "expected to be connected to 'testdb', got: {databases:?}"
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_live_connection_error_carries_server_message() -> Result<(), DbError> {
    containers::with_trust_postgres_port(|port| {
        // tokio-postgres' Display names the error kind only ("db error"); the
        // formatters must surface the server's own message and SQLSTATE code.
        let driver = PostgresDriver::new();
        let profile = ConnectionProfile::new(
            "live-postgres-keyword-missing-db",
            DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "127.0.0.1".to_string(),
                port,
                user: "testuser".to_string(),
                database: "missing_db".to_string(),
                ssl_mode: Some("disable".to_string()),
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        // The container reports readiness before its init restart, so early
        // attempts can fail with transient io errors ("Connection reset by
        // peer"). Only the server's rejection of the missing database ends
        // the retry loop; every other failure goes back around.
        containers::retry_db_operation(Duration::from_secs(30), || -> Result<(), DbError> {
            match driver.connect(&profile) {
                Err(DbError::ConnectionFailed(formatted)) => {
                    let message = formatted.to_display_string();
                    if message.contains("does not exist") && message.contains("3D000") {
                        Ok(())
                    } else {
                        Err(DbError::ConnectionFailed(formatted))
                    }
                }
                Err(other) => Err(other),
                Ok(_) => panic!("connecting to a missing database unexpectedly succeeded"),
            }
        })?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_schema_introspection() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER DEFAULT 0
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE test_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES test_users(id) ON DELETE CASCADE,
                amount NUMERIC(10, 2) NOT NULL
            )",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE INDEX idx_orders_user_id ON test_orders(user_id)",
        ))?;

        connection.execute(&QueryRequest::new(
            "CREATE VIEW test_user_view AS SELECT id, name FROM test_users",
        ))?;

        let schema = connection.schema()?;
        assert!(schema.is_relational());

        let databases = schema.databases();
        assert!(!databases.is_empty());

        let table = connection.table_details("postgres", Some("public"), "test_users")?;
        assert_eq!(table.name, "test_users");

        let columns = table.columns.as_ref().expect("columns should be loaded");
        assert!(columns.len() >= 4);

        let id_col = columns.iter().find(|c| c.name == "id").expect("id column");
        assert!(id_col.is_primary_key);
        assert!(!id_col.nullable);

        let name_col = columns
            .iter()
            .find(|c| c.name == "name")
            .expect("name column");
        assert!(!name_col.nullable);

        let email_col = columns
            .iter()
            .find(|c| c.name == "email")
            .expect("email column");
        assert!(email_col.nullable);

        let age_col = columns
            .iter()
            .find(|c| c.name == "age")
            .expect("age column");
        assert!(age_col.nullable);

        let indexes = table.indexes.as_ref().expect("indexes should be loaded");
        let idx_data = match indexes {
            dbflux_core::IndexData::Relational(v) => v,
            _ => panic!("expected relational index data"),
        };
        assert!(idx_data.iter().any(|i| i.is_primary));

        let relational = schema.as_relational().expect("should be relational schema");
        let has_view = relational
            .schemas
            .iter()
            .flat_map(|s| s.views.iter())
            .any(|v| v.name == "test_user_view");
        assert!(has_view, "view should appear in schema");

        let orders_table = connection.table_details("postgres", Some("public"), "test_orders")?;
        let fks = orders_table
            .foreign_keys
            .as_ref()
            .expect("foreign keys should be loaded");
        assert!(!fks.is_empty());
        let fk = &fks[0];
        assert_eq!(fk.referenced_table, "test_users");
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_columns, vec!["id"]);

        let schema_features = connection.schema_features();
        assert!(!schema_features.is_empty());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_crud_operations() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE crud_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER DEFAULT 0
            )",
        ))?;

        let insert_result = connection.insert_row(&RowInsert::new(
            "crud_test".to_string(),
            Some("public".to_string()),
            vec!["name".to_string(), "value".to_string()],
            vec![Value::Text("alice".to_string()), Value::Int(42)],
        ))?;
        assert_eq!(insert_result.affected_rows, 1);
        assert!(insert_result.returning_row.is_some());

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT * FROM crud_test WHERE name = 'alice'",
            ))?
            .rows;
        assert_eq!(rows.len(), 1);

        let update_result = connection.update_row(&RowPatch::new(
            RecordIdentity::composite(
                vec!["name".to_string()],
                vec![Value::Text("alice".to_string())],
            ),
            "crud_test".to_string(),
            Some("public".to_string()),
            vec![("value".to_string(), Value::Int(99))],
        ))?;
        assert_eq!(update_result.affected_rows, 1);
        assert!(update_result.returning_row.is_some());

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT value FROM crud_test WHERE name = 'alice'",
            ))?
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(99));

        let delete_result = connection.delete_row(&RowDelete::new(
            RecordIdentity::composite(
                vec!["name".to_string()],
                vec![Value::Text("alice".to_string())],
            ),
            "crud_test".to_string(),
            Some("public".to_string()),
        ))?;
        assert_eq!(delete_result.affected_rows, 1);

        let rows = connection
            .execute(&QueryRequest::new("SELECT * FROM crud_test"))?
            .rows;
        assert!(rows.is_empty());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon and the pinned pgvector PostgreSQL 16 image"]
fn postgres_pgvector_text_matches_server_output_and_crud_returning() -> Result<(), DbError> {
    containers::with_pgvector_postgres_16_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new("CREATE EXTENSION vector"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE pgvector_display (
                id INTEGER PRIMARY KEY,
                vector_value vector,
                halfvec_value halfvec,
                sparsevec_value sparsevec,
                vector_values vector[],
                halfvec_values halfvec[],
                sparsevec_values sparsevec[]
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO pgvector_display VALUES (
                1,
                '[1e-6,1e20,-1e-6,-1e20]',
                '[1.5,-2.25]',
                '{1:1e-6,4:-1e20}/4',
                ARRAY['[1e-6,1e20,-1e-6,-1e20]'::vector, NULL, '[2,3]'::vector],
                ARRAY['[1.5,-2.25]'::halfvec, NULL, '[3,4]'::halfvec],
                ARRAY['{1:1e-6,4:-1e20}/4'::sparsevec, NULL, '{2:3}/4'::sparsevec]
            )",
        ))?;

        let result = connection.execute(&QueryRequest::new(
            "SELECT
                vector_value, vector_value::text,
                halfvec_value, halfvec_value::text,
                sparsevec_value, sparsevec_value::text,
                vector_values, array_to_json(vector_values)::text,
                halfvec_values, array_to_json(halfvec_values)::text,
                sparsevec_values, array_to_json(sparsevec_values)::text,
                NULL::vector, NULL::vector[]
             FROM pgvector_display",
        ))?;
        let row = &result.rows[0];

        for (value_index, text_index) in [(0, 1), (2, 3), (4, 5)] {
            assert_eq!(row[value_index], row[text_index]);
        }
        assert_text_array_matches_server_text(&row[6], &row[7]);
        assert_text_array_matches_server_text(&row[8], &row[9]);
        assert_text_array_matches_server_text(&row[10], &row[11]);
        assert_eq!(row[12], Value::Null);
        assert_eq!(row[13], Value::Null);

        let inserted = connection.insert_row(&RowInsert::with_typed_assignments(
            "pgvector_display".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::new("id", Value::Int(2)),
                ColumnAssignment::typed(
                    "vector_value",
                    Value::Text("[1e-6,1e20,-1e-6,-1e20]".to_string()),
                    "vector",
                ),
            ],
        ))?;
        let inserted_value = inserted.returning_row.as_ref().and_then(|row| row.get(1));
        let server_value = connection
            .execute(&QueryRequest::new(
                "SELECT vector_value::text FROM pgvector_display WHERE id = 2",
            ))?
            .rows[0][0]
            .clone();
        assert_eq!(inserted_value, Some(&server_value));

        let updated = connection.update_row(&RowPatch::with_typed_changes(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(2)]),
            "pgvector_display".to_string(),
            Some("public".to_string()),
            vec![ColumnAssignment::typed(
                "vector_value",
                Value::Text("[2,3]".to_string()),
                "vector",
            )],
        ))?;
        let updated_value = updated.returning_row.as_ref().and_then(|row| row.get(1));
        let server_value = connection
            .execute(&QueryRequest::new(
                "SELECT vector_value::text FROM pgvector_display WHERE id = 2",
            ))?
            .rows[0][0]
            .clone();
        assert_eq!(updated_value, Some(&server_value));

        let deleted = connection.delete_row(&RowDelete::new(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(2)]),
            "pgvector_display".to_string(),
            Some("public".to_string()),
        ))?;
        assert_eq!(
            deleted.returning_row.as_ref().and_then(|row| row.get(1)),
            Some(&server_value)
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_text_search_text_matches_server_output() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE text_search_display (
                id INTEGER PRIMARY KEY,
                vector_value tsvector,
                query_value tsquery,
                vector_values tsvector[]
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO text_search_display VALUES (
                1,
                setweight(to_tsvector('english', 'The quick brown fox'), 'A')
                    || to_tsvector('english', 'jumps over it''s lazy dog'),
                tsquery_phrase(
                    to_tsquery('simple', 'fat:AB') || to_tsquery('simple', 'cat'),
                    !! tsquery_phrase(
                        to_tsquery('simple', 'rat'),
                        to_tsquery('simple', 'dog')
                    ),
                    3
                ) && to_tsquery('simple', 'bird:*'),
                ARRAY[to_tsvector('english', 'first row'), NULL]
            )",
        ))?;

        let result = connection.execute(&QueryRequest::new(
            "SELECT
                vector_value, vector_value::text,
                query_value, query_value::text,
                vector_values, array_to_json(vector_values)::text,
                ''::tsvector, ''::tsvector::text,
                NULL::tsvector, NULL::tsquery
             FROM text_search_display",
        ))?;
        let row = &result.rows[0];

        for (value_index, text_index) in [(0, 1), (2, 3), (6, 7)] {
            assert_eq!(row[value_index], row[text_index]);
        }
        assert_text_array_matches_server_text(&row[4], &row[5]);
        assert_eq!(row[8], Value::Null);
        assert_eq!(row[9], Value::Null);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_numeric_values_read_as_exact_decimals() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE numeric_display (
                id INTEGER PRIMARY KEY,
                amount NUMERIC(10, 2),
                unconstrained NUMERIC
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO numeric_display VALUES
                (1, 1123.40, 0.0001),
                (2, -12.5, 'NaN'),
                (3, 0, 'Infinity'),
                (4, NULL, '-Infinity'),
                (5, 99999999.99, ('1' || repeat('0', 400) || '.000125')::numeric)",
        ))?;

        let mut result = connection.execute(&QueryRequest::new(
            "SELECT amount, amount::text, unconstrained, unconstrained::text
             FROM numeric_display ORDER BY id",
        ))?;
        assert!(result.take_unsupported_types().is_empty());

        for row in &result.rows {
            for (value_index, text_index) in [(0, 1), (2, 3)] {
                match (&row[value_index], &row[text_index]) {
                    (Value::Null, Value::Null) => {}
                    (Value::Decimal(decimal), Value::Text(text)) => assert_eq!(decimal, text),
                    (value, text) => {
                        panic!("NUMERIC value {value:?} does not match server text {text:?}")
                    }
                }
            }
        }

        let rows = &result.rows;
        assert_eq!(rows[0][0], Value::Decimal("1123.40".to_string()));
        assert_eq!(rows[0][2], Value::Decimal("0.0001".to_string()));
        assert_eq!(rows[1][0], Value::Decimal("-12.50".to_string()));
        assert_eq!(rows[1][2], Value::Decimal("NaN".to_string()));
        assert_eq!(rows[2][0], Value::Decimal("0.00".to_string()));
        assert_eq!(rows[2][2], Value::Decimal("Infinity".to_string()));
        assert_eq!(rows[3][0], Value::Null);
        assert_eq!(rows[3][2], Value::Decimal("-Infinity".to_string()));
        assert_eq!(
            rows[4][2],
            Value::Decimal(format!("1{}.000125", "0".repeat(400)))
        );

        let browsed = connection.browse_table(
            &TableBrowseRequest::new(TableRef::with_schema("public", "numeric_display"))
                .with_filter("id = 1")
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 0,
                }),
        )?;
        assert_eq!(
            browsed.rows,
            vec![vec![
                Value::Int(1),
                Value::Decimal("1123.40".to_string()),
                Value::Decimal("0.0001".to_string()),
            ]]
        );

        let inserted = connection.insert_row(&RowInsert::new(
            "numeric_display".to_string(),
            Some("public".to_string()),
            vec!["id".to_string(), "amount".to_string()],
            vec![Value::Int(6), Value::Decimal("1123.4".to_string())],
        ))?;
        assert_eq!(
            inserted.returning_row,
            Some(vec![
                Value::Int(6),
                Value::Decimal("1123.40".to_string()),
                Value::Null,
            ])
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_numeric_arrays_read_as_exact_decimals() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let mut result = connection.execute(&QueryRequest::new(
            "SELECT ARRAY[1123.40, NULL, -0.0001, 'NaN']::numeric[], NULL::numeric[]",
        ))?;
        assert!(result.take_unsupported_types().is_empty());

        assert_eq!(
            result.rows,
            vec![vec![
                Value::Array(vec![
                    Value::Decimal("1123.40".to_string()),
                    Value::Null,
                    Value::Decimal("-0.0001".to_string()),
                    Value::Decimal("NaN".to_string()),
                ]),
                Value::Null,
            ]]
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_null_infinity_and_undecodable_values_stay_distinct() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE special_values (
                id INTEGER PRIMARY KEY,
                happened_at TIMESTAMP,
                happened_at_tz TIMESTAMPTZ,
                happened_on DATE,
                price MONEY
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "INSERT INTO special_values VALUES
                (1, 'infinity', '-infinity', 'infinity', 12.5),
                (2, NULL, NULL, NULL, NULL),
                (3, '2024-01-02 03:04:05', '-infinity', '-infinity', NULL)",
        ))?;

        let mut result = connection.execute(&QueryRequest::new(
            "SELECT happened_at, happened_at_tz, happened_on, price
             FROM special_values ORDER BY id",
        ))?;

        assert_eq!(
            result.rows,
            vec![
                vec![
                    Value::Text("infinity".to_string()),
                    Value::Text("-infinity".to_string()),
                    Value::Text("infinity".to_string()),
                    Value::Unsupported("money".to_string()),
                ],
                vec![Value::Null, Value::Null, Value::Null, Value::Null],
                vec![
                    Value::DateTime(
                        chrono::DateTime::from_timestamp(1_704_164_645, 0)
                            .expect("valid timestamp")
                    ),
                    Value::Text("-infinity".to_string()),
                    Value::Text("-infinity".to_string()),
                    Value::Null,
                ],
            ]
        );
        assert_eq!(result.take_unsupported_types(), vec!["money".to_string()]);

        let arrays = connection.execute(&QueryRequest::new(
            "SELECT ARRAY['infinity', '2024-01-02']::date[],
                    ARRAY['-infinity']::timestamp[],
                    ARRAY['infinity']::timestamptz[]",
        ))?;
        assert_eq!(
            arrays.rows,
            vec![vec![
                Value::Array(vec![
                    Value::Text("infinity".to_string()),
                    Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).expect("valid date")),
                ]),
                Value::Array(vec![Value::Text("-infinity".to_string())]),
                Value::Array(vec![Value::Text("infinity".to_string())]),
            ]]
        );

        let inserted = connection.insert_row(&RowInsert::new(
            "special_values".to_string(),
            Some("public".to_string()),
            vec![
                "id".to_string(),
                "happened_at".to_string(),
                "price".to_string(),
            ],
            vec![
                Value::Int(4),
                Value::Text("infinity".to_string()),
                Value::Decimal("3.5".to_string()),
            ],
        ))?;
        assert_eq!(
            inserted.returning_row,
            Some(vec![
                Value::Int(4),
                Value::Text("infinity".to_string()),
                Value::Null,
                Value::Null,
                Value::Unsupported("money".to_string()),
            ])
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Browse and count
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_browse_and_count() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE browse_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )",
        ))?;

        for i in 1..=25 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO browse_test (name) VALUES ('item_{}')",
                i
            )))?;
        }

        let table_ref = TableRef::with_schema("public", "browse_test");

        let count = connection.count_table(&TableCountRequest::new(table_ref.clone()))?;
        assert_eq!(count, 25);

        let filtered_count = connection.count_table(
            &TableCountRequest::new(table_ref.clone()).with_filter("name LIKE 'item_1%'"),
        )?;
        assert!(filtered_count > 0);
        assert!(filtered_count < 25);

        let page1 = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 0,
                })
                .with_order_by(vec![OrderByColumn::asc("id")]),
        )?;
        assert_eq!(page1.rows.len(), 10);

        let page2 = connection.browse_table(
            &TableBrowseRequest::new(table_ref.clone())
                .with_pagination(Pagination::Offset {
                    limit: 10,
                    offset: 10,
                })
                .with_order_by(vec![OrderByColumn::asc("id")]),
        )?;
        assert_eq!(page2.rows.len(), 10);
        assert_ne!(page1.rows[0], page2.rows[0]);

        let filtered = connection.browse_table(
            &TableBrowseRequest::new(table_ref)
                .with_filter("name = 'item_5'")
                .with_pagination(Pagination::Offset {
                    limit: 100,
                    offset: 0,
                }),
        )?;
        assert_eq!(filtered.rows.len(), 1);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Explain and describe
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_explain() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE explain_test (id SERIAL PRIMARY KEY, name TEXT)",
        ))?;

        let table_ref = TableRef::with_schema("public", "explain_test");
        let result = connection.explain(&ExplainRequest::new(table_ref))?;
        assert!(!result.rows.is_empty() || result.text_body.is_some());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_describe_table() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE describe_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                active BOOLEAN DEFAULT true
            )",
        ))?;

        let table_ref = TableRef::with_schema("public", "describe_test");
        let result = connection.describe_table(&DescribeRequest::new(table_ref))?;
        assert!(result.rows.len() >= 3);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Query cancellation
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_cancel_query() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let cancel_handle = connection.cancel_handle();
        let cancel_result = cancel_handle.cancel();
        assert!(cancel_result.is_ok());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Code generators
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_code_generators() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE codegen_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )",
        ))?;

        let generators = connection.code_generators();
        assert!(!generators.is_empty());

        let table = connection.table_details("postgres", Some("public"), "codegen_test")?;

        for generator in generators {
            let code = connection.generate_code(&generator.id, &table)?;
            assert!(
                !code.is_empty(),
                "generator '{}' returned empty code",
                generator.id
            );
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Document operations (should return NotSupported)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_document_ops_not_supported() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let browse_result = connection.browse_collection(
            &dbflux_core::CollectionBrowseRequest::new(CollectionRef::new("db", "col")),
        );
        assert!(matches!(browse_result, Err(DbError::NotSupported(_))));

        assert!(connection.key_value_api().is_none());

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Typed array literal emission (#76)
//
// Regression: prior to typed dialect plumbing, inserting/updating a `text[]`
// or `int4[]` column emitted `'<json>'::jsonb`, which Postgres rejected with
// `column "..." is of type text[] but expression is of type jsonb`.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_array_columns_round_trip() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE array_round_trip (
                id SERIAL PRIMARY KEY,
                tags TEXT[] NOT NULL,
                scores INTEGER[] NOT NULL,
                meta JSONB NOT NULL
            )",
        ))?;

        // 1. Insert with Value::Array — simulates "round-trip from PG read,
        //    untouched by the user" (the original #76 repro path).
        let insert_array_form = RowInsert::with_typed_assignments(
            "array_round_trip".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::typed(
                    "tags",
                    Value::Array(vec![
                        Value::Text("Espacio".to_string()),
                        Value::Text("hola".to_string()),
                    ]),
                    "_text",
                ),
                ColumnAssignment::typed(
                    "scores",
                    Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                    "_int4",
                ),
                ColumnAssignment::typed("meta", Value::Json(r#"{"k":"v"}"#.to_string()), "jsonb"),
            ],
        );
        let inserted = connection.insert_row(&insert_array_form)?;
        assert_eq!(inserted.affected_rows, 1);

        // 2. Insert with Value::Json(json-array-string) — simulates "user
        //    edited the cell as JSON text in the data grid".
        let insert_json_form = RowInsert::with_typed_assignments(
            "array_round_trip".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::typed(
                    "tags",
                    Value::Json(r#"["foo","bar"]"#.to_string()),
                    "_text",
                ),
                ColumnAssignment::typed("scores", Value::Json("[10, 20]".to_string()), "_int4"),
                ColumnAssignment::typed(
                    "meta",
                    Value::Json(r#"{"edited":true}"#.to_string()),
                    "jsonb",
                ),
            ],
        );
        let inserted = connection.insert_row(&insert_json_form)?;
        assert_eq!(inserted.affected_rows, 1);

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT tags, scores FROM array_round_trip ORDER BY id",
            ))?
            .rows;
        assert_eq!(rows.len(), 2);

        match &rows[0][0] {
            Value::Array(arr) => {
                assert_eq!(
                    arr,
                    &vec![
                        Value::Text("Espacio".to_string()),
                        Value::Text("hola".to_string()),
                    ]
                );
            }
            other => panic!("expected text[] array, got {:?}", other),
        }
        match &rows[0][1] {
            Value::Array(arr) => {
                assert_eq!(arr, &vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
            }
            other => panic!("expected int4[] array, got {:?}", other),
        }
        match &rows[1][0] {
            Value::Array(arr) => {
                assert_eq!(
                    arr,
                    &vec![
                        Value::Text("foo".to_string()),
                        Value::Text("bar".to_string()),
                    ]
                );
            }
            other => panic!("expected text[] array from JSON form, got {:?}", other),
        }

        // 3. UPDATE with a typed Array assignment — same dialect path.
        let update_result = connection.update_row(&RowPatch::with_typed_changes(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
            "array_round_trip".to_string(),
            Some("public".to_string()),
            vec![ColumnAssignment::typed(
                "tags",
                Value::Array(vec![Value::Text("updated".to_string())]),
                "_text",
            )],
        ))?;
        assert_eq!(update_result.affected_rows, 1);

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT tags FROM array_round_trip WHERE id = 1",
            ))?
            .rows;
        match &rows[0][0] {
            Value::Array(arr) => {
                assert_eq!(arr, &vec![Value::Text("updated".to_string())]);
            }
            other => panic!("expected updated text[] array, got {:?}", other),
        }

        // 4. Empty array round-trip.
        let insert_empty = RowInsert::with_typed_assignments(
            "array_round_trip".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::typed("tags", Value::Array(vec![]), "_text"),
                ColumnAssignment::typed("scores", Value::Array(vec![]), "_int4"),
                ColumnAssignment::typed("meta", Value::Json("{}".to_string()), "jsonb"),
            ],
        );
        connection.insert_row(&insert_empty)?;

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT tags, scores FROM array_round_trip ORDER BY id DESC LIMIT 1",
            ))?
            .rows;
        assert!(matches!(&rows[0][0], Value::Array(arr) if arr.is_empty()));
        assert!(matches!(&rows[0][1], Value::Array(arr) if arr.is_empty()));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Typed array literal emission via semantic update/upsert (#76, MCP path)
//
// Exercises the SqlUpdateRequest / SqlUpsertRequest plumbing that the MCP
// `update_records` and `upsert_record` tools go through, ensuring the typed
// dialect path is reached and array columns succeed end-to-end.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_semantic_update_and_upsert_array_columns() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE semantic_array_test (
                id INTEGER PRIMARY KEY,
                tags TEXT[] NOT NULL,
                meta JSONB NOT NULL
            )",
        ))?;

        // Seed a row to update.
        connection.insert_row(&RowInsert::with_typed_assignments(
            "semantic_array_test".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::new("id", Value::Int(1)),
                ColumnAssignment::typed(
                    "tags",
                    Value::Array(vec![Value::Text("a".to_string())]),
                    "_text",
                ),
                ColumnAssignment::typed("meta", Value::Json("{}".to_string()), "jsonb"),
            ],
        ))?;

        // Semantic UPDATE via SqlUpdateRequest::with_typed_changes — what the
        // MCP `update_records` tool builds after resolve_column_types.
        let filter = SemanticFilter::compare("id", WhereOperator::Eq, Value::Int(1));
        let update = SqlUpdateRequest::with_typed_changes(
            "semantic_array_test".to_string(),
            Some("public".to_string()),
            filter,
            vec![ColumnAssignment::typed(
                "tags",
                Value::Array(vec![
                    Value::Text("x".to_string()),
                    Value::Text("y".to_string()),
                ]),
                "_text",
            )],
        );

        connection.execute_semantic_request(&SemanticRequest::Mutation(
            MutationRequest::sql_update_many(update),
        ))?;

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT tags FROM semantic_array_test WHERE id = 1",
            ))?
            .rows;
        match &rows[0][0] {
            Value::Array(arr) => {
                assert_eq!(
                    arr,
                    &vec![Value::Text("x".to_string()), Value::Text("y".to_string())]
                );
            }
            other => panic!("expected updated text[] array, got {:?}", other),
        }

        // Semantic UPSERT via SqlUpsertRequest::with_typed_assignments —
        // exercises both insert-side and on-conflict-update typed literals.
        let upsert = SqlUpsertRequest::with_typed_assignments(
            "semantic_array_test".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::new("id", Value::Int(1)),
                ColumnAssignment::typed(
                    "tags",
                    Value::Array(vec![Value::Text("upserted".to_string())]),
                    "_text",
                ),
                ColumnAssignment::typed("meta", Value::Json(r#"{"v":2}"#.to_string()), "jsonb"),
            ],
            vec!["id".to_string()],
            vec![ColumnAssignment::typed(
                "tags",
                Value::Array(vec![Value::Text("upserted".to_string())]),
                "_text",
            )],
        );

        connection.execute_semantic_request(&SemanticRequest::Mutation(
            MutationRequest::sql_upsert(upsert),
        ))?;

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT tags FROM semantic_array_test WHERE id = 1",
            ))?
            .rows;
        match &rows[0][0] {
            Value::Array(arr) => {
                assert_eq!(arr, &vec![Value::Text("upserted".to_string())]);
            }
            other => panic!("expected upserted text[] array, got {:?}", other),
        }

        // Also exercise an insert via upsert (new id, no conflict).
        let upsert_new = SqlUpsertRequest::with_typed_assignments(
            "semantic_array_test".to_string(),
            Some("public".to_string()),
            vec![
                ColumnAssignment::new("id", Value::Int(2)),
                ColumnAssignment::typed(
                    "tags",
                    Value::Array(vec![Value::Text("new".to_string())]),
                    "_text",
                ),
                ColumnAssignment::typed("meta", Value::Json("{}".to_string()), "jsonb"),
            ],
            vec!["id".to_string()],
            vec![],
        );

        connection.execute_semantic_request(&SemanticRequest::Mutation(
            MutationRequest::sql_upsert(upsert_new),
        ))?;

        let rows = connection
            .execute(&QueryRequest::new(
                "SELECT tags FROM semantic_array_test WHERE id = 2",
            ))?
            .rows;
        assert!(
            matches!(&rows[0][0], Value::Array(arr) if arr == &vec![Value::Text("new".to_string())])
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Referential integrity toggle (data-transfer engine)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_set_referential_integrity_disables_and_restores_fk_checks() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE parent_ri (id INT PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE child_ri (id INT PRIMARY KEY, parent_id INT REFERENCES parent_ri(id))",
        ))?;

        // With RI enabled (default), inserting a child with no matching parent fails.
        let violates =
            connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (1, 999)"));
        assert!(violates.is_err(), "FK violation must fail with RI enabled");

        connection.set_referential_integrity(false)?;
        connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (1, 999)"))?;

        connection.set_referential_integrity(true)?;
        let still_violates =
            connection.execute(&QueryRequest::new("INSERT INTO child_ri VALUES (2, 998)"));
        assert!(
            still_violates.is_err(),
            "FK violation must fail again after RI is restored"
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Transaction state after a failed script
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_failed_script_rolls_back_the_transaction_it_opened() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE failed_script_rollback (id INT PRIMARY KEY)",
        ))?;

        let error = connection
            .execute(&QueryRequest::new(
                "BEGIN; \
                 INSERT INTO failed_script_rollback VALUES (1); \
                 INSERT INTO failed_script_rollback VALUES (1); \
                 COMMIT;",
            ))
            .expect_err("duplicate key must fail the script");

        let hint = error_hint(&error).expect("failed script error must carry a hint");
        assert!(
            hint.contains(TransactionStateNote::RolledBack.message()),
            "hint must report the rollback, got {hint:?}"
        );

        connection.execute(&QueryRequest::new("SELECT 1"))?;

        let rows = connection
            .execute(&QueryRequest::new("SELECT id FROM failed_script_rollback"))?
            .rows;
        assert!(rows.is_empty(), "the partial insert must be rolled back");

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_failed_script_leaves_an_earlier_transaction_aborted() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new(
            "CREATE TABLE failed_script_aborted (id INT PRIMARY KEY)",
        ))?;

        connection.execute(&QueryRequest::new("BEGIN"))?;

        let error = connection
            .execute(&QueryRequest::new(
                "INSERT INTO failed_script_aborted VALUES (1); \
                 INSERT INTO failed_script_aborted VALUES (1);",
            ))
            .expect_err("duplicate key must fail the script");

        let hint = error_hint(&error).expect("failed script error must carry a hint");
        assert!(
            hint.contains(TransactionStateNote::Aborted.message()),
            "hint must report the aborted transaction, got {hint:?}"
        );

        let probe_error = connection
            .execute(&QueryRequest::new("SELECT 1"))
            .expect_err("the earlier transaction must still be aborted");
        assert_eq!(error_code(&probe_error), Some("25P02"));

        connection.execute(&QueryRequest::new("ROLLBACK"))?;
        connection.execute(&QueryRequest::new("SELECT 1"))?;

        Ok(())
    })
}

fn assert_no_rows_truncated(result: &dbflux_core::QueryResult, count: usize) {
    assert_eq!(result.rows.len(), count);
    assert!(!result.rows_truncated());
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_limit_below_exact_and_over_retains_and_flags() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        let sql = "SELECT g FROM generate_series(1, 5) g";
        assert_no_rows_truncated(&connection.execute(&QueryRequest::new(sql))?, 5);
        assert_no_rows_truncated(
            &connection.execute(&QueryRequest::new(sql).with_limit(8))?,
            5,
        );
        assert_no_rows_truncated(
            &connection.execute(&QueryRequest::new(sql).with_limit(5))?,
            5,
        );
        let over = connection.execute(&QueryRequest::new(sql).with_limit(3))?;
        assert_eq!(over.rows.len(), 3);
        assert_eq!(over.rows[0][0], Value::Int(1));
        assert!(over.rows_truncated());
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_zero_limit_retains_nothing_only_when_rows_exist() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        let nonempty = connection.execute(&QueryRequest::new("SELECT 1").with_limit(0))?;
        assert!(nonempty.rows.is_empty());
        assert!(nonempty.rows_truncated());
        let empty = connection.execute(&QueryRequest::new("SELECT 1 WHERE FALSE").with_limit(0))?;
        assert!(empty.rows.is_empty());
        assert!(!empty.rows_truncated());
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_returning_above_cap_completes_effects_and_truncates() -> Result<(), DbError>
{
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE safety_returning (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 0)",
        ))?;
        let inserted = connection.execute(&QueryRequest::new("INSERT INTO safety_returning (id) SELECT g FROM generate_series(1, 5) g RETURNING id").with_limit(2))?;
        assert_eq!(inserted.rows.len(), 2);
        assert!(inserted.rows_truncated());
        assert_eq!(
            connection
                .execute(&QueryRequest::new("SELECT COUNT(*) FROM safety_returning"))?
                .rows[0][0],
            Value::Int(5)
        );
        let updated = connection.execute(
            &QueryRequest::new("UPDATE safety_returning SET v = 100 RETURNING id").with_limit(1),
        )?;
        assert_eq!(updated.rows.len(), 1);
        assert!(updated.rows_truncated());
        assert_eq!(
            connection
                .execute(&QueryRequest::new(
                    "SELECT COUNT(*) FROM safety_returning WHERE v = 100"
                ))?
                .rows[0][0],
            Value::Int(5)
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_late_stream_error_propagates_after_cap_reached() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        let result = connection.execute(
            &QueryRequest::new("SELECT g, 1 / (g - 3) FROM generate_series(1, 10) g").with_limit(2),
        );
        assert!(
            matches!(result, Err(DbError::QueryFailed(_))),
            "late division error must propagate: {result:?}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new("SELECT 1"))?
                .rows
                .len(),
            1
        );
        Ok(())
    })
}

/// Outcome of one script run against its own fresh table.
struct ScriptRun {
    outcome: Result<dbflux_core::QueryResult, DbError>,
    persisted: Vec<Vec<Value>>,
}

/// Creates `table`, runs `template` with `{table}` replaced, and reads back
/// what the script left committed or visible in the session.
fn run_script(
    connection: &dyn dbflux_core::Connection,
    table: &str,
    template: &str,
    limit: Option<u32>,
) -> Result<ScriptRun, DbError> {
    connection.execute(&QueryRequest::new(format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY)"
    )))?;

    let mut request = QueryRequest::new(template.replace("{table}", table));
    if let Some(limit) = limit {
        request = request.with_limit(limit);
    }
    let outcome = connection.execute(&request);

    let persisted = connection
        .execute(&QueryRequest::new(format!(
            "SELECT id FROM {table} ORDER BY id"
        )))?
        .rows;

    Ok(ScriptRun { outcome, persisted })
}

/// Runs `template` once unbounded and once with `limit`, each against its own
/// table, and asserts both runs persist the same rows and end the same way:
/// the same number of result sets on success, the same SQLSTATE and hint on
/// failure. Returns the bounded run for shape-specific assertions.
fn assert_bounded_matches_unbounded(
    connection: &dyn dbflux_core::Connection,
    name: &str,
    template: &str,
    limit: u32,
) -> Result<ScriptRun, DbError> {
    let unbounded = run_script(connection, &format!("{name}_unbounded"), template, None)?;
    let bounded = run_script(
        connection,
        &format!("{name}_bounded"),
        template,
        Some(limit),
    )?;

    assert_eq!(
        bounded.persisted, unbounded.persisted,
        "bounded run of {name} must persist what the unbounded run persists"
    );

    match (&unbounded.outcome, &bounded.outcome) {
        (Ok(unbounded_result), Ok(bounded_result)) => assert_eq!(
            bounded_result.result_set_count(),
            unbounded_result.result_set_count(),
            "bounded run of {name} must return one result set per statement"
        ),
        (Err(unbounded_error), Err(bounded_error)) => {
            assert!(
                !matches!(bounded_error, DbError::NotSupported(_)),
                "bounded run of {name} must execute, got {bounded_error:?}"
            );
            assert_eq!(
                error_code(bounded_error),
                error_code(unbounded_error),
                "bounded run of {name} must fail with the unbounded SQLSTATE: {bounded_error:?}"
            );
            assert_eq!(
                error_hint(bounded_error),
                error_hint(unbounded_error),
                "bounded run of {name} must report the unbounded transaction state"
            );
        }
        (unbounded_outcome, bounded_outcome) => panic!(
            "{name}: unbounded run returned {:?} but bounded run returned {:?}",
            unbounded_outcome
                .as_ref()
                .map(|result| result.result_set_count()),
            bounded_outcome
                .as_ref()
                .map(|result| result.result_set_count()),
        ),
    }

    Ok(bounded)
}

fn assert_result_set(result: &dbflux_core::QueryResult, rows: usize, truncated: bool) {
    assert_eq!(result.rows.len(), rows, "retained rows of {result:?}");
    assert_eq!(
        result.rows_truncated(),
        truncated,
        "truncation of {result:?}"
    );
}

fn assert_session_outside_transaction(connection: &dyn dbflux_core::Connection) {
    let probe = connection.execute(&QueryRequest::new("SAVEPOINT outside_probe"));
    assert!(
        matches!(probe, Err(ref error) if error_code(error) == Some("25P01")),
        "the session must be left outside any transaction block, got {probe:?}"
    );
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_shares_one_row_budget_across_result_sets() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let result = connection.execute(
            &QueryRequest::new(
                "SELECT g FROM generate_series(1, 3) g; \
                 SELECT g FROM generate_series(1, 4) g; \
                 SELECT 1 WHERE FALSE; \
                 SELECT g FROM generate_series(1, 2) g",
            )
            .with_limit(5),
        )?;

        let sets: Vec<_> = result.iter_result_sets().collect();
        assert_eq!(sets.len(), 4);
        assert_result_set(sets[0], 3, false);
        assert_eq!(sets[0].rows[0][0], Value::Int(1));
        assert_result_set(sets[1], 2, true);
        assert_result_set(sets[2], 0, false);
        assert_result_set(sets[3], 0, true);

        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_mutation_after_exhausted_budget_persists() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let bounded = assert_bounded_matches_unbounded(
            &*connection,
            "batch_after_cap",
            "INSERT INTO {table} SELECT g FROM generate_series(1, 5) g RETURNING id; \
             INSERT INTO {table} VALUES (100); \
             SELECT id FROM {table} ORDER BY id",
            2,
        )?;

        assert_eq!(bounded.persisted.len(), 6);
        let result = bounded.outcome?;
        let sets: Vec<_> = result.iter_result_sets().collect();
        assert_result_set(sets[0], 2, true);
        assert_eq!(sets[1].affected_rows, Some(1));
        assert_result_set(sets[2], 0, true);

        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_zero_limit_runs_every_statement() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let bounded = assert_bounded_matches_unbounded(
            &*connection,
            "batch_zero",
            "SELECT 1; INSERT INTO {table} VALUES (1); SELECT 1 WHERE FALSE",
            0,
        )?;

        assert_eq!(bounded.persisted, vec![vec![Value::Int(1)]]);
        let result = bounded.outcome?;
        let sets: Vec<_> = result.iter_result_sets().collect();
        assert_result_set(sets[0], 0, true);
        assert_result_set(sets[2], 0, false);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_failure_rolls_back_earlier_statements() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let bounded = assert_bounded_matches_unbounded(
            &*connection,
            "batch_failure",
            "INSERT INTO {table} VALUES (1); \
             INSERT INTO {table} VALUES (2); \
             INSERT INTO {table} VALUES (1); \
             INSERT INTO {table} VALUES (3)",
            1,
        )?;

        assert!(bounded.persisted.is_empty());
        assert_eq!(error_code(&bounded.outcome.unwrap_err()), Some("23505"));
        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_with_its_own_transaction_matches_unbounded() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let committed = assert_bounded_matches_unbounded(
            &*connection,
            "own_transaction_commit",
            "BEGIN; INSERT INTO {table} VALUES (1); SELECT id FROM {table}; COMMIT",
            0,
        )?;
        assert_eq!(committed.persisted, vec![vec![Value::Int(1)]]);
        assert_session_outside_transaction(&*connection);

        let failed = assert_bounded_matches_unbounded(
            &*connection,
            "own_transaction_failure",
            "BEGIN; \
             INSERT INTO {table} VALUES (1); \
             INSERT INTO {table} VALUES (1); \
             COMMIT",
            0,
        )?;
        assert!(failed.persisted.is_empty());
        let hint = error_hint(failed.outcome.as_ref().unwrap_err())
            .expect("failed script error must carry a hint");
        assert!(hint.contains(TransactionStateNote::RolledBack.message()));
        assert_session_outside_transaction(&*connection);

        let after_commit = assert_bounded_matches_unbounded(
            &*connection,
            "after_commit_failure",
            "BEGIN; \
             INSERT INTO {table} VALUES (1); \
             COMMIT; \
             INSERT INTO {table} VALUES (2); \
             SELECT 1 / 0",
            0,
        )?;
        assert_eq!(after_commit.persisted, vec![vec![Value::Int(1)]]);
        assert_session_outside_transaction(&*connection);

        let rolled_back = assert_bounded_matches_unbounded(
            &*connection,
            "own_transaction_rollback",
            "BEGIN; INSERT INTO {table} VALUES (1); ROLLBACK; INSERT INTO {table} VALUES (2)",
            0,
        )?;
        assert_eq!(rolled_back.persisted, vec![vec![Value::Int(2)]]);
        assert_session_outside_transaction(&*connection);

        let savepoint = assert_bounded_matches_unbounded(
            &*connection,
            "own_transaction_savepoint",
            "BEGIN; \
             INSERT INTO {table} VALUES (1); \
             SAVEPOINT batch_savepoint; \
             INSERT INTO {table} VALUES (2); \
             ROLLBACK TO SAVEPOINT batch_savepoint; \
             END",
            0,
        )?;
        assert_eq!(savepoint.persisted, vec![vec![Value::Int(1)]]);
        assert_session_outside_transaction(&*connection);

        let chained = assert_bounded_matches_unbounded(
            &*connection,
            "own_transaction_chain",
            "BEGIN; \
             INSERT INTO {table} VALUES (1); \
             COMMIT AND CHAIN; \
             INSERT INTO {table} VALUES (2); \
             COMMIT",
            0,
        )?;
        assert_eq!(chained.persisted.len(), 2);
        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_statements_before_begin_join_its_transaction() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let committed = assert_bounded_matches_unbounded(
            &*connection,
            "before_begin_commit",
            "INSERT INTO {table} VALUES (1); BEGIN; INSERT INTO {table} VALUES (2); COMMIT",
            0,
        )?;
        assert!(
            committed.outcome.is_ok(),
            "the warning for the adopted BEGIN must not surface as an error"
        );
        assert_eq!(committed.persisted.len(), 2);
        assert_session_outside_transaction(&*connection);

        let failed = assert_bounded_matches_unbounded(
            &*connection,
            "before_begin_failure",
            "INSERT INTO {table} VALUES (1); \
             BEGIN; \
             INSERT INTO {table} VALUES (2); \
             INSERT INTO {table} VALUES (2); \
             COMMIT",
            0,
        )?;
        assert!(failed.persisted.is_empty());
        assert_session_outside_transaction(&*connection);

        let stray_rollback = assert_bounded_matches_unbounded(
            &*connection,
            "stray_rollback",
            "INSERT INTO {table} VALUES (1); ROLLBACK; INSERT INTO {table} VALUES (2)",
            0,
        )?;
        assert_eq!(stray_rollback.persisted, vec![vec![Value::Int(2)]]);
        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_refuses_shapes_its_transaction_block_cannot_match() -> Result<(), DbError>
{
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        for (name, template) in [
            (
                "implicit_savepoint",
                "INSERT INTO {table} VALUES (1); SAVEPOINT s; INSERT INTO {table} VALUES (2)",
            ),
            (
                "prepare_transaction",
                "BEGIN; INSERT INTO {table} VALUES (1); PREPARE TRANSACTION 'batch'",
            ),
            (
                "implicit_commit_and_chain",
                "INSERT INTO {table} VALUES (1); COMMIT AND CHAIN; INSERT INTO {table} VALUES (2)",
            ),
        ] {
            let run = run_script(&*connection, name, template, Some(1))?;
            assert!(
                matches!(run.outcome, Err(DbError::NotSupported(_))),
                "{name} must be refused: {:?}",
                run.outcome.as_ref().map(|result| result.result_set_count())
            );
            assert!(
                run.persisted.is_empty(),
                "{name} must be refused before any effect"
            );
            assert_session_outside_transaction(&*connection);
        }
        Ok(())
    })
}

/// Runs `template` once unbounded and once with `limit`, each inside a session
/// transaction opened before the script, then commits that transaction and
/// asserts both runs end the same way, leave the session in the same state,
/// and persist the same rows. Returns the bounded run's persisted rows.
fn assert_bounded_matches_unbounded_in_session_transaction(
    connection: &dyn dbflux_core::Connection,
    name: &str,
    template: &str,
    limit: u32,
) -> Result<Vec<Vec<Value>>, DbError> {
    let mut runs = Vec::new();

    for (table, limit) in [
        (format!("{name}_unbounded"), None),
        (format!("{name}_bounded"), Some(limit)),
    ] {
        connection.execute(&QueryRequest::new(format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY)"
        )))?;
        connection.execute(&QueryRequest::new("BEGIN"))?;

        let mut request = QueryRequest::new(template.replace("{table}", &table));
        if let Some(limit) = limit {
            request = request.with_limit(limit);
        }
        let outcome = connection.execute(&request);

        let session_error = connection
            .execute(&QueryRequest::new("SELECT 1"))
            .err()
            .and_then(|error| error_code(&error).map(str::to_string));
        connection.execute(&QueryRequest::new("COMMIT"))?;

        let persisted = connection
            .execute(&QueryRequest::new(format!(
                "SELECT id FROM {table} ORDER BY id"
            )))?
            .rows;

        runs.push((outcome, session_error, persisted));
    }

    let (bounded_outcome, bounded_session, bounded_persisted) = runs.pop().expect("bounded run");
    let (unbounded_outcome, unbounded_session, unbounded_persisted) =
        runs.pop().expect("unbounded run");

    assert_eq!(
        bounded_persisted, unbounded_persisted,
        "bounded run of {name} must persist what the unbounded run persists"
    );
    assert_eq!(
        bounded_session, unbounded_session,
        "bounded run of {name} must leave the session transaction in the unbounded state"
    );
    match (&unbounded_outcome, &bounded_outcome) {
        (Ok(unbounded_result), Ok(bounded_result)) => assert_eq!(
            bounded_result.result_set_count(),
            unbounded_result.result_set_count()
        ),
        (Err(unbounded_error), Err(bounded_error)) => {
            assert!(
                !matches!(bounded_error, DbError::NotSupported(_)),
                "bounded run of {name} must execute, got {bounded_error:?}"
            );
            assert_eq!(error_code(bounded_error), error_code(unbounded_error));
            assert_eq!(error_hint(bounded_error), error_hint(unbounded_error));
        }
        (unbounded_outcome, bounded_outcome) => panic!(
            "{name}: unbounded run returned {:?} but bounded run returned {:?}",
            unbounded_outcome
                .as_ref()
                .map(|result| result.result_set_count()),
            bounded_outcome
                .as_ref()
                .map(|result| result.result_set_count()),
        ),
    }

    Ok(bounded_persisted)
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_in_session_transaction_matches_unbounded() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let joined = assert_bounded_matches_unbounded_in_session_transaction(
            &*connection,
            "session_join",
            "INSERT INTO {table} VALUES (1); SELECT id FROM {table}",
            0,
        )?;
        assert_eq!(joined, vec![vec![Value::Int(1)]]);

        let failed = assert_bounded_matches_unbounded_in_session_transaction(
            &*connection,
            "session_failure",
            "INSERT INTO {table} VALUES (1); INSERT INTO {table} VALUES (1)",
            0,
        )?;
        assert!(failed.is_empty());

        let nested_begin = assert_bounded_matches_unbounded_in_session_transaction(
            &*connection,
            "session_nested_begin",
            "INSERT INTO {table} VALUES (1); BEGIN; INSERT INTO {table} VALUES (2)",
            0,
        )?;
        assert_eq!(nested_begin.len(), 2);

        let committed_then_implicit = assert_bounded_matches_unbounded_in_session_transaction(
            &*connection,
            "session_commit_then_failure",
            "INSERT INTO {table} VALUES (1); \
             COMMIT; \
             INSERT INTO {table} VALUES (2); \
             INSERT INTO {table} VALUES (2)",
            0,
        )?;
        assert_eq!(committed_then_implicit, vec![vec![Value::Int(1)]]);

        let committed_then_success = assert_bounded_matches_unbounded_in_session_transaction(
            &*connection,
            "session_commit_then_success",
            "INSERT INTO {table} VALUES (1); COMMIT; INSERT INTO {table} VALUES (2)",
            0,
        )?;
        assert_eq!(committed_then_success.len(), 2);
        Ok(())
    })
}

fn assert_no_batch_probe_savepoint(connection: &dyn dbflux_core::Connection) {
    let release = connection.execute(&QueryRequest::new("RELEASE SAVEPOINT dbflux_batch_probe"));
    assert!(
        matches!(release, Err(ref error) if error_code(error) == Some("3B001")),
        "the batch probe savepoint must not be left behind, got {release:?}"
    );
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_probe_leaves_no_savepoint_and_keeps_aborted_state() -> Result<(), DbError>
{
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE probe_savepoint (id INTEGER PRIMARY KEY)",
        ))?;

        connection.execute(&QueryRequest::new("BEGIN"))?;
        connection.execute(
            &QueryRequest::new("INSERT INTO probe_savepoint VALUES (1); SELECT 1").with_limit(1),
        )?;
        assert_no_batch_probe_savepoint(&*connection);
        connection.execute(&QueryRequest::new("ROLLBACK"))?;

        connection.execute(&QueryRequest::new("BEGIN"))?;
        connection.execute(
            &QueryRequest::new(
                "CREATE FUNCTION probe_single() RETURNS int LANGUAGE sql \
                 BEGIN ATOMIC SELECT 1; END",
            )
            .with_limit(1),
        )?;
        assert_no_batch_probe_savepoint(&*connection);
        connection.execute(&QueryRequest::new("ROLLBACK"))?;

        connection.execute(&QueryRequest::new("BEGIN"))?;
        connection.execute(&QueryRequest::new("SAVEPOINT user_savepoint"))?;
        let batch_error = connection
            .execute(
                &QueryRequest::new(
                    "INSERT INTO probe_savepoint VALUES (1); INSERT INTO probe_savepoint VALUES (1)",
                )
                .with_limit(1),
            )
            .expect_err("duplicate key must fail the batch");
        assert_eq!(error_code(&batch_error), Some("23505"));
        connection.execute(&QueryRequest::new("ROLLBACK TO SAVEPOINT user_savepoint"))?;
        assert_no_batch_probe_savepoint(&*connection);
        connection.execute(&QueryRequest::new("ROLLBACK"))?;

        connection.execute(&QueryRequest::new("BEGIN"))?;
        connection.execute(&QueryRequest::new("SAVEPOINT user_savepoint"))?;
        connection
            .execute(&QueryRequest::new("SELECT 1 / 0"))
            .expect_err("division by zero must abort the transaction");

        let aborted_error = connection
            .execute(&QueryRequest::new("SELECT 1; SELECT 2").with_limit(1))
            .expect_err("a batch in an aborted transaction must fail");
        assert_eq!(error_code(&aborted_error), Some("25P02"));
        let hint = error_hint(&aborted_error).expect("aborted batch error must carry a hint");
        assert!(hint.contains(TransactionStateNote::Aborted.message()));

        connection.execute(&QueryRequest::new("ROLLBACK TO SAVEPOINT user_savepoint"))?;
        connection.execute(&QueryRequest::new("SELECT 1"))?;
        assert_no_batch_probe_savepoint(&*connection);
        connection.execute(&QueryRequest::new("ROLLBACK"))?;

        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_inside_open_session_transaction_joins_it() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE open_transaction_batch (id INTEGER PRIMARY KEY)",
        ))?;

        connection.execute(&QueryRequest::new("BEGIN"))?;
        let result = connection.execute(
            &QueryRequest::new(
                "INSERT INTO open_transaction_batch VALUES (1); \
                 SELECT id FROM open_transaction_batch",
            )
            .with_limit(0),
        )?;
        assert_result_set(&result.additional_results[0], 0, true);
        connection.execute(&QueryRequest::new("ROLLBACK"))?;

        assert!(
            connection
                .execute(&QueryRequest::new("SELECT id FROM open_transaction_batch"))?
                .rows
                .is_empty(),
            "the batch must run inside the session transaction, not commit on its own"
        );

        connection.execute(&QueryRequest::new("BEGIN"))?;
        let error = connection
            .execute(
                &QueryRequest::new(
                    "INSERT INTO open_transaction_batch VALUES (1); \
                     INSERT INTO open_transaction_batch VALUES (1);",
                )
                .with_limit(1),
            )
            .expect_err("duplicate key must fail the batch");
        let hint = error_hint(&error).expect("failed batch error must carry a hint");
        assert!(
            hint.contains(TransactionStateNote::Aborted.message()),
            "hint must report the aborted transaction, got {hint:?}"
        );

        let probe_error = connection
            .execute(&QueryRequest::new("SELECT 1"))
            .expect_err("the session transaction must still be aborted");
        assert_eq!(error_code(&probe_error), Some("25P02"));

        connection.execute(&QueryRequest::new("ROLLBACK"))?;
        connection.execute(&QueryRequest::new("SELECT 1"))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_keeps_dollar_quoted_bodies_whole() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let bounded = assert_bounded_matches_unbounded(
            &*connection,
            "dollar_body",
            "DO $$ BEGIN INSERT INTO {table} VALUES (1); INSERT INTO {table} VALUES (2); END $$; \
             SELECT id FROM {table} ORDER BY id",
            1,
        )?;

        assert_eq!(bounded.persisted.len(), 2);
        let result = bounded.outcome?;
        assert_result_set(&result.additional_results[0], 1, true);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_vacuum_fails_inside_the_batch_transaction() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let bounded = assert_bounded_matches_unbounded(
            &*connection,
            "batch_vacuum",
            "INSERT INTO {table} VALUES (1); VACUUM {table}",
            1,
        )?;

        assert!(bounded.persisted.is_empty());
        let error = bounded.outcome.unwrap_err();
        assert_eq!(error_code(&error), Some("25001"));
        assert!(
            error
                .to_string()
                .contains("VACUUM cannot run inside a transaction block"),
            "the server error must reach the user: {error}"
        );
        assert_session_outside_transaction(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_bounded_batch_ignores_trailing_comments_and_semicolons() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;

        let result = connection.execute(
            &QueryRequest::new("SELECT 1; SELECT 2;;; -- trailing line\n/* trailing block */")
                .with_limit(1),
        )?;

        let sets: Vec<_> = result.iter_result_sets().collect();
        assert_eq!(sets.len(), 2);
        assert_result_set(sets[0], 1, false);
        assert_result_set(sets[1], 0, true);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_refusal_preserves_existing_cancel_signal() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        let cancel_handle = connection.cancel_handle();

        cancel_handle.cancel()?;
        assert!(cancel_handle.is_cancelled());

        let mut timeout_request = QueryRequest::new("SELECT 1");
        timeout_request.statement_timeout = Some(Duration::from_secs(1));
        let timeout_result = connection.execute(&timeout_request);
        assert!(
            matches!(timeout_result, Err(DbError::NotSupported(_))),
            "deadline must be refused: {timeout_result:?}"
        );
        assert!(
            cancel_handle.is_cancelled(),
            "the timeout refusal erased an existing cancellation"
        );

        let mut metric_request = QueryRequest::new("SELECT 1").with_limit(0);
        metric_request.execution_context = Some(dbflux_core::ExecutionContext {
            source: Some(dbflux_core::ExecutionSourceContext::InstanceMetricQuery {
                metric_id: "pg.tps".to_string(),
                start_ms: 0,
                end_ms: 1,
            }),
            ..Default::default()
        });
        let metric_result = connection.execute(&metric_request);
        assert!(
            matches!(metric_result, Err(DbError::NotSupported(_))),
            "bounded metric must be refused: {metric_result:?}"
        );
        assert!(
            cancel_handle.is_cancelled(),
            "the bounded metric refusal erased an existing cancellation"
        );

        connection.execute(&QueryRequest::new("SELECT 1"))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_uncapped_batch_still_executes_later_statements() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE safety_legacy (n INTEGER); INSERT INTO safety_legacy VALUES (1), (2)",
        ))?;
        assert_eq!(
            connection
                .execute(&QueryRequest::new("SELECT COUNT(*) FROM safety_legacy"))?
                .rows[0][0],
            Value::Int(2)
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_statement_timeout_rejected_before_execution() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE safety_timeout (n INTEGER)",
        ))?;
        let mut request = QueryRequest::new("INSERT INTO safety_timeout VALUES (1)");
        request.statement_timeout = Some(Duration::from_secs(5));
        let result = connection.execute(&request);
        assert!(
            matches!(result, Err(DbError::NotSupported(_))),
            "deadline must be refused: {result:?}"
        );
        assert_eq!(
            connection
                .execute(&QueryRequest::new("SELECT COUNT(*) FROM safety_timeout"))?
                .rows[0][0],
            Value::Int(0)
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_bounded_metric_context_rejected_and_uncapped_control_runs()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        for limit in [0, 1] {
            let mut request = QueryRequest::new("SELECT 1").with_limit(limit);
            request.execution_context = Some(dbflux_core::ExecutionContext {
                source: Some(dbflux_core::ExecutionSourceContext::InstanceMetricQuery {
                    metric_id: "pg.tps".to_string(),
                    start_ms: 0,
                    end_ms: 1,
                }),
                ..Default::default()
            });
            let result = connection.execute(&request);
            assert!(
                matches!(result, Err(DbError::NotSupported(_))),
                "bounded metric must be refused: {result:?}"
            );
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_bounded_inspector_context_rejected_and_uncapped_control_runs()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        for limit in [0, 1] {
            let mut request = QueryRequest::new("SELECT 1").with_limit(limit);
            request.execution_context = Some(dbflux_core::ExecutionContext {
                source: Some(
                    dbflux_core::ExecutionSourceContext::InstanceInspectorQuery {
                        metric_id: "pg.activity".to_string(),
                    },
                ),
                ..Default::default()
            });
            let result = connection.execute(&request);
            assert!(
                matches!(result, Err(DbError::NotSupported(_))),
                "bounded inspector must be refused: {result:?}"
            );
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_capped_single_statement_with_trailing_comments_executes()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        for sql in [
            "SELECT 1; -- trailing line comment",
            "SELECT 1; /* trailing block comment */",
        ] {
            assert_no_rows_truncated(
                &connection.execute(&QueryRequest::new(sql).with_limit(1))?,
                1,
            );
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_capped_quoted_semicolons_and_dollar_quotes_execute() -> Result<(), DbError>
{
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        for (sql, expected) in [("SELECT ';'::text", ";"), ("SELECT $$a;b$$::text", "a;b")] {
            let result = connection.execute(&QueryRequest::new(sql).with_limit(1))?;
            assert_no_rows_truncated(&result, 1);
            assert_eq!(result.rows[0][0], Value::Text(expected.to_string()));
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_capped_single_statement_with_trailing_nested_comment_executes()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        let result = connection.execute(
            &QueryRequest::new("SELECT 1; /* outer /* inner */ still outer */").with_limit(1),
        )?;
        assert_no_rows_truncated(&result, 1);
        assert_eq!(result.rows[0][0], Value::Int(1));
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_query_safety_invalid_single_statement_with_trailing_nested_comment_reports_syntax_error()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        let result = connection
            .execute(&QueryRequest::new("SELECT FROM; /* outer /* inner */ tail */").with_limit(1));
        assert!(
            matches!(result, Err(DbError::SyntaxError(_))),
            "invalid single statement must report PostgreSQL query failure: {result:?}"
        );
        Ok(())
    })
}
