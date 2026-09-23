#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    Connection, ConnectionProfile, ConstraintKind, DbConfig, DbDriver, DbError, IndexData,
    QueryRequest, Value,
};
use dbflux_driver_mssql::MssqlDriver;
use dbflux_test_support::{containers, ddl_fixtures::SqlServerFixtures};
use std::time::Duration;

const TEST_DATABASE: &str = "dbflux_test";

fn connect_mssql(uri: String) -> Result<(Box<dyn Connection>, MssqlDriver), DbError> {
    let driver = MssqlDriver::new();
    let profile = ConnectionProfile::new(
        "ddl-mssql",
        DbConfig::SqlServer {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 1433,
            user: String::new(),
            database: None,
            instance: None,
            ssl_mode: Some("on".to_string()),
            trust_server_certificate: true,
            ssl_root_cert_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection =
        containers::retry_db_operation(Duration::from_secs(60), || -> Result<_, DbError> {
            let connection = driver.connect(&profile)?;
            connection.ping()?;
            Ok(connection)
        })?;

    connection.execute(&QueryRequest::new(format!(
        "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '{TEST_DATABASE}') \
         CREATE DATABASE [{TEST_DATABASE}]"
    )))?;
    connection.set_active_database(Some(TEST_DATABASE))?;

    Ok((connection, driver))
}

fn cleanup_test_tables(conn: &dyn Connection) {
    // Drop in FK order — orders references users, etc.
    let tables = [
        "orders",
        "order_items",
        "accounts",
        "products",
        "users",
        "alter_test",
        "truncate_test",
        "fk_child",
        "fk_parent",
    ];

    for table in tables {
        let _ = conn.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS dbo.[{}]",
            table
        )));
    }

    for view in ["active_users", "test_view"] {
        let _ = conn.execute(&QueryRequest::new(format!(
            "DROP VIEW IF EXISTS dbo.[{}]",
            view
        )));
    }

    // DROP INDEX needs the table reference, but if the table was already
    // dropped above the indexes went with it. We only need to clean up
    // indexes whose table might survive between tests; the table cleanup
    // covers everything we create in this suite.
}

// ---------------------------------------------------------------------------
// CREATE TABLE tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_identity_pk() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        assert_eq!(table_details.name, table.name);

        let columns = table_details
            .columns
            .as_ref()
            .expect("columns should be loaded");
        assert!(columns.len() >= 4);

        let id_col = columns.iter().find(|c| c.name == "id").expect("id column");
        assert!(id_col.is_primary_key);
        assert!(!id_col.nullable);

        let username_col = columns
            .iter()
            .find(|c| c.name == "username")
            .expect("username column");
        assert!(!username_col.nullable);

        let email_col = columns
            .iter()
            .find(|c| c.name == "email")
            .expect("email column");
        assert!(!email_col.nullable);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_composite_pk() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_composite_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let columns = table_details
            .columns
            .as_ref()
            .expect("columns should be loaded");

        let order_id_col = columns
            .iter()
            .find(|c| c.name == "order_id")
            .expect("order_id column");
        assert!(order_id_col.is_primary_key);

        let product_id_col = columns
            .iter()
            .find(|c| c.name == "product_id")
            .expect("product_id column");
        assert!(product_id_col.is_primary_key);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_with_fk() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let parent_table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

        let child_table = SqlServerFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&child_table.create_sql))?;

        let table_details =
            connection.table_details(TEST_DATABASE, Some("dbo"), &child_table.name)?;

        let fks = table_details
            .foreign_keys
            .as_ref()
            .expect("foreign keys should be loaded");
        assert!(!fks.is_empty());

        let fk = &fks[0];
        assert_eq!(fk.referenced_table, "users");
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.referenced_columns, vec!["id"]);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_with_check_constraint() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_with_check();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let constraints = table_details
            .constraints
            .as_ref()
            .expect("constraints should be loaded");
        assert!(!constraints.is_empty());

        let has_check = constraints
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::Check) && c.name.contains("positive_price"));
        assert!(has_check, "should have positive_price check constraint");

        // The CHECK should actually be enforced.
        let insert_result = connection.execute(&QueryRequest::new(
            "INSERT INTO products (name, price, stock) VALUES (N'bad', -10, 5)",
        ));
        assert!(insert_result.is_err(), "should violate check constraint");

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_with_unique_constraint() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_with_unique();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let constraints = table_details
            .constraints
            .as_ref()
            .expect("constraints should be loaded");

        let has_unique = constraints
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::Unique));
        assert!(has_unique, "should have unique constraint");

        connection.execute(&QueryRequest::new(
            "INSERT INTO accounts (email, username) VALUES (N'test@example.com', N'testuser')",
        ))?;

        let duplicate_result = connection.execute(&QueryRequest::new(
            "INSERT INTO accounts (email, username) VALUES (N'test@example.com', N'testuser2')",
        ));
        assert!(
            duplicate_result.is_err(),
            "should violate unique constraint"
        );

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CREATE INDEX tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_index_single_column() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let index = SqlServerFixtures::index_single_column();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let indexes = table_details
            .indexes
            .as_ref()
            .expect("indexes should be loaded");

        let index_list = match indexes {
            IndexData::Relational(list) => list,
            _ => panic!("expected relational index data"),
        };

        let has_index = index_list
            .iter()
            .any(|i| i.name == index.name && i.columns.contains(&"email".to_string()));
        assert!(has_index, "index should exist");

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_index_unique() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let index = SqlServerFixtures::index_unique();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let indexes = table_details
            .indexes
            .as_ref()
            .expect("indexes should be loaded");

        let index_list = match indexes {
            IndexData::Relational(list) => list,
            _ => panic!("expected relational index data"),
        };

        let found_index = index_list
            .iter()
            .find(|i| i.name == index.name)
            .expect("unique index should exist");
        assert!(found_index.is_unique, "index should be unique");

        connection.execute(&QueryRequest::new(
            "INSERT INTO users (username, email) VALUES (N'alice', N'alice@example.com')",
        ))?;

        let duplicate_result = connection.execute(&QueryRequest::new(
            "INSERT INTO users (username, email) VALUES (N'alice', N'bob@example.com')",
        ));
        assert!(
            duplicate_result.is_err(),
            "should violate unique index constraint"
        );

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_index_composite() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let users_table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&users_table.create_sql))?;

        let orders_table = SqlServerFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&orders_table.create_sql))?;

        let index = SqlServerFixtures::index_composite();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let table_details = connection.table_details(TEST_DATABASE, Some("dbo"), &index.table)?;
        let indexes = table_details
            .indexes
            .as_ref()
            .expect("indexes should be loaded");

        let index_list = match indexes {
            IndexData::Relational(list) => list,
            _ => panic!("expected relational index data"),
        };

        let found_index = index_list
            .iter()
            .find(|i| i.name == index.name)
            .expect("composite index should exist");
        assert_eq!(
            found_index.columns.len(),
            2,
            "should have two columns in composite index"
        );
        assert!(found_index.columns.contains(&"user_id".to_string()));
        assert!(found_index.columns.contains(&"status".to_string()));

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CREATE VIEW
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_view() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let view = SqlServerFixtures::view_simple();
        connection.execute(&QueryRequest::new(&view.create_sql))?;

        let schema = connection.schema_for_database(TEST_DATABASE)?;
        let has_view = schema.views.iter().any(|v| v.name == view.name);
        assert!(has_view, "view should appear in schema_for_database output");

        let result = connection.execute(&QueryRequest::new("SELECT * FROM active_users"))?;
        assert!(!result.columns.is_empty());

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// ALTER TABLE (add / drop column — no rename, driver flags that off)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_alter_table_add_column() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let scenario = SqlServerFixtures::alter_add_column();
        for sql in &scenario.setup_sql {
            connection.execute(&QueryRequest::new(sql))?;
        }

        let before = connection.table_details(TEST_DATABASE, Some("dbo"), "alter_test")?;
        let before_cols = before.columns.as_ref().expect("columns should exist");
        let before_count = before_cols.len();

        connection.execute(&QueryRequest::new(&scenario.test_sql))?;

        let after = connection.table_details(TEST_DATABASE, Some("dbo"), "alter_test")?;
        let after_cols = after.columns.as_ref().expect("columns should exist");
        assert_eq!(after_cols.len(), before_count + 1);
        assert!(after_cols.iter().any(|c| c.name == "age"));

        for sql in &scenario.cleanup_sql {
            let _ = connection.execute(&QueryRequest::new(sql));
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_alter_table_drop_column() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let scenario = SqlServerFixtures::alter_drop_column();
        for sql in &scenario.setup_sql {
            connection.execute(&QueryRequest::new(sql))?;
        }

        let before = connection.table_details(TEST_DATABASE, Some("dbo"), "alter_test")?;
        let before_cols = before.columns.as_ref().expect("columns should exist");
        let before_count = before_cols.len();
        assert!(before_cols.iter().any(|c| c.name == "age"));

        connection.execute(&QueryRequest::new(&scenario.test_sql))?;

        let after = connection.table_details(TEST_DATABASE, Some("dbo"), "alter_test")?;
        let after_cols = after.columns.as_ref().expect("columns should exist");
        assert_eq!(after_cols.len(), before_count - 1);
        assert!(!after_cols.iter().any(|c| c.name == "age"));

        for sql in &scenario.cleanup_sql {
            let _ = connection.execute(&QueryRequest::new(sql));
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// DROP TABLE / INDEX / VIEW
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_drop_table() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let before = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name);
        assert!(before.is_ok(), "table should exist");

        connection.execute(&QueryRequest::new(format!("DROP TABLE {}", table.name)))?;

        let after = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name);
        // After the drop the table details query returns no columns; we accept
        // either a hard error or an "empty" TableInfo depending on driver
        // surface, both of which mean "table is gone".
        if let Ok(details) = after {
            assert!(
                details
                    .columns
                    .as_ref()
                    .map(|c| c.is_empty())
                    .unwrap_or(true)
            );
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_drop_index() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let index = SqlServerFixtures::index_single_column();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let before = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let before_indexes = before.indexes.as_ref().expect("indexes should exist");
        let before_list = match before_indexes {
            IndexData::Relational(list) => list,
            _ => panic!("expected relational index data"),
        };
        assert!(before_list.iter().any(|i| i.name == index.name));

        // SQL Server requires the table reference on DROP INDEX.
        connection.execute(&QueryRequest::new(format!(
            "DROP INDEX {} ON {}",
            index.name, index.table
        )))?;

        let after = connection.table_details(TEST_DATABASE, Some("dbo"), &table.name)?;
        let after_indexes = after.indexes.as_ref().expect("indexes should exist");
        let after_list = match after_indexes {
            IndexData::Relational(list) => list,
            _ => panic!("expected relational index data"),
        };
        assert!(!after_list.iter().any(|i| i.name == index.name));

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_drop_view() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let view = SqlServerFixtures::view_simple();
        connection.execute(&QueryRequest::new(&view.create_sql))?;

        let before = connection.schema_for_database(TEST_DATABASE)?;
        assert!(before.views.iter().any(|v| v.name == view.name));

        connection.execute(&QueryRequest::new(format!("DROP VIEW {}", view.name)))?;

        let after = connection.schema_for_database(TEST_DATABASE)?;
        assert!(!after.views.iter().any(|v| v.name == view.name));

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// TRUNCATE
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_truncate_table() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE truncate_test (id INT IDENTITY(1,1) PRIMARY KEY, value NVARCHAR(50))",
        ))?;

        for i in 1..=10 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO truncate_test (value) VALUES (N'item_{}')",
                i
            )))?;
        }

        let before =
            connection.execute(&QueryRequest::new("SELECT COUNT(*) FROM truncate_test"))?;
        let count_before = match &before.rows[0][0] {
            Value::Int(n) => *n,
            other => panic!("expected integer count, got {:?}", other),
        };
        assert_eq!(count_before, 10);

        connection.execute(&QueryRequest::new("TRUNCATE TABLE truncate_test"))?;

        let after = connection.execute(&QueryRequest::new("SELECT COUNT(*) FROM truncate_test"))?;
        let count_after = match &after.rows[0][0] {
            Value::Int(n) => *n,
            other => panic!("expected integer count, got {:?}", other),
        };
        assert_eq!(count_after, 0);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Error scenarios
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_error_constraint_violation() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let table = SqlServerFixtures::table_with_check();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let result = connection.execute(&QueryRequest::new(
            "INSERT INTO products (name, price, stock) VALUES (N'bad', -5, 10)",
        ));
        match result {
            Err(DbError::ConstraintViolation(_)) => {}
            other => panic!("expected ConstraintViolation, got {:?}", other),
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_error_fk_violation() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let parent_table = SqlServerFixtures::table_identity_pk();
        connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

        let child_table = SqlServerFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&child_table.create_sql))?;

        let result = connection.execute(&QueryRequest::new(
            "INSERT INTO orders (user_id, total, status) VALUES (9999, 100.00, N'pending')",
        ));
        match result {
            Err(DbError::ConstraintViolation(_)) => {}
            other => panic!("expected ConstraintViolation for FK error, got {:?}", other),
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_error_missing_object_classified_as_not_found() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (connection, _) = connect_mssql(uri)?;
        cleanup_test_tables(&*connection);

        let result = connection.execute(&QueryRequest::new("SELECT * FROM dbo.no_such_table_xyz"));
        match result {
            Err(DbError::ObjectNotFound(_)) => {}
            other => panic!("expected ObjectNotFound, got {:?}", other),
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// DBF-161: faithful CREATE TABLE generation from reference creation metadata
// ---------------------------------------------------------------------------

/// Source fixture exercising every dimension the faithful generator must
/// preserve, restricted to collation-free types (numeric(38,0) identity
/// values beyond i64, decimal precision/scale, temporal scale, binary
/// length, column defaults, composite primary-key order ([Amount] before
/// [ID])). String columns carry a collation the structured contract cannot
/// express and are covered by the named-refusal tests below.
const GENERATION_SOURCE_SQL: &str = "CREATE TABLE dbo.generation_source (
    [ID] NUMERIC(38,0) IDENTITY(99999999999999999999999999999999999990, 3) NOT NULL,
    [Amount] DECIMAL(12,4) NOT NULL DEFAULT ((0)),
    [Blob] VARBINARY(16) NULL,
    [Stamp] DATETIME2(3) NULL,
    CONSTRAINT PK_generation_source PRIMARY KEY ([Amount], [ID])
)";

/// String-typed fixture for introspection-only dimension assertions and the
/// collation named-refusal behavior.
const GENERATION_STRINGS_SQL: &str = "CREATE TABLE dbo.generation_strings (
    [A] INT NOT NULL PRIMARY KEY,
    [Name] NVARCHAR(50) NOT NULL,
    [Bio] NVARCHAR(MAX) NULL,
    [Code] VARCHAR(40) NULL
)";

const GENERATION_TARGET_DB: &str = "dbflux_generation_target";

fn type_of<'a>(columns: &'a [dbflux_core::ColumnInfo], name: &str) -> &'a str {
    columns
        .iter()
        .find(|column| column.name == name)
        .unwrap_or_else(|| panic!("column {name} must be introspected"))
        .type_name
        .as_str()
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_generation_roundtrip_preserves_metadata_fidelity() -> Result<(), DbError>
{
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri.clone())?;

        source.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS dbo.[generation_source]"
        )))?;
        source.execute(&QueryRequest::new(GENERATION_SOURCE_SQL))?;

        // Introspect the reference source: table detail with exact type
        // dimensions plus the structured creation metadata.
        let source_table = source.table_details(TEST_DATABASE, Some("dbo"), "generation_source")?;
        let source_columns = source_table
            .columns
            .as_ref()
            .expect("source columns must be loaded");

        // Type dimensions, normalized only for case: sys.types spells
        // decimal/numeric canonically and lowercases everything.
        assert!(
            type_of(source_columns, "ID")
                .to_ascii_lowercase()
                .ends_with("(38,0)"),
            "identity column type must carry full precision, got: {}",
            type_of(source_columns, "ID")
        );
        assert_eq!(
            type_of(source_columns, "Blob").to_ascii_lowercase(),
            "varbinary(16)"
        );
        assert!(
            type_of(source_columns, "Amount")
                .to_ascii_lowercase()
                .ends_with("(12,4)"),
            "decimal scale must be preserved, got: {}",
            type_of(source_columns, "Amount")
        );
        assert_eq!(
            type_of(source_columns, "Stamp").to_ascii_lowercase(),
            "datetime2(3)",
            "temporal scale must be preserved"
        );
        // Identity must never leak into the type name.
        assert!(
            !type_of(source_columns, "ID")
                .to_ascii_lowercase()
                .contains("identity"),
            "identity must travel in creation metadata, not type_name"
        );

        let source_metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "generation_source")?
            .expect("MSSQL must report creation metadata for its own tables");

        assert_eq!(
            source_metadata.completeness,
            dbflux_core::MetadataCompleteness::Complete,
            "a collation-free fixture table with readable defaults must be fully observable"
        );
        assert!(
            source_metadata.blockers.is_empty(),
            "a collation-free fixture table must carry no blockers, got: {:?}",
            source_metadata.blockers
        );
        let identity = source_metadata.identity.as_ref().expect("identity spec");
        assert_eq!(identity.column, "ID");
        assert_eq!(
            identity.seed, "99999999999999999999999999999999999990",
            "seed must be read as exact server-converted text, not narrowed"
        );
        assert_eq!(identity.increment, "3");
        let primary_key = source_metadata.primary_key.as_ref().expect("pk spec");
        assert_eq!(
            primary_key.columns,
            vec!["Amount".to_string(), "ID".to_string()],
            "composite PK order must be the declared key order"
        );

        // Distinct target database on its own connection: generation must run
        // on the TARGET driver with REFERENCE-side metadata.
        source.execute(&QueryRequest::new(format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '{GENERATION_TARGET_DB}') \
             CREATE DATABASE [{GENERATION_TARGET_DB}]"
        )))?;
        source.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS [{GENERATION_TARGET_DB}].dbo.[generation_source]"
        )))?;
        let (target, _) = connect_mssql(uri.clone())?;
        target.set_active_database(Some(GENERATION_TARGET_DB))?;

        let ddl = target.generate_code_with_creation_metadata(
            "create_table",
            &source_table,
            Some(&source_metadata),
        )?;
        target.execute(&QueryRequest::new(&ddl))?;

        // Re-introspect the target and compare.
        let target_table =
            target.table_details(GENERATION_TARGET_DB, Some("dbo"), "generation_source")?;
        let target_columns = target_table
            .columns
            .as_ref()
            .expect("target columns must be loaded");

        for column in ["ID", "Blob", "Amount", "Stamp"] {
            assert_eq!(
                type_of(target_columns, column).to_ascii_lowercase(),
                type_of(source_columns, column).to_ascii_lowercase(),
                "column {column} type dimensions must survive the roundtrip"
            );
        }
        for column in ["ID", "Amount"] {
            let before = source_columns
                .iter()
                .find(|c| c.name == column)
                .expect("source column");
            let after = target_columns
                .iter()
                .find(|c| c.name == column)
                .expect("target column");
            assert_eq!(before.nullable, after.nullable, "nullability of {column}");
        }
        assert_eq!(
            target_columns
                .iter()
                .find(|c| c.name == "Amount")
                .expect("Amount column")
                .default_value,
            Some("((0))".to_string()),
            "column default must survive the roundtrip"
        );

        let target_metadata = target
            .table_creation_metadata(GENERATION_TARGET_DB, Some("dbo"), "generation_source")?
            .expect("target metadata");
        assert_eq!(target_metadata.identity, source_metadata.identity);
        assert_eq!(target_metadata.primary_key, source_metadata.primary_key);

        target.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS [{GENERATION_TARGET_DB}].dbo.[generation_source]"
        )))?;
        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[generation_source]",
        ))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_generation_refuses_without_reference_metadata() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri)?;
        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[generation_source]",
        ))?;
        source.execute(&QueryRequest::new(GENERATION_SOURCE_SQL))?;
        let source_table = source.table_details(TEST_DATABASE, Some("dbo"), "generation_source")?;

        // Old snapshots (and any reference without creation metadata) must
        // fail closed: no identity-less regeneration of an identity table.
        let error = source
            .generate_code_with_creation_metadata("create_table", &source_table, None)
            .expect_err("generation without reference metadata must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("recapture") || message.contains("creation metadata"),
            "refusal must name the missing reference metadata, got: {message}"
        );

        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[generation_source]",
        ))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_generation_refuses_nonclustered_pk_and_compression() -> Result<(), DbError>
{
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri.clone())?;

        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[gen_nc]; DROP TABLE IF EXISTS dbo.[gen_comp]",
        ))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.gen_nc ( \
                 [A] INT NOT NULL, \
                 [B] INT NOT NULL, \
                 [V] NVARCHAR(30) NULL, \
                 CONSTRAINT PK_gen_nc PRIMARY KEY NONCLUSTERED ([A], [B]) \
             )",
        ))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.gen_comp ( \
                 [A] INT NOT NULL IDENTITY(1, 1) PRIMARY KEY, \
                 [V] NVARCHAR(30) NULL \
             ) WITH (DATA_COMPRESSION = ROW)",
        ))?;

        for table in ["gen_nc", "gen_comp"] {
            let details = source.table_details(TEST_DATABASE, Some("dbo"), table)?;
            let metadata = source
                .table_creation_metadata(TEST_DATABASE, Some("dbo"), table)?
                .expect("metadata");

            let codes: Vec<&str> = metadata.blockers.iter().map(|b| b.code.as_str()).collect();
            match table {
                "gen_nc" => assert!(
                    codes.contains(&"nonclustered_primary_key"),
                    "a nonclustered PK must block faithful generation, got: {codes:?}"
                ),
                "gen_comp" => assert!(
                    codes.contains(&"data_compression"),
                    "row compression must block faithful generation, got: {codes:?}"
                ),
                _ => unreachable!(),
            }

            let error = source
                .generate_code_with_creation_metadata("create_table", &details, Some(&metadata))
                .expect_err("blocked semantics must refuse generation");
            assert!(error.to_string().contains("cannot express"), "got: {error}");
        }

        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[gen_nc]; DROP TABLE IF EXISTS dbo.[gen_comp]",
        ))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_introspection_reports_string_dimensions_but_generation_refuses_collation()
-> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri.clone())?;

        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[generation_strings]",
        ))?;
        source.execute(&QueryRequest::new(GENERATION_STRINGS_SQL))?;

        // String dimensions are still introspected exactly.
        let details = source.table_details(TEST_DATABASE, Some("dbo"), "generation_strings")?;
        let columns = details.columns.as_ref().expect("columns");
        assert_eq!(
            type_of(columns, "Name").to_ascii_lowercase(),
            "nvarchar(50)",
            "Unicode length must be rendered in characters, not UTF-16 bytes"
        );
        assert_eq!(
            type_of(columns, "Bio").to_ascii_lowercase(),
            "nvarchar(max)"
        );
        assert_eq!(type_of(columns, "Code").to_ascii_lowercase(), "varchar(40)");

        // Collation-bearing columns must be detected as unrepresentable by
        // the structured contract and must refuse generation by name.
        let metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "generation_strings")?
            .expect("metadata");
        let collation_blockers: Vec<&str> = metadata
            .blockers
            .iter()
            .filter(|b| b.code == "column_collation")
            .map(|b| b.column.as_deref().expect("column-scoped blocker"))
            .collect();
        assert!(
            collation_blockers.contains(&"Name")
                && collation_blockers.contains(&"Bio")
                && collation_blockers.contains(&"Code"),
            "every string column must be blocked by code 'column_collation', got: {:?}",
            metadata.blockers
        );

        let error = source
            .generate_code_with_creation_metadata("create_table", &details, Some(&metadata))
            .expect_err("collation semantics must refuse generation");
        assert!(
            error.to_string().contains("column_collation"),
            "refusal must name the blocker code, got: {error}"
        );

        source.execute(&QueryRequest::new(
            "DROP TABLE IF EXISTS dbo.[generation_strings]",
        ))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_refuses_collation_even_when_matching_source_db_default()
-> Result<(), DbError> {
    // Cross-database fidelity: a column whose collation equals the SOURCE
    // database default still gets the TARGET database default when created
    // without an explicit COLLATE clause, and the target default is unknown
    // at reference-introspection time. Matching the source default is
    // therefore NOT sufficient — the named refusal must fire in this case
    // too, not just for explicitly-declared collations.
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri.clone())?;

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_coll_db]"))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.gen_coll_db ( \
                 [A] INT NOT NULL PRIMARY KEY, \
                 [V] NVARCHAR(30) NULL \
             )",
        ))?;

        // Prove the fixture really is the matched-default case.
        let probe = source.execute(&QueryRequest::new(
            "SELECT CASE WHEN c.collation_name = \
                  CONVERT(NVARCHAR(256), DATABASEPROPERTYEX(DB_NAME(), 'Collation')) \
                  THEN 1 ELSE 0 END AS matches_default \
             FROM sys.columns c \
             JOIN sys.tables t ON t.object_id = c.object_id \
             WHERE t.name = 'gen_coll_db' AND c.name = 'V'",
        ))?;
        match &probe.rows[0][0] {
            Value::Int(1) => {}
            other => panic!(
                "fixture precondition: column collation should equal the source DB default, got {other:?}"
            ),
        }

        let details = source.table_details(TEST_DATABASE, Some("dbo"), "gen_coll_db")?;
        let metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "gen_coll_db")?
            .expect("metadata");
        assert!(
            metadata
                .blockers
                .iter()
                .any(|b| b.code == "column_collation"),
            "a collation matching the source DB default must still block (target default unknown), got: {:?}",
            metadata.blockers
        );
        let error = source
            .generate_code_with_creation_metadata("create_table", &details, Some(&metadata))
            .expect_err("must refuse");
        assert!(
            error.to_string().contains("column_collation"),
            "got: {error}"
        );

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_coll_db]"))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_refuses_unrepresented_identity_and_key_semantics() -> Result<(), DbError>
{
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri.clone())?;

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_nfr]"))?;
        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_desc]"))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.gen_nfr ( \
                 [ID] INT IDENTITY(1, 1) NOT FOR REPLICATION NOT NULL PRIMARY KEY, \
                 [V] INT NULL \
             )",
        ))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.gen_desc ( \
                 [A] INT NOT NULL, \
                 [B] INT NOT NULL, \
                 CONSTRAINT PK_gen_desc PRIMARY KEY ([A] DESC, [B]) \
             )",
        ))?;

        let nfr = source.table_details(TEST_DATABASE, Some("dbo"), "gen_nfr")?;
        let nfr_metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "gen_nfr")?
            .expect("metadata");
        assert!(
            nfr_metadata
                .blockers
                .iter()
                .any(|b| b.code == "identity_not_for_replication"),
            "NOT FOR REPLICATION identity must block faithful generation, got: {:?}",
            nfr_metadata.blockers
        );
        assert!(
            source
                .generate_code_with_creation_metadata("create_table", &nfr, Some(&nfr_metadata))
                .is_err(),
            "NOT FOR REPLICATION identity must refuse generation"
        );

        let desc = source.table_details(TEST_DATABASE, Some("dbo"), "gen_desc")?;
        let desc_metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "gen_desc")?
            .expect("metadata");
        assert!(
            desc_metadata
                .blockers
                .iter()
                .any(|b| b.code == "descending_primary_key"),
            "a descending PK key column must block faithful generation, got: {:?}",
            desc_metadata.blockers
        );
        assert!(
            source
                .generate_code_with_creation_metadata("create_table", &desc, Some(&desc_metadata))
                .is_err(),
            "a descending PK must refuse generation"
        );

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_nfr]"))?;
        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_desc]"))?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_capture_reports_unavailable_default_expression_for_bound_defaults()
-> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri.clone())?;

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_bound]"))?;
        source.execute(&QueryRequest::new(
            "DROP DEFAULT IF EXISTS dbo.[gen_bound_default]",
        ))?;
        source.execute(&QueryRequest::new(
            "CREATE DEFAULT dbo.gen_bound_default AS 7",
        ))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.gen_bound ( \
                 [A] INT NOT NULL PRIMARY KEY, \
                 [V] INT NULL \
             )",
        ))?;
        source.execute(&QueryRequest::new(
            "EXEC sp_bindefault 'dbo.gen_bound_default', 'dbo.gen_bound.[V]'",
        ))?;

        let details = source.table_details(TEST_DATABASE, Some("dbo"), "gen_bound")?;
        let metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "gen_bound")?
            .expect("metadata");

        // The bound default IS a known default (default_object_id <> 0) whose
        // definition is not available through sys.default_constraints; it
        // must surface as a named incompleteness, never as "no default".
        match &metadata.completeness {
            dbflux_core::MetadataCompleteness::Partial { missing } => assert!(
                missing.contains(&dbflux_core::MissingCreationMetadata::ColumnDefaultExpressions),
                "bound default must report ColumnDefaultExpressions missing, got {missing:?}"
            ),
            other => panic!("expected Partial completeness, got {other:?}"),
        }
        let v_column = details
            .columns
            .as_ref()
            .expect("columns")
            .iter()
            .find(|c| c.name == "V")
            .expect("V column");
        assert!(
            v_column.default_value.is_none(),
            "an unavailable default expression must not masquerade as a readable one"
        );
        let error = source
            .generate_code_with_creation_metadata("create_table", &details, Some(&metadata))
            .expect_err("generation must refuse");
        assert!(
            error.to_string().to_lowercase().contains("default"),
            "refusal must name the missing default expressions, got: {error}"
        );
        assert!(
            !error.to_string().contains('7'),
            "refusal must not leak source expression values, got: {error}"
        );

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_bound]"))?;
        source.execute(&QueryRequest::new(
            "DROP DEFAULT IF EXISTS dbo.[gen_bound_default]",
        ))?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// DBF-161 generator PR2: typed XML blockers and session database resolution
// ---------------------------------------------------------------------------

const TYPED_XML_COLLECTION: &str = "TypedXmlCollection";
const TYPED_XML_TABLE: &str = "typed_xml_source";

/// Fixture teardown: dropping the table first releases the collection so
/// the collection drop cannot fail on a dependent column. `IF EXISTS` /
/// existence-guarded SQL tolerates absent objects; genuine failures
/// propagate instead of being discarded.
fn drop_typed_xml_fixture(conn: &dyn Connection) -> Result<(), DbError> {
    conn.execute(&QueryRequest::new(format!(
        "DROP TABLE IF EXISTS dbo.[{TYPED_XML_TABLE}]"
    )))?;
    conn.execute(&QueryRequest::new(format!(
        "IF EXISTS (SELECT 1 FROM sys.xml_schema_collections x \n\
                     JOIN sys.schemas s ON s.schema_id = x.schema_id \n\
                     WHERE s.name = 'dbo' AND x.name = '{TYPED_XML_COLLECTION}') \n\
         DROP XML SCHEMA COLLECTION dbo.[{TYPED_XML_COLLECTION}]"
    )))?;
    Ok(())
}

/// Scalar helper: read the session's actual database straight from the
/// server so assertions never hardcode a database name.
fn query_session_database(conn: &dyn Connection) -> Option<String> {
    let result = conn
        .execute(&QueryRequest::new("SELECT DB_NAME()"))
        .expect("SELECT DB_NAME() must succeed");
    result.rows.into_iter().next().and_then(|row| {
        row.into_iter().next().and_then(|value| match value {
            Value::Text(name) => Some(name),
            _ => None,
        })
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_ddl_create_table_generation_refuses_typed_xml() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri)?;
        drop_typed_xml_fixture(&*source)?;

        source.execute(&QueryRequest::new(format!(
            "CREATE XML SCHEMA COLLECTION dbo.[{TYPED_XML_COLLECTION}] AS \n\
             N'<?xml version=\"1.0\" encoding=\"utf-16\"?> \n\
             <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" \n\
                         elementFormDefault=\"qualified\"> \n\
               <xsd:element name=\"item\"> \n\
                 <xsd:complexType> \n\
                   <xsd:sequence> \n\
                     <xsd:element name=\"value\" type=\"xsd:string\" nillable=\"true\"/> \n\
                   </xsd:sequence> \n\
                 </xsd:complexType> \n\
               </xsd:element> \n\
             </xsd:schema>'"
        )))?;
        source.execute(&QueryRequest::new(format!(
            "CREATE TABLE dbo.[{TYPED_XML_TABLE}] ( \n\
                 [Id] INT NOT NULL PRIMARY KEY CLUSTERED, \n\
                 [Payload] xml(DOCUMENT dbo.[{TYPED_XML_COLLECTION}]) NOT NULL \n\
             )"
        )))?;

        let details = source.table_details(TEST_DATABASE, Some("dbo"), TYPED_XML_TABLE)?;
        let metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), TYPED_XML_TABLE)?
            .expect("MSSQL must report creation metadata for its own tables");

        let blocker = metadata
            .blockers
            .iter()
            .find(|blocker| blocker.code == "typed_xml")
            .expect("typed XML must be reported as a named creation blocker");
        assert_eq!(
            blocker.column.as_deref(),
            Some("Payload"),
            "the blocker must name the typed XML column"
        );

        let error = source
            .generate_code_with_creation_metadata("create_table", &details, Some(&metadata))
            .expect_err("generation must refuse typed XML instead of emitting plain xml");
        let lowered = error.to_string().to_lowercase();
        assert!(
            lowered.contains("typed_xml"),
            "refusal must name the typed_xml blocker, got: {error}"
        );

        drop_typed_xml_fixture(&*source)?;
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_uri_default_login_resolves_actual_session_database() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        // A default-login URI names no database: strip the /master suffix so
        // the login itself decides which database the session starts in.
        let base = uri
            .rsplit_once('/')
            .map(|(base, _)| base.to_string())
            .expect("fixture URI must contain a /<database> suffix");

        let driver = MssqlDriver::new();
        let profile = ConnectionProfile::new(
            "ddl-mssql-default-login",
            DbConfig::SqlServer {
                use_uri: true,
                uri: Some(base),
                host: String::new(),
                port: 1433,
                user: String::new(),
                database: None,
                instance: None,
                ssl_mode: Some("on".to_string()),
                trust_server_certificate: true,
                ssl_root_cert_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );
        let connection =
            containers::retry_db_operation(Duration::from_secs(60), || -> Result<_, DbError> {
                let connection = driver.connect(&profile)?;
                connection.ping()?;
                Ok(connection)
            })?;

        let actual = query_session_database(&*connection)
            .expect("SELECT DB_NAME() must return the session database");
        assert_eq!(
            connection.active_database(),
            Some(actual.clone()),
            "a URI/default-login connect must report the actual session database, not None"
        );

        let snapshot = connection.schema()?;
        let dbflux_core::DataStructure::Relational(relational) = snapshot.structure else {
            panic!("MSSQL schema snapshot must be relational");
        };
        assert_eq!(
            relational.current_database,
            Some(actual),
            "the schema snapshot must carry the resolved session database"
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_direct_default_login_resolves_actual_session_database() -> Result<(), DbError> {
    use dbflux_core::secrecy::SecretString;

    containers::with_mssql_url(|uri| {
        // Direct host/port profile with database: None — the login default
        // applies, and the driver must resolve the actual session database.
        let base = uri
            .rsplit_once('/')
            .map(|(base, _)| base.to_string())
            .expect("fixture URI must contain a /<database> suffix");
        let port: u16 = base
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse().ok())
            .expect("fixture URI must carry a numeric port");

        let driver = MssqlDriver::new();
        let profile = ConnectionProfile::new(
            "ddl-mssql-direct-default-login",
            DbConfig::SqlServer {
                use_uri: false,
                uri: None,
                host: "127.0.0.1".to_string(),
                port,
                user: "sa".to_string(),
                database: None,
                instance: None,
                ssl_mode: Some("on".to_string()),
                trust_server_certificate: true,
                ssl_root_cert_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );
        let password = SecretString::from(dbflux_test_support::containers::MSSQL_TEST_PASSWORD);
        let connection =
            containers::retry_db_operation(Duration::from_secs(60), || -> Result<_, DbError> {
                let connection = driver.connect_with_password(&profile, Some(&password))?;
                connection.ping()?;
                Ok(connection)
            })?;

        let actual = query_session_database(&*connection)
            .expect("SELECT DB_NAME() must return the session database");
        assert_eq!(
            connection.active_database(),
            Some(actual),
            "a direct/default-login connect must report the actual session database, not None"
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn mssql_generate_code_create_table_requires_reference_creation_metadata() -> Result<(), DbError> {
    containers::with_mssql_url(|uri| {
        let (source, _) = connect_mssql(uri)?;
        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_plain]"))?;
        source.execute(&QueryRequest::new(
            "CREATE TABLE dbo.[gen_plain] ( \
                 [Id] INT NOT NULL PRIMARY KEY CLUSTERED, \
                 [Balance] DECIMAL(12,4) NOT NULL DEFAULT 0 \
             )",
        ))?;

        let details = source.table_details(TEST_DATABASE, Some("dbo"), "gen_plain")?;

        // The legacy `generate_code` seam cannot carry reference creation
        // metadata (identity seed/increment, primary-key order, blockers);
        // it must refuse with an actionable message instead of generating
        // lossy DDL or introspecting the target.
        let error = source
            .generate_code("create_table", &details)
            .expect_err("plain generate_code must refuse CREATE TABLE");
        let lowered = error.to_string().to_lowercase();
        assert!(
            lowered.contains("creation metadata"),
            "refusal must name the missing reference creation metadata, got: {error}"
        );
        assert!(
            lowered.contains("generate_code_with_creation_metadata"),
            "refusal must point at the metadata-aware seam, got: {error}"
        );

        // The metadata-aware seam generates faithfully for the same table.
        let metadata = source
            .table_creation_metadata(TEST_DATABASE, Some("dbo"), "gen_plain")?
            .expect("MSSQL must report creation metadata for its own tables");
        let ddl = source.generate_code_with_creation_metadata(
            "create_table",
            &details,
            Some(&metadata),
        )?;
        assert!(
            ddl.to_uppercase().contains("CREATE TABLE"),
            "metadata-aware generation must produce CREATE TABLE, got: {ddl}"
        );

        source.execute(&QueryRequest::new("DROP TABLE IF EXISTS dbo.[gen_plain]"))?;
        Ok(())
    })
}
