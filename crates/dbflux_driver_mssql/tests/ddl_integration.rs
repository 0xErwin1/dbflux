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
