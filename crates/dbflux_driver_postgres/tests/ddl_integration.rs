use dbflux_core::{
    ConnectionProfile, ConstraintKind, DbConfig, DbDriver, DbError, IndexData, QueryRequest,
    SslMode, Value,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::{containers, ddl_fixtures::PostgresFixtures};
use std::time::Duration;

fn connect_postgres(
    uri: String,
) -> Result<(Box<dyn dbflux_core::Connection>, PostgresDriver), DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "ddl-postgres",
        DbConfig::Postgres {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: "postgres".to_string(),
            ssl_mode: SslMode::Prefer,
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

fn cleanup_test_tables(conn: &dyn dbflux_core::Connection) {
    let tables = vec![
        "orders",
        "order_items",
        "users",
        "products",
        "accounts",
        "alter_test",
        "fk_parent",
        "fk_child",
        "truncate_test",
    ];

    for table in tables {
        let _ = conn.execute(&QueryRequest::new(format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            table
        )));
    }

    let views = vec!["active_users", "test_view"];
    for view in views {
        let _ = conn.execute(&QueryRequest::new(format!("DROP VIEW IF EXISTS {}", view)));
    }

    let indexes = vec![
        "idx_users_email",
        "idx_users_username_unique",
        "idx_orders_user_status",
    ];
    for index in indexes {
        let _ = conn.execute(&QueryRequest::new(format!(
            "DROP INDEX IF EXISTS {}",
            index
        )));
    }
}

// ---------------------------------------------------------------------------
// CREATE TABLE tests (5 tests)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_create_table_serial_pk() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &table.name)?;
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
fn postgres_ddl_create_table_composite_pk() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_composite_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &table.name)?;
        assert_eq!(table_details.name, table.name);

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
fn postgres_ddl_create_table_with_fk() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let parent_table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

        let child_table = PostgresFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&child_table.create_sql))?;

        let table_details =
            connection.table_details("postgres", Some("public"), &child_table.name)?;

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
fn postgres_ddl_create_table_with_check_constraint() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_with_check();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &table.name)?;
        assert_eq!(table_details.name, table.name);

        let constraints = table_details
            .constraints
            .as_ref()
            .expect("constraints should be loaded");
        assert!(!constraints.is_empty());

        let has_check = constraints
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::Check) && c.name.contains("positive_price"));
        assert!(has_check, "should have check constraint");

        let insert_result = connection.execute(&QueryRequest::new(
            "INSERT INTO products (name, price, stock) VALUES ('test', -10, 5)",
        ));
        assert!(insert_result.is_err(), "should violate check constraint");

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_create_table_with_unique_constraint() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_with_unique();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &table.name)?;

        let constraints = table_details
            .constraints
            .as_ref()
            .expect("constraints should be loaded");

        let has_unique = constraints
            .iter()
            .any(|c| matches!(c.kind, ConstraintKind::Unique));
        assert!(has_unique, "should have unique constraint");

        connection.execute(&QueryRequest::new(
            "INSERT INTO accounts (email, username) VALUES ('test@example.com', 'testuser')",
        ))?;

        let duplicate_result = connection.execute(&QueryRequest::new(
            "INSERT INTO accounts (email, username) VALUES ('test@example.com', 'testuser2')",
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
// CREATE INDEX tests (3 tests)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_create_index_single_column() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let index = PostgresFixtures::index_single_column();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &table.name)?;
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
fn postgres_ddl_create_index_unique() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let index = PostgresFixtures::index_unique();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &table.name)?;
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
            .expect("index should exist");
        assert!(found_index.is_unique, "index should be unique");

        connection.execute(&QueryRequest::new(
            "INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com')",
        ))?;

        let duplicate_result = connection.execute(&QueryRequest::new(
            "INSERT INTO users (username, email) VALUES ('alice', 'bob@example.com')",
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
fn postgres_ddl_create_index_composite() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let users_table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&users_table.create_sql))?;

        let orders_table = PostgresFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&orders_table.create_sql))?;

        let index = PostgresFixtures::index_composite();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let table_details = connection.table_details("postgres", Some("public"), &index.table)?;
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
            .expect("index should exist");
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
// CREATE VIEW test (1 test)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_create_view() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let view = PostgresFixtures::view_simple();
        connection.execute(&QueryRequest::new(&view.create_sql))?;

        let schema = connection.schema()?;
        let relational = schema.as_relational().expect("should be relational schema");

        let has_view = relational
            .schemas
            .iter()
            .flat_map(|s| s.views.iter())
            .any(|v| v.name == view.name);
        assert!(has_view, "view should appear in schema");

        let result = connection.execute(&QueryRequest::new("SELECT * FROM active_users"))?;
        assert!(!result.columns.is_empty());

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// ALTER TABLE tests (3 tests)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_alter_table_add_column() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let scenario = PostgresFixtures::alter_add_column();
        for sql in &scenario.setup_sql {
            connection.execute(&QueryRequest::new(sql))?;
        }

        let before = connection.table_details("postgres", Some("public"), "alter_test")?;
        let before_cols = before.columns.as_ref().expect("columns should exist");
        let before_count = before_cols.len();

        connection.execute(&QueryRequest::new(&scenario.test_sql))?;

        let after = connection.table_details("postgres", Some("public"), "alter_test")?;
        let after_cols = after.columns.as_ref().expect("columns should exist");
        let after_count = after_cols.len();

        assert_eq!(after_count, before_count + 1, "should have one more column");

        let has_age = after_cols.iter().any(|c| c.name == "age");
        assert!(has_age, "should have age column");

        for sql in &scenario.cleanup_sql {
            let _ = connection.execute(&QueryRequest::new(sql));
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_alter_table_drop_column() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let scenario = PostgresFixtures::alter_drop_column();
        for sql in &scenario.setup_sql {
            connection.execute(&QueryRequest::new(sql))?;
        }

        let before = connection.table_details("postgres", Some("public"), "alter_test")?;
        let before_cols = before.columns.as_ref().expect("columns should exist");
        let before_count = before_cols.len();
        assert!(before_cols.iter().any(|c| c.name == "age"));

        connection.execute(&QueryRequest::new(&scenario.test_sql))?;

        let after = connection.table_details("postgres", Some("public"), "alter_test")?;
        let after_cols = after.columns.as_ref().expect("columns should exist");
        let after_count = after_cols.len();

        assert_eq!(after_count, before_count - 1, "should have one less column");
        assert!(!after_cols.iter().any(|c| c.name == "age"));

        for sql in &scenario.cleanup_sql {
            let _ = connection.execute(&QueryRequest::new(sql));
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_alter_table_rename_column() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let scenario = PostgresFixtures::alter_rename_column();
        for sql in &scenario.setup_sql {
            connection.execute(&QueryRequest::new(sql))?;
        }

        let before = connection.table_details("postgres", Some("public"), "alter_test")?;
        let before_cols = before.columns.as_ref().expect("columns should exist");
        assert!(before_cols.iter().any(|c| c.name == "old_name"));

        connection.execute(&QueryRequest::new(&scenario.test_sql))?;

        let after = connection.table_details("postgres", Some("public"), "alter_test")?;
        let after_cols = after.columns.as_ref().expect("columns should exist");

        assert!(!after_cols.iter().any(|c| c.name == "old_name"));
        assert!(after_cols.iter().any(|c| c.name == "new_name"));

        for sql in &scenario.cleanup_sql {
            let _ = connection.execute(&QueryRequest::new(sql));
        }

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// DROP tests (3 tests)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_drop_table() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let before = connection.table_details("postgres", Some("public"), &table.name);
        assert!(before.is_ok(), "table should exist");

        connection.execute(&QueryRequest::new(format!("DROP TABLE {}", table.name)))?;

        let after = connection.table_details("postgres", Some("public"), &table.name);
        assert!(after.is_err(), "table should not exist");

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_drop_index() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let index = PostgresFixtures::index_single_column();
        connection.execute(&QueryRequest::new(&index.create_sql))?;

        let before = connection.table_details("postgres", Some("public"), &table.name)?;
        let before_indexes = before.indexes.as_ref().expect("indexes should exist");
        let before_list = match before_indexes {
            IndexData::Relational(list) => list,
            _ => panic!("expected relational index data"),
        };
        assert!(before_list.iter().any(|i| i.name == index.name));

        connection.execute(&QueryRequest::new(format!("DROP INDEX {}", index.name)))?;

        let after = connection.table_details("postgres", Some("public"), &table.name)?;
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
fn postgres_ddl_drop_view() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let view = PostgresFixtures::view_simple();
        connection.execute(&QueryRequest::new(&view.create_sql))?;

        let before_schema = connection.schema()?;
        let before_relational = before_schema
            .as_relational()
            .expect("should be relational schema");
        let has_view_before = before_relational
            .schemas
            .iter()
            .flat_map(|s| s.views.iter())
            .any(|v| v.name == view.name);
        assert!(has_view_before, "view should exist");

        connection.execute(&QueryRequest::new(format!("DROP VIEW {}", view.name)))?;

        let after_schema = connection.schema()?;
        let after_relational = after_schema
            .as_relational()
            .expect("should be relational schema");
        let has_view_after = after_relational
            .schemas
            .iter()
            .flat_map(|s| s.views.iter())
            .any(|v| v.name == view.name);
        assert!(!has_view_after, "view should not exist");

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// TRUNCATE test (1 test)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_truncate_table() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        connection.execute(&QueryRequest::new(
            "CREATE TABLE truncate_test (id SERIAL PRIMARY KEY, value TEXT)",
        ))?;

        for i in 1..=10 {
            connection.execute(&QueryRequest::new(format!(
                "INSERT INTO truncate_test (value) VALUES ('item_{}')",
                i
            )))?;
        }

        let before =
            connection.execute(&QueryRequest::new("SELECT COUNT(*) FROM truncate_test"))?;
        let count_before = match &before.rows[0][0] {
            Value::Int(n) => *n,
            _ => panic!("expected integer count"),
        };
        assert_eq!(count_before, 10);

        connection.execute(&QueryRequest::new("TRUNCATE TABLE truncate_test"))?;

        let after = connection.execute(&QueryRequest::new("SELECT COUNT(*) FROM truncate_test"))?;
        let count_after = match &after.rows[0][0] {
            Value::Int(n) => *n,
            _ => panic!("expected integer count"),
        };
        assert_eq!(count_after, 0);

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Error scenario tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_error_constraint_violation() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let table = PostgresFixtures::table_with_check();
        connection.execute(&QueryRequest::new(&table.create_sql))?;

        let result = connection.execute(&QueryRequest::new(
            "INSERT INTO products (name, price, stock) VALUES ('bad', -5, 10)",
        ));
        assert!(result.is_err());

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_error_fk_violation() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let parent_table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

        let child_table = PostgresFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&child_table.create_sql))?;

        let result = connection.execute(&QueryRequest::new(
            "INSERT INTO orders (user_id, total, status) VALUES (9999, 100.00, 'pending')",
        ));
        assert!(result.is_err());

        cleanup_test_tables(&*connection);
        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_ddl_error_drop_with_dependents() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let (connection, _) = connect_postgres(uri)?;
        cleanup_test_tables(&*connection);

        let parent_table = PostgresFixtures::table_serial_pk();
        connection.execute(&QueryRequest::new(&parent_table.create_sql))?;

        let child_table = PostgresFixtures::table_with_fk();
        connection.execute(&QueryRequest::new(&child_table.create_sql))?;

        let result = connection.execute(&QueryRequest::new("DROP TABLE users"));
        assert!(result.is_err(), "should fail to drop table with dependents");

        let cascade_result = connection.execute(&QueryRequest::new("DROP TABLE users CASCADE"));
        assert!(cascade_result.is_ok(), "should succeed with CASCADE option");

        cleanup_test_tables(&*connection);
        Ok(())
    })
}
