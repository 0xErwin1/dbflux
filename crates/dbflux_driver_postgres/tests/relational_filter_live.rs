//! Postgres live integration tests for the relational filter pipeline.
//!
//! These tests require a running Postgres container and are marked `#[ignore]`.
//! Run with: `cargo nextest run -p dbflux_driver_postgres --run-ignored all`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest, Value,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect_postgres(
    uri: String,
) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "relational-filter-live",
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

    Ok(connection)
}

fn setup_schema(connection: &dyn dbflux_core::Connection) -> Result<(), DbError> {
    connection.execute(&QueryRequest::new(
        "DROP TABLE IF EXISTS rf_posts CASCADE",
    ))?;
    connection.execute(&QueryRequest::new(
        "DROP TABLE IF EXISTS rf_users CASCADE",
    ))?;
    connection.execute(&QueryRequest::new(
        "DROP TABLE IF EXISTS rf_organizations CASCADE",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE rf_organizations (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE rf_users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            org_id INTEGER REFERENCES rf_organizations(id)
        )",
    ))?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE rf_posts (
            id SERIAL PRIMARY KEY,
            title TEXT,
            created_by_id INTEGER REFERENCES rf_users(id)
        )",
    ))?;

    connection.execute(&QueryRequest::new(
        "INSERT INTO rf_organizations VALUES (1, 'Acme'), (2, 'Other')",
    ))?;

    connection.execute(&QueryRequest::new(
        "INSERT INTO rf_users VALUES (1, 'alice@example.com', 1), (2, 'bob@example.com', 2)",
    ))?;

    connection.execute(&QueryRequest::new(
        "INSERT INTO rf_posts VALUES (10, 'Alice Post 1', 1), (11, 'Bob Post', 2), (12, 'Alice Post 2', 1)",
    ))?;

    Ok(())
}

// S-01: single-hop relational filter via live Postgres connection
#[test]
#[ignore = "requires Docker daemon"]
fn pg_relational_filter_single_hop() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let connection = connect_postgres(uri)?;
        setup_schema(connection.as_ref())?;

        let fks = connection.schema_foreign_keys("postgres", Some("public"))?;
        let post_fks: Vec<_> = fks
            .into_iter()
            .filter(|fk| fk.table_name == "rf_posts")
            .collect();

        assert!(!post_fks.is_empty(), "rf_posts should have FK to rf_users");

        let source = dbflux_core::SourceTable {
            schema: Some("public".to_string()),
            table: "rf_posts".to_string(),
            alias: "rf_posts".to_string(),
        };

        let dialect = connection.dialect();
        let lowering = dbflux_core::parse_and_resolve(
            "created_by.email = 'alice@example.com'",
            source,
            &post_fks,
            dialect,
        )
        .expect("should resolve single hop");

        assert_eq!(lowering.spec.joins.len(), 1);

        let select = dbflux_core::select_query_from_spec(&lowering.spec, dialect)
            .expect("build SQL");

        let mut request = QueryRequest::new(select.sql.clone());
        request.params = select.params.clone();

        let result = connection.execute(&request)?;

        assert_eq!(result.rows.len(), 2, "alice has 2 posts: {}", select.sql);

        Ok(())
    })
}

// S-02: multi-hop relational filter (two hops)
#[test]
#[ignore = "requires Docker daemon"]
fn pg_relational_filter_multi_hop() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let connection = connect_postgres(uri)?;
        setup_schema(connection.as_ref())?;

        let fks = connection.schema_foreign_keys("postgres", Some("public"))?;
        let post_fks: Vec<_> = fks
            .into_iter()
            .filter(|fk| fk.table_name == "rf_posts" || fk.table_name == "rf_users")
            .collect();

        let source = dbflux_core::SourceTable {
            schema: Some("public".to_string()),
            table: "rf_posts".to_string(),
            alias: "rf_posts".to_string(),
        };

        let dialect = connection.dialect();
        let lowering = dbflux_core::parse_and_resolve(
            "created_by.organization.name = 'Acme'",
            source,
            &post_fks,
            dialect,
        )
        .expect("should resolve two hops");

        assert_eq!(lowering.spec.joins.len(), 2);

        let select = dbflux_core::select_query_from_spec(&lowering.spec, dialect)
            .expect("build SQL");

        let mut request = QueryRequest::new(select.sql.clone());
        request.params = select.params.clone();

        let result = connection.execute(&request)?;

        assert_eq!(result.rows.len(), 2, "alice and only alice are in Acme, 2 posts: {}", select.sql);

        Ok(())
    })
}

// S-13: count parity — count query and data query return consistent results
#[test]
#[ignore = "requires Docker daemon"]
fn pg_count_parity() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let connection = connect_postgres(uri)?;
        setup_schema(connection.as_ref())?;

        let fks = connection.schema_foreign_keys("postgres", Some("public"))?;
        let post_fks: Vec<_> = fks
            .into_iter()
            .filter(|fk| fk.table_name == "rf_posts")
            .collect();

        let source = dbflux_core::SourceTable {
            schema: Some("public".to_string()),
            table: "rf_posts".to_string(),
            alias: "rf_posts".to_string(),
        };

        let dialect = connection.dialect();
        let lowering = dbflux_core::parse_and_resolve(
            "created_by.email = 'alice@example.com'",
            source,
            &post_fks,
            dialect,
        )
        .expect("should resolve");

        let select = dbflux_core::select_query_from_spec(&lowering.spec, dialect)
            .expect("build data SQL");
        let count = dbflux_core::count_query_from_spec(&lowering.spec, dialect);

        let mut data_request = QueryRequest::new(select.sql.clone());
        data_request.params = select.params.clone();

        let mut count_request = QueryRequest::new(count.sql.clone());
        count_request.params = count.params.clone();

        let data_result = connection.execute(&data_request)?;
        let count_result = connection.execute(&count_request)?;

        let data_count = data_result.rows.len();
        let count_value = match count_result.rows.first().and_then(|r| r.first()) {
            Some(Value::Int(n)) => *n as usize,
            Some(Value::Text(s)) => s.parse::<usize>().expect("count as text"),
            other => panic!("unexpected count value: {:?}", other),
        };

        assert_eq!(
            data_count, count_value,
            "data query and count query must agree: data={} count={}",
            data_count, count_value
        );
        assert_eq!(data_count, 2);

        Ok(())
    })
}

// ILIKE is Postgres-specific — verify it works with the PG dialect
#[test]
#[ignore = "requires Docker daemon"]
fn pg_relational_filter_ilike() -> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let connection = connect_postgres(uri)?;
        setup_schema(connection.as_ref())?;

        let fks = connection.schema_foreign_keys("postgres", Some("public"))?;
        let post_fks: Vec<_> = fks
            .into_iter()
            .filter(|fk| fk.table_name == "rf_posts")
            .collect();

        let source = dbflux_core::SourceTable {
            schema: Some("public".to_string()),
            table: "rf_posts".to_string(),
            alias: "rf_posts".to_string(),
        };

        let dialect = connection.dialect();
        let lowering = dbflux_core::parse_and_resolve(
            "created_by.email ILIKE '%ALICE%'",
            source,
            &post_fks,
            dialect,
        )
        .expect("should resolve ILIKE");

        let select = dbflux_core::select_query_from_spec(&lowering.spec, dialect)
            .expect("build SQL");

        let mut request = QueryRequest::new(select.sql.clone());
        request.params = select.params.clone();

        let result = connection.execute(&request)?;

        assert_eq!(result.rows.len(), 2, "ILIKE must match alice case-insensitively: {}", select.sql);

        Ok(())
    })
}
