#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    ColumnInfo, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect_postgres(uri: String) -> Result<Box<dyn Connection>, dbflux_core::DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "live-postgres-schema-columns",
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

fn assert_columns_equal(bulk: &ColumnInfo, per_table: &ColumnInfo, context: &str) {
    assert_eq!(bulk.name, per_table.name, "column name mismatch: {context}");
    assert_eq!(
        bulk.type_name, per_table.type_name,
        "column type mismatch: {context}"
    );
    assert_eq!(
        bulk.nullable, per_table.nullable,
        "column nullability mismatch: {context}"
    );
    assert_eq!(
        bulk.is_primary_key, per_table.is_primary_key,
        "column primary-key flag mismatch: {context}"
    );
    assert_eq!(
        bulk.default_value, per_table.default_value,
        "column default mismatch: {context}"
    );
    assert_eq!(
        bulk.enum_values, per_table.enum_values,
        "column enum values mismatch: {context}"
    );
}

#[test]
#[ignore = "requires Docker daemon"]
fn schema_columns_bulk_matches_table_details_and_resolves_out_of_search_path_enums()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let connection = connect_postgres(uri)?;

        // Schema deliberately NOT on search_path.
        connection.execute(&QueryRequest::new("CREATE SCHEMA other"))?;
        connection.execute(&QueryRequest::new(
            "CREATE TYPE other.mood AS ENUM ('sad', 'ok', 'happy')",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.owners (
                id integer PRIMARY KEY,
                name text,
                nickname text DEFAULT 'anonymous',
                mood other.mood
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE VIEW other.owner_names AS SELECT id, name FROM other.owners",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.pets (id integer PRIMARY KEY, owner_id integer)",
        ))?;
        // A `serial` primary key auto-creates the sequence other.feed_id_seq,
        // which the bulk result must not report as a relation.
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.feed (id serial PRIMARY KEY, note text)",
        ))?;

        let bulk = connection.schema_columns("postgres", Some("other"))?;

        // Group the bulk rows per relation so the checks below can compare
        // whole relation column lists against the per-table path.
        let mut by_table: std::collections::BTreeMap<&str, Vec<&ColumnInfo>> =
            std::collections::BTreeMap::new();
        for row in &bulk {
            by_table
                .entry(row.table_name.as_str())
                .or_default()
                .push(&row.column);
        }

        let expected_relations = ["feed", "owner_names", "owners", "pets"];
        for expected in expected_relations {
            assert!(
                by_table.contains_key(expected),
                "bulk result is missing relation {expected}; got {:?}",
                by_table.keys().collect::<Vec<_>>()
            );
        }
        // The index and sequence exclusions run before the length assertion:
        // a leaked relation also makes the length wrong, so ordering the
        // exclusions first is what makes the failure name the relation that
        // leaked instead of only reporting a wrong count.
        for index_name in ["owners_pkey", "pets_pkey", "feed_pkey"] {
            assert!(
                !by_table.contains_key(index_name),
                "index relation {index_name} must not appear in the bulk result"
            );
        }
        // A `serial` primary key creates the sequence other.feed_id_seq; the
        // bulk result reports untyped rows, so it must never leak through.
        assert!(
            !by_table.contains_key("feed_id_seq"),
            "sequence relation feed_id_seq must not appear in the bulk result; got {:?}",
            by_table.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            by_table.len(),
            expected_relations.len(),
            "bulk result must contain exactly the four relations; got {:?}",
            by_table.keys().collect::<Vec<_>>()
        );

        // Every relation's columns equal table_details field by field, in
        // order. Five of the six fields compared here (name, type_name,
        // nullable, default_value, is_primary_key) come from byte-identical
        // SQL expressions in both queries, so this loop proves grouping,
        // count and order but cannot catch a consistently wrong expression;
        // only the enum_values arm is a genuine cross-mechanism check.
        for (table, bulk_columns) in &by_table {
            let details = connection.table_details("postgres", Some("other"), table)?;
            let per_table_columns = details.columns.as_deref().unwrap_or_else(|| {
                panic!("table_details returned no columns for {table}");
            });
            assert_eq!(
                bulk_columns.len(),
                per_table_columns.len(),
                "column count mismatch for {table}"
            );
            for (bulk_column, per_table_column) in bulk_columns.iter().zip(per_table_columns) {
                assert_columns_equal(
                    bulk_column,
                    per_table_column,
                    &format!("{table}.{}", bulk_column.name),
                );
            }
        }

        // Pin the owners columns literally, independently of table_details:
        // the equivalence loop above compares fields produced by
        // byte-identical SQL in both queries, so it cannot distinguish a
        // wrong expression from a consistently wrong one.
        let owners_columns = by_table
            .get("owners")
            .unwrap_or_else(|| panic!("bulk result is missing owners"));
        let expected_owners_columns: [(&str, &str, bool, bool, Option<&str>); 4] = [
            ("id", "integer", false, true, None),
            ("name", "text", true, false, None),
            ("nickname", "text", true, false, Some("'anonymous'::text")),
            ("mood", "other.mood", true, false, None),
        ];
        assert_eq!(
            owners_columns.len(),
            expected_owners_columns.len(),
            "owners must have exactly four columns; got {:?}",
            owners_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
        );
        for (column, expected) in owners_columns.iter().zip(expected_owners_columns.iter()) {
            assert_eq!(
                column.name, expected.0,
                "owners column name mismatch: expected {} at this position",
                expected.0
            );
            assert_eq!(
                column.type_name, expected.1,
                "owners.{} type mismatch",
                column.name
            );
            assert_eq!(
                column.nullable, expected.2,
                "owners.{} nullability mismatch",
                column.name
            );
            assert_eq!(
                column.is_primary_key, expected.3,
                "owners.{} primary-key flag mismatch",
                column.name
            );
            assert_eq!(
                column.default_value.as_deref(),
                expected.4,
                "owners.{} default mismatch",
                column.name
            );
        }

        // The enum column outside search_path resolves its values in
        // enumsortorder. This is what proves the type-OID keying fix.
        let mood = by_table
            .get("owners")
            .and_then(|columns| columns.iter().find(|column| column.name == "mood"))
            .unwrap_or_else(|| panic!("bulk result is missing owners.mood"));
        assert_eq!(
            mood.enum_values,
            Some(vec![
                "sad".to_string(),
                "ok".to_string(),
                "happy".to_string()
            ]),
            "enum values for other.mood must resolve in enumsortorder despite the schema not being on search_path"
        );

        // format_type schema-qualifies the enum type because `other` is not on
        // search_path; `other.mood` is exactly the string the old name-keyed
        // enum lookup was compared against and missed.
        assert_eq!(
            mood.type_name, "other.mood",
            "format_type must schema-qualify enum types outside search_path"
        );

        Ok(())
    })
}
