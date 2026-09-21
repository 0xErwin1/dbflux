#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    ColumnInfo, Connection, ConnectionProfile, DbConfig, DbDriver, DbError, IndexData, QueryRequest,
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

/// One `(name, columns, is_unique, is_primary)` row, as both the bulk and
/// per-table index paths report it.
type IndexEntry<'a> = (&'a str, Vec<String>, bool, bool);

/// One `(name, columns, referenced_schema, referenced_table,
/// referenced_columns, on_delete, on_update)` row, as both foreign-key paths
/// report it.
type ForeignKeyEntry<'a> = (
    &'a str,
    Vec<String>,
    Option<&'a str>,
    &'a str,
    Vec<String>,
    Option<&'a str>,
    Option<&'a str>,
);

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

#[test]
#[ignore = "requires Docker daemon"]
fn schema_indexes_and_foreign_keys_bulk_match_table_details_including_partitioned_parent()
-> Result<(), DbError> {
    containers::with_postgres_url(|uri| {
        let connection = connect_postgres(uri)?;

        connection.execute(&QueryRequest::new("CREATE SCHEMA other"))?;
        // Partitioned parent: relkind = 'p', PK index lives on the parent.
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.events (
                id integer,
                day date,
                PRIMARY KEY (id, day)
            ) PARTITION BY RANGE (day)",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.events_2024
                PARTITION OF other.events
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        ))?;
        // One unique and one plain index on the same table: exercises flags
        // and index ordering within one relation.
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.tagged (
                id integer PRIMARY KEY,
                tag text
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE UNIQUE INDEX tagged_tag_key ON other.tagged (tag)",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE INDEX tagged_id_tag ON other.tagged (id, tag)",
        ))?;
        // Two-column FK: the composite case is where joining
        // key_column_usage against constraint_column_usage can mis-pair
        // columns, so it must be asserted explicitly.
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.parent_a (
                a integer,
                b integer,
                PRIMARY KEY (a, b)
            )",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.parent_b (x integer PRIMARY KEY)",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE other.child (
                id integer PRIMARY KEY,
                a integer,
                b integer,
                x integer,
                FOREIGN KEY (a, b) REFERENCES other.parent_a (a, b),
                FOREIGN KEY (x) REFERENCES other.parent_b (x)
            )",
        ))?;

        // --- Indexes: bulk vs per-table ---------------------------------

        let bulk_indexes = connection.schema_indexes("postgres", Some("other"))?;
        let mut indexes_by_table: std::collections::BTreeMap<&str, Vec<IndexEntry>> =
            std::collections::BTreeMap::new();
        for row in &bulk_indexes {
            indexes_by_table
                .entry(row.table_name.as_str())
                .or_default()
                .push((
                    row.name.as_str(),
                    row.columns.clone(),
                    row.is_unique,
                    row.is_primary,
                ));
        }
        for entries in indexes_by_table.values_mut() {
            entries.sort_by(|a, b| a.0.cmp(b.0));
        }

        // The fix: the partitioned parent is relkind = 'p' but carries its
        // own entry in pg_index; the bulk result must report it.
        let events_indexes = indexes_by_table.get("events").unwrap_or_else(|| {
            panic!(
                "partitioned parent other.events must appear in the bulk index result; got {:?}",
                indexes_by_table.keys().collect::<Vec<_>>()
            )
        });
        assert_eq!(
            events_indexes,
            &vec![(
                "events_pkey",
                vec!["id".to_string(), "day".to_string()],
                true,
                true
            )],
            "partitioned parent must carry its own primary-key index in the bulk result"
        );

        for (table, bulk_indexes_for_table) in &indexes_by_table {
            let details = connection.table_details("postgres", Some("other"), table)?;
            let per_table_indexes = match details.indexes {
                Some(IndexData::Relational(indexes)) => indexes,
                other => {
                    panic!("table_details({table}) returned non-relational index data: {other:?}")
                }
            };
            let mut per_table: Vec<(&str, Vec<String>, bool, bool)> = per_table_indexes
                .iter()
                .map(|index| {
                    (
                        index.name.as_str(),
                        index.columns.clone(),
                        index.is_unique,
                        index.is_primary,
                    )
                })
                .collect();
            per_table.sort_by(|a, b| a.0.cmp(b.0));
            assert_eq!(
                bulk_indexes_for_table, &per_table,
                "index mismatch for {table}: bulk vs table_details"
            );
        }

        // The partition must also still be reported by both paths.
        assert!(
            indexes_by_table.contains_key("events_2024"),
            "partition other.events_2024 must appear in the bulk index result"
        );

        // --- Foreign keys: bulk vs per-table -----------------------------

        let bulk_foreign_keys = connection.schema_foreign_keys("postgres", Some("other"))?;
        let mut foreign_keys_by_table: std::collections::BTreeMap<&str, Vec<ForeignKeyEntry>> =
            std::collections::BTreeMap::new();
        for row in &bulk_foreign_keys {
            foreign_keys_by_table
                .entry(row.table_name.as_str())
                .or_default()
                .push((
                    row.name.as_str(),
                    row.columns.clone(),
                    row.referenced_schema.as_deref(),
                    row.referenced_table.as_str(),
                    row.referenced_columns.clone(),
                    row.on_delete.as_deref(),
                    row.on_update.as_deref(),
                ));
        }
        for entries in foreign_keys_by_table.values_mut() {
            entries.sort_by(|a, b| a.0.cmp(b.0));
        }

        for (table, bulk_foreign_keys_for_table) in &foreign_keys_by_table {
            let details = connection.table_details("postgres", Some("other"), table)?;
            let per_table_foreign_keys = details.foreign_keys.unwrap_or_default();
            let mut per_table: Vec<ForeignKeyEntry> = per_table_foreign_keys
                .iter()
                .map(|fk| {
                    (
                        fk.name.as_str(),
                        fk.columns.clone(),
                        fk.referenced_schema.as_deref(),
                        fk.referenced_table.as_str(),
                        fk.referenced_columns.clone(),
                        fk.on_delete.as_deref(),
                        fk.on_update.as_deref(),
                    )
                })
                .collect();
            per_table.sort_by(|a, b| a.0.cmp(b.0));
            assert_eq!(
                bulk_foreign_keys_for_table, &per_table,
                "foreign key mismatch for {table}: bulk vs table_details"
            );
        }

        // The composite FK must keep both column pairs correctly paired;
        // this is where key_column_usage joined against
        // constraint_column_usage can go wrong.
        let child_foreign_keys = foreign_keys_by_table.get("child").unwrap_or_else(|| {
            panic!(
                "bulk foreign key result is missing child; got {:?}",
                foreign_keys_by_table.keys().collect::<Vec<_>>()
            )
        });
        let composite = child_foreign_keys
            .iter()
            .find(|(_name, _, _, referenced_table, _, _, _)| *referenced_table == "parent_a")
            .unwrap_or_else(|| {
                panic!(
                    "bulk foreign key result is missing child -> parent_a; got {:?}",
                    child_foreign_keys
                )
            });
        assert_eq!(
            (composite.1.as_slice(), composite.4.as_slice()),
            (
                ["a".to_string(), "b".to_string()].as_slice(),
                ["a".to_string(), "b".to_string()].as_slice()
            ),
            "composite foreign key columns must be present and paired correctly"
        );

        Ok(())
    })
}
