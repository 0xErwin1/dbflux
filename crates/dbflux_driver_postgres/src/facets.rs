//! The paradigm facets of a PostgreSQL table: tabular always, documents when
//! it has a `json`/`jsonb` column, a time series when TimescaleDB made it a
//! hypertable, and one vector facet per pgvector column.

use dbflux_core::{ObjectFacet, TableInfo, TableStorageHint, TimeSeriesFacet};
use postgres::Client;

/// Label of the storage hint that marks a TimescaleDB hypertable; its one
/// column is the time dimension.
pub(crate) const HYPERTABLE_HINT_LABEL: &str = "Hypertable";

pub(crate) fn table_facets(table: &TableInfo) -> Vec<ObjectFacet> {
    let mut facets = vec![ObjectFacet::Tabular];

    let has_json_column = table
        .columns
        .iter()
        .flatten()
        .any(|column| matches!(column.type_name.trim(), "json" | "jsonb"));
    if has_json_column {
        facets.push(ObjectFacet::Document);
    }

    if let Some(time_column) = hypertable_time_column(table) {
        facets.push(ObjectFacet::TimeSeries(TimeSeriesFacet {
            time_column: Some(time_column),
        }));
    }

    facets.extend(crate::vector::vector_facets(table));
    facets
}

/// The time dimension of `table` when its details mark it a hypertable.
fn hypertable_time_column(table: &TableInfo) -> Option<String> {
    table
        .storage_hints
        .iter()
        .flatten()
        .find(|hint| hint.label == HYPERTABLE_HINT_LABEL)
        .and_then(|hint| hint.columns.first().cloned())
}

/// The storage hint of a TimescaleDB hypertable, or `None` for a plain table
/// and on a server without TimescaleDB.
pub(crate) fn hypertable_hint(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Option<TableStorageHint>, postgres::Error> {
    // The information view only exists once the extension is installed, and
    // naming a missing relation fails the whole statement.
    let installed: bool = client
        .query_one(
            "SELECT to_regclass('timescaledb_information.dimensions') IS NOT NULL",
            &[],
        )?
        .get(0);
    if !installed {
        return Ok(None);
    }

    let row = client.query_opt(
        r#"
        SELECT column_name::text
        FROM timescaledb_information.dimensions
        WHERE hypertable_schema = $1
          AND hypertable_name = $2
          AND dimension_type = 'Time'
        ORDER BY dimension_number
        LIMIT 1
        "#,
        &[&schema, &table],
    )?;

    Ok(row.map(|row| TableStorageHint {
        label: HYPERTABLE_HINT_LABEL.to_string(),
        columns: vec![row.get(0)],
        detail: Some("TimescaleDB time dimension".to_string()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{CollectionPresentation, ColumnInfo, VectorFacet};

    fn column(name: &str, type_name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        }
    }

    fn table(columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: "items".to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns),
            pseudo_columns: Box::default(),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    #[test]
    fn a_vector_column_adds_a_vector_facet() {
        let facets = table_facets(&table(vec![
            column("id", "integer"),
            column("embedding", "vector(3)"),
            column("unsized", "vector"),
        ]));

        assert_eq!(
            facets,
            vec![
                ObjectFacet::Tabular,
                ObjectFacet::Vector(VectorFacet {
                    column: "embedding".to_string(),
                    dimension: Some(3),
                }),
                ObjectFacet::Vector(VectorFacet {
                    column: "unsized".to_string(),
                    dimension: None,
                }),
            ]
        );
    }

    #[test]
    fn other_column_types_add_no_facet() {
        let facets = table_facets(&table(vec![
            column("id", "integer"),
            column("half", "halfvec(3)"),
            column("sparse", "sparsevec(3)"),
            column("vectors", "vector_store"),
            column("tags", "text[]"),
            column("payloads", "jsonb[]"),
        ]));

        assert_eq!(facets, vec![ObjectFacet::Tabular]);
    }

    #[test]
    fn a_json_or_jsonb_column_adds_the_document_facet_once() {
        for columns in [
            vec![column("payload", "jsonb")],
            vec![column("payload", "json")],
            vec![column("a", "jsonb"), column("b", "json")],
        ] {
            assert_eq!(
                table_facets(&table(columns)),
                vec![ObjectFacet::Tabular, ObjectFacet::Document]
            );
        }
    }

    #[test]
    fn a_hypertable_adds_a_time_series_facet_on_its_time_dimension() {
        let mut readings = table(vec![
            column("time", "timestamp with time zone"),
            column("temperature", "double precision"),
        ]);
        readings.storage_hints = Some(vec![TableStorageHint {
            label: HYPERTABLE_HINT_LABEL.to_string(),
            columns: vec!["time".to_string()],
            detail: None,
        }]);

        assert_eq!(
            table_facets(&readings),
            vec![
                ObjectFacet::Tabular,
                ObjectFacet::TimeSeries(TimeSeriesFacet {
                    time_column: Some("time".to_string()),
                }),
            ]
        );
    }
}
