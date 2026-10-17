//! pgvector support: the vector facet of a table and nearest-neighbor SQL.

use dbflux_core::{
    CreateTableSpec, GeneratedMutation, GeneratedQuery, GeneratorError, MutationCategory,
    MutationRequest, MutationTemplateRequest, NearestNeighborRequest, ObjectFacet, QueryGenError,
    QueryGenerator, QueryLanguage, ReadTemplateRequest, SelectQuery, SqlMutationGenerator,
    TableInfo, Value, VectorDistance, VectorFacet, VisualMutationSpec, VisualQuerySpec,
};

use crate::driver::{POSTGRES_DIALECT, pg_quote_ident, pg_quote_string, value_to_pg_literal};

/// One vector facet per pgvector `vector` column of `table`.
pub(crate) fn vector_facets(table: &TableInfo) -> Vec<ObjectFacet> {
    table
        .columns
        .iter()
        .flatten()
        .filter_map(|column| {
            parse_vector_type(&column.type_name).map(|dimension| {
                ObjectFacet::Vector(VectorFacet {
                    column: column.name.clone(),
                    dimension,
                })
            })
        })
        .collect()
}

/// `Some(dimension)` when `type_name` is pgvector's `vector` or `vector(n)`.
fn parse_vector_type(type_name: &str) -> Option<Option<u32>> {
    let type_name = type_name.trim();
    let rest = type_name.strip_prefix("vector")?;

    if rest.is_empty() {
        return Some(None);
    }

    let dimension = rest.strip_prefix('(')?.strip_suffix(')')?.trim();
    dimension.parse::<u32>().ok().map(Some)
}

fn distance_operator(distance: VectorDistance) -> &'static str {
    match distance {
        VectorDistance::L2 => "<->",
        VectorDistance::Cosine => "<=>",
        VectorDistance::InnerProduct => "<#>",
    }
}

/// Text form of the anchor vector, as pgvector parses it.
fn anchor_text(anchor: &Value) -> Result<String, GeneratorError> {
    match anchor {
        Value::Text(text) => Ok(text.clone()),
        Value::Array(items) => {
            let parts = items
                .iter()
                .map(|item| match item {
                    Value::Int(number) => Ok(number.to_string()),
                    Value::Float(number) => Ok(number.to_string()),
                    Value::Decimal(number) => Ok(number.clone()),
                    _ => Err(GeneratorError::Unsupported(
                        "the anchor vector holds a non-numeric element".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", parts.join(",")))
        }
        Value::Null => Err(GeneratorError::Unsupported(
            "the anchor row has no vector".to_string(),
        )),
        _ => Err(GeneratorError::Unsupported(
            "the anchor value is not a vector".to_string(),
        )),
    }
}

pub(crate) fn nearest_neighbors_sql(
    request: &NearestNeighborRequest,
) -> Result<String, GeneratorError> {
    let anchor = anchor_text(&request.anchor)?;

    let table = match &request.table.schema {
        Some(schema) => format!(
            "{}.{}",
            pg_quote_ident(schema),
            pg_quote_ident(&request.table.name)
        ),
        None => pg_quote_ident(&request.table.name),
    };

    let column = pg_quote_ident(&request.column);
    // Ordering by the expression rather than its alias keeps the
    // query valid on a table that has its own `distance` column.
    let distance = format!(
        "{} {} {}::vector",
        column,
        distance_operator(request.distance),
        pg_quote_string(&anchor)
    );

    let mut sql = format!(
        "SELECT *, {distance} AS {alias} FROM {table} WHERE {column} IS NOT NULL",
        alias = dbflux_core::NEAREST_DISTANCE_COLUMN
    );

    // A key value that renders as NULL would make `NOT (key = NULL)` match
    // no row at all, so the anchor is kept rather than every row lost.
    let excludable = request
        .exclude_primary_key
        .iter()
        .all(|(_, value)| value_to_pg_literal(value) != "NULL");

    if excludable && !request.exclude_primary_key.is_empty() {
        let conditions = request
            .exclude_primary_key
            .iter()
            .map(|(column, value)| {
                format!(
                    "{} = {}",
                    pg_quote_ident(column),
                    value_to_pg_literal(value)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        sql.push_str(&format!(" AND NOT ({conditions})"));
    }

    sql.push_str(&format!(" ORDER BY {distance} LIMIT {}", request.limit));
    Ok(sql)
}

/// The shared SQL generator plus pgvector nearest-neighbor search.
pub(crate) struct PostgresQueryGenerator {
    inner: SqlMutationGenerator,
}

impl PostgresQueryGenerator {
    pub(crate) const fn new() -> Self {
        Self {
            inner: SqlMutationGenerator::new(&POSTGRES_DIALECT),
        }
    }
}

pub(crate) static POSTGRES_QUERY_GENERATOR: PostgresQueryGenerator = PostgresQueryGenerator::new();

impl QueryGenerator for PostgresQueryGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        self.inner.supported_categories()
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        self.inner.generate_mutation(mutation)
    }

    fn generate_template(&self, request: &MutationTemplateRequest<'_>) -> Option<GeneratedQuery> {
        self.inner.generate_template(request)
    }

    fn generate_read_template(&self, request: &ReadTemplateRequest<'_>) -> Option<GeneratedQuery> {
        self.inner.generate_read_template(request)
    }

    fn generate_select(
        &self,
        spec: &VisualQuerySpec,
    ) -> Result<Option<SelectQuery>, QueryGenError> {
        self.inner.generate_select(spec)
    }

    fn materialize_select_for_editor(&self, query: &SelectQuery) -> String {
        self.inner.materialize_select_for_editor(query)
    }

    fn generate_nearest_neighbors(
        &self,
        request: &NearestNeighborRequest,
    ) -> Result<GeneratedQuery, GeneratorError> {
        Ok(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: nearest_neighbors_sql(request)?,
        })
    }

    fn generate_update_from_spec(
        &self,
        spec: &VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        self.inner.generate_update_from_spec(spec)
    }

    fn generate_delete_from_spec(
        &self,
        spec: &VisualMutationSpec,
    ) -> Result<GeneratedMutation, GeneratorError> {
        self.inner.generate_delete_from_spec(spec)
    }

    fn generate_update_chunk_from_spec(
        &self,
        spec: &VisualMutationSpec,
        pk_cols: &[&str],
        pk_values: &[Vec<Value>],
    ) -> Result<GeneratedMutation, GeneratorError> {
        self.inner
            .generate_update_chunk_from_spec(spec, pk_cols, pk_values)
    }

    fn generate_delete_chunk_from_spec(
        &self,
        spec: &VisualMutationSpec,
        pk_cols: &[&str],
        pk_values: &[Vec<Value>],
    ) -> Result<GeneratedMutation, GeneratorError> {
        self.inner
            .generate_delete_chunk_from_spec(spec, pk_cols, pk_values)
    }

    fn generate_bulk_insert(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[String],
        column_types: &[Option<String>],
        rows: &[&[Value]],
    ) -> Result<Option<GeneratedQuery>, GeneratorError> {
        self.inner
            .generate_bulk_insert(schema, table, columns, column_types, rows)
    }

    fn generate_create_table(
        &self,
        spec: &CreateTableSpec,
    ) -> Result<Option<GeneratedQuery>, GeneratorError> {
        self.inner.generate_create_table(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::TableRef;

    fn request(distance: VectorDistance, exclude: Vec<(String, Value)>) -> NearestNeighborRequest {
        NearestNeighborRequest {
            table: TableRef::with_schema("public", "items"),
            database: Some("app".to_string()),
            column: "embedding".to_string(),
            anchor: Value::Text("[1,2,3]".to_string()),
            distance,
            limit: 10,
            exclude_primary_key: exclude,
        }
    }

    #[test]
    fn nearest_neighbors_use_the_operator_of_each_distance() {
        let expected = [
            (VectorDistance::L2, "<->"),
            (VectorDistance::Cosine, "<=>"),
            (VectorDistance::InnerProduct, "<#>"),
        ];

        for (distance, operator) in expected {
            let generated = POSTGRES_QUERY_GENERATOR
                .generate_nearest_neighbors(&request(distance, Vec::new()))
                .unwrap();

            assert_eq!(generated.language, QueryLanguage::Sql);
            assert_eq!(
                generated.text,
                format!(
                    "SELECT *, \"embedding\" {operator} '[1,2,3]'::vector AS nearest_distance \
                     FROM \"public\".\"items\" WHERE \"embedding\" IS NOT NULL \
                     ORDER BY \"embedding\" {operator} '[1,2,3]'::vector LIMIT 10"
                )
            );
        }
    }

    #[test]
    fn nearest_neighbors_exclude_the_anchor_row_by_primary_key() {
        let sql = nearest_neighbors_sql(&request(
            VectorDistance::L2,
            vec![
                ("id".to_string(), Value::Int(7)),
                ("region".to_string(), Value::Text("eu".to_string())),
            ],
        ))
        .unwrap();

        assert_eq!(
            sql,
            "SELECT *, \"embedding\" <-> '[1,2,3]'::vector AS nearest_distance \
             FROM \"public\".\"items\" WHERE \"embedding\" IS NOT NULL \
             AND NOT (\"id\" = 7 AND \"region\" = 'eu') \
             ORDER BY \"embedding\" <-> '[1,2,3]'::vector LIMIT 10"
        );
    }

    #[test]
    fn nearest_neighbors_quote_identifiers_and_the_anchor() {
        let request = NearestNeighborRequest {
            table: TableRef::new("odd\"table"),
            database: None,
            column: "emb\"col".to_string(),
            anchor: Value::Text("[1,2]'; DROP TABLE x; --".to_string()),
            distance: VectorDistance::Cosine,
            limit: 5,
            exclude_primary_key: vec![("k\"ey".to_string(), Value::Text("a'b".to_string()))],
        };

        assert_eq!(
            nearest_neighbors_sql(&request).unwrap(),
            "SELECT *, \"emb\"\"col\" <=> '[1,2]''; DROP TABLE x; --'::vector AS nearest_distance \
             FROM \"odd\"\"table\" WHERE \"emb\"\"col\" IS NOT NULL AND NOT (\"k\"\"ey\" = 'a''b') \
             ORDER BY \"emb\"\"col\" <=> '[1,2]''; DROP TABLE x; --'::vector LIMIT 5"
        );
    }

    #[test]
    fn nearest_neighbors_order_by_the_distance_expression_not_the_alias() {
        // A table may have its own `distance` column; `ORDER BY distance`
        // would then be ambiguous.
        let sql =
            nearest_neighbors_sql(&request(VectorDistance::InnerProduct, Vec::new())).unwrap();

        assert!(
            sql.ends_with(" ORDER BY \"embedding\" <#> '[1,2,3]'::vector LIMIT 10"),
            "{sql}"
        );
        assert!(!sql.contains("ORDER BY distance"), "{sql}");
    }

    #[test]
    fn a_primary_key_rendered_as_null_drops_the_exclusion() {
        for value in [Value::Null, Value::Unsupported("tid".to_string())] {
            let sql = nearest_neighbors_sql(&request(
                VectorDistance::L2,
                vec![
                    ("id".to_string(), Value::Int(7)),
                    ("tid".to_string(), value),
                ],
            ))
            .unwrap();

            assert_eq!(
                sql,
                "SELECT *, \"embedding\" <-> '[1,2,3]'::vector AS nearest_distance \
                 FROM \"public\".\"items\" WHERE \"embedding\" IS NOT NULL \
                 ORDER BY \"embedding\" <-> '[1,2,3]'::vector LIMIT 10"
            );
        }
    }

    #[test]
    fn a_null_anchor_is_rejected() {
        let mut request = request(VectorDistance::L2, Vec::new());
        request.anchor = Value::Null;

        assert!(matches!(
            nearest_neighbors_sql(&request),
            Err(GeneratorError::Unsupported(_))
        ));
    }
}
