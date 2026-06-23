use dbflux_core::{
    Comparator, DocumentDelete, DocumentInsert, DocumentUpdate, FilterNode, GeneratedQuery,
    LiteralValue, MutationCategory, MutationRequest, Predicate, PredicateValue, QueryGenError,
    QueryGenerator, QueryLanguage, VisualQuerySpec,
};

fn json_text(value: &serde_json::Value) -> Option<String> {
    serde_json::to_string(value).ok()
}

fn generate_insert(insert: &DocumentInsert) -> Option<String> {
    if insert.documents.is_empty() {
        return None;
    }

    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "op".to_string(),
        serde_json::Value::String("put".to_string()),
    );
    envelope.insert(
        "table".to_string(),
        serde_json::Value::String(insert.collection.clone()),
    );

    if let Some(database) = insert.database.as_ref() {
        envelope.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    if insert.documents.len() == 1 {
        envelope.insert("item".to_string(), insert.documents.first()?.clone());
    } else {
        envelope.insert(
            "items".to_string(),
            serde_json::Value::Array(insert.documents.clone()),
        );
    }

    json_text(&serde_json::Value::Object(envelope))
}

fn generate_update(update: &DocumentUpdate) -> Option<String> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "op".to_string(),
        serde_json::Value::String("update".to_string()),
    );
    envelope.insert(
        "table".to_string(),
        serde_json::Value::String(update.collection.clone()),
    );
    envelope.insert("key".to_string(), update.filter.filter.clone());
    envelope.insert("update".to_string(), update.update.clone());
    envelope.insert("many".to_string(), serde_json::Value::Bool(update.many));
    envelope.insert("upsert".to_string(), serde_json::Value::Bool(update.upsert));

    if let Some(database) = update.database.as_ref() {
        envelope.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    json_text(&serde_json::Value::Object(envelope))
}

fn generate_delete(delete: &DocumentDelete) -> Option<String> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "op".to_string(),
        serde_json::Value::String("delete".to_string()),
    );
    envelope.insert(
        "table".to_string(),
        serde_json::Value::String(delete.collection.clone()),
    );
    envelope.insert("key".to_string(), delete.filter.filter.clone());
    envelope.insert("many".to_string(), serde_json::Value::Bool(delete.many));

    if let Some(database) = delete.database.as_ref() {
        envelope.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    json_text(&serde_json::Value::Object(envelope))
}

/// Quote a PartiQL identifier (table or attribute name) with double quotes,
/// escaping embedded double quotes by doubling them.
fn quote_partiql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Render a PartiQL scalar literal. Text and timestamp values are single-quoted
/// (single quotes doubled); numbers and booleans are emitted bare; null becomes
/// the `NULL` keyword.
fn partiql_literal(value: &LiteralValue) -> String {
    match value {
        LiteralValue::Text(text) | LiteralValue::Timestamp(text) => {
            format!("'{}'", text.replace('\'', "''"))
        }
        LiteralValue::Integer(value) => value.to_string(),
        LiteralValue::Float(value) => value.to_string(),
        LiteralValue::Bool(value) => value.to_string(),
        LiteralValue::Null => "NULL".to_string(),
    }
}

/// Render a single filter predicate as a PartiQL boolean expression.
fn partiql_predicate(predicate: &Predicate) -> Result<String, QueryGenError> {
    let column = quote_partiql_identifier(&predicate.column);

    let render_binary = |symbol: &str, value: &PredicateValue| match value {
        PredicateValue::Single(literal) => {
            Ok(format!("{column} {symbol} {}", partiql_literal(literal)))
        }
        _ => Err(QueryGenError::InvalidSpec(format!(
            "operator {symbol} requires a single value"
        ))),
    };

    match predicate.comparator {
        Comparator::Eq => render_binary("=", &predicate.value),
        Comparator::Neq => render_binary("<>", &predicate.value),
        Comparator::Gt => render_binary(">", &predicate.value),
        Comparator::Lt => render_binary("<", &predicate.value),
        Comparator::Gte => render_binary(">=", &predicate.value),
        Comparator::Lte => render_binary("<=", &predicate.value),
        Comparator::In => match &predicate.value {
            PredicateValue::List(values) if !values.is_empty() => {
                let rendered: Vec<String> = values.iter().map(partiql_literal).collect();
                Ok(format!("{column} IN [{}]", rendered.join(", ")))
            }
            _ => Err(QueryGenError::InvalidSpec(
                "IN requires a non-empty value list".to_string(),
            )),
        },
        Comparator::IsNull => Ok(format!("{column} IS NULL")),
        Comparator::IsNotNull => Ok(format!("{column} IS NOT NULL")),
        Comparator::Like | Comparator::ILike => Err(QueryGenError::InvalidSpec(
            "DynamoDB PartiQL does not support LIKE/ILIKE in the visual builder".to_string(),
        )),
    }
}

/// Render a filter tree as a PartiQL boolean expression. Groups are parenthesized
/// and joined by their boolean operator.
fn partiql_filter(node: &FilterNode) -> Result<Option<String>, QueryGenError> {
    match node {
        FilterNode::Predicate(predicate) => Ok(Some(partiql_predicate(predicate)?)),
        FilterNode::Group { op, children } => {
            let mut rendered = Vec::new();
            for child in children {
                if let Some(expression) = partiql_filter(child)? {
                    rendered.push(expression);
                }
            }

            if rendered.is_empty() {
                return Ok(None);
            }

            let joiner = match op {
                dbflux_core::BoolOp::And => " AND ",
                dbflux_core::BoolOp::Or => " OR ",
            };

            if rendered.len() == 1 {
                Ok(rendered.into_iter().next())
            } else {
                Ok(Some(format!("({})", rendered.join(joiner))))
            }
        }
    }
}

/// Build the PartiQL `SELECT` text for a visual read spec.
///
/// DynamoDB PartiQL has no `LIMIT` or `ORDER BY` keyword: row limiting travels
/// out-of-band as the `execute_statement` limit argument, and sort-key direction
/// as the query envelope's `scan_index_forward`. This generator therefore emits
/// only the `SELECT ... FROM ... [WHERE ...]` shape; `spec.limit`, `spec.offset`,
/// and `spec.sort` are intentionally not encoded into the text and are carried by
/// the execution path instead.
fn generate_partiql_read(spec: &VisualQuerySpec) -> Result<GeneratedQuery, QueryGenError> {
    if spec.source.table.trim().is_empty() {
        return Err(QueryGenError::InvalidSpec(
            "source table must not be empty".to_string(),
        ));
    }

    if !spec.joins.is_empty() {
        return Err(QueryGenError::InvalidSpec(
            "DynamoDB PartiQL reads do not support joins".to_string(),
        ));
    }

    if spec.is_grouped() {
        return Err(QueryGenError::InvalidSpec(
            "DynamoDB PartiQL reads do not support GROUP BY or aggregates".to_string(),
        ));
    }

    let table = quote_partiql_identifier(&spec.source.table);
    let mut text = format!("SELECT * FROM {table}");

    if let Some(filter) = spec.filter.as_ref()
        && let Some(where_clause) = partiql_filter(filter)?
    {
        text.push_str(" WHERE ");
        text.push_str(&where_clause);
    }

    Ok(GeneratedQuery {
        language: QueryLanguage::Custom("DynamoDB".to_string()),
        text,
    })
}

pub struct DynamoQueryGenerator;

impl QueryGenerator for DynamoQueryGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Document]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let text = match mutation {
            MutationRequest::DocumentInsert(insert) => generate_insert(insert)?,
            MutationRequest::DocumentUpdate(update) => generate_update(update)?,
            MutationRequest::DocumentDelete(delete) => generate_delete(delete)?,
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Custom("DynamoDB".to_string()),
            text,
        })
    }

    fn generate_read_from_spec(
        &self,
        spec: &VisualQuerySpec,
    ) -> Result<Option<GeneratedQuery>, QueryGenError> {
        generate_partiql_read(spec).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::DynamoQueryGenerator;
    use crate::query_parser::parse_command_envelope;
    use dbflux_core::{
        BoolOp, Comparator, DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate,
        FilterNode, LiteralValue, MutationRequest, Predicate, PredicateValue, Projection,
        QueryGenError, QueryGenerator, QueryLanguage, SortEntry, SourceTable, VisualAggregateSpec,
        VisualQuerySpec, VisualSortDirection,
    };
    use serde_json::json;

    fn read_spec(table: &str, filter: Option<FilterNode>) -> VisualQuerySpec {
        VisualQuerySpec {
            source: SourceTable {
                schema: None,
                table: table.to_string(),
                alias: table.to_string(),
            },
            projection: Projection::All,
            joins: Vec::new(),
            filter,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            having: None,
            sort: Vec::new(),
            limit: None,
            offset: 0,
        }
    }

    fn eq_predicate(column: &str, value: LiteralValue) -> Predicate {
        Predicate {
            source_alias: "t".to_string(),
            column: column.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(value),
            node_id: 0,
        }
    }

    #[test]
    fn generate_read_from_spec_emits_partiql_select_without_filter() {
        let generator = DynamoQueryGenerator;
        let spec = read_spec("users", None);

        let generated = generator
            .generate_read_from_spec(&spec)
            .expect("read generation should succeed")
            .expect("DynamoDB generator should emit a read");

        assert_eq!(generated.text, "SELECT * FROM \"users\"");
        assert_eq!(
            generated.language,
            QueryLanguage::Custom("DynamoDB".to_string())
        );
    }

    #[test]
    fn generate_read_from_spec_emits_where_for_single_predicate() {
        let generator = DynamoQueryGenerator;
        let filter = FilterNode::Group {
            op: BoolOp::And,
            children: vec![FilterNode::Predicate(eq_predicate(
                "pk",
                LiteralValue::Text("U#1".to_string()),
            ))],
        };
        let spec = read_spec("users", Some(filter));

        let generated = generator
            .generate_read_from_spec(&spec)
            .expect("read generation should succeed")
            .expect("DynamoDB generator should emit a read");

        assert_eq!(
            generated.text,
            "SELECT * FROM \"users\" WHERE \"pk\" = 'U#1'"
        );
    }

    #[test]
    fn generate_read_from_spec_joins_group_with_boolean_operator() {
        let generator = DynamoQueryGenerator;
        let filter = FilterNode::Group {
            op: BoolOp::And,
            children: vec![
                FilterNode::Predicate(eq_predicate("pk", LiteralValue::Text("U#1".to_string()))),
                FilterNode::Predicate(eq_predicate("score", LiteralValue::Integer(10))),
            ],
        };
        let spec = read_spec("users", Some(filter));

        let generated = generator
            .generate_read_from_spec(&spec)
            .expect("read generation should succeed")
            .expect("DynamoDB generator should emit a read");

        assert_eq!(
            generated.text,
            "SELECT * FROM \"users\" WHERE (\"pk\" = 'U#1' AND \"score\" = 10)"
        );
    }

    #[test]
    fn generate_read_from_spec_omits_limit_and_sort_from_text() {
        let generator = DynamoQueryGenerator;
        let mut spec = read_spec("users", None);
        spec.limit = Some(25);
        spec.sort = vec![SortEntry {
            source_alias: "t".to_string(),
            column: "sk".to_string(),
            direction: VisualSortDirection::Desc,
        }];

        let generated = generator
            .generate_read_from_spec(&spec)
            .expect("read generation should succeed")
            .expect("DynamoDB generator should emit a read");

        assert_eq!(generated.text, "SELECT * FROM \"users\"");
        assert!(!generated.text.to_ascii_uppercase().contains("LIMIT"));
        assert!(!generated.text.to_ascii_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn generate_read_from_spec_rejects_joins_and_grouping() {
        let generator = DynamoQueryGenerator;
        let mut spec = read_spec("users", None);
        spec.aggregates = vec![VisualAggregateSpec {
            function: dbflux_core::AggFn::CountStar,
            source_alias: None,
            column: None,
            alias: "count".to_string(),
        }];

        let error = generator
            .generate_read_from_spec(&spec)
            .expect_err("grouped spec must be rejected");
        assert!(matches!(error, QueryGenError::InvalidSpec(_)));
    }

    #[test]
    fn generated_insert_update_delete_envelopes_are_parseable() {
        let generator = DynamoQueryGenerator;

        let insert = MutationRequest::DocumentInsert(DocumentInsert::one(
            "users".to_string(),
            json!({"pk":"U#1","name":"alice"}),
        ));
        let insert_query = generator
            .generate_mutation(&insert)
            .expect("insert envelope should be generated");
        parse_command_envelope(&insert_query.text).expect("insert envelope should be parseable");

        let insert_many = MutationRequest::DocumentInsert(DocumentInsert::many(
            "users".to_string(),
            vec![json!({"pk":"U#2"}), json!({"pk":"U#3"})],
        ));
        let insert_many_query = generator
            .generate_mutation(&insert_many)
            .expect("insert-many envelope should be generated");
        parse_command_envelope(&insert_many_query.text)
            .expect("insert-many envelope should be parseable");

        let update = MutationRequest::DocumentUpdate(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(json!({"pk":"U#1"})),
            json!({"name":"bob"}),
        ));
        let update_query = generator
            .generate_mutation(&update)
            .expect("update envelope should be generated");
        parse_command_envelope(&update_query.text).expect("update envelope should be parseable");

        let update_upsert = MutationRequest::DocumentUpdate(
            DocumentUpdate::new(
                "users".to_string(),
                DocumentFilter::new(json!({"pk":"U#1"})),
                json!({"name":"bob"}),
            )
            .upsert(),
        );
        let update_upsert_query = generator
            .generate_mutation(&update_upsert)
            .expect("upsert update envelope should be generated");
        parse_command_envelope(&update_upsert_query.text)
            .expect("upsert update envelope should be parseable");

        let delete = MutationRequest::DocumentDelete(DocumentDelete::new(
            "users".to_string(),
            DocumentFilter::new(json!({"pk":"U#1"})),
        ));
        let delete_query = generator
            .generate_mutation(&delete)
            .expect("delete envelope should be generated");
        parse_command_envelope(&delete_query.text).expect("delete envelope should be parseable");
    }
}
