use dbflux_core::{
    Comparator, FilterNode, GeneratedQuery, LiteralValue, MutationCategory, MutationRequest,
    Predicate, PredicateValue, Projection, QueryGenError, QueryGenerator, QueryLanguage, SortEntry,
    VisualQuerySpec, VisualSortDirection,
};
use serde_json::{Value, json};

pub struct MongoShellGenerator;

impl QueryGenerator for MongoShellGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Document]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let text = match mutation {
            MutationRequest::DocumentInsert(insert) => generate_insert(insert),
            MutationRequest::DocumentUpdate(update) => generate_update(update),
            MutationRequest::DocumentDelete(delete) => generate_delete(delete),
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::MongoQuery,
            text,
        })
    }

    fn document_patch_query(
        &self,
        request: &dbflux_core::DocumentPatchRequest,
    ) -> Option<GeneratedQuery> {
        if request.patch.is_empty() {
            return None;
        }

        let mut operators = Vec::new();

        if !request.patch.set.is_empty() {
            let entries: Vec<String> = request
                .patch
                .set
                .iter()
                .map(|(path, value)| {
                    format!(
                        "{}: {}",
                        shell_key(&dbflux_core::field_path_to_dotted(path)),
                        shell_literal(value)
                    )
                })
                .collect();
            operators.push(format!("$set: {{ {} }}", entries.join(", ")));
        }

        if !request.patch.unset.is_empty() {
            let entries: Vec<String> = request
                .patch
                .unset
                .iter()
                .map(|path| {
                    format!(
                        "{}: \"\"",
                        shell_key(&dbflux_core::field_path_to_dotted(path))
                    )
                })
                .collect();
            operators.push(format!("$unset: {{ {} }}", entries.join(", ")));
        }

        Some(GeneratedQuery {
            language: QueryLanguage::MongoQuery,
            text: format!(
                "{}.updateOne({}, {{ {} }})",
                collection_accessor(&request.collection.name),
                identity_literal(&request.identity),
                operators.join(", ")
            ),
        })
    }

    fn document_replace_query(
        &self,
        request: &dbflux_core::DocumentReplaceRequest,
    ) -> Option<GeneratedQuery> {
        let body = match &request.document {
            dbflux_core::Value::Document(fields) => {
                let entries: Vec<String> = fields
                    .iter()
                    .filter(|(key, _)| !request.identity.iter().any(|(field, _)| field == *key))
                    .map(|(key, value)| format!("{}: {}", shell_key(key), shell_literal(value)))
                    .collect();
                format!("{{ {} }}", entries.join(", "))
            }
            other => shell_literal(other),
        };

        Some(GeneratedQuery {
            language: QueryLanguage::MongoQuery,
            text: format!(
                "{}.replaceOne({}, {})",
                collection_accessor(&request.collection.name),
                identity_literal(&request.identity),
                body
            ),
        })
    }

    fn aggregate_query(
        &self,
        request: &dbflux_core::CollectionAggregateRequest,
    ) -> Option<GeneratedQuery> {
        let pipeline =
            serde_json::to_string_pretty(&Value::Array(request.pipeline.clone())).ok()?;

        Some(GeneratedQuery {
            language: QueryLanguage::MongoQuery,
            text: format!(
                "{}.aggregate({})",
                collection_accessor(&request.collection.name),
                pipeline
            ),
        })
    }

    fn generate_read_from_spec(
        &self,
        spec: &VisualQuerySpec,
    ) -> Result<Option<GeneratedQuery>, QueryGenError> {
        let text = generate_find(spec)?;

        Ok(Some(GeneratedQuery {
            language: QueryLanguage::MongoQuery,
            text,
        }))
    }
}

/// Renders a `VisualQuerySpec` into a MongoDB shell `find(...)` read.
///
/// The shape is `db.<collection>.find(<filter>, <projection?>).sort(<doc>)
/// .limit(N).skip(M)`, where the `.sort`/`.limit`/`.skip` segments are emitted
/// only when the spec carries the corresponding data. SQL-only constructs
/// (joins, GROUP BY, HAVING, aggregates) have no MongoDB `find` equivalent and
/// are rejected as `InvalidSpec` rather than silently dropped.
fn generate_find(spec: &VisualQuerySpec) -> Result<String, QueryGenError> {
    if !spec.joins.is_empty() {
        return Err(QueryGenError::InvalidSpec(
            "MongoDB find() does not support joins".to_string(),
        ));
    }
    if !spec.group_by.is_empty() || !spec.aggregates.is_empty() || spec.having.is_some() {
        return Err(QueryGenError::InvalidSpec(
            "MongoDB find() does not support GROUP BY / HAVING / aggregates".to_string(),
        ));
    }
    if spec.source.table.trim().is_empty() {
        return Err(QueryGenError::InvalidSpec(
            "MongoDB find() requires a collection name".to_string(),
        ));
    }

    let collection = collection_accessor(&spec.source.table);
    let filter = render_filter(spec.filter.as_ref());
    let projection = render_projection(&spec.projection);

    let mut text = match projection {
        Some(proj) => format!("{collection}.find({filter}, {proj})"),
        None => format!("{collection}.find({filter})"),
    };

    if let Some(sort_doc) = render_sort(&spec.sort) {
        text.push_str(&format!(".sort({sort_doc})"));
    }

    if let Some(limit) = spec.limit {
        text.push_str(&format!(".limit({limit})"));
    }

    if spec.offset > 0 {
        text.push_str(&format!(".skip({})", spec.offset));
    }

    Ok(text)
}

/// Render the collection accessor for the MongoDB shell. A simple identifier
/// uses dot access (`db.<name>`); any other name (containing `.`, quotes,
/// whitespace, parentheses, etc.) is JSON-escaped and rendered via bracket
/// access (`db["<escaped>"]`) so the generated text stays valid.
fn collection_accessor(name: &str) -> String {
    let is_simple_identifier = !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(index, character)| match character {
                'A'..='Z' | 'a'..='z' | '_' => true,
                '0'..='9' => index > 0,
                _ => false,
            });

    if is_simple_identifier {
        format!("db.{name}")
    } else {
        format!("db[{}]", Value::String(name.to_string()))
    }
}

/// An order-preserving MongoDB document (object).
///
/// `serde_json::Map` is backed by a `BTreeMap` in this build (no `preserve_order`
/// feature), so it reorders keys alphabetically — which would scramble the
/// authored order of a generated query (e.g. `$options` ahead of `$regex`).
/// The generated text is read by humans, so the document is rendered from an
/// ordered list of entries instead, while leaf values keep `serde_json`'s
/// correct escaping and number formatting.
enum MongoValue {
    Document(Vec<(String, MongoValue)>),
    Array(Vec<MongoValue>),
    Leaf(Value),
}

impl MongoValue {
    fn render(&self) -> String {
        match self {
            MongoValue::Leaf(value) => value.to_string(),
            MongoValue::Array(items) => {
                let rendered: Vec<String> = items.iter().map(MongoValue::render).collect();
                format!("[{}]", rendered.join(","))
            }
            MongoValue::Document(entries) => {
                let rendered: Vec<String> = entries
                    .iter()
                    .map(|(key, value)| {
                        format!("{}:{}", Value::String(key.clone()), value.render())
                    })
                    .collect();
                format!("{{{}}}", rendered.join(","))
            }
        }
    }
}

/// Renders the filter tree as a compact MongoDB query document.
///
/// `None` (or a tree that produces no predicates) renders as the empty document
/// `{}`, matching the shell convention for "match everything".
fn render_filter(filter: Option<&FilterNode>) -> String {
    filter
        .and_then(filter_node_to_value)
        .unwrap_or_else(|| MongoValue::Document(Vec::new()))
        .render()
}

fn filter_node_to_value(node: &FilterNode) -> Option<MongoValue> {
    use dbflux_core::BoolOp;

    match node {
        FilterNode::Predicate(pred) => predicate_to_value(pred),
        FilterNode::Group { op, children } => {
            let parts: Vec<MongoValue> = children.iter().filter_map(filter_node_to_value).collect();

            match parts.len() {
                0 => None,
                1 => parts.into_iter().next(),
                _ => {
                    let key = match op {
                        BoolOp::And => "$and",
                        BoolOp::Or => "$or",
                    };
                    Some(MongoValue::Document(vec![(
                        key.to_string(),
                        MongoValue::Array(parts),
                    )]))
                }
            }
        }
    }
}

/// Maps a single predicate to a `{ field: <condition> }` document.
///
/// Predicates whose column is empty (still being authored in the UI) are
/// skipped, mirroring the SQL builder's tolerance for partially-edited rows.
fn predicate_to_value(pred: &Predicate) -> Option<MongoValue> {
    let field = pred.column.trim();
    if field.is_empty() {
        return None;
    }

    let condition = match pred.comparator {
        Comparator::Eq => MongoValue::Leaf(predicate_scalar(&pred.value)),
        Comparator::Neq => op_document("$ne", predicate_scalar(&pred.value)),
        Comparator::Gt => op_document("$gt", predicate_scalar(&pred.value)),
        Comparator::Lt => op_document("$lt", predicate_scalar(&pred.value)),
        Comparator::Gte => op_document("$gte", predicate_scalar(&pred.value)),
        Comparator::Lte => op_document("$lte", predicate_scalar(&pred.value)),
        Comparator::In => op_document("$in", predicate_list(&pred.value)),
        Comparator::IsNull => op_document("$eq", Value::Null),
        Comparator::IsNotNull => op_document("$ne", Value::Null),
        Comparator::Like => op_document("$regex", regex_text(&pred.value)),
        Comparator::ILike => MongoValue::Document(vec![
            (
                "$regex".to_string(),
                MongoValue::Leaf(regex_text(&pred.value)),
            ),
            (
                "$options".to_string(),
                MongoValue::Leaf(Value::String("i".to_string())),
            ),
        ]),
    };

    Some(MongoValue::Document(vec![(field.to_string(), condition)]))
}

fn op_document(operator: &str, value: Value) -> MongoValue {
    MongoValue::Document(vec![(operator.to_string(), MongoValue::Leaf(value))])
}

fn predicate_scalar(value: &PredicateValue) -> Value {
    match value {
        PredicateValue::Single(literal) => literal_to_value(literal),
        PredicateValue::None => Value::Null,
        PredicateValue::List(items) => Value::Array(items.iter().map(literal_to_value).collect()),
    }
}

fn predicate_list(value: &PredicateValue) -> Value {
    match value {
        PredicateValue::List(items) => Value::Array(items.iter().map(literal_to_value).collect()),
        PredicateValue::Single(literal) => Value::Array(vec![literal_to_value(literal)]),
        PredicateValue::None => Value::Array(Vec::new()),
    }
}

fn regex_text(value: &PredicateValue) -> Value {
    match value {
        PredicateValue::Single(LiteralValue::Text(text)) => Value::String(text.clone()),
        other => predicate_scalar(other),
    }
}

fn literal_to_value(literal: &LiteralValue) -> Value {
    match literal {
        LiteralValue::Text(text) => Value::String(text.clone()),
        LiteralValue::Integer(n) => json!(n),
        LiteralValue::Float(f) => json!(f),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Timestamp(text) => Value::String(text.clone()),
        LiteralValue::Null => Value::Null,
    }
}

/// Renders the projection document, or `None` for `Projection::All` (no second
/// `find` argument). Explicit columns map to `{ "col": 1, ... }`; an explicit
/// projection with no usable columns also collapses to `None`.
fn render_projection(projection: &Projection) -> Option<String> {
    match projection {
        Projection::All => None,
        Projection::Explicit(columns) => {
            let mut entries: Vec<(String, MongoValue)> = Vec::new();
            for column in columns {
                let key = column
                    .alias
                    .clone()
                    .unwrap_or_else(|| column.column.clone());
                if !key.trim().is_empty() {
                    entries.push((key, MongoValue::Leaf(json!(1))));
                }
            }

            if entries.is_empty() {
                None
            } else {
                Some(MongoValue::Document(entries).render())
            }
        }
    }
}

/// Renders the sort document `{ "field": 1|-1, ... }`, where ascending is `1`
/// and descending is `-1`. Returns `None` when there is nothing to sort by.
fn render_sort(sort: &[SortEntry]) -> Option<String> {
    if sort.is_empty() {
        return None;
    }

    let mut entries: Vec<(String, MongoValue)> = Vec::new();
    for entry in sort {
        if entry.column.trim().is_empty() {
            continue;
        }
        let direction = match entry.direction {
            VisualSortDirection::Asc => 1,
            VisualSortDirection::Desc => -1,
        };
        entries.push((entry.column.clone(), MongoValue::Leaf(json!(direction))));
    }

    if entries.is_empty() {
        None
    } else {
        Some(MongoValue::Document(entries).render())
    }
}

/// A field name as mongosh accepts it: bare when it is a plain identifier,
/// quoted otherwise (dotted paths, spaces).
fn shell_key(key: &str) -> String {
    let is_identifier = !key.is_empty()
        && key
            .chars()
            .enumerate()
            .all(|(index, character)| match character {
                'A'..='Z' | 'a'..='z' | '_' | '$' => true,
                '0'..='9' => index > 0,
                _ => false,
            });

    if is_identifier {
        key.to_string()
    } else {
        Value::String(key.to_string()).to_string()
    }
}

/// A typed value as mongosh source, with the constructors that keep its BSON
/// type (`ObjectId(...)`, `Decimal128(...)`, `ISODate(...)`).
fn shell_literal(value: &dbflux_core::Value) -> String {
    use dbflux_core::Value as Typed;

    match value {
        Typed::Null => "null".to_string(),
        Typed::Bool(boolean) => boolean.to_string(),
        Typed::Int(integer) => integer.to_string(),
        Typed::Float(float) => {
            if float.is_finite() && float.fract() == 0.0 {
                format!("{float:.1}")
            } else {
                float.to_string()
            }
        }
        Typed::Text(text) => Value::String(text.clone()).to_string(),
        Typed::Decimal(decimal) => format!("Decimal128({})", Value::String(decimal.clone())),
        Typed::DateTime(date_time) => format!(
            "ISODate({})",
            Value::String(date_time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        ),
        Typed::Date(date) => format!("ISODate({})", Value::String(format!("{date}T00:00:00Z"))),
        Typed::Time(time) => Value::String(time.to_string()).to_string(),
        Typed::ObjectId(object_id) => format!("ObjectId({})", Value::String(object_id.clone())),
        Typed::Bytes(bytes) => format!("BinData(0, <{} bytes>)", bytes.len()),
        Typed::Json(text) => text.clone(),
        Typed::Unsupported(type_name) => format!("<{type_name}>"),
        Typed::Array(items) => {
            let rendered: Vec<String> = items.iter().map(shell_literal).collect();
            format!("[{}]", rendered.join(", "))
        }
        Typed::Document(fields) => {
            if fields.is_empty() {
                return "{}".to_string();
            }
            let rendered: Vec<String> = fields
                .iter()
                .map(|(key, field)| format!("{}: {}", shell_key(key), shell_literal(field)))
                .collect();
            format!("{{ {} }}", rendered.join(", "))
        }
    }
}

fn identity_literal(identity: &dbflux_core::DocumentIdentity) -> String {
    let entries: Vec<String> = identity
        .iter()
        .map(|(field, value)| format!("{}: {}", shell_key(field), shell_literal(value)))
        .collect();
    format!("{{ {} }}", entries.join(", "))
}

// The `documents.len() == 1` guard ensures `[0]` is always in bounds.
#[allow(clippy::indexing_slicing)]
fn generate_insert(insert: &dbflux_core::DocumentInsert) -> String {
    let collection = &insert.collection;

    if insert.documents.len() == 1 {
        let doc = serde_json::to_string_pretty(&insert.documents[0]).unwrap_or_default();
        format!("db.{collection}.insertOne({doc})")
    } else {
        let docs = serde_json::to_string_pretty(&insert.documents).unwrap_or_default();
        format!("db.{collection}.insertMany({docs})")
    }
}

fn generate_update(update: &dbflux_core::DocumentUpdate) -> String {
    let collection = &update.collection;
    let filter = serde_json::to_string_pretty(&update.filter.filter).unwrap_or_default();
    let update_doc = serde_json::to_string_pretty(&update.update).unwrap_or_default();

    let method = if update.many {
        "updateMany"
    } else {
        "updateOne"
    };

    if update.upsert {
        format!("db.{collection}.{method}({filter}, {update_doc}, {{ upsert: true }})")
    } else {
        format!("db.{collection}.{method}({filter}, {update_doc})")
    }
}

fn generate_delete(delete: &dbflux_core::DocumentDelete) -> String {
    let collection = &delete.collection;
    let filter = serde_json::to_string_pretty(&delete.filter.filter).unwrap_or_default();

    let method = if delete.many {
        "deleteMany"
    } else {
        "deleteOne"
    };

    format!("db.{collection}.{method}({filter})")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate};

    fn patch_request(patch: dbflux_core::DocumentPatch) -> dbflux_core::DocumentPatchRequest {
        dbflux_core::DocumentPatchRequest {
            collection: dbflux_core::CollectionRef::new("catalog", "products"),
            identity: vec![(
                "_id".to_string(),
                dbflux_core::Value::ObjectId("6ab43361d1f8e4b0c2a1e4b0".into()),
            )],
            patch,
        }
    }

    #[test]
    fn patch_query_shows_the_exact_set_and_unset() {
        let request = patch_request(dbflux_core::DocumentPatch {
            set: vec![(
                dbflux_core::parse_field_path("price.amount"),
                dbflux_core::Value::Decimal("119.00".into()),
            )],
            unset: vec![dbflux_core::parse_field_path("legacy")],
        });

        let query = MongoShellGenerator
            .document_patch_query(&request)
            .expect("a patch renders");

        assert_eq!(
            query.text,
            "db.products.updateOne({ _id: ObjectId(\"6ab43361d1f8e4b0c2a1e4b0\") }, \
             { $set: { \"price.amount\": Decimal128(\"119.00\") }, $unset: { legacy: \"\" } })"
        );
    }

    #[test]
    fn aggregate_query_renders_the_pipeline_on_the_collection() {
        use dbflux_core::LanguageService;

        let request = dbflux_core::CollectionAggregateRequest::new(
            dbflux_core::CollectionRef::new("shop", "order-lines"),
            vec![
                json!({ "$match": { "paid": true } }),
                json!({ "$out": "archive" }),
            ],
            100,
        );

        let query = MongoShellGenerator
            .aggregate_query(&request)
            .expect("a pipeline renders");

        assert!(
            query.text.starts_with("db[\"order-lines\"].aggregate([") && query.text.ends_with("])"),
            "unexpected text: {}",
            query.text
        );
        assert!(dbflux_core::aggregate_writes_output(&query.text));
        assert_eq!(
            crate::language_service::MongoLanguageService.detect_dangerous(&query.text),
            Some(dbflux_core::DangerousQueryKind::MongoAggregateWrite)
        );
    }

    #[test]
    fn empty_patch_has_no_query() {
        let request = patch_request(dbflux_core::DocumentPatch::default());

        assert!(MongoShellGenerator.document_patch_query(&request).is_none());
    }

    #[test]
    fn replace_query_leaves_the_identity_out_of_the_body() {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert(
            "_id".to_string(),
            dbflux_core::Value::ObjectId("6ab43361d1f8e4b0c2a1e4b0".into()),
        );
        fields.insert("sku".to_string(), dbflux_core::Value::Text("CAT-1".into()));

        let request = dbflux_core::DocumentReplaceRequest {
            collection: dbflux_core::CollectionRef::new("catalog", "products"),
            identity: vec![(
                "_id".to_string(),
                dbflux_core::Value::ObjectId("6ab43361d1f8e4b0c2a1e4b0".into()),
            )],
            document: dbflux_core::Value::Document(fields),
        };

        let query = MongoShellGenerator
            .document_replace_query(&request)
            .expect("a replacement renders");

        assert_eq!(
            query.text,
            "db.products.replaceOne({ _id: ObjectId(\"6ab43361d1f8e4b0c2a1e4b0\") }, { sku: \"CAT-1\" })"
        );
    }

    #[test]
    fn insert_one_document() {
        let insert = DocumentInsert::one(
            "users".to_string(),
            serde_json::json!({"name": "Alice", "age": 30}),
        );
        let mutation = MutationRequest::DocumentInsert(insert);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.language, QueryLanguage::MongoQuery);
        assert!(result.text.contains("insertOne"));
        assert!(result.text.contains("Alice"));
    }

    #[test]
    fn insert_many_documents() {
        let insert = DocumentInsert::many(
            "users".to_string(),
            vec![
                serde_json::json!({"name": "Alice"}),
                serde_json::json!({"name": "Bob"}),
            ],
        );
        let mutation = MutationRequest::DocumentInsert(insert);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("insertMany"));
    }

    #[test]
    fn update_one_document() {
        let update = DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::by_id("abc123"),
            serde_json::json!({"$set": {"name": "Bob"}}),
        );
        let mutation = MutationRequest::DocumentUpdate(update);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("updateOne"));
        assert!(result.text.contains("$set"));
    }

    #[test]
    fn update_many_with_upsert() {
        let update = DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"status": "old"})),
            serde_json::json!({"$set": {"archived": true}}),
        )
        .many()
        .upsert();
        let mutation = MutationRequest::DocumentUpdate(update);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("updateMany"));
        assert!(result.text.contains("upsert: true"));
    }

    #[test]
    fn delete_one_document() {
        let delete = DocumentDelete::new("users".to_string(), DocumentFilter::by_id("abc123"));
        let mutation = MutationRequest::DocumentDelete(delete);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("deleteOne"));
    }

    #[test]
    fn delete_many_documents() {
        let delete = DocumentDelete::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"archived": true})),
        )
        .many();
        let mutation = MutationRequest::DocumentDelete(delete);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("deleteMany"));
    }

    // =========================================================================
    // generate_read_from_spec — VisualQuerySpec -> find().sort().limit().skip()
    // =========================================================================

    use dbflux_core::{BoolOp, ProjectedColumn, SourceTable, VisualSortDirection};

    fn source(table: &str) -> SourceTable {
        SourceTable {
            schema: None,
            table: table.to_string(),
            alias: table.to_string(),
        }
    }

    fn read_spec(table: &str) -> VisualQuerySpec {
        VisualQuerySpec {
            source: source(table),
            projection: Projection::All,
            joins: vec![],
            filter: None,
            group_by: vec![],
            aggregates: vec![],
            having: None,
            sort: vec![],
            limit: None,
            offset: 0,
        }
    }

    fn eq_predicate(column: &str, value: LiteralValue) -> FilterNode {
        FilterNode::Predicate(Predicate {
            source_alias: String::new(),
            column: column.to_string(),
            comparator: Comparator::Eq,
            value: PredicateValue::Single(value),
            node_id: 0,
        })
    }

    fn sort_entry(column: &str, direction: VisualSortDirection) -> SortEntry {
        SortEntry {
            source_alias: String::new(),
            column: column.to_string(),
            direction,
        }
    }

    #[test]
    fn read_spec_filter_sort_limit_skip() {
        let mut spec = read_spec("users");
        spec.filter = Some(FilterNode::Group {
            op: BoolOp::And,
            children: vec![
                eq_predicate("status", LiteralValue::Text("active".to_string())),
                FilterNode::Predicate(Predicate {
                    source_alias: String::new(),
                    column: "age".to_string(),
                    comparator: Comparator::Gt,
                    value: PredicateValue::Single(LiteralValue::Integer(18)),
                    node_id: 0,
                }),
            ],
        });
        spec.sort = vec![
            sort_entry("created_at", VisualSortDirection::Desc),
            sort_entry("name", VisualSortDirection::Asc),
        ];
        spec.limit = Some(25);
        spec.offset = 10;

        let result = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .expect("read spec must generate")
            .expect("MongoDB generator emits read text");

        assert_eq!(result.language, QueryLanguage::MongoQuery);
        assert_eq!(
            result.text,
            r#"db.users.find({"$and":[{"status":"active"},{"age":{"$gt":18}}]}).sort({"created_at":-1,"name":1}).limit(25).skip(10)"#
        );
    }

    #[test]
    fn read_spec_empty_filter_renders_empty_document() {
        let spec = read_spec("events");

        let result = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .unwrap()
            .unwrap();

        assert_eq!(result.text, "db.events.find({})");
    }

    #[test]
    fn read_spec_non_identifier_collection_uses_bracket_access() {
        let spec = read_spec("orders.2024");

        let result = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .unwrap()
            .unwrap();

        assert_eq!(result.text, r#"db["orders.2024"].find({})"#);
    }

    #[test]
    fn read_spec_explicit_projection_emits_projection_argument() {
        let mut spec = read_spec("users");
        spec.projection = Projection::Explicit(vec![
            ProjectedColumn {
                source_alias: "users".to_string(),
                column: "name".to_string(),
                alias: None,
            },
            ProjectedColumn {
                source_alias: "users".to_string(),
                column: "email".to_string(),
                alias: Some("contact".to_string()),
            },
        ]);
        spec.filter = Some(eq_predicate("active", LiteralValue::Bool(true)));

        let result = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .unwrap()
            .unwrap();

        assert_eq!(
            result.text,
            r#"db.users.find({"active":true}, {"name":1,"contact":1})"#
        );
    }

    #[test]
    fn read_spec_in_and_regex_operators() {
        let mut spec = read_spec("users");
        spec.filter = Some(FilterNode::Group {
            op: BoolOp::Or,
            children: vec![
                FilterNode::Predicate(Predicate {
                    source_alias: String::new(),
                    column: "role".to_string(),
                    comparator: Comparator::In,
                    value: PredicateValue::List(vec![
                        LiteralValue::Text("admin".to_string()),
                        LiteralValue::Text("staff".to_string()),
                    ]),
                    node_id: 0,
                }),
                FilterNode::Predicate(Predicate {
                    source_alias: String::new(),
                    column: "name".to_string(),
                    comparator: Comparator::ILike,
                    value: PredicateValue::Single(LiteralValue::Text("al".to_string())),
                    node_id: 0,
                }),
            ],
        });

        let result = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .unwrap()
            .unwrap();

        assert_eq!(
            result.text,
            r#"db.users.find({"$or":[{"role":{"$in":["admin","staff"]}},{"name":{"$regex":"al","$options":"i"}}]})"#
        );
    }

    #[test]
    fn read_spec_rejects_joins() {
        use dbflux_core::{JoinKind, JoinOn, JoinStep};

        let mut spec = read_spec("orders");
        spec.joins = vec![JoinStep {
            kind: JoinKind::Inner,
            from_alias: "orders".to_string(),
            to_schema: None,
            to_table: "users".to_string(),
            to_alias: "users".to_string(),
            on: JoinOn::RawExpression("orders.user_id = users.id".to_string()),
        }];

        let err = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .expect_err("joins are not expressible in find()");
        assert!(matches!(err, QueryGenError::InvalidSpec(_)));
    }

    #[test]
    fn read_spec_rejects_grouping() {
        use dbflux_core::GroupByEntry;

        let mut spec = read_spec("orders");
        spec.group_by = vec![GroupByEntry {
            source_alias: "orders".to_string(),
            column: "status".to_string(),
        }];

        let err = MongoShellGenerator
            .generate_read_from_spec(&spec)
            .expect_err("grouping is not expressible in find()");
        assert!(matches!(err, QueryGenError::InvalidSpec(_)));
    }

    #[test]
    fn sql_mutation_returns_none() {
        let patch = dbflux_core::RowPatch::new(
            dbflux_core::RecordIdentity::composite(
                vec!["id".to_string()],
                vec![dbflux_core::Value::Int(1)],
            ),
            "users".to_string(),
            None,
            vec![("name".to_string(), dbflux_core::Value::Text("test".into()))],
        );
        let mutation = MutationRequest::SqlUpdate(patch);

        assert!(MongoShellGenerator.generate_mutation(&mutation).is_none());
    }
}
