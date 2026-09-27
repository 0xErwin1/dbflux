//! The visual query builder's codec for MongoDB.
//!
//! Slots are strict JSON that `json_to_bson` reads back into the BSON the
//! driver runs: object ids as `{"$oid": ...}`, dates as `{"$date": ...}` with
//! an RFC 3339 string, and regular expressions as `$regex` / `$options`.
//!
//! Filter documents follow one canonical shape, so every rendered slot parses
//! back to the same spec:
//! - the root `And` group renders flat (`{"a": 1, "b": 2}`) while its keys are
//!   unique and as `{"$and": [...]}` once two of them collide;
//! - an `Or` group renders as `{"$or": [...]}`, a nested `And` group as
//!   `{"$and": [...]}`, and every array element holds exactly one child.
//!
//! A root `And` group whose only child is a group reads back as that group,
//! which filters the same documents.

use std::collections::HashSet;
use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use dbflux_core::{
    DbError, DocumentAccumulatorKind, DocumentCombinator, DocumentCondition, DocumentFieldType,
    DocumentFilterGroup, DocumentFilterNode, DocumentFindSlots, DocumentGroupStage,
    DocumentOperator, DocumentProjection, DocumentProjectionMode, DocumentQueryCodec,
    DocumentQueryMode, DocumentQuerySpec, DocumentSlot, DocumentSlotParse, DocumentSortDirection,
    DocumentSortKey, DocumentValue, UnrepresentableClause, normalize_relaxed_json,
};
use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, SeqAccess, Visitor};

#[derive(Debug, Clone, Copy, Default)]
pub struct MongoDocumentCodec;

/// Hexadecimal digits of an ObjectId.
const OBJECT_ID_HEX_DIGITS: usize = 24;

impl DocumentQueryCodec for MongoDocumentCodec {
    fn render_find(&self, spec: &DocumentQuerySpec) -> Result<DocumentFindSlots, DbError> {
        ensure_mode(spec, DocumentQueryMode::Find)?;
        ensure_valid(spec)?;

        Ok(DocumentFindSlots {
            filter: filter_document(&spec.filter)?.to_text(),
            projection: projection_document(&spec.projection).to_text(),
            sort: sort_document(&spec.sort).to_text(),
            limit: spec.limit,
            skip: spec.skip,
        })
    }

    fn render_pipeline(&self, spec: &DocumentQuerySpec) -> Result<String, DbError> {
        ensure_mode(spec, DocumentQueryMode::Aggregate)?;
        ensure_valid(spec)?;

        let mut stages = Vec::new();

        if !spec.filter.is_empty() {
            stages.push(stage("$match", filter_document(&spec.filter)?));
        }

        if let Some(group) = &spec.group {
            stages.push(stage("$group", group_document(group)?));
        }

        if !spec.sort.is_empty() {
            let sort = match &spec.group {
                Some(group) => group_sort_keys(group, &spec.sort),
                None => spec.sort.clone(),
            };
            stages.push(stage("$sort", sort_document(&sort)));
        }

        if let Some(skip) = spec.skip {
            stages.push(stage("$skip", Json::Number(skip.into())));
        }

        if let Some(limit) = spec.limit {
            stages.push(stage("$limit", Json::Number(limit.into())));
        }

        Ok(Json::Array(stages).to_text())
    }

    fn render_preview(
        &self,
        spec: &DocumentQuerySpec,
        collection: &str,
    ) -> Result<String, DbError> {
        let accessor = collection_accessor(collection);

        if spec.mode == DocumentQueryMode::Aggregate {
            return Ok(format!(
                "{accessor}.aggregate({})",
                self.render_pipeline(spec)?
            ));
        }

        let slots = self.render_find(spec)?;

        let mut text = if spec.projection.is_empty() {
            format!("{accessor}.find({})", slots.filter)
        } else {
            format!("{accessor}.find({}, {})", slots.filter, slots.projection)
        };

        if !spec.sort.is_empty() {
            text.push_str(&format!(".sort({})", slots.sort));
        }

        if let Some(limit) = spec.limit {
            text.push_str(&format!(".limit({limit})"));
        }

        if let Some(skip) = spec.skip {
            text.push_str(&format!(".skip({skip})"));
        }

        Ok(text)
    }

    fn parse_find(&self, slots: &DocumentFindSlots) -> DocumentSlotParse {
        let mut unrepresentable = Vec::new();

        let filter = parse_filter_slot(&slots.filter, &mut unrepresentable);
        let projection = parse_projection_slot(&slots.projection, &mut unrepresentable);
        let sort = parse_sort_slot(&slots.sort, &mut unrepresentable);

        DocumentSlotParse {
            spec: DocumentQuerySpec {
                mode: DocumentQueryMode::Find,
                filter,
                projection,
                sort,
                limit: slots.limit,
                skip: slots.skip,
                group: None,
            },
            unrepresentable,
        }
    }

    fn field_type(&self, type_name: &str) -> Option<DocumentFieldType> {
        match type_name.to_ascii_lowercase().as_str() {
            "string" => Some(DocumentFieldType::String),
            "int32" | "int64" | "int" | "long" => Some(DocumentFieldType::Integer),
            "double" | "decimal128" | "decimal" => Some(DocumentFieldType::Decimal),
            "date" => Some(DocumentFieldType::Date),
            "boolean" | "bool" => Some(DocumentFieldType::Bool),
            "objectid" => Some(DocumentFieldType::ObjectId),
            "array" => Some(DocumentFieldType::Array),
            "object" | "document" => Some(DocumentFieldType::Object),
            _ => None,
        }
    }

    fn operator_label(&self, operator: DocumentOperator) -> &'static str {
        operator_key(operator)
    }

    fn object_id_wrapper(&self) -> Option<(&'static str, &'static str)> {
        Some(("ObjectId(", ")"))
    }

    /// The driver runs every 24-digit hexadecimal string as an ObjectId.
    fn object_id_hex_digits(&self) -> Option<usize> {
        Some(OBJECT_ID_HEX_DIGITS)
    }
}

/// A JSON value that keeps object keys in document order.
///
/// `serde_json::Map` only preserves insertion order when some crate in the
/// build enables its `preserve_order` feature, and key order carries meaning in
/// sort documents, so the codec reads and writes slots through this type.
#[derive(Debug, Clone, PartialEq)]
enum Json {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    fn object(key: &str, value: Json) -> Json {
        Json::Object(vec![(key.to_string(), value)])
    }

    fn string(value: impl Into<String>) -> Json {
        Json::String(value.into())
    }

    fn to_text(&self) -> String {
        let mut text = String::new();
        self.write(&mut text);
        text
    }

    fn write(&self, text: &mut String) {
        match self {
            Json::Null => text.push_str("null"),
            Json::Bool(value) => text.push_str(if *value { "true" } else { "false" }),
            Json::Number(number) => text.push_str(&number.to_string()),
            Json::String(value) => {
                text.push_str(&serde_json::Value::String(value.clone()).to_string())
            }
            Json::Array(items) => {
                text.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        text.push_str(", ");
                    }
                    item.write(text);
                }
                text.push(']');
            }
            Json::Object(entries) => {
                text.push('{');
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index > 0 {
                        text.push_str(", ");
                    }
                    Json::String(key.clone()).write(text);
                    text.push_str(": ");
                    value.write(text);
                }
                text.push('}');
            }
        }
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonVisitor)
    }
}

struct JsonVisitor;

impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Json, E> {
        Ok(Json::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Json, E> {
        Ok(Json::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Json, E> {
        Ok(Json::Number(value.into()))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Json, E> {
        serde_json::Number::from_f64(value)
            .map(Json::Number)
            .ok_or_else(|| E::custom("number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Json, E> {
        Ok(Json::string(value))
    }

    fn visit_string<E>(self, value: String) -> Result<Json, E> {
        Ok(Json::String(value))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Json, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = sequence.next_element::<Json>()? {
            items.push(item);
        }
        Ok(Json::Array(items))
    }

    /// Rejects duplicate keys: execution keeps only the last one, so reading
    /// every duplicate would describe a different query than the one that runs.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut entries: Vec<(String, Json)> = Vec::new();
        while let Some((key, value)) = map.next_entry::<String, Json>()? {
            if entries.iter().any(|(existing, _)| *existing == key) {
                return Err(A::Error::custom(format!("duplicate key `{key}`")));
            }
            entries.push((key, value));
        }
        Ok(Json::Object(entries))
    }
}

fn ensure_mode(spec: &DocumentQuerySpec, expected: DocumentQueryMode) -> Result<(), DbError> {
    if spec.mode == expected {
        return Ok(());
    }

    Err(DbError::query_failed(format!(
        "expected a {expected:?} query, got a {:?} query",
        spec.mode
    )))
}

fn ensure_valid(spec: &DocumentQuerySpec) -> Result<(), DbError> {
    let problems = spec.validate();
    if problems.is_empty() {
        return Ok(());
    }

    let messages: Vec<String> = problems.iter().map(ToString::to_string).collect();
    Err(DbError::query_failed(messages.join("; ")))
}

fn stage(name: &str, body: Json) -> Json {
    Json::object(name, body)
}

fn combinator_key(combinator: DocumentCombinator) -> &'static str {
    match combinator {
        DocumentCombinator::And => "$and",
        DocumentCombinator::Or => "$or",
    }
}

fn combinator_of(key: &str) -> Option<DocumentCombinator> {
    match key {
        "$and" => Some(DocumentCombinator::And),
        "$or" => Some(DocumentCombinator::Or),
        _ => None,
    }
}

fn operator_key(operator: DocumentOperator) -> &'static str {
    match operator {
        DocumentOperator::Eq => "$eq",
        DocumentOperator::Ne => "$ne",
        DocumentOperator::Gt => "$gt",
        DocumentOperator::Gte => "$gte",
        DocumentOperator::Lt => "$lt",
        DocumentOperator::Lte => "$lte",
        DocumentOperator::In => "$in",
        DocumentOperator::Nin => "$nin",
        DocumentOperator::Regex => "$regex",
        DocumentOperator::Exists => "$exists",
        DocumentOperator::ElemMatch => "$elemMatch",
        DocumentOperator::Size => "$size",
        DocumentOperator::All => "$all",
    }
}

fn operator_of(key: &str) -> Option<DocumentOperator> {
    DocumentOperator::ALL
        .into_iter()
        .find(|operator| operator_key(*operator) == key)
}

/// The filter document of a whole group: the find filter, a `$match` body or
/// an `$elemMatch` body.
fn filter_document(group: &DocumentFilterGroup) -> Result<Json, DbError> {
    if group.is_empty() {
        return Ok(Json::Object(Vec::new()));
    }

    if group.combinator == DocumentCombinator::Or {
        return Ok(Json::Object(vec![group_entry(group)?]));
    }

    let mut entries = Vec::new();
    for child in &group.children {
        entries.extend(node_entries(child)?);
    }

    let mut keys = HashSet::new();
    if entries.iter().all(|(key, _)| keys.insert(key.as_str())) {
        Ok(Json::Object(entries))
    } else {
        Ok(Json::Object(vec![group_entry(group)?]))
    }
}

fn group_entry(group: &DocumentFilterGroup) -> Result<(String, Json), DbError> {
    let elements = group
        .children
        .iter()
        .map(|child| node_entries(child).map(Json::Object))
        .collect::<Result<Vec<_>, _>>()?;

    Ok((
        combinator_key(group.combinator).to_string(),
        Json::Array(elements),
    ))
}

fn node_entries(node: &DocumentFilterNode) -> Result<Vec<(String, Json)>, DbError> {
    match node {
        DocumentFilterNode::Condition(condition) => condition_entries(condition),
        DocumentFilterNode::Group(group) => Ok(vec![group_entry(group)?]),
    }
}

/// A condition on a field is one `path: test` entry. A condition without a
/// path (inside `$elemMatch`) contributes its operator entries directly.
fn condition_entries(condition: &DocumentCondition) -> Result<Vec<(String, Json)>, DbError> {
    let operand = value_json(&condition.value)?;
    let operator = condition.operator;

    if condition.path.is_empty() {
        return Ok(match (operator, operand) {
            (DocumentOperator::Regex, Json::Object(entries)) => entries,
            (operator, operand) => vec![(operator_key(operator).to_string(), operand)],
        });
    }

    let test = match operator {
        DocumentOperator::Eq | DocumentOperator::Regex => operand,
        operator => Json::object(operator_key(operator), operand),
    };

    Ok(vec![(condition.path.clone(), test)])
}

fn value_json(value: &DocumentValue) -> Result<Json, DbError> {
    match value {
        DocumentValue::String(text) => {
            ensure_not_object_id_text(text)?;
            Ok(Json::string(text.clone()))
        }
        DocumentValue::Integer(number) => Ok(Json::Number((*number).into())),
        DocumentValue::Decimal(number) => serde_json::Number::from_f64(*number)
            .map(Json::Number)
            .ok_or_else(|| DbError::query_failed(format!("{number} is not a finite number"))),
        DocumentValue::Bool(value) => Ok(Json::Bool(*value)),
        DocumentValue::Null => Ok(Json::Null),
        DocumentValue::Date(date) => Ok(Json::object(
            "$date",
            Json::string(date.to_rfc3339_opts(SecondsFormat::Millis, true)),
        )),
        DocumentValue::ObjectId(hex) => {
            if !is_object_id(hex) {
                return Err(DbError::query_failed(format!(
                    "`{hex}` is not a 24-digit hexadecimal ObjectId"
                )));
            }
            Ok(Json::object("$oid", Json::string(hex.clone())))
        }
        DocumentValue::Regex { pattern, options } => {
            ensure_not_object_id_text(pattern)?;
            let mut entries = vec![("$regex".to_string(), Json::string(pattern.clone()))];
            if !options.is_empty() {
                entries.push(("$options".to_string(), Json::string(options.clone())));
            }
            Ok(Json::Object(entries))
        }
        DocumentValue::List(items) => items
            .iter()
            .map(value_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Json::Array),
        DocumentValue::Nested(group) => filter_document(group),
    }
}

fn is_object_id(hex: &str) -> bool {
    hex.len() == OBJECT_ID_HEX_DIGITS && hex.chars().all(|character| character.is_ascii_hexdigit())
}

/// The driver runs every 24-digit hexadecimal JSON string as an ObjectId, so
/// such a string cannot be sent as text.
fn ensure_not_object_id_text(text: &str) -> Result<(), DbError> {
    if is_object_id(text) {
        return Err(DbError::query_failed(format!(
            "`{text}` would run as an ObjectId, not as text; use an ObjectId value instead"
        )));
    }

    Ok(())
}

fn projection_document(projection: &DocumentProjection) -> Json {
    let flag = match projection.mode {
        DocumentProjectionMode::Include => 1,
        DocumentProjectionMode::Exclude => 0,
    };

    Json::Object(
        projection
            .fields
            .iter()
            .map(|field| (field.clone(), Json::Number(flag.into())))
            .collect(),
    )
}

fn sort_document(sort: &[DocumentSortKey]) -> Json {
    Json::Object(
        sort.iter()
            .map(|key| {
                let direction: i64 = match key.direction {
                    DocumentSortDirection::Ascending => 1,
                    DocumentSortDirection::Descending => -1,
                };
                (key.path.clone(), Json::Number(direction.into()))
            })
            .collect(),
    )
}

/// Sort keys over the `$group` output: a single group key is the `_id` field
/// itself, a compound key is `_id.<output name>`, accumulators keep their name.
fn group_sort_keys(group: &DocumentGroupStage, sort: &[DocumentSortKey]) -> Vec<DocumentSortKey> {
    sort.iter()
        .map(|key| {
            let path = if !group.keys.contains(&key.path) {
                key.path.clone()
            } else if group.keys.len() == 1 {
                "_id".to_string()
            } else {
                format!("_id.{}", group_key_name(&key.path))
            };
            DocumentSortKey::new(path, key.direction)
        })
        .collect()
}

fn group_key_name(key: &str) -> String {
    key.replace('.', "_")
}

/// The `$group` body. Output field names cannot contain `.` or start with `$`,
/// so a compound `_id` names each key after its path with `.` replaced by `_`.
fn group_document(group: &DocumentGroupStage) -> Result<Json, DbError> {
    let id = match group.keys.as_slice() {
        [] => Json::Null,
        [key] => Json::string(format!("${key}")),
        keys => {
            let mut names = HashSet::new();
            let mut entries = Vec::new();

            for key in keys {
                let name = group_key_name(key);
                if !names.insert(name.clone()) {
                    return Err(DbError::query_failed(format!(
                        "group keys collide on the output name `{name}`"
                    )));
                }
                entries.push((name, Json::string(format!("${key}"))));
            }

            Json::Object(entries)
        }
    };

    let mut entries = vec![("_id".to_string(), id)];

    for accumulator in &group.accumulators {
        let name = accumulator.name.trim();
        if name == "_id" || name.contains('.') || name.starts_with('$') {
            return Err(DbError::query_failed(format!(
                "`{name}` cannot name a group output field"
            )));
        }

        let expression = match &accumulator.kind {
            DocumentAccumulatorKind::Count => Json::object("$sum", Json::Number(1.into())),
            DocumentAccumulatorKind::Sum { path } => {
                Json::object("$sum", Json::string(format!("${}", path.trim())))
            }
            DocumentAccumulatorKind::Avg { path } => {
                Json::object("$avg", Json::string(format!("${}", path.trim())))
            }
        };

        entries.push((name.to_string(), expression));
    }

    Ok(Json::Object(entries))
}

/// Mirrors the query generator's accessor: dot access for identifiers,
/// bracket access for any other collection name.
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
        format!("db[{}]", serde_json::Value::String(name.to_string()))
    }
}

enum SlotDocument {
    Empty,
    Entries(Vec<(String, Json)>),
    Invalid,
}

fn read_slot(text: &str) -> SlotDocument {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return SlotDocument::Empty;
    }

    match serde_json::from_str::<Json>(&normalize_relaxed_json(trimmed)) {
        Ok(Json::Object(entries)) => SlotDocument::Entries(entries),
        Ok(_) | Err(_) => SlotDocument::Invalid,
    }
}

fn unrepresentable(slot: DocumentSlot, text: String) -> UnrepresentableClause {
    UnrepresentableClause { slot, text }
}

fn parse_filter_slot(text: &str, clauses: &mut Vec<UnrepresentableClause>) -> DocumentFilterGroup {
    match read_slot(text) {
        SlotDocument::Empty => DocumentFilterGroup::default(),
        SlotDocument::Invalid => {
            clauses.push(unrepresentable(
                DocumentSlot::Filter,
                text.trim().to_string(),
            ));
            DocumentFilterGroup::default()
        }
        SlotDocument::Entries(entries) => {
            let (group, rejected) = parse_filter_entries(&entries, PathScope::Document);
            clauses.extend(
                rejected
                    .into_iter()
                    .map(|json| unrepresentable(DocumentSlot::Filter, json.to_text())),
            );
            group
        }
    }
}

/// Where a filter document is read: at document level every key is a field
/// path or `$and` / `$or`; inside `$elemMatch` other `$` keys test the array
/// element itself.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PathScope {
    Document,
    Element,
}

/// Reads a filter document into a group, returning the parts it cannot hold
/// as single-entry documents. An `$or` that cannot be read completely is
/// returned whole, because dropping a branch would change what it matches.
fn parse_filter_entries(
    entries: &[(String, Json)],
    scope: PathScope,
) -> (DocumentFilterGroup, Vec<Json>) {
    if let [(key, Json::Array(elements))] = entries
        && let Some(combinator) = combinator_of(key)
    {
        let mut children = Vec::new();
        let mut rejected = Vec::new();

        for element in elements {
            match parse_element(element, scope) {
                Ok(node) => children.push(node),
                Err(()) => rejected.push(element.clone()),
            }
        }

        if combinator == DocumentCombinator::Or && !rejected.is_empty() {
            return (
                DocumentFilterGroup::default(),
                vec![Json::Object(entries.to_vec())],
            );
        }

        return (DocumentFilterGroup::new(combinator, children), rejected);
    }

    let mut children = Vec::new();
    let mut rejected = Vec::new();

    for (key, value) in entries {
        match parse_entry(key, value, entries, scope) {
            Ok(nodes) => children.extend(nodes),
            Err(()) => rejected.push(Json::object(key, value.clone())),
        }
    }

    (
        DocumentFilterGroup::new(DocumentCombinator::And, children),
        rejected,
    )
}

/// One element of an `$and` / `$or` array, read completely or not at all.
fn parse_element(element: &Json, scope: PathScope) -> Result<DocumentFilterNode, ()> {
    let Json::Object(entries) = element else {
        return Err(());
    };

    if let [(key, value)] = entries.as_slice()
        && combinator_of(key).is_some()
    {
        return parse_entry(key, value, entries, scope)?
            .into_iter()
            .next()
            .ok_or(());
    }

    let (mut group, rejected) = parse_filter_entries(entries, scope);
    if !rejected.is_empty() || group.is_empty() {
        return Err(());
    }

    if group.children.len() == 1 {
        return group.children.pop().ok_or(());
    }

    Ok(DocumentFilterNode::Group(group))
}

fn parse_entry(
    key: &str,
    value: &Json,
    siblings: &[(String, Json)],
    scope: PathScope,
) -> Result<Vec<DocumentFilterNode>, ()> {
    if let Some(combinator) = combinator_of(key) {
        let Json::Array(elements) = value else {
            return Err(());
        };

        let children = elements
            .iter()
            .map(|element| parse_element(element, scope))
            .collect::<Result<Vec<_>, _>>()?;
        if children.is_empty() {
            return Err(());
        }

        return Ok(vec![DocumentFilterNode::Group(DocumentFilterGroup::new(
            combinator, children,
        ))]);
    }

    if key.starts_with('$') {
        if scope == PathScope::Document {
            return Err(());
        }

        let condition = parse_operator("", key, value, siblings)?;
        return Ok(condition
            .map(DocumentFilterNode::Condition)
            .into_iter()
            .collect());
    }

    parse_field(key, value)
}

/// Reads `path: test`, where the test is a value (equality) or a document of
/// operators, each of which becomes one condition.
fn parse_field(path: &str, test: &Json) -> Result<Vec<DocumentFilterNode>, ()> {
    if let Json::Object(entries) = test
        && entries.iter().any(|(key, _)| key.starts_with('$'))
    {
        if let Some(value) = extended_value(entries)? {
            return Ok(vec![equality(path, value)?]);
        }

        let mut nodes = Vec::new();
        for (key, operand) in entries {
            if let Some(condition) = parse_operator(path, key, operand, entries)? {
                nodes.push(DocumentFilterNode::Condition(condition));
            }
        }

        if nodes.is_empty() {
            return Err(());
        }

        return Ok(nodes);
    }

    Ok(vec![equality(path, parse_value(test)?)?])
}

fn equality(path: &str, value: DocumentValue) -> Result<DocumentFilterNode, ()> {
    if !DocumentOperator::Eq.accepts(&value) {
        return Err(());
    }

    Ok(DocumentFilterNode::Condition(DocumentCondition::new(
        path,
        DocumentOperator::Eq,
        value,
    )))
}

/// Reads one `$operator: operand` entry. `$options` belongs to the `$regex`
/// among its siblings and yields no condition of its own.
fn parse_operator(
    path: &str,
    key: &str,
    operand: &Json,
    siblings: &[(String, Json)],
) -> Result<Option<DocumentCondition>, ()> {
    let has_sibling = |name: &str| siblings.iter().any(|(sibling, _)| sibling == name);

    if key == "$options" {
        return if has_sibling("$regex") {
            Ok(None)
        } else {
            Err(())
        };
    }

    let operator = operator_of(key).ok_or(())?;

    let value = match operator {
        DocumentOperator::Regex => {
            let Json::String(pattern) = operand else {
                return Err(());
            };
            if is_object_id(pattern) {
                return Err(());
            }

            let options = match siblings.iter().find(|(sibling, _)| sibling == "$options") {
                None => String::new(),
                Some((_, Json::String(options))) => options.clone(),
                Some(_) => return Err(()),
            };

            DocumentValue::Regex {
                pattern: pattern.clone(),
                options,
            }
        }
        DocumentOperator::ElemMatch => {
            let Json::Object(body) = operand else {
                return Err(());
            };

            let (group, rejected) = parse_filter_entries(body, PathScope::Element);
            if !rejected.is_empty() || group.is_empty() {
                return Err(());
            }

            DocumentValue::Nested(group)
        }
        _ => parse_value(operand)?,
    };

    if !operator.accepts(&value) {
        return Err(());
    }

    Ok(Some(DocumentCondition::new(path, operator, value)))
}

fn parse_value(json: &Json) -> Result<DocumentValue, ()> {
    match json {
        Json::Null => Ok(DocumentValue::Null),
        Json::Bool(value) => Ok(DocumentValue::Bool(*value)),
        Json::Number(number) => match number.as_i64() {
            Some(integer) => Ok(DocumentValue::Integer(integer)),
            None => number.as_f64().map(DocumentValue::Decimal).ok_or(()),
        },
        Json::String(text) if is_object_id(text) => Ok(DocumentValue::ObjectId(text.clone())),
        Json::String(text) => Ok(DocumentValue::String(text.clone())),
        Json::Array(items) => items
            .iter()
            .map(parse_value)
            .collect::<Result<Vec<_>, _>>()
            .map(DocumentValue::List),
        Json::Object(entries) => extended_value(entries)?.ok_or(()),
    }
}

/// `{"$oid": ...}` and `{"$date": ...}` values; `None` for any other document.
fn extended_value(entries: &[(String, Json)]) -> Result<Option<DocumentValue>, ()> {
    let [(key, Json::String(text))] = entries else {
        return Ok(None);
    };

    match key.as_str() {
        "$oid" if is_object_id(text) => Ok(Some(DocumentValue::ObjectId(text.clone()))),
        "$oid" => Err(()),
        "$date" => DateTime::parse_from_rfc3339(text)
            .map(|date| Some(DocumentValue::Date(date.with_timezone(&Utc))))
            .map_err(|_| ()),
        _ => Ok(None),
    }
}

/// A projection flag: `1` / `true` includes a field, `0` / `false` excludes it.
fn projection_flag(json: &Json) -> Option<bool> {
    match json {
        Json::Bool(flag) => Some(*flag),
        Json::Number(number) => match number.as_i64() {
            Some(1) => Some(true),
            Some(0) => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Reads a projection. The first flag on a field other than `_id` sets the
/// mode; entries of the other mode, expressions and `_id: 0` are kept as text,
/// because the builder never hides `_id`.
fn parse_projection_slot(
    text: &str,
    clauses: &mut Vec<UnrepresentableClause>,
) -> DocumentProjection {
    let entries = match read_slot(text) {
        SlotDocument::Empty => return DocumentProjection::default(),
        SlotDocument::Invalid => {
            clauses.push(unrepresentable(
                DocumentSlot::Projection,
                text.trim().to_string(),
            ));
            return DocumentProjection::default();
        }
        SlotDocument::Entries(entries) => entries,
    };

    let include = entries
        .iter()
        .filter(|(key, _)| key != "_id")
        .find_map(|(_, value)| projection_flag(value))
        .unwrap_or(true);

    let mode = if include {
        DocumentProjectionMode::Include
    } else {
        DocumentProjectionMode::Exclude
    };

    let mut fields = Vec::new();

    for (key, value) in entries {
        match projection_flag(&value) {
            Some(true) if include => fields.push(key),
            Some(true) if key == "_id" => {}
            Some(false) if !include && key != "_id" => fields.push(key),
            _ => clauses.push(unrepresentable(
                DocumentSlot::Projection,
                Json::object(&key, value).to_text(),
            )),
        }
    }

    DocumentProjection { mode, fields }
}

fn parse_sort_slot(text: &str, clauses: &mut Vec<UnrepresentableClause>) -> Vec<DocumentSortKey> {
    let entries = match read_slot(text) {
        SlotDocument::Empty => return Vec::new(),
        SlotDocument::Invalid => {
            clauses.push(unrepresentable(DocumentSlot::Sort, text.trim().to_string()));
            return Vec::new();
        }
        SlotDocument::Entries(entries) => entries,
    };

    let mut keys = Vec::new();

    for (path, value) in entries {
        let direction = match &value {
            Json::Number(number) => match number.as_i64() {
                Some(1) => Some(DocumentSortDirection::Ascending),
                Some(-1) => Some(DocumentSortDirection::Descending),
                _ => None,
            },
            _ => None,
        };

        match direction {
            Some(direction) => keys.push(DocumentSortKey::new(path, direction)),
            None => clauses.push(unrepresentable(
                DocumentSlot::Sort,
                Json::object(&path, value).to_text(),
            )),
        }
    }

    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{TimeZone, Utc};
    use dbflux_core::{
        DocumentAccumulator, DocumentAccumulatorKind, DocumentCombinator, DocumentCondition,
        DocumentFilterGroup, DocumentFilterNode, DocumentGroupStage, DocumentOperator,
        DocumentProjection, DocumentProjectionMode, DocumentQueryMode, DocumentSlot,
        DocumentSortDirection, DocumentSortKey, DocumentValue, UnrepresentableClause,
    };

    use DocumentOperator::*;

    const OBJECT_ID: &str = "64b7f0a1c2d3e4f5a6b7c8d9";

    fn condition(
        path: &str,
        operator: DocumentOperator,
        value: DocumentValue,
    ) -> DocumentFilterNode {
        DocumentFilterNode::Condition(DocumentCondition::new(path, operator, value))
    }

    fn group(
        combinator: DocumentCombinator,
        children: Vec<DocumentFilterNode>,
    ) -> DocumentFilterGroup {
        DocumentFilterGroup::new(combinator, children)
    }

    fn and(children: Vec<DocumentFilterNode>) -> DocumentFilterNode {
        DocumentFilterNode::Group(group(DocumentCombinator::And, children))
    }

    fn or(children: Vec<DocumentFilterNode>) -> DocumentFilterNode {
        DocumentFilterNode::Group(group(DocumentCombinator::Or, children))
    }

    fn find(filter: DocumentFilterGroup) -> DocumentQuerySpec {
        DocumentQuerySpec {
            filter,
            ..DocumentQuerySpec::default()
        }
    }

    fn find_all(children: Vec<DocumentFilterNode>) -> DocumentQuerySpec {
        find(group(DocumentCombinator::And, children))
    }

    fn text(value: &str) -> DocumentValue {
        DocumentValue::String(value.to_string())
    }

    fn slots(filter: &str, projection: &str, sort: &str) -> DocumentFindSlots {
        DocumentFindSlots {
            filter: filter.to_string(),
            projection: projection.to_string(),
            sort: sort.to_string(),
            limit: None,
            skip: None,
        }
    }

    fn clause(slot: DocumentSlot, text: &str) -> UnrepresentableClause {
        UnrepresentableClause {
            slot,
            text: text.to_string(),
        }
    }

    fn assert_round_trip(spec: &DocumentQuerySpec) -> DocumentFindSlots {
        let codec = MongoDocumentCodec;
        let rendered = codec.render_find(spec).expect("spec renders");
        let parsed = codec.parse_find(&rendered);

        assert!(
            parsed.is_complete(),
            "unrepresentable: {:?}",
            parsed.unrepresentable
        );
        assert_eq!(&parsed.spec, spec, "slots: {rendered:?}");

        rendered
    }

    fn sample_date() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 3, 9, 14, 30, 5).unwrap() + chrono::Duration::milliseconds(250)
    }

    #[test]
    fn every_operator_and_value_type_round_trips() {
        let elem_match = DocumentValue::Nested(group(
            DocumentCombinator::And,
            vec![
                condition("sku", Eq, text("A-1")),
                condition("qty", Gte, DocumentValue::Integer(2)),
            ],
        ));
        let cases = [
            (Eq, text("Ada"), r#"{"field": "Ada"}"#),
            (Eq, DocumentValue::Integer(7), r#"{"field": 7}"#),
            (Eq, DocumentValue::Decimal(2.5), r#"{"field": 2.5}"#),
            (Eq, DocumentValue::Bool(false), r#"{"field": false}"#),
            (Eq, DocumentValue::Null, r#"{"field": null}"#),
            (
                Eq,
                DocumentValue::Date(sample_date()),
                r#"{"field": {"$date": "2024-03-09T14:30:05.250Z"}}"#,
            ),
            (
                Eq,
                DocumentValue::ObjectId(OBJECT_ID.to_string()),
                r#"{"field": {"$oid": "64b7f0a1c2d3e4f5a6b7c8d9"}}"#,
            ),
            (
                Eq,
                DocumentValue::List(vec![text("a"), text("b")]),
                r#"{"field": ["a", "b"]}"#,
            ),
            (Ne, text("x"), r#"{"field": {"$ne": "x"}}"#),
            (Gt, DocumentValue::Integer(1), r#"{"field": {"$gt": 1}}"#),
            (
                Gte,
                DocumentValue::Decimal(1.5),
                r#"{"field": {"$gte": 1.5}}"#,
            ),
            (
                Lt,
                DocumentValue::Date(sample_date()),
                r#"{"field": {"$lt": {"$date": "2024-03-09T14:30:05.250Z"}}}"#,
            ),
            (
                Lte,
                DocumentValue::Integer(-3),
                r#"{"field": {"$lte": -3}}"#,
            ),
            (
                In,
                DocumentValue::List(vec![
                    DocumentValue::Integer(1),
                    DocumentValue::ObjectId(OBJECT_ID.to_string()),
                ]),
                r#"{"field": {"$in": [1, {"$oid": "64b7f0a1c2d3e4f5a6b7c8d9"}]}}"#,
            ),
            (
                Nin,
                DocumentValue::List(vec![text("a")]),
                r#"{"field": {"$nin": ["a"]}}"#,
            ),
            (
                Regex,
                DocumentValue::Regex {
                    pattern: "^ad".to_string(),
                    options: "i".to_string(),
                },
                r#"{"field": {"$regex": "^ad", "$options": "i"}}"#,
            ),
            (
                Regex,
                DocumentValue::Regex {
                    pattern: "a\"b".to_string(),
                    options: String::new(),
                },
                r#"{"field": {"$regex": "a\"b"}}"#,
            ),
            (
                Exists,
                DocumentValue::Bool(true),
                r#"{"field": {"$exists": true}}"#,
            ),
            (
                ElemMatch,
                elem_match,
                r#"{"field": {"$elemMatch": {"sku": "A-1", "qty": {"$gte": 2}}}}"#,
            ),
            (
                Size,
                DocumentValue::Integer(3),
                r#"{"field": {"$size": 3}}"#,
            ),
            (
                All,
                DocumentValue::List(vec![text("red"), text("blue")]),
                r#"{"field": {"$all": ["red", "blue"]}}"#,
            ),
        ];

        for (operator, value, expected) in cases {
            let spec = find_all(vec![condition("field", operator, value)]);
            let rendered = assert_round_trip(&spec);
            assert_eq!(rendered.filter, expected);
        }
    }

    #[test]
    fn elem_match_can_test_the_array_element_itself() {
        let spec = find_all(vec![condition(
            "scores",
            ElemMatch,
            DocumentValue::Nested(group(
                DocumentCombinator::And,
                vec![
                    condition("", Gte, DocumentValue::Integer(80)),
                    condition("", Lt, DocumentValue::Integer(85)),
                    condition(
                        "",
                        Regex,
                        DocumentValue::Regex {
                            pattern: "^9".to_string(),
                            options: "m".to_string(),
                        },
                    ),
                ],
            )),
        )]);

        let rendered = assert_round_trip(&spec);
        assert_eq!(
            rendered.filter,
            r#"{"scores": {"$elemMatch": {"$gte": 80, "$lt": 85, "$regex": "^9", "$options": "m"}}}"#
        );
    }

    #[test]
    fn root_and_renders_flat_until_keys_collide() {
        let spec = find_all(vec![
            condition("age", Gt, DocumentValue::Integer(30)),
            condition("name", Eq, text("Ada")),
        ]);
        let rendered = assert_round_trip(&spec);
        assert_eq!(rendered.filter, r#"{"age": {"$gt": 30}, "name": "Ada"}"#);

        let spec = find_all(vec![
            condition("age", Gt, DocumentValue::Integer(30)),
            condition("age", Lt, DocumentValue::Integer(40)),
        ]);
        let rendered = assert_round_trip(&spec);
        assert_eq!(
            rendered.filter,
            r#"{"$and": [{"age": {"$gt": 30}}, {"age": {"$lt": 40}}]}"#
        );
    }

    #[test]
    fn nested_and_or_groups_round_trip() {
        let spec = find_all(vec![
            condition("status", Eq, text("active")),
            or(vec![
                condition("age", Lt, DocumentValue::Integer(18)),
                and(vec![
                    condition("age", Gte, DocumentValue::Integer(65)),
                    condition("retired", Eq, DocumentValue::Bool(true)),
                ]),
            ]),
            and(vec![
                condition("a", Exists, DocumentValue::Bool(true)),
                condition("b", Exists, DocumentValue::Bool(false)),
            ]),
        ]);
        let rendered = assert_round_trip(&spec);
        assert_eq!(
            rendered.filter,
            concat!(
                r#"{"status": "active", "$or": [{"age": {"$lt": 18}}, "#,
                r#"{"$and": [{"age": {"$gte": 65}}, {"retired": true}]}], "#,
                r#""$and": [{"a": {"$exists": true}}, {"b": {"$exists": false}}]}"#
            )
        );

        let spec = find(group(
            DocumentCombinator::Or,
            vec![
                condition("a", Eq, DocumentValue::Integer(1)),
                or(vec![condition("b", Eq, DocumentValue::Integer(2))]),
            ],
        ));
        let rendered = assert_round_trip(&spec);
        assert_eq!(
            rendered.filter,
            r#"{"$or": [{"a": 1}, {"$or": [{"b": 2}]}]}"#
        );

        let spec = find_all(vec![or(vec![]), or(vec![])]);
        assert!(MongoDocumentCodec.render_find(&spec).is_err());
    }

    #[test]
    fn projection_sort_limit_and_skip_round_trip() {
        let mut spec = find_all(vec![]);
        spec.projection = DocumentProjection {
            mode: DocumentProjectionMode::Include,
            fields: vec!["name".to_string(), "address.city".to_string()],
        };
        spec.sort = vec![
            DocumentSortKey::new("score", DocumentSortDirection::Descending),
            DocumentSortKey::new("name", DocumentSortDirection::Ascending),
        ];
        spec.limit = Some(50);
        spec.skip = Some(100);

        let rendered = assert_round_trip(&spec);
        assert_eq!(rendered.filter, "{}");
        assert_eq!(rendered.projection, r#"{"name": 1, "address.city": 1}"#);
        assert_eq!(rendered.sort, r#"{"score": -1, "name": 1}"#);
        assert_eq!(rendered.limit, Some(50));
        assert_eq!(rendered.skip, Some(100));

        spec.projection = DocumentProjection {
            mode: DocumentProjectionMode::Exclude,
            fields: vec!["password".to_string()],
        };
        spec.sort = vec![
            DocumentSortKey::new("a", DocumentSortDirection::Ascending),
            DocumentSortKey::new("c", DocumentSortDirection::Ascending),
            DocumentSortKey::new("b", DocumentSortDirection::Descending),
        ];
        let rendered = assert_round_trip(&spec);
        assert_eq!(rendered.projection, r#"{"password": 0}"#);
        assert_eq!(rendered.sort, r#"{"a": 1, "c": 1, "b": -1}"#);
    }

    #[test]
    fn empty_spec_renders_empty_slots() {
        let rendered = assert_round_trip(&DocumentQuerySpec::default());

        assert_eq!(rendered, slots("{}", "{}", "{}"));
        assert_eq!(
            MongoDocumentCodec.parse_find(&slots(" ", "", "")).spec,
            DocumentQuerySpec::default()
        );
    }

    #[test]
    fn parse_reads_relaxed_and_equivalent_forms() {
        let parsed = MongoDocumentCodec.parse_find(&slots(
            "{ age: { $gt: 30, $lte: 40 }, name: { $eq: 'Ada' }, _id: { $in: [ { $oid: '64b7f0a1c2d3e4f5a6b7c8d9' } ] } }",
            "{ _id: 1, name: true }",
            "{ age: -1 }",
        ));

        assert!(parsed.is_complete(), "{:?}", parsed.unrepresentable);
        assert_eq!(
            parsed.spec.filter,
            group(
                DocumentCombinator::And,
                vec![
                    condition("age", Gt, DocumentValue::Integer(30)),
                    condition("age", Lte, DocumentValue::Integer(40)),
                    condition("name", Eq, text("Ada")),
                    condition(
                        "_id",
                        In,
                        DocumentValue::List(vec![DocumentValue::ObjectId(OBJECT_ID.to_string())])
                    ),
                ]
            )
        );
        assert_eq!(
            parsed.spec.projection,
            DocumentProjection {
                mode: DocumentProjectionMode::Include,
                fields: vec!["_id".to_string(), "name".to_string()],
            }
        );
        assert_eq!(
            parsed.spec.sort,
            vec![DocumentSortKey::new(
                "age",
                DocumentSortDirection::Descending
            )]
        );
    }

    #[test]
    fn unrepresentable_filter_clauses_are_kept_as_text() {
        let parsed = MongoDocumentCodec.parse_find(&slots(
            r#"{"$expr": {"$gt": ["$a", "$b"]}, "age": {"$gt": 1}, "$where": "this.a > 1", "n": {"$mod": [2, 0]}, "address": {"city": "Lima"}, "$text": {"$search": "x"}}"#,
            "",
            "",
        ));

        assert_eq!(
            parsed.spec.filter,
            group(
                DocumentCombinator::And,
                vec![condition("age", Gt, DocumentValue::Integer(1))]
            )
        );
        assert_eq!(
            parsed.unrepresentable,
            vec![
                clause(DocumentSlot::Filter, r#"{"$expr": {"$gt": ["$a", "$b"]}}"#),
                clause(DocumentSlot::Filter, r#"{"$where": "this.a > 1"}"#),
                clause(DocumentSlot::Filter, r#"{"n": {"$mod": [2, 0]}}"#),
                clause(DocumentSlot::Filter, r#"{"address": {"city": "Lima"}}"#),
                clause(DocumentSlot::Filter, r#"{"$text": {"$search": "x"}}"#),
            ]
        );
    }

    #[test]
    fn an_or_with_an_unrepresentable_branch_is_kept_whole() {
        let filter = r#"{"$or": [{"a": 1}, {"$expr": {"$eq": ["$a", "$b"]}}]}"#;
        let parsed = MongoDocumentCodec.parse_find(&slots(filter, "", ""));

        assert!(parsed.spec.filter.is_empty());
        assert_eq!(
            parsed.unrepresentable,
            vec![clause(DocumentSlot::Filter, filter)]
        );

        let parsed = MongoDocumentCodec.parse_find(&slots(
            r#"{"$and": [{"a": 1}, {"$where": "true"}]}"#,
            "",
            "",
        ));
        assert_eq!(
            parsed.spec.filter,
            group(
                DocumentCombinator::And,
                vec![condition("a", Eq, DocumentValue::Integer(1))]
            )
        );
        assert_eq!(
            parsed.unrepresentable,
            vec![clause(DocumentSlot::Filter, r#"{"$where": "true"}"#)]
        );
    }

    #[test]
    fn unrepresentable_projection_and_sort_entries_are_kept_as_text() {
        let parsed = MongoDocumentCodec.parse_find(&slots(
            "not json",
            r#"{"name": 1, "items": {"$slice": 5}, "_id": 0, "secret": 0}"#,
            r#"{"score": {"$meta": "textScore"}, "name": 1}"#,
        ));

        assert!(parsed.spec.filter.is_empty());
        assert_eq!(
            parsed.spec.projection,
            DocumentProjection {
                mode: DocumentProjectionMode::Include,
                fields: vec!["name".to_string()],
            }
        );
        assert_eq!(
            parsed.spec.sort,
            vec![DocumentSortKey::new(
                "name",
                DocumentSortDirection::Ascending
            )]
        );
        assert_eq!(
            parsed.unrepresentable,
            vec![
                clause(DocumentSlot::Filter, "not json"),
                clause(DocumentSlot::Projection, r#"{"items": {"$slice": 5}}"#),
                clause(DocumentSlot::Projection, r#"{"_id": 0}"#),
                clause(DocumentSlot::Projection, r#"{"secret": 0}"#),
                clause(DocumentSlot::Sort, r#"{"score": {"$meta": "textScore"}}"#),
            ]
        );
    }

    fn aggregate_spec() -> DocumentQuerySpec {
        DocumentQuerySpec {
            mode: DocumentQueryMode::Aggregate,
            filter: group(
                DocumentCombinator::And,
                vec![condition("status", Eq, text("paid"))],
            ),
            sort: vec![DocumentSortKey::new(
                "revenue",
                DocumentSortDirection::Descending,
            )],
            limit: Some(10),
            skip: Some(5),
            group: Some(DocumentGroupStage {
                keys: vec!["customer.country".to_string()],
                accumulators: vec![
                    DocumentAccumulator {
                        name: "orders".to_string(),
                        kind: DocumentAccumulatorKind::Count,
                    },
                    DocumentAccumulator {
                        name: "revenue".to_string(),
                        kind: DocumentAccumulatorKind::Sum {
                            path: "total".to_string(),
                        },
                    },
                    DocumentAccumulator {
                        name: "average".to_string(),
                        kind: DocumentAccumulatorKind::Avg {
                            path: "total".to_string(),
                        },
                    },
                ],
            }),
            ..DocumentQuerySpec::default()
        }
    }

    #[test]
    fn pipeline_renders_match_group_sort_skip_and_limit() {
        let pipeline = MongoDocumentCodec
            .render_pipeline(&aggregate_spec())
            .expect("pipeline renders");

        assert_eq!(
            pipeline,
            concat!(
                r#"[{"$match": {"status": "paid"}}, "#,
                r#"{"$group": {"_id": "$customer.country", "orders": {"$sum": 1}, "#,
                r#""revenue": {"$sum": "$total"}, "average": {"$avg": "$total"}}}, "#,
                r#"{"$sort": {"revenue": -1}}, {"$skip": 5}, {"$limit": 10}]"#
            )
        );
        assert!(dbflux_core::parse_aggregate_pipeline(&pipeline).is_ok());
    }

    #[test]
    fn pipeline_group_ids_cover_zero_one_and_many_keys() {
        let mut spec = aggregate_spec();
        spec.filter = DocumentFilterGroup::default();
        spec.sort.clear();
        spec.limit = None;
        spec.skip = None;

        let group_stage = spec.group.as_mut().unwrap();
        group_stage.keys.clear();
        group_stage.accumulators.truncate(1);
        assert_eq!(
            MongoDocumentCodec.render_pipeline(&spec).unwrap(),
            r#"[{"$group": {"_id": null, "orders": {"$sum": 1}}}]"#
        );

        spec.group.as_mut().unwrap().keys =
            vec!["status".to_string(), "customer.country".to_string()];
        assert_eq!(
            MongoDocumentCodec.render_pipeline(&spec).unwrap(),
            r#"[{"$group": {"_id": {"status": "$status", "customer_country": "$customer.country"}, "orders": {"$sum": 1}}}]"#
        );
    }

    #[test]
    fn rendering_rejects_specs_mongodb_cannot_run() {
        let codec = MongoDocumentCodec;

        assert!(codec.render_find(&aggregate_spec()).is_err());
        assert!(codec.render_pipeline(&find_all(vec![])).is_err());

        let invalid = find_all(vec![condition("age", In, DocumentValue::Integer(3))]);
        assert!(codec.render_find(&invalid).is_err());

        let bad_object_id = find_all(vec![condition(
            "_id",
            Eq,
            DocumentValue::ObjectId("not-hex".to_string()),
        )]);
        assert!(codec.render_find(&bad_object_id).is_err());

        let not_finite = find_all(vec![condition("x", Eq, DocumentValue::Decimal(f64::NAN))]);
        assert!(codec.render_find(&not_finite).is_err());

        for name in ["_id", "a.b", "$total"] {
            let mut spec = aggregate_spec();
            spec.group.as_mut().unwrap().accumulators[0].name = name.to_string();
            assert!(codec.render_pipeline(&spec).is_err(), "{name} accepted");
        }

        let mut spec = aggregate_spec();
        spec.group.as_mut().unwrap().keys = vec!["a.b".to_string(), "a_b".to_string()];
        assert!(codec.render_pipeline(&spec).is_err());
    }

    #[test]
    fn preview_reads_like_the_mongo_shell() {
        let codec = MongoDocumentCodec;

        assert_eq!(
            codec
                .render_preview(&DocumentQuerySpec::default(), "users")
                .unwrap(),
            "db.users.find({})"
        );

        let mut spec = find_all(vec![condition("age", Gt, DocumentValue::Integer(30))]);
        spec.projection.fields = vec!["name".to_string()];
        spec.sort = vec![DocumentSortKey::new(
            "age",
            DocumentSortDirection::Descending,
        )];
        spec.limit = Some(20);
        spec.skip = Some(40);
        assert_eq!(
            codec.render_preview(&spec, "users").unwrap(),
            r#"db.users.find({"age": {"$gt": 30}}, {"name": 1}).sort({"age": -1}).limit(20).skip(40)"#
        );

        let spec = find_all(vec![condition("a", Eq, DocumentValue::Integer(1))]);
        assert_eq!(
            codec.render_preview(&spec, "my.logs").unwrap(),
            r#"db["my.logs"].find({"a": 1})"#
        );

        assert_eq!(
            codec.render_preview(&aggregate_spec(), "orders").unwrap(),
            format!(
                "db.orders.aggregate({})",
                codec.render_pipeline(&aggregate_spec()).unwrap()
            )
        );
    }

    #[test]
    fn rendered_slots_convert_to_the_bson_the_driver_runs() {
        let spec = find_all(vec![
            condition("created", Gte, DocumentValue::Date(sample_date())),
            condition("_id", Eq, DocumentValue::ObjectId(OBJECT_ID.to_string())),
        ]);
        let rendered = MongoDocumentCodec.render_find(&spec).unwrap();
        let json = dbflux_core::parse_relaxed_json(&rendered.filter).unwrap();
        let filter = crate::driver::json_to_bson_doc(&json).unwrap();

        let created = filter.get_document("created").unwrap();
        assert_eq!(
            created.get("$gte"),
            Some(&bson::Bson::DateTime(bson::DateTime::from_millis(
                sample_date().timestamp_millis()
            )))
        );
        assert_eq!(
            filter.get("_id"),
            Some(&bson::Bson::ObjectId(
                bson::oid::ObjectId::parse_str(OBJECT_ID).unwrap()
            ))
        );
    }

    #[test]
    fn hexadecimal_strings_read_as_the_object_ids_the_driver_runs() {
        let parsed = MongoDocumentCodec.parse_find(&slots(
            &format!(
                r#"{{"owner": "{OBJECT_ID}", "tags": {{"$in": ["{OBJECT_ID}", "x"]}}, "refs": {{"$all": ["{OBJECT_ID}"]}}, "n": {{"$nin": ["{OBJECT_ID}"]}}}}"#
            ),
            "",
            "",
        ));
        let object_id = || DocumentValue::ObjectId(OBJECT_ID.to_string());

        assert!(parsed.is_complete(), "{:?}", parsed.unrepresentable);
        assert_eq!(
            parsed.spec.filter,
            group(
                DocumentCombinator::And,
                vec![
                    condition("owner", Eq, object_id()),
                    condition(
                        "tags",
                        In,
                        DocumentValue::List(vec![object_id(), text("x")])
                    ),
                    condition("refs", All, DocumentValue::List(vec![object_id()])),
                    condition("n", Nin, DocumentValue::List(vec![object_id()])),
                ]
            )
        );

        let filter = format!(r#"{{"code": {{"$regex": "{OBJECT_ID}"}}}}"#);
        let parsed = MongoDocumentCodec.parse_find(&slots(&filter, "", ""));
        assert!(parsed.spec.filter.is_empty());
        assert_eq!(
            parsed.unrepresentable,
            vec![clause(DocumentSlot::Filter, &filter)]
        );
    }

    #[test]
    fn hexadecimal_strings_cannot_be_rendered_as_strings() {
        let codec = MongoDocumentCodec;

        let as_string = find_all(vec![condition("code", Eq, text(OBJECT_ID))]);
        let error = codec.render_find(&as_string).unwrap_err().to_string();
        assert!(error.contains("ObjectId"), "{error}");

        let in_list = find_all(vec![condition(
            "code",
            In,
            DocumentValue::List(vec![text(OBJECT_ID)]),
        )]);
        assert!(codec.render_find(&in_list).is_err());

        let as_pattern = find_all(vec![condition(
            "code",
            Regex,
            DocumentValue::Regex {
                pattern: OBJECT_ID.to_string(),
                options: String::new(),
            },
        )]);
        assert!(codec.render_find(&as_pattern).is_err());
    }

    #[test]
    fn duplicate_keys_are_never_parsed() {
        let cases = [
            slots(r#"{"a": {"$gt": 1}, "a": {"$lt": 5}}"#, "", ""),
            slots(
                r#"{"a": {"$regex": "x", "$options": "i", "$options": "m"}}"#,
                "",
                "",
            ),
            slots(r#"{"$or": [{"a": 1, "a": 2}]}"#, "", ""),
            slots("", r#"{"name": 1, "name": 0}"#, ""),
            slots("", "", r#"{"age": 1, "age": -1}"#),
        ];

        for case in cases {
            let parsed = MongoDocumentCodec.parse_find(&case);

            assert_eq!(parsed.spec, DocumentQuerySpec::default(), "{case:?}");
            assert_eq!(parsed.unrepresentable.len(), 1, "{case:?}");
        }
    }

    #[test]
    fn pipeline_sorts_group_keys_on_their_output_field() {
        let mut spec = aggregate_spec();
        spec.sort = vec![
            DocumentSortKey::new("customer.country", DocumentSortDirection::Ascending),
            DocumentSortKey::new("orders", DocumentSortDirection::Descending),
        ];
        let pipeline = MongoDocumentCodec.render_pipeline(&spec).unwrap();
        assert!(
            pipeline.contains(r#"{"$sort": {"_id": 1, "orders": -1}}"#),
            "{pipeline}"
        );

        spec.group.as_mut().unwrap().keys =
            vec!["status".to_string(), "customer.country".to_string()];
        let pipeline = MongoDocumentCodec.render_pipeline(&spec).unwrap();
        assert!(
            pipeline.contains(r#"{"$sort": {"_id.customer_country": 1, "orders": -1}}"#),
            "{pipeline}"
        );

        spec.sort = vec![DocumentSortKey::new(
            "total",
            DocumentSortDirection::Ascending,
        )];
        assert!(MongoDocumentCodec.render_pipeline(&spec).is_err());
    }

    #[test]
    fn field_types_follow_the_schema_sampler_names() {
        let codec = MongoDocumentCodec;
        let cases = [
            ("String", Some(DocumentFieldType::String)),
            ("Int32", Some(DocumentFieldType::Integer)),
            ("Int64", Some(DocumentFieldType::Integer)),
            ("long", Some(DocumentFieldType::Integer)),
            ("Double", Some(DocumentFieldType::Decimal)),
            ("Decimal128", Some(DocumentFieldType::Decimal)),
            ("Date", Some(DocumentFieldType::Date)),
            ("Boolean", Some(DocumentFieldType::Bool)),
            ("bool", Some(DocumentFieldType::Bool)),
            ("ObjectId", Some(DocumentFieldType::ObjectId)),
            ("Array", Some(DocumentFieldType::Array)),
            ("Object", Some(DocumentFieldType::Object)),
            ("document", Some(DocumentFieldType::Object)),
            ("Binary", None),
            (dbflux_core::NULL_TYPE_NAME, None),
        ];

        for (type_name, expected) in cases {
            assert_eq!(codec.field_type(type_name), expected, "{type_name}");
        }
    }

    #[test]
    fn the_builder_spells_operators_and_object_ids_the_mongodb_way() {
        let codec = MongoDocumentCodec;

        assert_eq!(codec.operator_label(Gte), "$gte");
        assert_eq!(codec.operator_label(ElemMatch), "$elemMatch");
        assert_eq!(codec.object_id_wrapper(), Some(("ObjectId(", ")")));
        assert_eq!(codec.object_id_hex_digits(), Some(OBJECT_ID.len()));
    }
}
