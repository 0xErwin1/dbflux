//! Schema inferred from a sample of documents.
//!
//! A driver walks the sampled documents and reports every field it finds with
//! its native type name; [`SchemaSampleAccumulator`] turns those observations
//! into per-field statistics: how often the field is present, how its values
//! split across types, and a short summary of the values themselves.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{CollectionRef, Value};

bitflags::bitflags! {
    /// Document-collection features a connection offers, reported by
    /// `Connection::document_features`. Kept apart from `DriverCapabilities`
    /// so document seams do not use up the shared capability bits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct DocumentFeatures: u32 {
        /// Collection browses accept native projection and sort documents next
        /// to the filter, and `Connection::sample_collection_schema` samples the
        /// collection. Gates the four-slot query bar, its field-path
        /// completion, and the Schema view.
        const QUERY_SLOTS = 1 << 0;

        /// `Connection::patch_document`, `replace_document` and
        /// `fetch_document` work. Gates per-field document edits and the
        /// server-change check made before they are written.
        const FIELD_PATCH = 1 << 1;

        /// `Connection::aggregate_collection` runs aggregation pipelines.
        /// Gates the Aggregate view of a collection.
        const AGGREGATE = 1 << 2;
    }
}

/// Asks a driver to sample `sample_size` documents of a collection, optionally
/// restricted by a native filter document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchemaRequest {
    pub collection: CollectionRef,
    pub sample_size: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
}

impl CollectionSchemaRequest {
    pub fn new(collection: CollectionRef, sample_size: u32) -> Self {
        Self {
            collection,
            sample_size,
            filter: None,
        }
    }
}

/// The schema a sample of documents shows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionSchemaSample {
    /// Documents the sample actually read.
    pub sampled_documents: u64,
    /// Documents in the collection, when the driver knows (possibly estimated).
    pub total_documents: Option<u64>,
    /// Fields in the order they first appear, `_id` first.
    pub fields: Vec<FieldSchemaStats>,
}

impl CollectionSchemaSample {
    /// Field paths of the sample, in display order.
    pub fn field_paths(&self) -> Vec<String> {
        self.fields.iter().map(|field| field.path.clone()).collect()
    }

    /// Statistics for one dotted field path.
    pub fn field(&self, path: &str) -> Option<&FieldSchemaStats> {
        self.fields.iter().find(|field| field.path == path)
    }
}

/// Statistics for one field path across the sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSchemaStats {
    /// Dotted path (`customer.email`, `items.sku`).
    pub path: String,
    /// Sampled documents that hold the field at least once.
    pub presence: u64,
    /// Value count per native type, most common first.
    pub types: Vec<FieldTypeShare>,
    pub summary: FieldValueSummary,
}

impl FieldSchemaStats {
    /// Share of the sampled documents that hold the field, from 0.0 to 1.0.
    pub fn presence_ratio(&self, sampled_documents: u64) -> f32 {
        if sampled_documents == 0 {
            return 0.0;
        }

        (self.presence as f64 / sampled_documents as f64) as f32
    }

    /// Values observed for the field across every type.
    pub fn value_count(&self) -> u64 {
        self.types.iter().map(|share| share.count).sum()
    }

    /// Whether the field holds more than one non-null type, which usually
    /// means some writer stored it differently.
    pub fn has_mixed_types(&self) -> bool {
        self.types
            .iter()
            .filter(|share| share.type_name != NULL_TYPE_NAME)
            .count()
            > 1
    }

    /// The most common type name.
    pub fn dominant_type(&self) -> Option<&str> {
        self.types.first().map(|share| share.type_name.as_str())
    }
}

/// How many values of a field had one native type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldTypeShare {
    pub type_name: String,
    pub count: u64,
}

/// A short description of the values a field holds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValueSummary {
    /// Every present value is different (identifiers).
    Unique,
    /// Too many different values to list them; `count` is how many.
    Distinct { count: u64, capped: bool },
    /// The few values the field takes, most common first.
    TopValues(Vec<ValueShare>),
    /// Smallest and largest value of an ordered field.
    Range { min: Value, max: Value },
    /// Element counts of an array field.
    ArrayLength { min: u64, max: u64, median: u64 },
    /// A nested document with up to `fields` keys.
    Nested { fields: u64 },
    /// No value to describe (every value was null).
    Empty,
}

/// One value and how many times it appeared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueShare {
    pub value: Value,
    pub count: u64,
}

/// One field of one sampled document, as a driver reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct SampledField {
    pub path: String,
    pub type_name: String,
    pub value: Value,
}

impl SampledField {
    pub fn new(path: impl Into<String>, type_name: impl Into<String>, value: Value) -> Self {
        Self {
            path: path.into(),
            type_name: type_name.into(),
            value,
        }
    }
}

/// Type name a driver reports for an explicit null.
pub const NULL_TYPE_NAME: &str = "Null";

/// Values a field may take before its summary lists them instead of counting.
const TOP_VALUE_LIMIT: usize = 6;

/// Distinct values tracked per field; beyond it the count is a lower bound.
const DISTINCT_TRACKING_LIMIT: usize = 2_000;

#[derive(Default)]
struct FieldAccumulator {
    presence: u64,
    type_counts: HashMap<String, u64>,
    distinct: BTreeMap<Value, u64>,
    distinct_capped: bool,
    scalar_values: u64,
    identifier_values: u64,
    ordered_values: u64,
    minimum: Option<Value>,
    maximum: Option<Value>,
    array_lengths: Vec<u64>,
    nested_field_count: u64,
    nested_values: u64,
}

impl FieldAccumulator {
    fn observe(&mut self, type_name: &str, value: &Value) {
        *self.type_counts.entry(type_name.to_string()).or_insert(0) += 1;

        match value {
            Value::Null => {}
            Value::Array(items) => self.array_lengths.push(items.len() as u64),
            Value::Document(fields) => {
                self.nested_values += 1;
                self.nested_field_count = self.nested_field_count.max(fields.len() as u64);
            }
            scalar => self.observe_scalar(scalar),
        }
    }

    fn observe_scalar(&mut self, value: &Value) {
        self.scalar_values += 1;

        if matches!(value, Value::ObjectId(_)) {
            self.identifier_values += 1;
        }

        if matches!(
            value,
            Value::Int(_)
                | Value::Float(_)
                | Value::Decimal(_)
                | Value::DateTime(_)
                | Value::Date(_)
                | Value::Time(_)
        ) {
            self.ordered_values += 1;
            self.track_range(value);
        }

        if let Some(count) = self.distinct.get_mut(value) {
            *count += 1;
        } else if self.distinct.len() < DISTINCT_TRACKING_LIMIT {
            self.distinct.insert(value.clone(), 1);
        } else {
            self.distinct_capped = true;
        }
    }

    fn track_range(&mut self, value: &Value) {
        let comparable = numeric_key(value);

        let is_smaller = match (&self.minimum, comparable) {
            (None, _) => true,
            (Some(current), Some(key)) => numeric_key(current).is_some_and(|known| key < known),
            (Some(current), None) => value < current,
        };
        if is_smaller {
            self.minimum = Some(value.clone());
        }

        let is_larger = match (&self.maximum, comparable) {
            (None, _) => true,
            (Some(current), Some(key)) => numeric_key(current).is_some_and(|known| key > known),
            (Some(current), None) => value > current,
        };
        if is_larger {
            self.maximum = Some(value.clone());
        }
    }

    fn finish(self, path: String) -> FieldSchemaStats {
        let mut types: Vec<FieldTypeShare> = self
            .type_counts
            .into_iter()
            .map(|(type_name, count)| FieldTypeShare { type_name, count })
            .collect();
        types.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.type_name.cmp(&right.type_name))
        });

        let summary = summarize(
            self.presence,
            self.scalar_values,
            self.identifier_values,
            self.ordered_values,
            self.distinct,
            self.distinct_capped,
            self.minimum,
            self.maximum,
            self.array_lengths,
            self.nested_values,
            self.nested_field_count,
        );

        FieldSchemaStats {
            path,
            presence: self.presence,
            types,
            summary,
        }
    }
}

/// Numbers of different representations compare by magnitude, so a range
/// over a field that mixes integers, doubles and decimals stays meaningful.
fn numeric_key(value: &Value) -> Option<f64> {
    match value {
        Value::Int(integer) => Some(*integer as f64),
        Value::Float(float) => Some(*float),
        Value::Decimal(decimal) => decimal.parse::<f64>().ok(),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn summarize(
    presence: u64,
    scalar_values: u64,
    identifier_values: u64,
    ordered_values: u64,
    distinct: BTreeMap<Value, u64>,
    distinct_capped: bool,
    minimum: Option<Value>,
    maximum: Option<Value>,
    mut array_lengths: Vec<u64>,
    nested_values: u64,
    nested_field_count: u64,
) -> FieldValueSummary {
    let array_values = array_lengths.len() as u64;

    if array_values > 0 && array_values >= scalar_values && array_values >= nested_values {
        array_lengths.sort_unstable();
        let min = array_lengths.first().copied().unwrap_or_default();
        let max = array_lengths.last().copied().unwrap_or_default();
        let median = array_lengths
            .get(array_lengths.len() / 2)
            .copied()
            .unwrap_or_default();
        return FieldValueSummary::ArrayLength { min, max, median };
    }

    if nested_values > 0 && nested_values >= scalar_values {
        return FieldValueSummary::Nested {
            fields: nested_field_count,
        };
    }

    if scalar_values == 0 {
        return FieldValueSummary::Empty;
    }

    let distinct_count = distinct.len() as u64;
    let all_distinct = !distinct_capped && distinct_count == scalar_values && presence > 1;

    if identifier_values * 2 >= scalar_values && all_distinct {
        return FieldValueSummary::Unique;
    }

    if !distinct_capped && distinct.len() <= TOP_VALUE_LIMIT && !all_distinct {
        let mut shares: Vec<ValueShare> = distinct
            .into_iter()
            .map(|(value, count)| ValueShare { value, count })
            .collect();
        shares.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.value.cmp(&right.value))
        });
        return FieldValueSummary::TopValues(shares);
    }

    if ordered_values * 2 >= scalar_values
        && let (Some(min), Some(max)) = (minimum, maximum)
    {
        return FieldValueSummary::Range { min, max };
    }

    FieldValueSummary::Distinct {
        count: distinct_count,
        capped: distinct_capped,
    }
}

/// Collects field observations document by document.
#[derive(Default)]
pub struct SchemaSampleAccumulator {
    sampled_documents: u64,
    fields: HashMap<String, FieldAccumulator>,
    order: Vec<String>,
}

impl SchemaSampleAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the fields of one sampled document. A path reported several
    /// times in one document (fields of documents inside an array) counts
    /// once towards presence and once per value towards the type split.
    pub fn observe_document(&mut self, fields: impl IntoIterator<Item = SampledField>) {
        self.sampled_documents += 1;

        let mut present: HashSet<String> = HashSet::new();

        for field in fields {
            if !self.fields.contains_key(&field.path) {
                self.order.push(field.path.clone());
            }

            let accumulator = self.fields.entry(field.path.clone()).or_default();

            if present.insert(field.path) {
                accumulator.presence += 1;
            }

            accumulator.observe(&field.type_name, &field.value);
        }
    }

    pub fn sampled_documents(&self) -> u64 {
        self.sampled_documents
    }

    /// The finished statistics, `_id` first and every other field in the order
    /// it first appeared.
    pub fn finish(mut self, total_documents: Option<u64>) -> CollectionSchemaSample {
        let mut order = std::mem::take(&mut self.order);
        if let Some(position) = order.iter().position(|path| path == "_id") {
            let id = order.remove(position);
            order.insert(0, id);
        }

        let mut fields_by_path: BTreeMap<String, FieldAccumulator> = self.fields.drain().collect();

        let fields = order
            .into_iter()
            .filter_map(|path| {
                fields_by_path
                    .remove(&path)
                    .map(|accumulator| accumulator.finish(path))
            })
            .collect();

        CollectionSchemaSample {
            sampled_documents: self.sampled_documents,
            total_documents,
            fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe(accumulator: &mut SchemaSampleAccumulator, fields: &[(&str, &str, Value)]) {
        accumulator.observe_document(
            fields.iter().map(|(path, type_name, value)| {
                SampledField::new(*path, *type_name, value.clone())
            }),
        );
    }

    #[test]
    fn presence_counts_documents_not_values() {
        let mut accumulator = SchemaSampleAccumulator::new();
        observe(
            &mut accumulator,
            &[
                ("items.sku", "String", Value::Text("a".into())),
                ("items.sku", "String", Value::Text("b".into())),
            ],
        );
        observe(
            &mut accumulator,
            &[("status", "String", Value::Text("x".into()))],
        );

        let sample = accumulator.finish(None);
        let sku = sample.field("items.sku").expect("sku stats");

        assert_eq!(sample.sampled_documents, 2);
        assert_eq!(sku.presence, 1);
        assert_eq!(sku.value_count(), 2);
        assert!((sku.presence_ratio(sample.sampled_documents) - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn type_distribution_is_sorted_and_mixed_types_are_flagged() {
        let mut accumulator = SchemaSampleAccumulator::new();
        for _ in 0..3 {
            observe(
                &mut accumulator,
                &[("total", "Decimal128", Value::Decimal("1".into()))],
            );
        }
        observe(&mut accumulator, &[("total", "Double", Value::Float(1.0))]);

        let sample = accumulator.finish(Some(10));
        let total = sample.field("total").expect("total stats");

        assert_eq!(total.dominant_type(), Some("Decimal128"));
        assert_eq!(
            total.types,
            vec![
                FieldTypeShare {
                    type_name: "Decimal128".into(),
                    count: 3
                },
                FieldTypeShare {
                    type_name: "Double".into(),
                    count: 1
                },
            ]
        );
        assert!(total.has_mixed_types());
        assert_eq!(sample.total_documents, Some(10));
    }

    #[test]
    fn null_next_to_one_type_is_not_mixed() {
        let mut accumulator = SchemaSampleAccumulator::new();
        observe(
            &mut accumulator,
            &[("tier", "String", Value::Text("free".into()))],
        );
        observe(&mut accumulator, &[("tier", NULL_TYPE_NAME, Value::Null)]);

        let sample = accumulator.finish(None);

        assert!(!sample.field("tier").expect("tier").has_mixed_types());
    }

    #[test]
    fn few_values_are_listed_most_common_first() {
        let mut accumulator = SchemaSampleAccumulator::new();
        for status in ["paid", "paid", "failed", "paid", "shipped"] {
            observe(
                &mut accumulator,
                &[("status", "String", Value::Text(status.into()))],
            );
        }

        let sample = accumulator.finish(None);

        let FieldValueSummary::TopValues(shares) = &sample.field("status").expect("status").summary
        else {
            panic!("expected top values");
        };
        assert_eq!(shares.first().map(|share| share.count), Some(3));
        assert_eq!(
            shares.first().map(|share| share.value.clone()),
            Some(Value::Text("paid".into()))
        );
    }

    #[test]
    fn object_ids_are_unique_and_other_distinct_text_is_counted() {
        let mut accumulator = SchemaSampleAccumulator::new();
        for index in 0..10 {
            observe(
                &mut accumulator,
                &[
                    ("_id", "ObjectId", Value::ObjectId(format!("{index:024}"))),
                    ("email", "String", Value::Text(format!("user{index}@x.io"))),
                ],
            );
        }

        let sample = accumulator.finish(None);

        assert_eq!(
            sample.field("_id").expect("id").summary,
            FieldValueSummary::Unique
        );
        assert_eq!(
            sample.field("email").expect("email").summary,
            FieldValueSummary::Distinct {
                count: 10,
                capped: false
            }
        );
    }

    #[test]
    fn numbers_report_their_range_across_representations() {
        let mut accumulator = SchemaSampleAccumulator::new();
        let values = [
            Value::Int(5),
            Value::Float(99.5),
            Value::Decimal("1284.00".into()),
            Value::Int(-2),
            Value::Float(12.0),
            Value::Int(7),
            Value::Int(8),
        ];
        for value in values {
            observe(&mut accumulator, &[("total", "Number", value)]);
        }

        let sample = accumulator.finish(None);

        assert_eq!(
            sample.field("total").expect("total").summary,
            FieldValueSummary::Range {
                min: Value::Int(-2),
                max: Value::Decimal("1284.00".into()),
            }
        );
    }

    #[test]
    fn arrays_report_length_statistics() {
        let mut accumulator = SchemaSampleAccumulator::new();
        for length in [1usize, 2, 2, 14] {
            observe(
                &mut accumulator,
                &[("items", "Array", Value::Array(vec![Value::Null; length]))],
            );
        }

        let sample = accumulator.finish(None);

        assert_eq!(
            sample.field("items").expect("items").summary,
            FieldValueSummary::ArrayLength {
                min: 1,
                max: 14,
                median: 2
            }
        );
    }

    #[test]
    fn id_comes_first_and_the_rest_keep_document_order() {
        let mut accumulator = SchemaSampleAccumulator::new();
        observe(
            &mut accumulator,
            &[
                ("status", "String", Value::Text("a".into())),
                ("_id", "ObjectId", Value::ObjectId("0".repeat(24))),
                ("created_at", "Date", Value::Null),
            ],
        );

        let sample = accumulator.finish(None);

        assert_eq!(sample.field_paths(), vec!["_id", "status", "created_at"]);
    }
}
