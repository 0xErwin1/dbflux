//! Translation between the driver-agnostic document edit and schema model in
//! `dbflux_core` and MongoDB: typed values to BSON, document identities to
//! filters, field patches to `$set` / `$unset`, and sampled documents to field
//! observations.

use bson::{Bson, Document, doc};
use dbflux_core::{
    DbError, DocumentIdentity, DocumentPatch, SampledField, Value, field_path_to_dotted,
};

/// Converts a typed value to the BSON the server stores, keeping the type the
/// grid showed: decimals stay `Decimal128`, date-times stay `Date`, object ids
/// stay `ObjectId`. Text is never reinterpreted as another type.
pub(crate) fn value_to_bson(value: &Value) -> Result<Bson, DbError> {
    Ok(match value {
        Value::Null => Bson::Null,
        Value::Bool(boolean) => Bson::Boolean(*boolean),
        Value::Int(integer) => match i32::try_from(*integer) {
            Ok(small) => Bson::Int32(small),
            Err(_) => Bson::Int64(*integer),
        },
        Value::Float(float) => Bson::Double(*float),
        Value::Text(text) => Bson::String(text.clone()),
        Value::Bytes(bytes) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: bytes.clone(),
        }),
        Value::Json(text) => {
            let json: serde_json::Value = serde_json::from_str(text)
                .map_err(|error| DbError::query_failed(format!("Invalid JSON value: {error}")))?;
            value_to_bson(&dbflux_core::document_json_to_value(&json))?
        }
        Value::Decimal(decimal) => {
            let parsed = decimal.parse::<bson::Decimal128>().map_err(|error| {
                DbError::query_failed(format!("Invalid decimal \"{decimal}\": {error}"))
            })?;
            Bson::Decimal128(parsed)
        }
        Value::DateTime(date_time) => {
            Bson::DateTime(bson::DateTime::from_millis(date_time.timestamp_millis()))
        }
        Value::Date(date) => {
            let midnight = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::query_failed(format!("Invalid date {date}")))?;
            Bson::DateTime(bson::DateTime::from_millis(
                midnight.and_utc().timestamp_millis(),
            ))
        }
        Value::Time(time) => Bson::String(time.to_string()),
        Value::Array(items) => Bson::Array(
            items
                .iter()
                .map(value_to_bson)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Document(fields) => {
            let mut document = Document::new();
            for (key, field) in fields {
                document.insert(key.clone(), value_to_bson(field)?);
            }
            Bson::Document(document)
        }
        Value::ObjectId(object_id) => {
            let parsed = bson::oid::ObjectId::parse_str(object_id).map_err(|error| {
                DbError::query_failed(format!("Invalid ObjectId \"{object_id}\": {error}"))
            })?;
            Bson::ObjectId(parsed)
        }
        Value::Unsupported(type_name) => {
            return Err(DbError::query_failed(format!(
                "A {type_name} value cannot be written back"
            )));
        }
    })
}

/// The BSON stored at `path` in `document`, following array indexes.
pub(crate) fn bson_at_path<'a>(document: &'a Document, path: &[String]) -> Option<&'a Bson> {
    let (first, rest) = path.split_first()?;
    let mut cursor = document.get(first)?;

    for segment in rest {
        cursor = match cursor {
            Bson::Document(fields) => fields.get(segment)?,
            Bson::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }

    Some(cursor)
}

fn numeric_mismatch(value: &str, target: &str) -> DbError {
    DbError::query_failed(format!(
        "{value} cannot be stored in a field that holds {target} without changing its type"
    ))
}

fn integer_as(existing: &Bson, integer: i64) -> Result<Option<Bson>, DbError> {
    Ok(match existing {
        Bson::Int32(_) => {
            Some(Bson::Int32(i32::try_from(integer).map_err(|_| {
                numeric_mismatch(&integer.to_string(), "Int32")
            })?))
        }
        Bson::Int64(_) => Some(Bson::Int64(integer)),
        Bson::Double(_) => Some(Bson::Double(integer as f64)),
        Bson::Decimal128(_) => Some(Bson::Decimal128(
            integer
                .to_string()
                .parse::<bson::Decimal128>()
                .map_err(|error| DbError::query_failed(error.to_string()))?,
        )),
        _ => None,
    })
}

/// Converts `value` to BSON of the type the field already holds on the
/// server (`existing`), so an edit never changes a field's numeric width:
/// Int32 stays Int32, Int64 stays Int64, Double stays Double and Decimal128
/// stays Decimal128. Nested documents and arrays are matched field by field.
/// Only a field the server does not have yet gets its type inferred.
pub(crate) fn conform_to_existing(value: &Value, existing: Option<&Bson>) -> Result<Bson, DbError> {
    let Some(existing) = existing else {
        return value_to_bson(value);
    };

    match value {
        Value::Int(integer) => {
            if let Some(conformed) = integer_as(existing, *integer)? {
                return Ok(conformed);
            }
        }
        Value::Float(float) => match existing {
            Bson::Double(_) => return Ok(Bson::Double(*float)),
            Bson::Decimal128(_) => {
                return float
                    .to_string()
                    .parse::<bson::Decimal128>()
                    .map(Bson::Decimal128)
                    .map_err(|error| DbError::query_failed(error.to_string()));
            }
            Bson::Int32(_) | Bson::Int64(_) => {
                if float.fract() != 0.0 || !float.is_finite() {
                    return Err(numeric_mismatch(&float.to_string(), "an integer"));
                }
                if let Some(conformed) = integer_as(existing, *float as i64)? {
                    return Ok(conformed);
                }
            }
            _ => {}
        },
        Value::Decimal(decimal) => match existing {
            Bson::Double(_) => {
                return decimal
                    .parse::<f64>()
                    .map(Bson::Double)
                    .map_err(|_| numeric_mismatch(decimal, "Double"));
            }
            Bson::Int32(_) | Bson::Int64(_) => {
                let integer = decimal
                    .parse::<i64>()
                    .map_err(|_| numeric_mismatch(decimal, "an integer"))?;
                if let Some(conformed) = integer_as(existing, integer)? {
                    return Ok(conformed);
                }
            }
            _ => {}
        },
        Value::Document(fields) => {
            if let Bson::Document(existing_fields) = existing {
                let mut document = Document::new();
                for (key, field) in fields {
                    document.insert(
                        key.clone(),
                        conform_to_existing(field, existing_fields.get(key))?,
                    );
                }
                return Ok(Bson::Document(document));
            }
        }
        Value::Array(items) => {
            if let Bson::Array(existing_items) = existing {
                return items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| conform_to_existing(item, existing_items.get(index)))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Bson::Array);
            }
        }
        _ => {}
    }

    value_to_bson(value)
}

/// The filter that selects exactly the document with `identity`.
pub(crate) fn identity_filter(identity: &DocumentIdentity) -> Result<Document, DbError> {
    if identity.is_empty() {
        return Err(DbError::query_failed(
            "A document edit needs the document's identity (_id)".to_string(),
        ));
    }

    let mut filter = Document::new();
    for (field, value) in identity {
        filter.insert(field.clone(), value_to_bson(value)?);
    }

    Ok(filter)
}

/// `{ $set: {...}, $unset: {...} }` for a field patch, with dotted paths.
/// `current` is the server copy; each set value keeps the type of the value it
/// replaces.
pub(crate) fn patch_update_document(
    patch: &DocumentPatch,
    current: Option<&Document>,
) -> Result<Document, DbError> {
    if patch.is_empty() {
        return Err(DbError::query_failed(
            "The document patch has no changes".to_string(),
        ));
    }

    let mut update = Document::new();

    if !patch.set.is_empty() {
        let mut set = Document::new();
        for (path, value) in &patch.set {
            let existing = current.and_then(|document| bson_at_path(document, path));
            set.insert(
                field_path_to_dotted(path),
                conform_to_existing(value, existing)?,
            );
        }
        update.insert("$set", set);
    }

    if !patch.unset.is_empty() {
        let mut unset = Document::new();
        for path in &patch.unset {
            unset.insert(field_path_to_dotted(path), "");
        }
        update.insert("$unset", unset);
    }

    Ok(update)
}

/// The replacement body for a whole-document replace. The identity fields are
/// dropped: MongoDB refuses a replacement that changes `_id`, and it keeps the
/// existing one when the body leaves it out.
pub(crate) fn replacement_document(
    document: &Value,
    identity: &DocumentIdentity,
    current: Option<&Document>,
) -> Result<Document, DbError> {
    let existing = current.map(|document| Bson::Document(document.clone()));
    let Bson::Document(mut replacement) = conform_to_existing(document, existing.as_ref())? else {
        return Err(DbError::query_failed(
            "A replacement must be a document".to_string(),
        ));
    };

    for (field, _) in identity {
        replacement.remove(field);
    }

    Ok(replacement)
}

/// Type name shown for a BSON value in the schema view.
pub(crate) fn schema_type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "Double",
        Bson::String(_) => "String",
        Bson::Document(_) => "Object",
        Bson::Array(_) => "Array",
        Bson::Binary(_) => "Binary",
        Bson::ObjectId(_) => "ObjectId",
        Bson::Boolean(_) => "Boolean",
        Bson::DateTime(_) => "Date",
        Bson::Null | Bson::Undefined => dbflux_core::NULL_TYPE_NAME,
        Bson::RegularExpression(_) => "Regex",
        Bson::Int32(_) => "Int32",
        Bson::Int64(_) => "Int64",
        Bson::Timestamp(_) => "Timestamp",
        Bson::Decimal128(_) => "Decimal128",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "JavaScript",
        Bson::Symbol(_) => "Symbol",
        Bson::MaxKey => "MaxKey",
        Bson::MinKey => "MinKey",
        Bson::DbPointer(_) => "DBPointer",
    }
}

/// Every field of a sampled document, nested documents and documents inside
/// arrays included (`items.sku`), with its BSON type name.
pub(crate) fn sampled_fields(
    document: &Document,
    to_value: impl Fn(&Bson) -> Value + Copy,
) -> Vec<SampledField> {
    let mut fields = Vec::new();
    collect_sampled_fields(document, "", to_value, &mut fields);
    fields
}

fn collect_sampled_fields(
    document: &Document,
    prefix: &str,
    to_value: impl Fn(&Bson) -> Value + Copy,
    fields: &mut Vec<SampledField>,
) {
    for (key, bson) in document {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        fields.push(SampledField::new(
            path.clone(),
            schema_type_name(bson),
            to_value(bson),
        ));

        match bson {
            Bson::Document(nested) => collect_sampled_fields(nested, &path, to_value, fields),
            Bson::Array(items) => {
                for item in items {
                    if let Bson::Document(nested) = item {
                        collect_sampled_fields(nested, &path, to_value, fields);
                    }
                }
            }
            _ => {}
        }
    }
}

/// The aggregation pipeline that draws a random sample, optionally from the
/// documents a filter matches.
pub(crate) fn sample_pipeline(filter: Option<Document>, sample_size: u32) -> Vec<Document> {
    let mut pipeline = Vec::new();

    if let Some(filter) = filter.filter(|filter| !filter.is_empty()) {
        pipeline.push(doc! { "$match": filter });
    }

    pipeline.push(doc! { "$sample": { "size": i64::from(sample_size.max(1)) } });
    pipeline
}

/// A user pipeline with a trailing `$limit` of `limit + 1`, so the result can
/// tell a pipeline that yields exactly `limit` documents from one that yields
/// more. A pipeline that ends in `$out` or `$merge` is left as is: those
/// stages must come last and return no documents.
pub(crate) fn limited_aggregate_pipeline(mut pipeline: Vec<Document>, limit: u32) -> Vec<Document> {
    let writes_output = pipeline
        .last()
        .is_some_and(|stage| stage.contains_key("$out") || stage.contains_key("$merge"));

    if !writes_output {
        pipeline.push(doc! { "$limit": i64::from(limit) + 1 });
    }

    pipeline
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::parse_field_path;
    use std::collections::BTreeMap;

    #[test]
    fn value_to_bson_keeps_decimal_date_and_object_id_types() {
        let decimal = value_to_bson(&Value::Decimal("119.00".into())).unwrap();
        assert!(matches!(decimal, Bson::Decimal128(_)));
        assert_eq!(decimal.to_string(), "119.00");

        let object_id = value_to_bson(&Value::ObjectId("66f0c2a1e4b0c2a1e4b0c2a1".into())).unwrap();
        assert!(matches!(object_id, Bson::ObjectId(_)));

        let text = value_to_bson(&Value::Text("66f0c2a1e4b0c2a1e4b0c2a1".into())).unwrap();
        assert!(matches!(text, Bson::String(_)));

        let date = value_to_bson(&Value::DateTime(
            chrono::DateTime::from_timestamp_millis(1_000).unwrap(),
        ))
        .unwrap();
        assert_eq!(date, Bson::DateTime(bson::DateTime::from_millis(1_000)));
    }

    #[test]
    fn small_integers_are_stored_as_int32() {
        assert_eq!(value_to_bson(&Value::Int(7)).unwrap(), Bson::Int32(7));
        assert_eq!(
            value_to_bson(&Value::Int(i64::from(i32::MAX) + 1)).unwrap(),
            Bson::Int64(i64::from(i32::MAX) + 1)
        );
    }

    #[test]
    fn patch_becomes_set_and_unset_with_dotted_paths() {
        let patch = DocumentPatch {
            set: vec![(
                parse_field_path("price.amount"),
                Value::Decimal("119.00".into()),
            )],
            unset: vec![parse_field_path("legacy")],
        };

        let update = patch_update_document(&patch, None).unwrap();

        let set = update.get_document("$set").unwrap();
        assert!(matches!(set.get("price.amount"), Some(Bson::Decimal128(_))));
        let unset = update.get_document("$unset").unwrap();
        assert_eq!(unset.get("legacy"), Some(&Bson::String(String::new())));
    }

    #[test]
    fn empty_patch_is_rejected() {
        assert!(patch_update_document(&DocumentPatch::default(), None).is_err());
    }

    #[test]
    fn replacement_drops_the_identity_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("_id".to_string(), Value::Int(1));
        fields.insert("name".to_string(), Value::Text("lamp".into()));

        let replacement = replacement_document(
            &Value::Document(fields),
            &vec![("_id".into(), Value::Int(1))],
            None,
        )
        .unwrap();

        assert!(!replacement.contains_key("_id"));
        assert_eq!(replacement.get_str("name").unwrap(), "lamp");
    }

    #[test]
    fn sampled_fields_walk_nested_documents_and_arrays() {
        let document = doc! {
            "_id": 1,
            "customer": { "email": "a@b.c" },
            "items": [ { "sku": "x" }, { "sku": "y" } ],
        };

        let fields = sampled_fields(&document, |_| Value::Null);
        let paths: Vec<(&str, &str)> = fields
            .iter()
            .map(|field| (field.path.as_str(), field.type_name.as_str()))
            .collect();

        assert_eq!(
            paths,
            vec![
                ("_id", "Int32"),
                ("customer", "Object"),
                ("customer.email", "String"),
                ("items", "Array"),
                ("items.sku", "String"),
                ("items.sku", "String"),
            ]
        );
    }

    #[test]
    fn edited_numbers_keep_the_width_the_field_had() {
        let int = Value::Int(7);

        assert_eq!(
            conform_to_existing(&int, Some(&Bson::Int32(1))).unwrap(),
            Bson::Int32(7)
        );
        assert_eq!(
            conform_to_existing(&int, Some(&Bson::Int64(1))).unwrap(),
            Bson::Int64(7)
        );
        assert_eq!(
            conform_to_existing(&int, Some(&Bson::Double(1.5))).unwrap(),
            Bson::Double(7.0)
        );

        let decimal =
            conform_to_existing(&int, Some(&Bson::Decimal128("1.00".parse().unwrap()))).unwrap();
        assert!(matches!(decimal, Bson::Decimal128(_)));
        assert_eq!(decimal.to_string(), "7");
    }

    #[test]
    fn a_large_integer_stays_int64_and_does_not_overflow_int32() {
        let large = Value::Int(i64::from(i32::MAX) + 1);

        assert_eq!(
            conform_to_existing(&large, Some(&Bson::Int64(1))).unwrap(),
            Bson::Int64(i64::from(i32::MAX) + 1)
        );
        assert!(conform_to_existing(&large, Some(&Bson::Int32(1))).is_err());
    }

    #[test]
    fn doubles_and_decimals_keep_their_type() {
        assert_eq!(
            conform_to_existing(&Value::Float(2.5), Some(&Bson::Double(1.0))).unwrap(),
            Bson::Double(2.5)
        );
        assert_eq!(
            conform_to_existing(&Value::Float(3.0), Some(&Bson::Int64(1))).unwrap(),
            Bson::Int64(3)
        );
        assert!(conform_to_existing(&Value::Float(3.5), Some(&Bson::Int32(1))).is_err());

        let decimal = conform_to_existing(
            &Value::Decimal("119.00".into()),
            Some(&Bson::Decimal128("405.00".parse().unwrap())),
        )
        .unwrap();
        assert!(matches!(decimal, Bson::Decimal128(_)));
        assert_eq!(
            conform_to_existing(&Value::Decimal("2.5".into()), Some(&Bson::Double(1.0))).unwrap(),
            Bson::Double(2.5)
        );
    }

    #[test]
    fn a_new_field_gets_an_inferred_type() {
        assert_eq!(
            conform_to_existing(&Value::Int(7), None).unwrap(),
            Bson::Int32(7)
        );
        assert_eq!(
            conform_to_existing(&Value::Int(i64::from(i32::MAX) + 1), None).unwrap(),
            Bson::Int64(i64::from(i32::MAX) + 1)
        );
    }

    #[test]
    fn patch_and_replacement_follow_the_server_types() {
        let current = doc! {
            "_id": 1,
            "stock": Bson::Int64(3),
            "price": { "amount": Bson::Double(1.5), "qty": Bson::Int32(2) },
            "items": [ { "qty": Bson::Int64(1) } ],
        };

        let patch = DocumentPatch {
            set: vec![
                (parse_field_path("stock"), Value::Int(4)),
                (parse_field_path("price.qty"), Value::Int(5)),
                (parse_field_path("items.0.qty"), Value::Int(6)),
            ],
            unset: Vec::new(),
        };
        let update = patch_update_document(&patch, Some(&current)).unwrap();
        let set = update.get_document("$set").unwrap();
        assert_eq!(set.get("stock"), Some(&Bson::Int64(4)));
        assert_eq!(set.get("price.qty"), Some(&Bson::Int32(5)));
        assert_eq!(set.get("items.0.qty"), Some(&Bson::Int64(6)));

        let mut price = BTreeMap::new();
        price.insert("amount".to_string(), Value::Int(2));
        price.insert("qty".to_string(), Value::Int(9));
        let mut fields = BTreeMap::new();
        fields.insert("stock".to_string(), Value::Int(8));
        fields.insert("price".to_string(), Value::Document(price));

        let replacement = replacement_document(
            &Value::Document(fields),
            &vec![("_id".into(), Value::Int(1))],
            Some(&current),
        )
        .unwrap();
        assert_eq!(replacement.get("stock"), Some(&Bson::Int64(8)));
        let price = replacement.get_document("price").unwrap();
        assert_eq!(price.get("amount"), Some(&Bson::Double(2.0)));
        assert_eq!(price.get("qty"), Some(&Bson::Int32(9)));
    }

    #[test]
    fn aggregate_pipelines_get_a_limit_one_past_the_cap() {
        let pipeline = limited_aggregate_pipeline(vec![doc! { "$match": { "paid": true } }], 50);

        assert_eq!(
            pipeline,
            vec![
                doc! { "$match": { "paid": true } },
                doc! { "$limit": 51_i64 }
            ]
        );
    }

    #[test]
    fn aggregate_pipelines_that_write_keep_their_last_stage() {
        let out = limited_aggregate_pipeline(vec![doc! { "$out": "archive" }], 50);
        let merge = limited_aggregate_pipeline(
            vec![
                doc! { "$match": {} },
                doc! { "$merge": { "into": "totals" } },
            ],
            50,
        );

        assert_eq!(out, vec![doc! { "$out": "archive" }]);
        assert_eq!(merge.len(), 2);
    }

    #[test]
    fn sample_pipeline_matches_before_sampling() {
        let pipeline = sample_pipeline(Some(doc! { "status": "paid" }), 500);

        assert_eq!(pipeline.len(), 2);
        assert!(pipeline[0].contains_key("$match"));
        assert_eq!(
            pipeline[1]
                .get_document("$sample")
                .unwrap()
                .get_i64("size")
                .unwrap(),
            500
        );
    }
}
