use std::collections::BTreeSet;
use std::time::Duration;

use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
use dbflux_transfer::PageShape;

use super::document_shape::DocumentPageShape;

fn column(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: String::new(),
        kind: ColumnKind::Unknown,
        nullable: true,
        is_primary_key: false,
    }
}

fn address(city: &str, zip: Option<&str>) -> Value {
    let mut fields = vec![("city".to_string(), Value::Text(city.to_string()))];
    if let Some(zip) = zip {
        fields.push(("zip".to_string(), Value::Text(zip.to_string())));
    }
    Value::Document(fields.into_iter().collect())
}

#[test]
fn document_pages_keep_the_first_page_columns_and_report_later_fields() {
    let first = QueryResult::json(
        vec![column("name"), column("_id"), column("address")],
        vec![vec![
            Value::Text("a".into()),
            Value::Int(1),
            address("Lima", None),
        ]],
        Duration::ZERO,
    );
    let later = QueryResult::json(
        vec![column("_id"), column("address"), column("email")],
        vec![vec![
            Value::Int(2),
            address("Quito", Some("170")),
            Value::Text("b@x".into()),
        ]],
        Duration::ZERO,
    );

    let expanded: BTreeSet<String> = ["address".to_string()].into_iter().collect();
    let mut shape = DocumentPageShape::new(expanded, vec!["_id".to_string()]);

    let columns = shape.columns(&first);
    let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["_id", "name", "address.city"],
        "`_id` first and the expanded object as its child fields, as the grid shows them"
    );
    assert!(columns[0].is_primary_key);

    assert_eq!(
        shape.rows(first),
        vec![vec![
            Value::Int(1),
            Value::Text("a".into()),
            Value::Text("Lima".into()),
        ]]
    );
    assert_eq!(
        shape.rows(later),
        vec![vec![
            Value::Int(2),
            Value::Null,
            Value::Text("Quito".into())
        ]],
        "a missing field writes null"
    );
    assert_eq!(
        shape.dropped_columns(),
        vec!["address.zip".to_string(), "email".to_string()]
    );
}
