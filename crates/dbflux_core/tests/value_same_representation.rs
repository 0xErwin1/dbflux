use dbflux_core::{DocumentServerState, Value, assess_server_change};
use std::collections::BTreeMap;

fn document(entries: &[(&str, Value)]) -> Value {
    Value::Document(
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

/// `same_representation` is reflexive and symmetric, and every pair it joins
/// is also equal under `==`: representation equality is a finer equivalence
/// than the one `Ord` defines.
#[test]
fn same_representation_is_reflexive_symmetric_and_implies_equality() {
    let samples = vec![
        Value::Null,
        Value::Bool(true),
        Value::Int(1),
        Value::Float(1.0),
        Value::Float(f64::NAN),
        Value::Text("1".into()),
        Value::Decimal("1.00".into()),
        Value::Array(vec![Value::Int(1)]),
        Value::Array(vec![Value::Float(1.0)]),
        document(&[("qty", Value::Int(1))]),
        document(&[("qty", Value::Float(1.0))]),
    ];

    for (index, a) in samples.iter().enumerate() {
        assert!(
            a.same_representation(a),
            "{a:?} must keep its representation"
        );

        for b in samples.iter().skip(index) {
            assert_eq!(
                a.same_representation(b),
                b.same_representation(a),
                "{a:?} and {b:?} must agree in both directions"
            );

            if a.same_representation(b) {
                assert!(
                    a == b,
                    "{a:?} and {b:?} share a representation, so they are equal"
                );
            }
        }
    }
}

/// The comparison reaches through arrays and nested documents: a numeric
/// promotion at any depth is a representation change, and an identical
/// nesting of identical representations is not.
#[test]
fn same_representation_compares_through_arrays_and_documents() {
    let promoted = Value::Float(1.0);

    assert!(
        !Value::Array(vec![Value::Int(1)])
            .same_representation(&Value::Array(vec![promoted.clone()]))
    );
    assert!(
        !document(&[("qty", Value::Int(1))])
            .same_representation(&document(&[("qty", promoted.clone())]))
    );
    assert!(
        !document(&[("price", document(&[("amount", Value::Int(1))]),)])
            .same_representation(&document(&[("price", document(&[("amount", promoted)]),)]))
    );

    assert!(
        Value::Array(vec![Value::Int(1)]).same_representation(&Value::Array(vec![Value::Int(1)]))
    );
    assert!(
        document(&[("qty", Value::Int(1))])
            .same_representation(&document(&[("qty", Value::Int(1))]))
    );
}

/// A server-side edit that only changes a numeric representation
/// (`1` stored as `1.0`) is still a change the edit must be checked against,
/// not an unmodified document.
#[test]
fn a_representation_only_server_edit_is_a_change() {
    let loaded = document(&[
        ("_id", Value::ObjectId("a".repeat(24))),
        ("qty", Value::Int(1)),
    ]);
    let current = document(&[
        ("_id", Value::ObjectId("a".repeat(24))),
        ("qty", Value::Float(1.0)),
    ]);

    let state = assess_server_change(&loaded, Some(&current), &[], false);

    let DocumentServerState::Changed(change) = state else {
        panic!("expected a server change, got {state:?}");
    };
    assert_eq!(change.changed_paths, vec![vec!["qty".to_string()]]);
    assert!(change.can_apply_on_top(), "the edit writes other paths");
}
