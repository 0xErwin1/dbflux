use dbflux_core::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Samples spanning every comparison shape of `Value`: each variant, the
/// 2^53 boundary where `as f64` promotion loses the low bit, both zero
/// signs, two NaN bit patterns, the infinities, and pairs that differ only
/// in the numeric representation of a nested field.
fn samples() -> Vec<(&'static str, Value)> {
    let document = |value: Value| Value::Document(BTreeMap::from([("qty".to_string(), value)]));

    vec![
        ("null", Value::Null),
        ("bool_true", Value::Bool(true)),
        ("bool_false", Value::Bool(false)),
        ("int_zero", Value::Int(0)),
        ("int_one", Value::Int(1)),
        ("int_negative", Value::Int(-1)),
        ("int_2_pow_53", Value::Int(9_007_199_254_740_992)),
        ("int_2_pow_53_plus_1", Value::Int(9_007_199_254_740_993)),
        ("int_max", Value::Int(i64::MAX)),
        ("int_min", Value::Int(i64::MIN)),
        ("float_zero", Value::Float(0.0)),
        ("float_negative_zero", Value::Float(-0.0)),
        ("float_one", Value::Float(1.0)),
        ("float_fraction", Value::Float(1.5)),
        ("float_2_pow_53", Value::Float(9_007_199_254_740_992.0)),
        ("float_nan", Value::Float(f64::NAN)),
        (
            "float_nan_other_bits",
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        ),
        ("float_infinity", Value::Float(f64::INFINITY)),
        ("float_negative_infinity", Value::Float(f64::NEG_INFINITY)),
        ("text", Value::Text("1".into())),
        ("decimal", Value::Decimal("1.00".into())),
        ("array_int", Value::Array(vec![Value::Int(1)])),
        ("array_float", Value::Array(vec![Value::Float(1.0)])),
        ("document_int", document(Value::Int(1))),
        ("document_float", document(Value::Float(1.0))),
        ("object_id", Value::ObjectId("a".repeat(24))),
    ]
}

#[test]
fn equality_matches_ordering_over_the_sample_grid() {
    let samples = samples();

    for (a_name, a) in &samples {
        for (b_name, b) in &samples {
            assert_eq!(
                a == b,
                a.cmp(b) == Ordering::Equal,
                "{a_name} == {b_name} must agree with cmp"
            );
        }
    }
}

#[test]
fn every_sample_equals_itself() {
    for (name, value) in samples() {
        let same = value.clone();
        assert!(value == same, "{name} must equal itself, NaN included");
    }
}

#[test]
fn equality_is_transitive_over_the_sample_grid() {
    let samples = samples();

    for (a_name, a) in &samples {
        for (b_name, b) in &samples {
            if a == b {
                for (c_name, c) in &samples {
                    if b == c {
                        assert!(
                            a == c,
                            "{a_name} == {b_name} == {c_name}, but {a_name} != {c_name}"
                        );
                    }
                }
            }
        }
    }
}
