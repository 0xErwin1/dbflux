//! Regression probes for issue #1311: `Value`'s ordering and equality at the
//! numeric boundaries.

use dbflux_core::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;

const TWO_POW_53: i64 = 9_007_199_254_740_992;
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

fn document(value: Value) -> Value {
    Value::Document(BTreeMap::from([("qty".to_string(), value)]))
}

fn grid() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Bool(true),
        Value::Int(i64::MIN),
        Value::Int(i64::MIN + 1),
        Value::Int(-1),
        Value::Int(0),
        Value::Int(1),
        Value::Int(2),
        Value::Int(TWO_POW_53),
        Value::Int(TWO_POW_53 + 1),
        Value::Int(i64::MAX - 1023),
        Value::Int(i64::MAX),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Float(-1.5),
        Value::Float(1.0),
        Value::Float(1.5),
        Value::Float(TWO_POW_53 as f64),
        Value::Float((TWO_POW_53 + 2) as f64),
        Value::Float(9_223_372_036_854_774_784.0),
        Value::Float(TWO_POW_63),
        Value::Float(-TWO_POW_63),
        Value::Float(-9_223_372_036_854_777_856.0),
        Value::Float(f64::MAX),
        Value::Float(f64::MIN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(f64::NAN),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Float(f64::from_bits(0xfff8_0000_0000_0000)),
        Value::Text("1".into()),
        Value::Decimal("1.00".into()),
        Value::Array(vec![Value::Int(1)]),
        Value::Array(vec![Value::Float(1.0)]),
        document(Value::Int(1)),
        document(Value::Float(1.0)),
        Value::ObjectId("a".repeat(24)),
    ]
}

/// `cmp` is a total order, including its equal class (`<=` transitivity),
/// which is what promoting an `i64` through `as f64` breaks first.
#[test]
fn cmp_is_a_total_order_over_every_numeric_boundary() {
    let grid = grid();

    for a in &grid {
        assert_eq!(a.cmp(a), Ordering::Equal, "{a:?}");
    }
    for a in &grid {
        for b in &grid {
            assert_eq!(a.cmp(b), b.cmp(a).reverse(), "{a:?}/{b:?}");
        }
    }
    for a in &grid {
        for b in &grid {
            if a.cmp(b) == Ordering::Greater {
                continue;
            }
            for c in &grid {
                if b.cmp(c) != Ordering::Greater {
                    assert_ne!(a.cmp(c), Ordering::Greater, "{a:?} <= {b:?} <= {c:?}");
                }
            }
        }
    }
}

/// Equality is exactly `cmp == Equal`, and an equivalence.
#[test]
fn equality_is_exactly_cmp_is_equal_and_an_equivalence() {
    let grid = grid();

    for a in &grid {
        assert!(a == a, "{a:?} must equal itself");
        for b in &grid {
            assert_eq!(a == b, a.cmp(b) == Ordering::Equal, "{a:?}/{b:?}");
            assert_eq!(a == b, b == a, "{a:?}/{b:?}");
            if a == b {
                for c in &grid {
                    if b == c {
                        assert!(a == c, "{a:?} == {b:?} == {c:?}");
                    }
                }
            }
        }
    }
}

/// The exact `i64`/`f64` boundaries, where promoting through `as f64` rounded
/// `i64::MAX` up to `2^63` and merged neighbours above `2^53`.
#[test]
fn int_float_boundaries_are_exact() {
    let cases = [
        (
            Value::Int(i64::MAX),
            Value::Float(TWO_POW_63),
            Ordering::Less,
        ),
        (
            Value::Int(i64::MAX - 1023),
            Value::Float(9_223_372_036_854_774_784.0),
            Ordering::Equal,
        ),
        (
            Value::Int(i64::MIN),
            Value::Float(-TWO_POW_63),
            Ordering::Equal,
        ),
        (
            Value::Int(i64::MIN),
            Value::Float(-9_223_372_036_854_777_856.0),
            Ordering::Greater,
        ),
        (
            Value::Int(TWO_POW_53),
            Value::Float(TWO_POW_53 as f64),
            Ordering::Equal,
        ),
        (
            Value::Int(TWO_POW_53 + 1),
            Value::Float(TWO_POW_53 as f64),
            Ordering::Greater,
        ),
        (Value::Int(1), Value::Float(1.5), Ordering::Less),
        (Value::Int(2), Value::Float(1.5), Ordering::Greater),
        (Value::Int(-1), Value::Float(-1.5), Ordering::Greater),
        (Value::Int(-2), Value::Float(-1.5), Ordering::Less),
        (Value::Int(0), Value::Float(0.0), Ordering::Equal),
        (Value::Int(0), Value::Float(-0.0), Ordering::Greater),
        (Value::Float(-0.0), Value::Float(0.0), Ordering::Less),
        (
            Value::Int(i64::MAX),
            Value::Float(f64::INFINITY),
            Ordering::Less,
        ),
        (
            Value::Int(i64::MIN),
            Value::Float(f64::NEG_INFINITY),
            Ordering::Greater,
        ),
        (Value::Int(0), Value::Float(f64::NAN), Ordering::Less),
        (
            Value::Int(0),
            Value::Float(f64::from_bits(0xfff8_0000_0000_0000)),
            Ordering::Greater,
        ),
    ];

    for (left, right, expected) in cases {
        assert_eq!(left.cmp(&right), expected, "{left:?} vs {right:?}");
        assert_eq!(
            right.cmp(&left),
            expected.reverse(),
            "{left:?} vs {right:?} reversed"
        );
        assert_eq!(
            left == right,
            expected == Ordering::Equal,
            "{left:?} == {right:?}"
        );
    }
}
