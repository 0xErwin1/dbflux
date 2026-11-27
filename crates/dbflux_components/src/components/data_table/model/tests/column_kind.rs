use super::*;
use dbflux_core::ColumnMeta;
use gpui::TextAlign;

fn spec_for(type_name: &str) -> ColumnSpec {
    ColumnSpec::from(&ColumnMeta {
        name: "value".to_string(),
        type_name: type_name.to_string(),
        kind: dbflux_core::ColumnKind::Unknown,
        nullable: true,
        is_primary_key: false,
    })
}

#[test]
fn non_scalar_and_non_numeric_types_are_not_numeric_columns() {
    for type_name in ["point", "interval", "int4range", "int[]", "_int4"] {
        let spec = spec_for(type_name);

        assert!(
            !matches!(spec.kind, ColumnKind::Integer | ColumnKind::Float),
            "{type_name} must not be a numeric kind, got {:?}",
            spec.kind
        );
        assert_eq!(spec.align, TextAlign::Left, "{type_name} is left-aligned");
    }
}

#[test]
fn scalar_numeric_types_stay_right_aligned() {
    let expectations = [
        ("int4", ColumnKind::Integer),
        ("bigint", ColumnKind::Integer),
        ("serial", ColumnKind::Integer),
        ("numeric(10,2)", ColumnKind::Float),
        ("double precision", ColumnKind::Float),
        ("money", ColumnKind::Float),
        ("smallmoney", ColumnKind::Float),
    ];

    for (type_name, kind) in expectations {
        let spec = spec_for(type_name);

        assert_eq!(spec.kind, kind, "{type_name}");
        assert_eq!(spec.align, TextAlign::Right, "{type_name} is right-aligned");
    }
}

#[test]
fn json_bool_and_bytes_keep_their_kind() {
    for (type_name, kind) in [
        ("jsonb", ColumnKind::Json),
        ("bool", ColumnKind::Bool),
        ("bytea", ColumnKind::Bytes),
    ] {
        let spec = spec_for(type_name);

        assert_eq!(spec.kind, kind, "{type_name}");
        assert_eq!(spec.align, TextAlign::Left, "{type_name} is left-aligned");
    }
}
