//! Shapes a nearest-neighbor result for the grid: the distance first, the
//! anchor row pinned on top, and distances people can read.

use dbflux_core::{
    ColumnKind, ColumnMeta, NEAREST_DISTANCE_COLUMN, QueryResult, Value, VectorDistance,
};

/// Decimal places a distance is shown with.
const DISTANCE_DECIMALS: usize = 4;

/// The column header of the distance column.
pub(crate) fn distance_column_label(distance: VectorDistance) -> String {
    match distance {
        VectorDistance::L2 => dbflux_i18n::t!("document.data.grid.nearest.column.l2"),
        VectorDistance::Cosine => dbflux_i18n::t!("document.data.grid.nearest.column.cosine"),
        VectorDistance::InnerProduct => {
            dbflux_i18n::t!("document.data.grid.nearest.column.inner_product")
        }
    }
}

/// A distance as the grid shows it.
///
/// pgvector's inner-product operator returns the negative product so that
/// ascending order puts the closest row first; the grid shows the product
/// itself. A distance that is not a number (cosine against an all-zero
/// vector) shows a dash.
pub(crate) fn format_distance(value: &Value, distance: VectorDistance) -> Value {
    let number = match value {
        Value::Float(number) => Some(*number),
        Value::Int(number) => Some(*number as f64),
        Value::Decimal(text) | Value::Text(text) => text.parse::<f64>().ok(),
        _ => None,
    };

    let Some(number) = number else {
        return value.clone();
    };

    if !number.is_finite() {
        return Value::Text("\u{2014}".to_string());
    }

    let shown = match distance {
        VectorDistance::InnerProduct => -number,
        VectorDistance::L2 | VectorDistance::Cosine => number,
    };

    // `-0.0000` reads as a sign error; a product of exactly zero shows 0.
    let shown = if shown == 0.0 { 0.0 } else { shown };

    Value::Text(format!("{shown:.DISTANCE_DECIMALS$}"))
}

/// The neighbors as the grid shows them: the distance column first, under a
/// header that names the distance, then the table's columns; the anchor row
/// first, marked in the distance column, then the neighbors closest first.
///
/// `anchor_row` holds the anchor's values by column name. A result without
/// the distance column (a driver that names it otherwise) is returned with
/// only the anchor row added.
pub(crate) fn present_neighbors(
    result: QueryResult,
    anchor_row: &[(String, Value)],
    distance: VectorDistance,
) -> QueryResult {
    let mut result = result;
    // The query appends the distance after `*`, so the last column with the
    // name is the computed one; a table column of the same name keeps its own.
    let distance_index = result
        .columns
        .iter()
        .rposition(|column| column.name == NEAREST_DISTANCE_COLUMN);

    let order: Vec<usize> = match distance_index {
        Some(index) => std::iter::once(index)
            .chain((0..result.columns.len()).filter(|position| *position != index))
            .collect(),
        None => (0..result.columns.len()).collect(),
    };

    let columns: Vec<ColumnMeta> = order
        .iter()
        .filter_map(|index| result.columns.get(*index).cloned())
        .enumerate()
        .map(|(position, column)| {
            if position == 0 && distance_index.is_some() {
                ColumnMeta {
                    name: distance_column_label(distance),
                    kind: ColumnKind::Text,
                    ..column
                }
            } else {
                column
            }
        })
        .collect();

    let anchor_label = Value::Text(dbflux_i18n::t!("document.data.grid.nearest.anchor"));
    let anchor = order
        .iter()
        .map(|index| {
            if Some(*index) == distance_index {
                return anchor_label.clone();
            }

            result
                .columns
                .get(*index)
                .and_then(|column| {
                    anchor_row
                        .iter()
                        .find(|(name, _)| *name == column.name)
                        .map(|(_, value)| value.clone())
                })
                .unwrap_or(Value::Null)
        })
        .collect();

    let mut rows = Vec::with_capacity(result.rows.len() + 1);
    rows.push(anchor);
    rows.extend(result.rows.drain(..).map(|row| {
        order
            .iter()
            .map(|index| {
                let value = row.get(*index).cloned().unwrap_or(Value::Null);
                if Some(*index) == distance_index {
                    format_distance(&value, distance)
                } else {
                    value
                }
            })
            .collect()
    }));

    result.columns = columns;
    result.rows = rows;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn meta(name: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "text".into(),
            nullable: true,
            is_primary_key: false,
            kind: ColumnKind::Text,
        }
    }

    fn neighbors(distances: &[f64]) -> QueryResult {
        QueryResult::table(
            vec![meta("id"), meta("label"), meta(NEAREST_DISTANCE_COLUMN)],
            distances
                .iter()
                .enumerate()
                .map(|(index, distance)| {
                    vec![
                        Value::Int(index as i64 + 10),
                        Value::Text(format!("n{index}")),
                        Value::Float(*distance),
                    ]
                })
                .collect(),
            None,
            Duration::ZERO,
        )
    }

    fn anchor() -> Vec<(String, Value)> {
        vec![
            ("id".to_string(), Value::Int(2)),
            ("label".to_string(), Value::Text("second".into())),
            ("embedding".to_string(), Value::Text("[0,1,0]".into())),
        ]
    }

    #[test]
    fn the_distance_comes_first_under_a_header_that_names_it() {
        let shown = present_neighbors(neighbors(&[1.5]), &anchor(), VectorDistance::L2);
        let names: Vec<&str> = shown.columns.iter().map(|c| c.name.as_str()).collect();

        assert_eq!(names, vec!["L2 distance", "id", "label"]);
    }

    #[test]
    fn the_anchor_row_is_pinned_first_and_marked() {
        let shown = present_neighbors(neighbors(&[1.5, 2.0]), &anchor(), VectorDistance::L2);

        assert_eq!(shown.rows.len(), 3);
        assert_eq!(
            shown.rows[0],
            vec![
                Value::Text("anchor".into()),
                Value::Int(2),
                Value::Text("second".into())
            ]
        );
        assert_eq!(shown.rows[1][1], Value::Int(10));
        assert_eq!(shown.rows[2][1], Value::Int(11));
    }

    #[test]
    fn distances_show_four_decimals() {
        let shown = present_neighbors(neighbors(&[1.234_567_891]), &anchor(), VectorDistance::L2);

        assert_eq!(shown.rows[1][0], Value::Text("1.2346".into()));
    }

    #[test]
    fn inner_product_shows_the_product_itself() {
        let shown = present_neighbors(
            neighbors(&[-15.0, -12.25]),
            &anchor(),
            VectorDistance::InnerProduct,
        );

        assert_eq!(shown.columns[0].name, "Inner product");
        assert_eq!(shown.rows[1][0], Value::Text("15.0000".into()));
        assert_eq!(shown.rows[2][0], Value::Text("12.2500".into()));
    }

    #[test]
    fn a_distance_that_is_not_a_number_shows_a_dash() {
        let shown = present_neighbors(
            neighbors(&[0.25, f64::NAN]),
            &anchor(),
            VectorDistance::Cosine,
        );

        assert_eq!(shown.rows[1][0], Value::Text("0.2500".into()));
        assert_eq!(shown.rows[2][0], Value::Text("\u{2014}".into()));
    }

    #[test]
    fn a_zero_inner_product_has_no_sign() {
        assert_eq!(
            format_distance(&Value::Float(0.0), VectorDistance::InnerProduct),
            Value::Text("0.0000".into())
        );
    }

    #[test]
    fn a_table_column_named_like_the_distance_keeps_its_name_and_place() {
        let result = QueryResult::table(
            vec![
                meta("id"),
                meta(NEAREST_DISTANCE_COLUMN),
                meta(NEAREST_DISTANCE_COLUMN),
            ],
            vec![vec![
                Value::Int(10),
                Value::Text("own".into()),
                Value::Float(2.5),
            ]],
            None,
            Duration::ZERO,
        );

        let shown = present_neighbors(result, &anchor(), VectorDistance::L2);
        let names: Vec<&str> = shown.columns.iter().map(|c| c.name.as_str()).collect();

        assert_eq!(names, vec!["L2 distance", "id", NEAREST_DISTANCE_COLUMN]);
        assert_eq!(
            shown.rows[1],
            vec![
                Value::Text("2.5000".into()),
                Value::Int(10),
                Value::Text("own".into())
            ]
        );
    }
}
