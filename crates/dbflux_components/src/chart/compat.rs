//! The adapter from `dbflux_core` column metadata to the chart column
//! contract `bolt::chart` reads.
//!
//! The chart in `bolt` never sees a `dbflux_core` type: its pickers and
//! detectors read `ChartColumn { name, kind }`. The product adapts its own
//! metadata once, here, and every wrapper reuses this conversion.

use bolt::chart::ChartColumn;
use bolt::data::ColumnKind as BoltColumnKind;
use dbflux_core::ColumnKind;

/// Map a `dbflux_core::ColumnKind` onto Bolt's column kind.
///
/// Core's kinds are a subset of Bolt's: every variant has a direct
/// counterpart, so the mapping is total and lossless.
pub(crate) fn chart_kind(kind: ColumnKind) -> BoltColumnKind {
    match kind {
        ColumnKind::Timestamp => BoltColumnKind::Timestamp,
        ColumnKind::Float => BoltColumnKind::Float,
        ColumnKind::Integer => BoltColumnKind::Integer,
        ColumnKind::Text => BoltColumnKind::Text,
        // `ColumnKind` is `#[non_exhaustive]`; any kind a newer core adds has
        // no Bolt counterpart yet, and an unclassifiable column reads as
        // unknown to a chart exactly as it does to a grid.
        _ => BoltColumnKind::Unknown,
    }
}

/// Adapt a column-metadata list to the chart column contract.
pub(crate) fn chart_columns(columns: &[dbflux_core::ColumnMeta]) -> Vec<ChartColumn> {
    columns
        .iter()
        .map(|meta| ChartColumn::new(meta.name.clone(), chart_kind(meta.kind)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(name: &str, kind: ColumnKind) -> dbflux_core::ColumnMeta {
        dbflux_core::ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn every_core_kind_maps_onto_its_bolt_counterpart() {
        assert_eq!(chart_kind(ColumnKind::Timestamp), BoltColumnKind::Timestamp);
        assert_eq!(chart_kind(ColumnKind::Float), BoltColumnKind::Float);
        assert_eq!(chart_kind(ColumnKind::Integer), BoltColumnKind::Integer);
        assert_eq!(chart_kind(ColumnKind::Text), BoltColumnKind::Text);
        assert_eq!(chart_kind(ColumnKind::Unknown), BoltColumnKind::Unknown);
    }

    #[test]
    fn chart_columns_keep_the_names_and_order_of_the_metadata() {
        let columns = vec![
            meta("ts", ColumnKind::Timestamp),
            meta("cpu", ColumnKind::Float),
        ];

        let adapted = chart_columns(&columns);
        assert_eq!(
            adapted,
            vec![
                ChartColumn::new("ts", BoltColumnKind::Timestamp),
                ChartColumn::new("cpu", BoltColumnKind::Float),
            ]
        );
    }
}
