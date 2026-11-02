//! The AxisBar pill row, adapted from `dbflux_core` columns and the product's
//! own icons and catalogue.
//!
//! The element itself lives in `bolt::chart::axis_bar`; it reads the
//! product-neutral column contract and takes its icons and translated labels
//! as a supplied [`AxisBarChrome`]. These wrappers keep the historical
//! signatures — `&[ColumnMeta]` and no chrome argument — so call sites keep
//! compiling untouched, and answer the chrome from `AppIcon` and
//! `dbflux_i18n` exactly as the inline code did before the move.

use gpui::{App, Window};
use gpui::{IntoElement, SharedString};

use bolt::chart::{AggKind, AxisBarChrome, BindingSpec};
use dbflux_core::ColumnMeta;

pub use bolt::chart::axis_bar::{AxisPickerOption, AxisPill};

use crate::icons::AppIcon;
use crate::semantic::ChartColors;

/// Translated label shown on the "Agg" pill for a given aggregation kind.
fn agg_label(kind: AggKind) -> SharedString {
    match kind {
        AggKind::None => dbflux_i18n::t!("chart.axis_bar.aggregation.none").into(),
        AggKind::Sum => dbflux_i18n::t!("chart.axis_bar.aggregation.sum").into(),
        AggKind::Avg => dbflux_i18n::t!("chart.axis_bar.aggregation.avg").into(),
        AggKind::Min => dbflux_i18n::t!("chart.axis_bar.aggregation.min").into(),
        AggKind::Max => dbflux_i18n::t!("chart.axis_bar.aggregation.max").into(),
    }
}

/// The chrome the AxisBar draws: the product's icons and translated labels.
pub fn axis_bar_chrome() -> AxisBarChrome {
    AxisBarChrome {
        x_time_icon: AppIcon::Clock.into(),
        x_numeric_icon: AppIcon::Hash.into(),
        y_icon: AppIcon::Hash.into(),
        group_icon: AppIcon::Tag.into(),
        agg_icon: AppIcon::Sigma.into(),
        chevron: AppIcon::ChevronDown.into(),
        group_role: dbflux_i18n::t!("chart.axis_bar.group").into(),
        agg_label,
    }
}

/// The rows the picker of `pill` lists, in display order: X takes time and
/// numeric columns, Y numeric columns, Group "none" then text columns, Agg
/// every aggregation kind. Column roles come from `ColumnKind` only.
pub fn axis_picker_options(
    pill: AxisPill,
    bindings: &BindingSpec,
    columns: &[ColumnMeta],
) -> Vec<AxisPickerOption> {
    bolt::chart::axis_bar::axis_picker_options(
        pill,
        bindings,
        &super::compat::chart_columns(columns),
    )
}

/// Render the AxisBar pill row.
///
/// # Parameters
///
/// - `bindings`: current `BindingSpec`; drives pill labels.
/// - `columns`: column metadata from the current `QueryResult`.
/// - `open_pill`: which pill's picker is currently shown (`None` = all closed).
/// - `highlighted`: the row of the open picker the keyboard is on, an index
///   into [`axis_picker_options`] for that pill.
/// - `on_pill_click`: called when the user clicks a pill header (to open/close
///   its picker). Receives the clicked `AxisPill`.
/// - `on_x_select`: called when the user picks a column for the X axis.
///   Receives the column index.
/// - `on_y_toggle`: called when the user toggles a Y column. Receives the
///   column index and the new checked state.
/// - `on_group_select`: called when the user picks a group-by column.
///   Receives `Some(col_idx)` for a column or `None` for "none".
/// - `on_agg_select`: called when the user picks an aggregation kind.
///
/// The high parameter count is intentional: each callback has a distinct type
/// signature that cannot be collapsed without boxing (and thus heap allocation
/// per render). The `#[allow]` suppresses the clippy lint for this case.
#[allow(clippy::too_many_arguments)]
pub fn axis_bar_element<FPill, FX, FY, FGroup, FAgg>(
    bindings: &BindingSpec,
    columns: &[ColumnMeta],
    open_pill: Option<AxisPill>,
    highlighted: Option<usize>,
    colors: &ChartColors,
    on_pill_click: FPill,
    on_x_select: FX,
    on_y_toggle: FY,
    on_group_select: FGroup,
    on_agg_select: FAgg,
) -> impl IntoElement
where
    FPill: Fn(AxisPill, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FX: Fn(usize, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FY: Fn(usize, bool, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FGroup: Fn(Option<usize>, &mut Window, &mut App) + Clone + Send + Sync + 'static,
    FAgg: Fn(AggKind, &mut Window, &mut App) + Clone + Send + Sync + 'static,
{
    let chrome = axis_bar_chrome();

    bolt::chart::axis_bar::axis_bar_element(
        bindings,
        &super::compat::chart_columns(columns),
        &chrome,
        open_pill,
        highlighted,
        colors,
        on_pill_click,
        on_x_select,
        on_y_toggle,
        on_group_select,
        on_agg_select,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::ColumnKind;

    fn make_col(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_owned(),
            type_name: String::new(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    #[test]
    fn picker_options_follow_the_column_kinds_of_the_core_metadata() {
        let cols = [
            make_col("ts", ColumnKind::Timestamp),
            make_col("cpu", ColumnKind::Float),
            make_col("host", ColumnKind::Text),
            make_col("seq", ColumnKind::Integer),
        ];
        let bindings = BindingSpec {
            x: 0,
            y: vec![1],
            group_by: None,
            filter: None,
            aggregation: AggKind::None,
        };

        assert_eq!(
            axis_picker_options(AxisPill::X, &bindings, &cols),
            vec![
                AxisPickerOption::X(0),
                AxisPickerOption::X(1),
                AxisPickerOption::X(3)
            ]
        );
        assert_eq!(
            axis_picker_options(AxisPill::Group, &bindings, &cols),
            vec![
                AxisPickerOption::Group(None),
                AxisPickerOption::Group(Some(2))
            ]
        );
    }

    #[test]
    fn agg_label_matches_kind() {
        let pairs: &[(AggKind, &str)] = &[
            (AggKind::None, "none"),
            (AggKind::Sum, "sum"),
            (AggKind::Avg, "avg"),
            (AggKind::Min, "min"),
            (AggKind::Max, "max"),
        ];
        for (kind, expected) in pairs {
            assert_eq!(agg_label(*kind), *expected, "mismatch for {:?}", kind);
        }
    }

    #[test]
    fn chart_axis_bar_aggregation_keys_resolve_in_both_locales() {
        let keys = [
            "chart.axis_bar.aggregation.none",
            "chart.axis_bar.aggregation.sum",
            "chart.axis_bar.aggregation.avg",
            "chart.axis_bar.aggregation.min",
            "chart.axis_bar.aggregation.max",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(
                !en.is_empty() && !en.starts_with("en."),
                "en missing for {key}, got {en:?}"
            );
            assert!(
                !es.is_empty() && !es.starts_with("es."),
                "es missing for {key}, got {es:?}"
            );
        }
    }

    #[test]
    fn agg_label_sum_diverges_between_locales() {
        let en = dbflux_i18n::t!("chart.axis_bar.aggregation.sum", locale = "en");
        let es = dbflux_i18n::t!("chart.axis_bar.aggregation.sum", locale = "es");
        assert_eq!(en, "sum");
        assert_eq!(es, "suma");
        assert_ne!(en, es);
    }

    #[test]
    fn axis_bar_group_key_resolves() {
        let en = dbflux_i18n::t!("chart.axis_bar.group", locale = "en");
        let es = dbflux_i18n::t!("chart.axis_bar.group", locale = "es");
        assert_eq!(en, "Group");
        assert_eq!(es, "Grupo");
        assert_ne!(en, es);
    }
}
