//! Rendering and table-model construction for `DumpAnalysisDocument`.
//!
//! Layout, top to bottom: a summary header (total keys, total serialized
//! bytes, and the analyzer's `size_caveat()` shown verbatim), then two
//! stacked read-only data tables — "Largest keys" and "By prefix".

use std::sync::Arc;

use dbflux_components::components::data_table::TableModel;
use dbflux_components::components::data_table::model::{
    CellValue, ColumnKind, ColumnSpec, RowData,
};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{DumpKeyEntry, DumpPrefixEntry, SortDirection};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::{DumpAnalysisDocument, DumpAnalysisPhase};

/// Builds the "Largest keys" table model, in the order the entries are given
/// (the analyzer already returns them sorted descending by serialized size).
pub(super) fn largest_keys_table_model(entries: &[DumpKeyEntry]) -> TableModel {
    let columns = vec![
        ColumnSpec {
            id: Arc::from("key"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.largest_keys.column.key"
            )),
            kind: ColumnKind::Text,
            align: TextAlign::Left,
            type_name: Arc::from(""),
        },
        ColumnSpec {
            id: Arc::from("type"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.largest_keys.column.type"
            )),
            kind: ColumnKind::Text,
            align: TextAlign::Left,
            type_name: Arc::from(""),
        },
        ColumnSpec {
            id: Arc::from("size"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.largest_keys.column.size"
            )),
            kind: ColumnKind::Integer,
            align: TextAlign::Right,
            type_name: Arc::from(""),
        },
        ColumnSpec {
            id: Arc::from("expiry"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.largest_keys.column.expiry"
            )),
            kind: ColumnKind::Text,
            align: TextAlign::Left,
            type_name: Arc::from(""),
        },
    ];

    let rows = entries
        .iter()
        .map(|entry| RowData {
            cells: vec![
                CellValue::text(&entry.key),
                CellValue::text(&entry.type_name),
                CellValue::text(&crate::buckets_table::format_bytes(entry.serialized_bytes)),
                CellValue::text(&format_expiry(entry.expires_at_ms)),
            ],
        })
        .collect();

    TableModel::new(columns, rows)
}

/// Builds the "By prefix" table model, in the order the entries are given
/// (the analyzer already returns them sorted descending by serialized size).
pub(super) fn prefix_rollup_table_model(entries: &[DumpPrefixEntry]) -> TableModel {
    let columns = vec![
        ColumnSpec {
            id: Arc::from("prefix"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.by_prefix.column.prefix"
            )),
            kind: ColumnKind::Text,
            align: TextAlign::Left,
            type_name: Arc::from(""),
        },
        ColumnSpec {
            id: Arc::from("count"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.by_prefix.column.count"
            )),
            kind: ColumnKind::Integer,
            align: TextAlign::Right,
            type_name: Arc::from(""),
        },
        ColumnSpec {
            id: Arc::from("size"),
            title: Arc::from(dbflux_i18n::t!(
                "document.dump_analysis.done.by_prefix.column.size"
            )),
            kind: ColumnKind::Integer,
            align: TextAlign::Right,
            type_name: Arc::from(""),
        },
    ];

    let rows = entries
        .iter()
        .map(|entry| RowData {
            cells: vec![
                CellValue::text(&entry.prefix),
                CellValue::text(&entry.key_count.to_string()),
                CellValue::text(&crate::buckets_table::format_bytes(entry.serialized_bytes)),
            ],
        })
        .collect();

    TableModel::new(columns, rows)
}

/// Formats an optional absolute expiry (epoch milliseconds) as a readable
/// UTC timestamp, or the "no expiry" label when the key carries none.
pub(super) fn format_expiry(expires_at_ms: Option<i64>) -> String {
    match expires_at_ms {
        Some(ms) => match chrono::DateTime::from_timestamp_millis(ms) {
            Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            None => dbflux_i18n::t!("document.dump_analysis.done.largest_keys.no_expiry"),
        },
        None => dbflux_i18n::t!("document.dump_analysis.done.largest_keys.no_expiry"),
    }
}

/// Sorts the largest-keys entries in place by the given column index and
/// direction. Unknown column indices leave the order unchanged.
pub(super) fn sort_largest_keys(
    entries: &mut [DumpKeyEntry],
    column_ix: usize,
    direction: SortDirection,
) {
    match column_ix {
        0 => entries.sort_by(|a, b| a.key.cmp(&b.key)),
        1 => entries.sort_by(|a, b| a.type_name.cmp(&b.type_name)),
        2 => entries.sort_by_key(|entry| entry.serialized_bytes),
        3 => entries.sort_by_key(|entry| entry.expires_at_ms),
        _ => return,
    }

    if direction == SortDirection::Descending {
        entries.reverse();
    }
}

/// Sorts the prefix-rollup entries in place by the given column index and
/// direction. Unknown column indices leave the order unchanged.
pub(super) fn sort_prefix_rollup(
    entries: &mut [DumpPrefixEntry],
    column_ix: usize,
    direction: SortDirection,
) {
    match column_ix {
        0 => entries.sort_by(|a, b| a.prefix.cmp(&b.prefix)),
        1 => entries.sort_by_key(|entry| entry.key_count),
        2 => entries.sort_by_key(|entry| entry.serialized_bytes),
        _ => return,
    }

    if direction == SortDirection::Descending {
        entries.reverse();
    }
}

impl Render for DumpAnalysisDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let body: AnyElement = match &self.phase {
            DumpAnalysisPhase::Parsing {
                bytes_read,
                total_bytes,
            } => div()
                .flex()
                .flex_col()
                .gap(Spacing::MD)
                .p(Spacing::MD)
                .child(Text::body(dbflux_i18n::t!(
                    "document.dump_analysis.parsing.title"
                )))
                .child(Text::caption(
                    crate::labels::dump_analysis_parsing_progress(*bytes_read, *total_bytes),
                ))
                .child(
                    dbflux_components::controls::Button::new(
                        "dump-analysis-cancel",
                        dbflux_i18n::t!("document.dump_analysis.parsing.cancel"),
                    )
                    .on_click(cx.listener(|this, _, _, cx| this.cancel_analysis(cx))),
                )
                .into_any_element(),

            DumpAnalysisPhase::Cancelled => div()
                .flex()
                .flex_col()
                .gap(Spacing::MD)
                .p(Spacing::MD)
                .child(Text::body(dbflux_i18n::t!(
                    "document.dump_analysis.cancelled.title"
                )))
                .into_any_element(),

            DumpAnalysisPhase::Failed(error) => div()
                .flex()
                .flex_col()
                .gap(Spacing::MD)
                .p(Spacing::MD)
                .child(Text::body(crate::labels::dump_analysis_error_message(error)).danger())
                .into_any_element(),

            DumpAnalysisPhase::Done(report) => {
                let mut column = div().flex().flex_col().size_full().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::XS)
                        .p(Spacing::MD)
                        .border_b_1()
                        .border_color(theme.border)
                        .child(Text::body(crate::labels::dump_analysis_summary_line(
                            report.total_keys,
                            report.total_serialized_bytes,
                        )))
                        .when(self.multiple_analyzers_matched, |el| {
                            el.child(Text::caption(dbflux_i18n::t!(
                                "document.dump_analysis.multiple_analyzers_note",
                                analyzer = self.analyzer_display_name
                            )))
                        })
                        .child(Text::caption(self.size_caveat).warning()),
                );

                if let Some(largest_keys_table) = self.largest_keys_table.clone() {
                    column = column.child(
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_h(px(0.0))
                            .child(div().px(Spacing::MD).pt(Spacing::MD).child(Text::body(
                                dbflux_i18n::t!("document.dump_analysis.done.largest_keys.title"),
                            )))
                            .child(div().flex_1().min_h(px(0.0)).child(largest_keys_table)),
                    );
                }

                if let Some(prefix_rollup_table) = self.prefix_rollup_table.clone() {
                    column = column.child(
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_h(px(0.0))
                            .child(div().px(Spacing::MD).pt(Spacing::MD).child(Text::body(
                                dbflux_i18n::t!("document.dump_analysis.done.by_prefix.title"),
                            )))
                            .child(div().flex_1().min_h(px(0.0)).child(prefix_rollup_table)),
                    );
                }

                column.into_any_element()
            }
        };

        div()
            .id("dump-analysis-document")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(body)
    }
}
