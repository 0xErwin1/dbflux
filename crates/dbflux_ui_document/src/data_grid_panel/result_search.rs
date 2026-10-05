//! The result sub-toolbar of a query result (AppByzEditor, IslEditor): the
//! caption naming the statement behind the result, and "Search in results",
//! which narrows the rows to those with a cell containing the text.
//!
//! The search filters `DataGridPanel::result` itself, the way the local sort
//! reorders it, because every consumer of a grid row (the inspector, the
//! context menu's copy actions) reads the row at the same index of
//! `result.rows`. The rows the search hid are kept aside and come back when
//! the text is cleared or a new result arrives.

use super::{DataGridPanel, DataSource};
use dbflux_components::controls::{Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Icon;
use dbflux_components::result_panel::{SegmentPosition, ToolbarSegment};
use dbflux_components::tokens::EditorMetrics;
use dbflux_components::typography::AppFonts;
use dbflux_core::Row;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Search and caption state of a query-result panel.
#[derive(Default)]
pub(super) struct ResultSearch {
    /// The search field; only query results have one.
    input: Option<Entity<InputState>>,
    /// Lower-cased search text, empty when no search is active.
    needle: String,
    /// Every row of the result while a search hides some, in the order they
    /// had when the search started.
    all_rows: Option<Vec<Row>>,
    /// The local sort's original order for `all_rows`, when one was active.
    all_order: Option<Vec<usize>>,
    /// "Statement L7–16 · orders", set by the document that ran the query.
    caption: Option<SharedString>,
    _subscription: Option<Subscription>,
}

/// Whether any cell of `row` contains `needle` (already lower-cased).
fn row_matches(row: &Row, needle: &str) -> bool {
    row.iter()
        .any(|value| value.as_display_string().to_lowercase().contains(needle))
}

impl DataGridPanel {
    /// Creates the search field of a query-result panel.
    pub(super) fn install_result_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.source, DataSource::QueryResult { .. }) {
            return;
        }

        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.code.result.search_placeholder"))
        });

        let subscription = cx.subscribe(&input, |this, input, event: &InputEvent, cx| {
            if let InputEvent::Change = event {
                let text = input.read(cx).value().to_string();
                this.set_result_search(&text, cx);
            }
        });

        self.result_search.input = Some(input);
        self.result_search._subscription = Some(subscription);
    }

    /// Names the statement behind this result in the sub-toolbar.
    pub fn set_result_caption(&mut self, caption: Option<SharedString>, cx: &mut Context<Self>) {
        if self.result_search.caption != caption {
            self.result_search.caption = caption;
            cx.notify();
        }
    }

    pub fn result_caption(&self) -> Option<&SharedString> {
        self.result_search.caption.as_ref()
    }

    /// Shows only the rows with a cell containing `text` (case-insensitive);
    /// an empty `text` shows every row again.
    pub fn set_result_search(&mut self, text: &str, cx: &mut Context<Self>) {
        let needle = text.trim().to_lowercase();
        if needle == self.result_search.needle {
            return;
        }

        self.restore_searched_rows();
        self.result_search.needle = needle;
        self.apply_result_search();

        // The rows put back keep the order they had when the search started,
        // so an active sort is applied again over what is now shown.
        if let Some(sort) = self.grid_table.local_sort_state {
            self.apply_local_sort(sort.column_ix, sort.direction, cx);
        }

        self.chrome.derived_json = None;
        self.chrome.derived_text = None;
        self.pending.rebuild = true;
        cx.notify();
    }

    /// Hides the rows of a fresh `self.result` that do not match the active
    /// search. Called when a new result replaces the previous one, whose
    /// hidden rows no longer apply.
    pub(super) fn reapply_result_search_to_new_rows(&mut self) {
        self.result_search.all_rows = None;
        self.result_search.all_order = None;
        self.apply_result_search();
    }

    /// Puts back the rows the active search hid.
    fn restore_searched_rows(&mut self) {
        if let Some(rows) = self.result_search.all_rows.take() {
            self.result.rows = rows;
            self.grid_table.original_row_order = self.result_search.all_order.take();
        }
    }

    /// Keeps only the matching rows of `self.result`, setting the others
    /// aside. The local sort's original order is filtered alongside, so
    /// clearing that sort still restores the rows it permuted.
    fn apply_result_search(&mut self) {
        if self.result_search.needle.is_empty() {
            return;
        }

        let needle = self.result_search.needle.clone();
        let rows = std::mem::take(&mut self.result.rows);
        let order = self.grid_table.original_row_order.take();

        let kept: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row_matches(row, &needle))
            .map(|(index, _)| index)
            .collect();

        self.result.rows = kept
            .iter()
            .filter_map(|&index| rows.get(index).cloned())
            .collect();
        self.grid_table.original_row_order = order.as_ref().map(|order| {
            kept.iter()
                .filter_map(|&index| order.get(index).copied())
                .collect()
        });

        self.result_search.all_rows = Some(rows);
        self.result_search.all_order = order;
    }

    /// The sub-toolbar segments of a query result: the statement caption
    /// after the view switch, the search field at the right end.
    pub(super) fn result_toolbar_segments(entity: &Entity<Self>, cx: &App) -> Vec<ToolbarSegment> {
        let panel = entity.read(cx);
        let mut segments = Vec::new();

        if let Some(caption) = panel.result_search.caption.clone() {
            segments.push(ToolbarSegment {
                position: SegmentPosition::Left,
                index: 1,
                builder: Box::new(move |_window, cx| {
                    div()
                        .id("result-statement-caption")
                        .flex_shrink_0()
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(EditorMetrics::RESULT_CAPTION_FONT)
                        .text_color(cx.theme().muted_foreground)
                        .child(caption.clone())
                        .into_any_element()
                }),
            });
        }

        if let Some(input) = panel.result_search.input.clone() {
            segments.push(ToolbarSegment {
                position: SegmentPosition::Right,
                index: 0,
                builder: Box::new(move |_window, cx| {
                    div()
                        .ml_auto()
                        .flex_shrink_0()
                        .w(EditorMetrics::RESULT_SEARCH_WIDTH)
                        .child(
                            Input::new(&input)
                                .id("result-search")
                                .small()
                                .cleanable(true)
                                .prefix(
                                    Icon::new(AppIcon::Search)
                                        .size(EditorMetrics::RESULT_SEARCH_ICON)
                                        .color(cx.theme().muted_foreground),
                                ),
                        )
                        .into_any_element()
                }),
            });
        }

        segments
    }
}

#[cfg(test)]
mod tests {
    use super::super::DataGridPanel;
    use dbflux_components::result_panel::SegmentPosition;
    use dbflux_components::theme;
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, SortDirection, Value};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use gpui_component::Root;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    fn people() -> QueryResult {
        let columns = ["id", "name"]
            .into_iter()
            .map(|name| ColumnMeta {
                name: name.to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            })
            .collect();
        let rows = [("1", "Alice"), ("2", "Bob"), ("3", "Malik"), ("4", "Carol")]
            .into_iter()
            .map(|(id, name)| vec![Value::Text(id.to_string()), Value::Text(name.to_string())])
            .collect();

        QueryResult::table(columns, rows, None, Duration::ZERO)
    }

    fn rendered_result(cx: &mut TestAppContext) -> (Entity<DataGridPanel>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                AppStateEntity::new_with_storage_runtime(
                    StorageRuntime::in_memory().expect("isolated storage runtime"),
                )
                .expect("test storage setup")
            })
        });

        let holder = Rc::new(RefCell::new(None));
        let handle = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                DataGridPanel::new_for_result(
                    Arc::new(people()),
                    "SELECT id, name FROM people".to_string(),
                    None,
                    app_state.clone(),
                    window,
                    cx,
                )
            });
            handle.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });
        window.run_until_parked();

        let panel = holder.borrow().clone().expect("panel should be created");
        (panel, window)
    }

    fn names(panel: &DataGridPanel) -> Vec<String> {
        panel
            .result
            .rows
            .iter()
            .filter_map(|row| row.get(1))
            .map(Value::as_display_string)
            .collect()
    }

    #[gpui::test]
    fn search_keeps_only_the_rows_with_a_matching_cell(cx: &mut TestAppContext) {
        let (panel, window) = rendered_result(cx);

        window.update(|_, app| {
            panel.update(app, |panel, cx| panel.set_result_search("AL", cx));
        });
        window.run_until_parked();

        window.update(|_, app| {
            let panel = panel.read(app);
            assert_eq!(names(panel), vec!["Alice", "Malik"]);

            let grid_rows = panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .row_count();
            assert_eq!(grid_rows, 2, "the grid shows only the matching rows");
        });

        window.update(|_, app| {
            panel.update(app, |panel, cx| panel.set_result_search("", cx));
        });
        window.run_until_parked();

        window.update(|_, app| {
            assert_eq!(
                names(panel.read(app)),
                vec!["Alice", "Bob", "Malik", "Carol"],
                "clearing the search brings every row back in its order"
            );
        });
    }

    #[gpui::test]
    fn clearing_the_search_keeps_the_local_sort(cx: &mut TestAppContext) {
        let (panel, window) = rendered_result(cx);

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.set_result_search("a", cx);
                panel.apply_local_sort(1, SortDirection::Descending, cx);
                panel.set_result_search("", cx);
            });
        });
        window.run_until_parked();

        window.update(|_, app| {
            assert_eq!(
                names(panel.read(app)),
                vec!["Malik", "Carol", "Bob", "Alice"],
                "the rows that come back are sorted like the ones shown"
            );
        });
    }

    #[gpui::test]
    fn a_new_result_is_searched_with_the_active_text(cx: &mut TestAppContext) {
        let (panel, window) = rendered_result(cx);

        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel.set_result_search("bob", cx);
                panel.set_query_result(
                    Arc::new(people()),
                    "SELECT id, name FROM people".to_string(),
                    None,
                    cx,
                );
            });
        });
        window.run_until_parked();

        window.update(|_, app| {
            assert_eq!(names(panel.read(app)), vec!["Bob"]);
        });
    }

    #[gpui::test]
    fn the_sub_toolbar_carries_the_caption_and_the_search_field(cx: &mut TestAppContext) {
        let (panel, window) = rendered_result(cx);

        window.update(|_, app| {
            let segments = DataGridPanel::result_toolbar_segments(&panel, app);
            let positions: Vec<SegmentPosition> =
                segments.iter().map(|segment| segment.position).collect();
            assert_eq!(
                positions,
                vec![SegmentPosition::Right],
                "a result without a caption shows only the search field"
            );

            panel.update(app, |panel, cx| {
                panel.set_result_caption(Some("Statement L1–1 · people".into()), cx);
            });

            let segments = DataGridPanel::result_toolbar_segments(&panel, app);
            let positions: Vec<(SegmentPosition, u16)> = segments
                .iter()
                .map(|segment| (segment.position, segment.index))
                .collect();
            assert_eq!(
                positions,
                vec![(SegmentPosition::Left, 1), (SegmentPosition::Right, 0)]
            );
        });
    }

    #[gpui::test]
    fn a_query_result_footer_says_it_is_read_only(cx: &mut TestAppContext) {
        let (_panel, window) = rendered_result(cx);

        assert!(
            window.debug_bounds("footer-read-only").is_some(),
            "a result that cannot be edited says so in its footer"
        );
    }
}
