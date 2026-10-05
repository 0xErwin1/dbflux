//! Statement awareness of the code editor (AppByzEditor, IslEditor): the
//! statement gutter with a run marker per statement, the statement count in
//! the toolbar, and the statement caption above each result.
//!
//! Statements always come from the effective language's statement splitter
//! (`QueryLanguage::statement_ranges`), the same one that counts the
//! statements of a script before it runs. A language without a splitter
//! shows no gutter, no count and no caption.

use super::*;
use dbflux_components::tokens::{ChromeColors, EditorMetrics};
use gpui_base::input::{GutterStatementStyle, GutterStatements};
use std::ops::Range;

/// Gutter colours and sizes from the active theme: line-2 markers, the tint
/// for the statement under the cursor, the byzantine bar.
fn gutter_style(theme: &gpui_component::theme::Theme) -> GutterStatementStyle {
    let tint = ChromeColors::tint(theme);

    GutterStatementStyle {
        marker_slot_width: EditorMetrics::GUTTER_MARKER_SLOT,
        marker_size: EditorMetrics::GUTTER_MARKER_SIZE,
        marker_icon_size: EditorMetrics::GUTTER_MARKER_ICON,
        bar_gap: EditorMetrics::GUTTER_BAR_GAP,
        bar_width: EditorMetrics::GUTTER_BAR_WIDTH,
        marker: theme.input,
        active_marker: tint,
        active_line_number: tint,
        bar: theme.primary,
        statement_fill: tint.opacity(EditorMetrics::STATEMENT_FILL_ALPHA),
        cursor_line_fill: tint.opacity(EditorMetrics::CURSOR_LINE_FILL_ALPHA),
    }
}

/// The toolbar's run summary (AppByzEditor): "3 statements · last run
/// 0.32 s", either part alone when the other is unknown, `None` when both
/// are.
pub(super) fn run_summary_label(
    statement_count: Option<usize>,
    last_run_seconds: Option<f64>,
) -> Option<String> {
    let statements = statement_count.map(|count| {
        if count == 1 {
            dbflux_i18n::t!("document.code.toolbar.statements.one", count = count)
        } else {
            dbflux_i18n::t!("document.code.toolbar.statements.many", count = count)
        }
    });
    let last_run = last_run_seconds.map(crate::labels::code_toolbar_last_run_label);

    match (statements, last_run) {
        (Some(statements), Some(last_run)) => Some(format!("{statements} · {last_run}")),
        (Some(part), None) | (None, Some(part)) => Some(part),
        (None, None) => None,
    }
}

/// One-based line of byte `offset` in `text`.
fn line_at(text: &str, offset: usize) -> usize {
    text.get(..offset)
        .map_or(0, |prefix| prefix.matches('\n').count())
        + 1
}

/// Caption of one statement: its lines in the buffer and the tables it
/// reads or writes, "Statement L7–16 · orders, customers".
fn statement_caption(buffer: &str, range: Range<usize>) -> Option<String> {
    let statement = buffer.get(range.clone())?;

    let first_line = line_at(buffer, range.start);
    let last_line = first_line + statement.matches('\n').count();

    let mut tables: Vec<String> = Vec::new();
    for reference in dbflux_core::extract_referenced_tables(statement) {
        if !tables.contains(&reference.table) {
            tables.push(reference.table);
        }
    }

    let lines = dbflux_i18n::t!(
        "document.code.result.statement_lines",
        start = first_line,
        end = last_line
    );

    if tables.is_empty() {
        Some(lines)
    } else {
        Some(format!("{lines} · {}", tables.join(", ")))
    }
}

/// Captions for the `set_count` result sets that running `query` produced,
/// in result order.
///
/// `query` is located in `buffer` at `origin` when it still starts there,
/// otherwise at its first occurrence. Its statements pair with the result
/// sets one to one; when the counts differ (a statement that returns no set,
/// a driver that merges them) no set gets a caption, since any pairing would
/// be a guess.
pub(super) fn result_statement_captions(
    language: &QueryLanguage,
    buffer: &str,
    query: &str,
    origin: Option<usize>,
    set_count: usize,
) -> Vec<Option<String>> {
    let unnamed = vec![None; set_count];

    let query_start = origin
        .filter(|&start| {
            buffer
                .get(start..)
                .is_some_and(|tail| tail.starts_with(query))
        })
        .or_else(|| buffer.find(query));

    let Some(query_start) = query_start else {
        return unnamed;
    };

    let Some(ranges) = language.statement_ranges(query) else {
        return unnamed;
    };

    if ranges.len() != set_count {
        return unnamed;
    }

    ranges
        .into_iter()
        .map(|range| {
            statement_caption(
                buffer,
                (query_start + range.start)..(query_start + range.end),
            )
        })
        .collect()
}

impl CodeDocument {
    /// Number of statements in the buffer, or `None` for a language without
    /// a statement splitter.
    pub(super) fn statement_count(&self) -> Option<usize> {
        self.editor.statement_ranges.as_ref().map(Vec::len)
    }

    /// Re-splits the buffer into statements and reinstalls the gutter. Runs
    /// after every edit and whenever the effective language changes.
    pub(super) fn refresh_statements(&mut self, cx: &mut Context<Self>) {
        let text = self.editor.input_state.read(cx).value();
        let ranges = self.effective_language().statement_ranges(&text);

        let unchanged = ranges == self.editor.statement_ranges
            && self.editor.gutter_style.is_some()
            && self.editor.gutter_shown == self.shows_statement_gutter();
        if unchanged {
            return;
        }

        self.editor.statement_ranges = ranges;
        self.install_statement_gutter(cx);
    }

    /// Reinstalls the gutter when the theme changed since it was installed,
    /// so its colours follow a palette switch.
    pub(super) fn sync_statement_gutter_style(&mut self, cx: &mut Context<Self>) {
        let current = gutter_style(cx.theme());

        if self
            .editor
            .gutter_style
            .is_some_and(|style| style != current)
        {
            self.install_statement_gutter(cx);
        }
    }

    /// The gutter shows for a query buffer the user can run; read-only
    /// buffers (routine definitions) and in-process scripts have none.
    fn shows_statement_gutter(&self) -> bool {
        !self.read_only && self.supports_connection_context()
    }

    fn install_statement_gutter(&mut self, cx: &mut Context<Self>) {
        let style = gutter_style(cx.theme());
        let shown = self.shows_statement_gutter();
        self.editor.gutter_style = Some(style);
        self.editor.gutter_shown = shown;

        let document = cx.entity().downgrade();

        let gutter = self
            .editor
            .statement_ranges
            .clone()
            .filter(|_| shown)
            .map(|ranges| GutterStatements {
                ranges,
                style,
                on_run: Rc::new(move |range, window, cx| {
                    let updated = document.update(cx, |document, cx| {
                        document.run_statement(range, window, cx);
                    });

                    if let Err(error) = updated {
                        log::debug!("code document released before its statement ran: {error}");
                    }
                }),
            });

        self.editor
            .input_state
            .update(cx, |state, cx| state.set_gutter_statements(gutter, cx));
    }

    /// Runs the statement at `range` of the buffer, as its gutter marker
    /// does.
    pub(super) fn run_statement(
        &mut self,
        range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.read_only || self.state == DocumentState::Executing {
            return;
        }

        let text = self.editor.input_state.read(cx).value();
        let Some(statement) = text.get(range.clone()) else {
            return;
        };

        let statement = statement.to_string();
        self.execution.query_origin = Some(range.start);
        self.run_query_text(statement, false, window, cx);
    }
}

#[cfg(test)]
mod tests {
    use super::{result_statement_captions, run_summary_label};
    use dbflux_core::QueryLanguage;

    const BUFFER: &str = "SELECT count(*) FROM orders;\n\nSELECT c.country,\n       o.total\nFROM orders o\nJOIN customers c ON c.id = o.customer_id;\n";

    #[test]
    fn run_summary_joins_the_statement_count_and_the_last_run() {
        assert_eq!(
            run_summary_label(Some(3), Some(0.3214)).as_deref(),
            Some("3 statements · last run 0.32 s")
        );
        assert_eq!(
            run_summary_label(Some(1), None).as_deref(),
            Some("1 statement")
        );
        assert_eq!(
            run_summary_label(None, Some(1.0)).as_deref(),
            Some("last run 1.00 s")
        );
        assert_eq!(run_summary_label(None, None), None);
    }

    #[test]
    fn a_single_statement_run_names_its_lines_and_tables() {
        let query = "SELECT c.country,\n       o.total\nFROM orders o\nJOIN customers c ON c.id = o.customer_id";
        let origin = BUFFER.find("SELECT c.country");

        let captions = result_statement_captions(&QueryLanguage::Sql, BUFFER, query, origin, 1);

        assert_eq!(
            captions,
            vec![Some("Statement L3–6 · orders, customers".to_string())]
        );
    }

    #[test]
    fn a_whole_buffer_run_pairs_statements_with_result_sets() {
        let captions = result_statement_captions(&QueryLanguage::Sql, BUFFER, BUFFER, Some(0), 2);

        assert_eq!(
            captions,
            vec![
                Some("Statement L1–1 · orders".to_string()),
                Some("Statement L3–6 · orders, customers".to_string()),
            ]
        );
    }

    #[test]
    fn mismatched_counts_leave_every_result_unnamed() {
        let captions = result_statement_captions(&QueryLanguage::Sql, BUFFER, BUFFER, Some(0), 1);

        assert_eq!(captions, vec![None]);
    }

    #[test]
    fn a_language_without_a_splitter_gets_no_caption() {
        let query = "db.orders.find()";

        let captions =
            result_statement_captions(&QueryLanguage::MongoQuery, query, query, Some(0), 1);

        assert_eq!(captions, vec![None]);
    }

    #[test]
    fn a_stale_origin_falls_back_to_the_first_occurrence() {
        let query = "SELECT count(*) FROM orders";

        let captions = result_statement_captions(&QueryLanguage::Sql, BUFFER, query, Some(40), 1);

        assert_eq!(captions, vec![Some("Statement L1–1 · orders".to_string())]);
    }
}

#[cfg(test)]
mod gutter_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use crate::code::CodeDocument;
    use dbflux_components::theme;
    use dbflux_components::tokens::EditorMetrics;
    use dbflux_core::QueryLanguage;
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::modals::test_host::host_modal;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, Modifiers, TestAppContext, VisualTestContext, point};

    const SCRIPT: &str = "SELECT 1;\n\nSELECT name\nFROM people;\n";

    /// A SQL editor with no connection holding `SCRIPT`. Running anything in
    /// it ends in the "no active connection" toast, which tells a run apart.
    fn open_editor(
        cx: &mut TestAppContext,
    ) -> (
        Entity<CodeDocument>,
        Entity<ToastHost>,
        &mut VisualTestContext,
    ) {
        cx.update(theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);
        let toasts = cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host: host.clone() });
            host
        });
        let app_state = cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("in-memory storage"),
            )
            .expect("app state")
        });

        let (document, _outside, window) = host_modal(cx, move |window, cx| {
            CodeDocument::new_with_language(app_state, None, QueryLanguage::Sql, window, cx)
        });

        window.update(|window, cx| {
            document.update(cx, |document, cx| document.set_content(SCRIPT, window, cx));
        });
        window.run_until_parked();

        (document, toasts, window)
    }

    fn ran_a_query(window: &mut VisualTestContext, toasts: &Entity<ToastHost>) -> bool {
        let expected = dbflux_i18n::t!("document.code.execution.toast.no_active_connection");
        window.update(|_, cx| toasts.read(cx).last_toast_title()) == Some(expected)
    }

    #[gpui::test]
    fn every_statement_gets_a_run_marker(cx: &mut TestAppContext) {
        let (document, _toasts, window) = open_editor(cx);

        window.update(|_, cx| {
            let document = document.read(cx);
            let editor = document.editor.input_state.read(cx);
            let gutter = editor
                .gutter_statements()
                .expect("a SQL buffer carries the statement gutter");

            let statements: Vec<&str> = gutter
                .ranges
                .iter()
                .filter_map(|range| SCRIPT.get(range.clone()))
                .collect();
            assert_eq!(statements, vec!["SELECT 1", "SELECT name\nFROM people"]);
            assert_eq!(document.statement_count(), Some(2));
            assert_eq!(
                editor.line_height(),
                Some(dbflux_components::fonts::editor_line_height(cx)),
                "code rows follow the editor line height (22 px at the default size)"
            );
        });
    }

    #[gpui::test]
    fn the_statement_under_the_cursor_is_the_active_one(cx: &mut TestAppContext) {
        let (document, _toasts, window) = open_editor(cx);

        window.update(|_, cx| {
            let editor = document.read(cx).editor.input_state.read(cx);
            let gutter = editor.gutter_statements().expect("statement gutter");
            let text = editor.text();

            let inside_second = SCRIPT.find("FROM").expect("second statement");
            let after_first_separator = SCRIPT.find(';').expect("first separator") + 1;
            let blank_line = after_first_separator + 1;

            assert_eq!(
                gutter
                    .statement_at(text, inside_second)
                    .and_then(|range| SCRIPT.get(range)),
                Some("SELECT name\nFROM people")
            );
            assert_eq!(
                gutter
                    .statement_at(text, after_first_separator)
                    .and_then(|range| SCRIPT.get(range)),
                Some("SELECT 1"),
                "a cursor right after the `;` still belongs to its statement"
            );
            assert_eq!(gutter.statement_at(text, blank_line), None);
        });
    }

    #[gpui::test]
    fn clicking_a_run_marker_runs_its_statement(cx: &mut TestAppContext) {
        let (document, toasts, window) = open_editor(cx);

        let (second_start, marker) = window.update(|_, cx| {
            let editor = document.read(cx).editor.input_state.read(cx);
            let gutter = editor.gutter_statements().expect("statement gutter");
            let second = gutter.ranges.get(1).cloned().expect("second statement");

            let line = editor
                .range_to_bounds(&(second.start..second.start))
                .expect("the second statement is laid out");
            let line_height = editor.line_height().expect("the editor is laid out");
            let gutter_left = editor.input_bounds().origin.x;

            (
                second.start,
                point(
                    gutter_left + EditorMetrics::GUTTER_MARKER_SLOT / 2.0,
                    line.origin.y + line_height / 2.0,
                ),
            )
        });

        window.simulate_click(marker, Modifiers::default());
        window.run_until_parked();

        assert!(ran_a_query(window, &toasts), "the marker ran a query");
        assert_eq!(
            window.update(|_, cx| document.read(cx).execution.query_origin),
            Some(second_start),
            "the marker ran the statement it stands beside"
        );
    }

    #[gpui::test]
    fn a_language_without_a_splitter_has_no_gutter(cx: &mut TestAppContext) {
        cx.update(theme::init);
        let app_state = cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("in-memory storage"),
            )
            .expect("app state")
        });

        let (document, _outside, window) = host_modal(cx, move |window, cx| {
            CodeDocument::new_with_language(app_state, None, QueryLanguage::MongoQuery, window, cx)
        });

        window.update(|window, cx| {
            document.update(cx, |document, cx| {
                document.set_content("db.people.find()", window, cx)
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let document = document.read(cx);
            assert!(
                document
                    .editor
                    .input_state
                    .read(cx)
                    .gutter_statements()
                    .is_none()
            );
            assert_eq!(document.statement_count(), None);
        });
    }
}
