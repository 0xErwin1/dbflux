//! The text view of `SpreadsheetDocument` and the switch between it and the
//! table.
//!
//! The text view shows the values of the shown sheet as CSV, for copying and
//! searching. It is a rendering, not a file: it cannot be edited and has no
//! save of its own. Every row the table shows is one record, in table order,
//! pending edits included: a staged value shows as typed, and an appended
//! row shows where the table shows it. Each other cell is the whole value
//! the reader shows for it, not its formula, with the line breaks the
//! table's cell flattens and the length it shortens. Records are rendered by
//! the delimited writer's rules (`dbflux_delimited::render_record`) with a
//! comma, the double quote and UTF-8, each ending with a line feed: a field
//! that holds a comma, a quote, a line break or surrounding spaces is
//! quoted, and a quote inside it is doubled.
//!
//! The text is built on the background executor, from the table's model and
//! a copy of its pending edits, when it is drawn after something it depends
//! on changed: the table notifies on every change of its rows and edits, and
//! a sheet switch shows a new table. It is never built per frame, and a
//! build that a later change made obsolete is dropped. The text stops at a
//! record boundary before it would pass [`TEXT_LIMITS`], and a note below it
//! says how many of the sheet's rows it shows.
//!
//! The text sits in the code editor component the delimited document's text
//! view uses, read-only, without a language, in its monospace font, with
//! line numbers, its selection, and its find panel. Lines wrap only when one
//! is longer than [`WRAP_LINE_BYTES`].
//!
//! Keys: `t` (`CycleDocumentView`) switches between the table and the text
//! in the `Results` context. While the editor holds the keyboard its context
//! is `TextInput`, so Escape first hands the keyboard to the tab and Enter
//! hands it back, as in the delimited document. The editor follows the Vim
//! mode setting, with motions and yanks only.

use dbflux_components::components::data_table::TableModel;
use dbflux_components::components::data_table::model::{EditBuffer, VisualRowSource};
use dbflux_components::controls::InputEvent;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::DocumentMetrics;
use dbflux_components::vim::{VimBinding, VimHost};
use dbflux_delimited::{Dialect, EditLocation, render_record};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::EditorState;

use super::document::SpreadsheetDocument;

/// The most bytes of text the text view shows. The same as the delimited
/// document's text view: the editor holds the whole text and lays it out,
/// and it is built for about this much.
pub(super) const MAX_TEXT_BYTES: usize = 8 * 1024 * 1024;

/// The most lines the text view shows. The same as the delimited document's
/// text view: the editor component is built for about this many lines.
pub(super) const MAX_TEXT_LINES: usize = 50_000;

/// A line longer than this many bytes makes the text view wrap its lines,
/// as the delimited text view does.
pub(super) const WRAP_LINE_BYTES: usize = 10_000;

/// What the text holds at most. Both are checked before a record is added,
/// so the text always ends at a record boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TextLimits {
    pub(super) max_bytes: usize,
    pub(super) max_lines: usize,
}

/// The limits of the text view.
pub(super) const TEXT_LIMITS: TextLimits = TextLimits {
    max_bytes: MAX_TEXT_BYTES,
    max_lines: MAX_TEXT_LINES,
};

/// Which view shows the sheet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SheetView {
    #[default]
    Table,
    Text,
}

/// The values of a sheet as CSV.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SheetText {
    pub(super) text: String,

    /// How many of the table's rows the text holds.
    pub(super) shown_rows: usize,

    /// How many rows the table shows.
    pub(super) total_rows: usize,

    /// Whether a line is longer than [`WRAP_LINE_BYTES`].
    pub(super) wraps: bool,
}

/// The editor of the text view.
struct TextEditor {
    input: Entity<EditorState>,

    /// Vim mode for the editor, built with it.
    vim: VimBinding,

    _subscription: Subscription,
}

/// A built text, before the editor is given it.
struct ArrivedText {
    sheet: Option<usize>,
    result: Result<SheetText, String>,
}

/// What the text the editor holds says about the sheet.
#[derive(Default)]
struct ShownText {
    sheet: Option<usize>,
    rows: Option<(usize, usize)>,
    failure: Option<String>,
}

/// The view the document shows and the state of its text view.
pub(super) struct SheetTextState {
    view: SheetView,

    /// Built the first time the text is shown, because the editor needs the
    /// window.
    editor: Option<TextEditor>,

    /// Whether the text is out of date and is built again the next time it
    /// is drawn.
    stale: bool,

    /// Counts the changes the text depends on. A build carries the count it
    /// was started at, and its text is dropped when it changed meanwhile.
    generation: u64,

    /// Whether a build of the current generation runs.
    building: bool,

    /// The running build. Replacing it drops the one it replaces.
    _build: Option<Task<()>>,

    /// The text of the last build, until it is drawn.
    arrived: Option<ArrivedText>,

    /// What the editor's text holds.
    shown: ShownText,

    /// Whether the editor holds the keyboard. Escape takes it away, and
    /// Enter, a click into the editor or showing the text gives it back.
    has_keyboard: bool,

    /// Set when the view changed, so the next render hands the keyboard to
    /// the view now shown, which needs the window.
    pending_focus: bool,

    limits: TextLimits,
}

impl Default for SheetTextState {
    fn default() -> Self {
        Self {
            view: SheetView::Table,
            editor: None,
            stale: true,
            generation: 0,
            building: false,
            _build: None,
            arrived: None,
            shown: ShownText::default(),
            has_keyboard: false,
            pending_focus: false,
            limits: TEXT_LIMITS,
        }
    }
}

/// Renders the rows `buffer` shows over `model` as CSV: one record per row
/// in table order, each cell its whole value, a staged value as typed, and
/// every record ended by a line feed. Stops before the record
/// that would take the text past `limits`.
///
/// # Errors
///
/// The writer's refusal of a field, which a comma-separated UTF-8 dialect
/// with a quote character never gives.
pub(super) fn render_sheet_csv(
    model: &TableModel,
    buffer: &EditBuffer,
    limits: TextLimits,
) -> Result<SheetText, String> {
    let dialect = Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: false,
        encoding: encoding_rs::UTF_8,
    };

    let columns = model.col_count();
    let rows: Vec<VisualRowSource> = buffer
        .compute_visual_order()
        .into_iter()
        .filter(|source| match source {
            VisualRowSource::Base(row) => !buffer.is_pending_delete(*row),
            VisualRowSource::Insert(_) => true,
        })
        .collect();

    let mut bytes = Vec::new();
    let mut lines = 0;
    let mut shown_rows = 0;
    let mut wraps = false;

    for (index, source) in rows.iter().enumerate() {
        let fields = row_fields(model, buffer, *source, columns);

        let mut record = render_record(&fields, &dialect, &EditLocation::Inserted(index))
            .map_err(|error| error.to_string())?;
        record.push(b'\n');

        let record_lines = record.iter().filter(|byte| **byte == b'\n').count();

        if bytes.len() + record.len() > limits.max_bytes || lines + record_lines > limits.max_lines
        {
            break;
        }

        wraps = wraps
            || record
                .split(|byte| *byte == b'\n')
                .any(|line| line.len() > WRAP_LINE_BYTES);

        bytes.extend_from_slice(&record);
        lines += record_lines;
        shown_rows += 1;
    }

    let text = String::from_utf8(bytes).map_err(|error| error.to_string())?;

    Ok(SheetText {
        text,
        shown_rows,
        total_rows: rows.len(),
        wraps,
    })
}

/// The text of every cell of the row `source`, padded to `columns`.
fn row_fields(
    model: &TableModel,
    buffer: &EditBuffer,
    source: VisualRowSource,
    columns: usize,
) -> Vec<String> {
    match source {
        VisualRowSource::Base(row) => {
            let mut fields: Vec<String> = (0..columns)
                .map(|column| {
                    model
                        .cell(row, column)
                        .map(|cell| cell.edit_text())
                        .unwrap_or_default()
                })
                .collect();

            for (column, value) in buffer.row_changes(row) {
                if let Some(field) = fields.get_mut(column) {
                    *field = value.edit_text();
                }
            }

            fields
        }

        VisualRowSource::Insert(index) => {
            let cells = buffer.get_pending_insert_by_idx(index);

            (0..columns)
                .map(|column| {
                    cells
                        .and_then(|cells| cells.get(column))
                        .map(|cell| cell.edit_text())
                        .unwrap_or_default()
                })
                .collect()
        }
    }
}

impl VimHost for SpreadsheetDocument {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.text
            .editor
            .as_ref()
            .and_then(|editor| editor.vim.for_input(input))
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.text
            .editor
            .as_mut()
            .and_then(|editor| editor.vim.for_input_mut(input))
    }

    /// The text is never edited: motions and yanks, no edits.
    fn vim_read_only(&self, _input: EntityId, _cx: &App) -> bool {
        true
    }
}

impl SpreadsheetDocument {
    /// The view that shows the sheet.
    pub fn view(&self) -> SheetView {
        self.text.view
    }

    /// Whether the view can be switched now: the workbook is loaded and no
    /// prompt is open.
    pub fn can_switch_view(&self) -> bool {
        self.loaded().is_some()
            && self.download_prompt_message().is_none()
            && self.save_as_prompt.is_none()
    }

    /// Shows `view`. The keyboard goes to it on the next render. A cell
    /// edit still open in the table is staged first, so the text shows it.
    /// Does nothing when the view cannot be switched now.
    pub fn show_view(&mut self, view: SheetView, cx: &mut Context<Self>) {
        if !self.can_switch_view() || self.text.view == view {
            return;
        }

        if view == SheetView::Text {
            self.commit_inline_edit(cx);
            self.mark_text_stale(cx);
        }

        self.text.view = view;
        self.text.has_keyboard = view == SheetView::Text;
        self.text.pending_focus = true;

        cx.notify();
    }

    /// Shows the table when the text is shown, and the text otherwise.
    pub fn toggle_view(&mut self, cx: &mut Context<Self>) {
        let view = match self.text.view {
            SheetView::Table => SheetView::Text,
            SheetView::Text => SheetView::Table,
        };

        self.show_view(view, cx);
    }

    /// Marks the text to be built again, and draws the document again while
    /// the text is shown so that it is. A text that arrived and was not drawn
    /// yet is dropped: it may belong to the sheet shown before.
    pub(super) fn mark_text_stale(&mut self, cx: &mut Context<Self>) {
        self.text.stale = true;
        self.text.generation += 1;
        self.text.building = false;
        self.text.arrived = None;

        if self.text.view == SheetView::Text {
            cx.notify();
        }
    }

    /// The editor of the text view, once the text was shown.
    pub(super) fn text_input(&self) -> Option<&Entity<EditorState>> {
        self.text.editor.as_ref().map(|editor| &editor.input)
    }

    /// How many rows the editor's text holds and how many the sheet shows,
    /// when the text was cut.
    pub(super) fn text_cut(&self) -> Option<(usize, usize)> {
        self.text.shown.rows.filter(|(shown, total)| shown < total)
    }

    /// Replaces the limits of the text, which is then built again.
    #[cfg(test)]
    pub(super) fn set_text_limits(&mut self, limits: TextLimits) {
        self.text.limits = limits;
        self.text.stale = true;
        self.text.generation += 1;
        self.text.arrived = None;
    }

    /// Key context entries the workspace adds while this tab owns the
    /// keyboard: the Vim mode of the text view's editor while the text is
    /// shown.
    pub fn key_context_entries(&self, cx: &App) -> Vec<(SharedString, SharedString)> {
        self.text
            .editor
            .as_ref()
            .filter(|_| self.text.view == SheetView::Text)
            .and_then(|editor| editor.vim.key_context_entry(cx))
            .into_iter()
            .collect()
    }

    /// Whether the text view's editor holds the keyboard.
    pub(super) fn text_has_keyboard(&self) -> bool {
        self.text.view == SheetView::Text
            && self.text.has_keyboard
            && self.text.editor.is_some()
            && self.shown_sheet().is_some()
    }

    /// Whether Enter gives the keyboard back to the text view's editor: the
    /// text is shown and its editor does not hold the keyboard.
    pub(super) fn can_take_text_keyboard(&self) -> bool {
        self.text.view == SheetView::Text
            && !self.text.has_keyboard
            && self.text.editor.is_some()
            && self.shown_sheet().is_some()
    }

    /// Hands the keyboard from the text view's editor to the tab.
    pub(super) fn release_text_keyboard(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.text.has_keyboard = false;
        self.focus_handle().focus(window, cx);
        cx.notify();
    }

    /// Gives the keyboard to the text view's editor while the text of a
    /// shown sheet is shown. Returns false otherwise. A text view that is
    /// not built yet takes the keyboard when it is.
    pub(super) fn focus_text(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.text.view != SheetView::Text || self.shown_sheet().is_none() {
            return false;
        }

        self.text.has_keyboard = true;

        match self.text_input().cloned() {
            Some(input) => input.update(cx, |state, cx| state.focus(window, cx)),

            None => {
                self.text.pending_focus = true;
                self.focus_handle().focus(window, cx);
            }
        }

        cx.notify();
        true
    }

    /// Builds the editor, gives it the text that arrived, starts a build of
    /// a stale text, and hands the keyboard to the view that was just shown.
    /// Called from `render`, which has the window the editor needs.
    pub(super) fn prepare_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.text.view == SheetView::Text && self.shown_sheet().is_some() {
            if self.text.editor.is_none() {
                self.build_text_editor(window, cx);
            }

            if let Some(arrived) = self.text.arrived.take() {
                self.show_arrived_text(arrived, window, cx);
            }

            if self.text.stale {
                self.start_text_build(cx);
            }
        }

        if std::mem::take(&mut self.text.pending_focus) {
            self.focus(window, cx);
        }
    }

    fn build_text_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .line_number(true)
                .soft_wrap(false)
        });
        let vim = VimBinding::new(input.clone(), window, cx);

        let subscription = cx.subscribe_in(
            &input,
            window,
            |this, _, event: &InputEvent, _window, cx| {
                // A click into the editor takes the keyboard back.
                if matches!(event, InputEvent::Focus) && !this.text.has_keyboard {
                    this.text.has_keyboard = true;
                    cx.notify();
                }
            },
        );

        self.text.editor = Some(TextEditor {
            input: input.clone(),
            vim,
            _subscription: subscription,
        });

        VimBinding::follow_setting(self, input.entity_id(), cx);
    }

    /// Builds the text of the shown sheet on the background executor, from
    /// its model and a copy of its pending edits.
    fn start_text_build(&mut self, cx: &mut Context<Self>) {
        let Some(shown) = self.shown_sheet() else {
            return;
        };

        let model = shown.model.table_model();
        let buffer = shown.table_state.read(cx).edit_buffer().clone();
        let sheet = self.active_sheet();
        let limits = self.text.limits;
        let generation = self.text.generation;

        self.text.stale = false;
        self.text.building = true;

        let task = cx
            .background_executor()
            .spawn(async move { render_sheet_csv(&model, &buffer, limits) });

        self.text._build = Some(cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.text_arrived(generation, ArrivedText { sheet, result }, cx);
                })
                .ok();
            });
        }));
    }

    /// Keeps a built text for the next render, unless a change made it
    /// obsolete while it was built.
    fn text_arrived(&mut self, generation: u64, arrived: ArrivedText, cx: &mut Context<Self>) {
        if generation != self.text.generation {
            return;
        }

        self.text.building = false;
        self.text.arrived = Some(arrived);

        cx.notify();
    }

    /// Puts a built text in the editor. The editor keeps its scroll position
    /// and cursor for a new text of the same sheet, and starts at the top
    /// for another sheet.
    fn show_arrived_text(
        &mut self,
        arrived: ArrivedText,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(input) = self.text_input().cloned() else {
            return;
        };

        let same_sheet = self.text.shown.sheet == arrived.sheet;

        let (text, wraps, shown) = match arrived.result {
            Ok(built) => (
                SharedString::from(built.text),
                built.wraps,
                ShownText {
                    sheet: arrived.sheet,
                    rows: Some((built.shown_rows, built.total_rows)),
                    failure: None,
                },
            ),

            Err(cause) => (
                SharedString::default(),
                false,
                ShownText {
                    sheet: arrived.sheet,
                    rows: None,
                    failure: Some(cause),
                },
            ),
        };

        self.text.shown = shown;

        input.update(cx, |state, cx| {
            let scroll = state.scroll_offset();
            let cursor = if same_sheet { state.cursor() } else { 0 };

            state.set_value(text, window, cx);
            state.set_soft_wrap(wraps, window, cx);

            if same_sheet {
                state.set_scroll_offset(scroll, cx);
            }

            // Scrolls only when the cursor is out of view.
            let cursor = cursor.min(state.text().len());
            state.set_selected_range(cursor..cursor, cx);
        });
    }

    // -- Rendering -----------------------------------------------------------

    /// The switch between the table and the text. `None` until the workbook
    /// is loaded.
    pub(super) fn render_view_switch(&self, cx: &Context<Self>) -> Option<AnyElement> {
        self.loaded()?;

        let document = cx.entity().downgrade();

        Some(
            SegmentedControl::new(
                vec![
                    SegmentedItem::new("table", dbflux_i18n::t!("document.spreadsheet.view.table")),
                    SegmentedItem::new("text", dbflux_i18n::t!("document.spreadsheet.view.text")),
                ],
                match self.text.view {
                    SheetView::Table => "table",
                    SheetView::Text => "text",
                },
                move |id, _window, cx| {
                    let view = if id.as_ref() == "text" {
                        SheetView::Text
                    } else {
                        SheetView::Table
                    };

                    if let Some(document) = document.upgrade() {
                        document.update(cx, |document, cx| document.show_view(view, cx));
                    }
                },
            )
            .group("spreadsheet-view")
            .into_any_element(),
        )
    }

    /// The text view: the read-only editor, the Vim mode indicator while Vim
    /// mode is on, and below them the notes of [`Self::text_notes`].
    pub(super) fn render_text_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let editor = self
            .text
            .editor
            .as_ref()
            .map(|editor| render_text_editor(editor, cx));

        let notes = self.text_notes();

        div()
            .id("spreadsheet-text-view")
            .debug_selector(|| "spreadsheet-text-view".to_string())
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .children(editor)
            .when(!notes.is_empty(), |body| {
                body.child(
                    div()
                        .id("spreadsheet-text-notes")
                        .flex()
                        .flex_col()
                        .flex_shrink_0()
                        .gap(DocumentMetrics::GAP)
                        .px(DocumentMetrics::PADDING_X)
                        .py(DocumentMetrics::GAP)
                        .border_t_1()
                        .border_color(cx.theme().border)
                        .children(notes),
                )
            })
            .into_any_element()
    }

    /// What the text view says below the text: that the text is being
    /// written, why it could not be, and that it holds only the first rows
    /// of the sheet.
    fn text_notes(&self) -> Vec<AnyElement> {
        let mut notes = Vec::new();

        if self.text.building || self.text.stale {
            notes.push(
                Text::caption(dbflux_i18n::t!("document.spreadsheet.text.building"))
                    .muted_foreground()
                    .into_any_element(),
            );
        }

        if let Some(cause) = &self.text.shown.failure {
            notes.push(
                Text::caption(dbflux_i18n::t!(
                    "document.spreadsheet.text.render_failed",
                    cause = cause
                ))
                .danger()
                .into_any_element(),
            );
        }

        if let Some((shown, total)) = self.text_cut() {
            notes.push(
                div()
                    .id("spreadsheet-text-cut")
                    .debug_selector(|| "spreadsheet-text-cut".to_string())
                    .child(
                        Text::caption(crate::labels::spreadsheet_text_cut(shown, total)).warning(),
                    )
                    .into_any_element(),
            );
        }

        notes
    }
}

/// The read-only editor with Vim's listeners on its container and the mode
/// indicator below it, as the delimited text view draws its editor.
fn render_text_editor(editor: &TextEditor, cx: &mut Context<SpreadsheetDocument>) -> AnyElement {
    let element = div()
        .flex_1()
        .min_h_0()
        .overflow_hidden()
        .child(editor.vim.editor(true).appearance(false).w_full().h_full());
    let indicator = editor.vim.render_indicator(cx);
    let wrapper = editor
        .vim
        .leader_scope(div().flex_1().min_h_0().flex().flex_col(), cx);

    let input = editor.vim.input_id();

    VimBinding::capture_run_command(VimBinding::wire(wrapper, input, cx), input, cx)
        .child(element)
        .children(indicator)
        .into_any_element()
}
