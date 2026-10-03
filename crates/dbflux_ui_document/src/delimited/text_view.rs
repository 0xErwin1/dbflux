//! The text view of `DelimitedDocument` and the switch between it and the
//! table.
//!
//! The two views show the same data: the page model and the table's edit
//! buffer stay the only pending state, and switching never touches either,
//! so pending edits and the table's cursor survive it both ways. An inline
//! cell edit that is open when the text is shown is committed first, so the
//! text includes it.
//!
//! The text is read-only and is rendered by `text.rs`, raw or aligned. It is
//! worked out again only when it is drawn after something it depends on
//! changed: the table notifies on every change of its rows, edits, pages and
//! dialect, and the mode can change. It is never worked out per frame.
//! Rendering it again keeps the editor's scroll position and cursor, and
//! showing the text puts the cursor on the line of the table's active row.
//!
//! The text sits in the code editor component the code document and the
//! object editor use, without a language, so nothing highlights it, in its
//! monospace font, with line numbers, and wrapped only when a line is longer
//! than the object editors allow unwrapped.
//!
//! Keys: `t` (`CycleDocumentView`) switches between the table and the text,
//! and `Shift+T` (`CycleResultView`) between raw and aligned, both in the
//! `Results` context. While the editor holds the keyboard its context is
//! `TextInput`, where letters are text, so Escape first hands the keyboard to
//! the tab, as it does in the object editor, and Enter hands it back. The
//! editors' save key saves in both cases: the document takes it before the
//! keymap while the text is shown, because the `Results` context binds none.

use dbflux_components::controls::InputEvent;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::DocumentMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Editor, EditorState};

use super::document::DelimitedDocument;
use super::text::{
    ALIGNED_MAX_WIDTH, RenderedText, TEXT_LIMITS, TextEnd, TextLayout, TextRow, render_aligned,
    render_raw,
};
use dbflux_components::components::data_table::model::VisualRowSource;

/// Which view shows the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DelimitedView {
    #[default]
    Table,
    Text,
}

/// How the text view shows the records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TextMode {
    /// The exact text of the file, as a save would write it.
    #[default]
    Raw,

    /// The fields padded to column widths, for reading.
    Aligned,
}

/// The editor of the text view and the text it shows.
pub(super) struct TextView {
    pub(super) input: Entity<EditorState>,

    /// Where each row of the table is in the text the editor holds, or why
    /// the pending changes cannot be shown as text. The text itself is the
    /// editor's (`EditorState::text`): it is handed over once, not kept
    /// twice.
    pub(super) layout: Result<TextLayout, String>,

    _subscription: Subscription,
}

/// The view the document shows and the state of its text view.
#[derive(Default)]
pub(super) struct TextViewState {
    pub(super) view: DelimitedView,
    pub(super) mode: TextMode,

    /// Built the first time the text is shown, because the editor needs the
    /// window.
    pub(super) shown: Option<TextView>,

    /// Whether the text is out of date and is worked out again the next
    /// time it is drawn.
    pub(super) stale: bool,

    /// Whether the editor holds the keyboard. Escape takes it away, and
    /// Enter, a click into the editor or showing the text gives it back.
    pub(super) has_keyboard: bool,

    /// Set when the view changed, so the next render hands the keyboard to
    /// the view now shown, which needs the window.
    pub(super) pending_focus: bool,
}

impl DelimitedDocument {
    /// The view that shows the file.
    pub fn view(&self) -> DelimitedView {
        self.text.view
    }

    /// How the text view shows the records.
    pub fn text_mode(&self) -> TextMode {
        self.text.mode
    }

    /// Whether the view can be switched now: the file is loaded and no
    /// dialog is open.
    pub fn can_switch_view(&self) -> bool {
        self.loaded().is_some() && !self.has_open_dialog()
    }

    /// Shows `view`. The keyboard goes to it on the next render, and the
    /// text view puts its cursor on the line of the table's active row.
    /// Does nothing when the view cannot be switched now.
    pub fn show_view(&mut self, view: DelimitedView, cx: &mut Context<Self>) {
        if !self.can_switch_view() || self.text.view == view {
            return;
        }

        if view == DelimitedView::Text {
            self.commit_inline_edit(cx);
            self.text.stale = true;
        }

        self.text.view = view;
        self.text.has_keyboard = view == DelimitedView::Text;
        self.text.pending_focus = true;

        cx.notify();
    }

    /// Shows the table when the text is shown, and the text otherwise.
    pub fn toggle_view(&mut self, cx: &mut Context<Self>) {
        let view = match self.text.view {
            DelimitedView::Table => DelimitedView::Text,
            DelimitedView::Text => DelimitedView::Table,
        };

        self.show_view(view, cx);
    }

    /// Shows the text as `mode`.
    pub fn set_text_mode(&mut self, mode: TextMode, cx: &mut Context<Self>) {
        if self.text.mode == mode {
            return;
        }

        self.text.mode = mode;
        self.text.stale = true;

        cx.notify();
    }

    /// Switches the text between raw and aligned.
    pub fn cycle_text_mode(&mut self, cx: &mut Context<Self>) {
        let mode = match self.text.mode {
            TextMode::Raw => TextMode::Aligned,
            TextMode::Aligned => TextMode::Raw,
        };

        self.set_text_mode(mode, cx);
    }

    /// Where each row of the table is in the text the text view shows, once
    /// it was shown. With the editor's text ([`Self::text_input`]), this is
    /// what an edit of the text is compared against.
    pub(super) fn rendered_text(&self) -> Option<&TextLayout> {
        self.text
            .shown
            .as_ref()
            .and_then(|view| view.layout.as_ref().ok())
    }

    /// The editor of the text view, once the text was shown.
    pub(super) fn text_input(&self) -> Option<&Entity<EditorState>> {
        self.text.shown.as_ref().map(|view| &view.input)
    }

    /// Why the pending changes cannot be shown as text, when they cannot.
    pub(super) fn text_failure(&self) -> Option<&str> {
        self.text
            .shown
            .as_ref()
            .and_then(|view| view.layout.as_ref().err())
            .map(String::as_str)
    }

    /// Marks the text to be worked out again, and draws the document again
    /// while the text is shown so that it is.
    pub(super) fn mark_text_stale(&mut self, cx: &mut Context<Self>) {
        self.text.stale = true;

        if self.text.view == DelimitedView::Text {
            cx.notify();
        }
    }

    /// Whether the text view's editor holds the keyboard.
    pub(super) fn text_has_keyboard(&self) -> bool {
        self.text.view == DelimitedView::Text && self.text.has_keyboard && self.text.shown.is_some()
    }

    /// Whether Enter gives the keyboard back to the text view's editor: the
    /// text is shown and its editor does not hold the keyboard.
    pub(super) fn can_take_text_keyboard(&self) -> bool {
        self.text.view == DelimitedView::Text
            && !self.text.has_keyboard
            && self.text.shown.is_some()
    }

    /// The save key of the editors, taken on the document while the text is
    /// shown. Once Escape gave the keyboard to the tab, its context is
    /// `Results`, which binds no save key, and the table that answers its own
    /// save key is not drawn. While the editor holds the keyboard the key is
    /// taken here too, so it saves once. The keys come from the keymap: the
    /// ones the editors bind to `SaveQuery`.
    pub(super) fn take_text_view_save_key(
        &mut self,
        event: &KeyDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.text.view != DelimitedView::Text || !is_save_key(&event.keystroke) {
            return;
        }

        cx.stop_propagation();
        self.save(cx);
    }

    /// Hands the keyboard from the text view's editor to the tab.
    pub(super) fn release_text_keyboard(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.text.has_keyboard = false;
        self.focus_handle().focus(window, cx);
        cx.notify();
    }

    /// Gives the keyboard to the text view's editor while the text is
    /// shown. Returns false when the table is shown. A text view that is not
    /// built yet takes the keyboard when it is.
    pub(super) fn focus_text(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.text.view != DelimitedView::Text || self.loaded().is_none() {
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

    /// Closes the table's inline editor with its value staged, so the text
    /// shows it.
    fn commit_inline_edit(&self, cx: &mut Context<Self>) {
        let Some(table_state) = self.table_state().cloned() else {
            return;
        };

        table_state.update(cx, |state, cx| {
            if state.is_editing() {
                state.stop_editing(true, cx);
            }
        });
    }

    /// Brings the text view up to date before it is drawn, and hands the
    /// keyboard to the view that was just shown. Called from `render`,
    /// which has the window the editor needs.
    pub(super) fn prepare_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.loaded().is_none() {
            return;
        }

        if self.text.view == DelimitedView::Table {
            if std::mem::take(&mut self.text.pending_focus) {
                self.focus(window, cx);
            }

            return;
        }

        if self.text.shown.is_none() || self.text.stale {
            self.refresh_text(window, cx);
        }

        if std::mem::take(&mut self.text.pending_focus) {
            self.show_active_row_in_text(window, cx);
        }
    }

    /// Works the text out again and puts it in the editor, built the first
    /// time. An editor that already showed text keeps its scroll position,
    /// and its cursor stays on the row it was on where the new text still
    /// shows that row, which a switch between raw and aligned text needs.
    fn refresh_text(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let cursor_row = self.text.shown.as_ref().and_then(|view| {
            let cursor = view.input.read(cx).cursor();

            view.layout.as_ref().ok()?.row_at(cursor)
        });

        let (text, layout) = match self.render_text(cx) {
            Ok(RenderedText { text, layout }) => (SharedString::from(text), Ok(layout)),
            Err(failure) => (SharedString::default(), Err(failure)),
        };

        self.text.stale = false;

        let kept_cursor = layout.as_ref().ok().and_then(|layout| {
            let (row, within) = cursor_row?;

            layout.offset_in(row, within)
        });

        let wraps = layout.as_ref().is_ok_and(|layout| layout.wraps);

        let input = match &mut self.text.shown {
            Some(view) => {
                view.layout = layout;
                view.input.clone()
            }

            None => {
                let input = cx.new(|cx| {
                    EditorState::new(window, cx)
                        .line_number(true)
                        .soft_wrap(wraps)
                });

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

                self.text.shown = Some(TextView {
                    input: input.clone(),
                    layout,
                    _subscription: subscription,
                });

                input
            }
        };

        input.update(cx, |state, cx| {
            let scroll = state.scroll_offset();
            let cursor = kept_cursor.unwrap_or_else(|| state.cursor());

            state.set_value(text, window, cx);
            state.set_soft_wrap(wraps, window, cx);
            state.set_scroll_offset(scroll, cx);

            // Scrolls only when the cursor is out of view.
            let cursor = cursor.min(state.text().len());
            state.set_selected_range(cursor..cursor, cx);
        });
    }

    /// The text of the loaded records in the mode the text view shows, or
    /// why the pending changes cannot be shown.
    fn render_text(&self, cx: &App) -> Result<RenderedText, String> {
        let Some(loaded) = self.loaded() else {
            return Err(String::new());
        };

        let edits = loaded.table_state.read(cx).edit_buffer();

        match self.text.mode {
            TextMode::Raw => render_raw(
                &loaded.page_model,
                &loaded.source_span,
                edits,
                &loaded.dialect,
                TEXT_LIMITS,
            )
            .map_err(|error| error.to_string()),

            TextMode::Aligned => Ok(render_aligned(&loaded.page_model, edits, TEXT_LIMITS)),
        }
    }

    /// Puts the editor's cursor at the start of the table's active row,
    /// which scrolls it into view, and gives the editor the keyboard. The
    /// row is found by its byte offset, not its line: in text whose records
    /// end with a bare carriage return every record is on one line.
    fn show_active_row_in_text(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let offset = self.active_row_offset(cx);

        let Some(input) = self.text_input().cloned() else {
            return;
        };

        self.text.has_keyboard = true;

        input.update(cx, |state, cx| {
            let offset = offset.min(state.text().len());

            state.set_selected_range(offset..offset, cx);
            state.focus(window, cx);
        });
    }

    /// The byte offset of the table's active row in the text. A row the text
    /// leaves out, a deleted one, goes to the next row it shows. The start of
    /// the text without an active row or a row after it in the text.
    pub(super) fn active_row_offset(&self, cx: &App) -> usize {
        let (Some(loaded), Some(layout)) = (self.loaded(), self.rendered_text()) else {
            return 0;
        };

        let state = loaded.table_state.read(cx);

        let Some(active) = state.selection().active else {
            return 0;
        };

        state
            .edit_buffer()
            .compute_visual_order()
            .into_iter()
            .skip(active.row)
            .find_map(|source| {
                let row = match source {
                    VisualRowSource::Base(row) => TextRow::Base(row),
                    VisualRowSource::Insert(index) => TextRow::Insert(index),
                };

                layout.offset_in(row, 0)
            })
            .unwrap_or(0)
    }

    // -- Rendering -----------------------------------------------------------

    /// The switch between the table and the text, and, while the text is
    /// shown, between raw and aligned. `None` until the file is loaded.
    pub(super) fn render_view_controls(&self, cx: &Context<Self>) -> Option<AnyElement> {
        self.loaded()?;

        let document = cx.entity().downgrade();

        let view = SegmentedControl::new(
            vec![
                SegmentedItem::new("table", dbflux_i18n::t!("document.delimited.view.table")),
                SegmentedItem::new("text", dbflux_i18n::t!("document.delimited.view.text")),
            ],
            match self.text.view {
                DelimitedView::Table => "table",
                DelimitedView::Text => "text",
            },
            move |id, _window, cx| {
                let view = if id.as_ref() == "text" {
                    DelimitedView::Text
                } else {
                    DelimitedView::Table
                };

                if let Some(document) = document.upgrade() {
                    document.update(cx, |document, cx| document.show_view(view, cx));
                }
            },
        )
        .group("delimited-view");

        let mode = (self.text.view == DelimitedView::Text).then(|| {
            let document = cx.entity().downgrade();

            SegmentedControl::new(
                vec![
                    SegmentedItem::new("raw", dbflux_i18n::t!("document.delimited.view.raw")),
                    SegmentedItem::new(
                        "aligned",
                        dbflux_i18n::t!("document.delimited.view.aligned"),
                    ),
                ],
                match self.text.mode {
                    TextMode::Raw => "raw",
                    TextMode::Aligned => "aligned",
                },
                move |id, _window, cx| {
                    let mode = if id.as_ref() == "aligned" {
                        TextMode::Aligned
                    } else {
                        TextMode::Raw
                    };

                    if let Some(document) = document.upgrade() {
                        document.update(cx, |document, cx| document.set_text_mode(mode, cx));
                    }
                },
            )
            .group("delimited-text-mode")
        });

        Some(
            div()
                .id("delimited-view-controls")
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .child(view)
                .children(mode)
                .into_any_element(),
        )
    }

    /// The text view: the read-only editor and, below it, what the text
    /// leaves out or why it cannot be shown.
    pub(super) fn render_text_body(&self, cx: &Context<Self>) -> AnyElement {
        let editor = self.text_input().map(|input| {
            Editor::new(input)
                .readonly(true)
                .appearance(false)
                .w_full()
                .h_full()
        });

        let notes = self.text_notes();

        div()
            .id("delimited-text-view")
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(div().flex_1().min_h_0().overflow_hidden().children(editor))
            .when(!notes.is_empty(), |body| {
                body.child(
                    div()
                        .id("delimited-text-notes")
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

    /// What the text view says below the text: why it cannot show the
    /// pending changes, that the file has more records than are loaded, that
    /// it shows only the first loaded records, and how aligned text marks
    /// what it changes.
    pub(super) fn text_notes(&self) -> Vec<Text> {
        let mut notes = Vec::new();

        if let Some(cause) = self.text_failure() {
            notes.push(
                Text::caption(dbflux_i18n::t!(
                    "document.delimited.text.render_failed",
                    cause = cause
                ))
                .danger(),
            );
        }

        if let Some(layout) = self.rendered_text() {
            match layout.end {
                TextEnd::Complete => {}

                TextEnd::MoreInFile => notes.push(
                    Text::caption(dbflux_i18n::t!("document.delimited.text.more_in_file"))
                        .muted_foreground(),
                ),

                TextEnd::Capped { shown } => {
                    let loaded = self
                        .loaded()
                        .map_or(0, |loaded| loaded.page_model.records().len());

                    notes.push(
                        Text::caption(dbflux_i18n::t!(
                            "document.delimited.text.capped",
                            shown = shown,
                            loaded = loaded
                        ))
                        .warning(),
                    );
                }
            }
        }

        if self.text.mode == TextMode::Aligned {
            notes.push(
                Text::caption(dbflux_i18n::t!(
                    "document.delimited.text.aligned_hint",
                    width = ALIGNED_MAX_WIDTH
                ))
                .muted_foreground(),
            );
        }

        notes
    }
}

/// Whether `keystroke` is the save key the editors bind to `SaveQuery`, as
/// the effective keymap says.
fn is_save_key(keystroke: &Keystroke) -> bool {
    use dbflux_app::keymap::{Command, ContextId};

    let keymap = dbflux_ui_base::keymap::effective_keymap();

    keymap
        .keys_for_command(ContextId::Editor, Command::SaveQuery)
        .is_some_and(|keys| {
            keys.is_single()
                && *keys.first() == dbflux_ui_base::keymap::key_chord_from_gpui(keystroke)
        })
}
