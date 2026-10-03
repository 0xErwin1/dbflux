//! The text view of `DelimitedDocument` and the switch between it and the
//! table.
//!
//! The two views show the same data: the page model and the table's edit
//! buffer stay the only pending state, and switching never touches either,
//! so pending edits and the table's cursor survive it both ways. An inline
//! cell edit that is open when the text is shown is committed first, so the
//! text includes it.
//!
//! The text is rendered by `text.rs`, raw or aligned. It is worked out again
//! only when it is drawn after something it depends on changed: the table
//! notifies on every change of its rows, edits, pages and dialect, and the
//! mode can change. It is never worked out per frame. Rendering it again
//! keeps the editor's scroll position and cursor, and showing the text puts
//! the cursor on the line of the table's active row.
//!
//! Aligned text is read-only. Raw text is editable while it shows every
//! loaded record and the rows can be edited: an edit of part of the loaded
//! records cannot be mapped back to them safely, so a capped text stays
//! read-only and says why. An edit of the text is not pending state of its
//! own: it is applied to the table's edit buffer and the page model
//! (`text_edit.rs` works out how) when the user leaves the raw text, for
//! the table or the aligned text, and before anything that needs the
//! table's state runs: a save, the next page, the rest of the file, a
//! reload, a dialect change and the save of a closing tab. A discard throws
//! the edit away without reading it, with the rest of the pending changes.
//! The
//! text is then rendered again from the pending state, so it shows what a
//! save writes. The document is dirty while the editor's text differs from
//! the text it was given. Text that cannot be applied stays in the editor,
//! the action that needed it does not run, and the reason is reported and
//! shown below the text until the text changes. The editor's own undo works
//! inside the text, and its history ends when the text is rendered again.
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

use std::borrow::Cow;

use dbflux_components::controls::InputEvent;
use dbflux_components::primitives::{SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::DocumentMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Editor, EditorState};

use super::document::DelimitedDocument;
use super::editing::install_page_model;
use super::page_model::{PageModel, PageModelError};
use super::text::{
    ALIGNED_MAX_WIDTH, RenderedText, TEXT_LIMITS, TextEnd, TextLayout, TextRow, check_pending,
    render_aligned, render_raw,
};
use super::text_edit::{TextEditError, TextEdits, apply_to_pending, text_edits};
use dbflux_components::components::data_table::DataTableState;
use dbflux_components::components::data_table::model::{CellValue, VisualRowSource};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

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

    /// Where each row of the table is in the text the editor was given, or
    /// why the pending changes cannot be shown as text.
    pub(super) layout: Result<TextLayout, String>,

    /// The text the editor was given, which the user's edit of it is
    /// compared with. The editor shares its bytes.
    pub(super) rendered: SharedString,

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

    /// Whether the editor's text differs from the text it was given.
    pub(super) edited: bool,

    /// Why the edited text could not be applied the last time it was tried,
    /// until the text changes.
    pub(super) apply_error: Option<String>,
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

        if self.text.view == DelimitedView::Text && !self.apply_text(cx) {
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

    /// Shows the text as `mode`. Leaving the raw text applies an edit of it
    /// first, and stays when that cannot be applied.
    pub fn set_text_mode(&mut self, mode: TextMode, cx: &mut Context<Self>) {
        if self.text.mode == mode || !self.apply_text(cx) {
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

        // An edit of the text is applied before anything changes the
        // pending state, so a text that is still edited is never replaced.
        if self.text.shown.is_none() || (self.text.stale && !self.text.edited) {
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
        self.text.edited = false;
        self.text.apply_error = None;

        let kept_cursor = layout.as_ref().ok().and_then(|layout| {
            let (row, within) = cursor_row?;

            layout.offset_in(row, within)
        });

        let wraps = layout.as_ref().is_ok_and(|layout| layout.wraps);

        let input = match &mut self.text.shown {
            Some(view) => {
                view.layout = layout;
                view.rendered = text.clone();
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
                    |this, _, event: &InputEvent, _window, cx| match event {
                        // A click into the editor takes the keyboard back.
                        InputEvent::Focus if !this.text.has_keyboard => {
                            this.text.has_keyboard = true;
                            cx.notify();
                        }

                        InputEvent::Change => this.follow_text_change(cx),

                        _ => {}
                    },
                );

                self.text.shown = Some(TextView {
                    input: input.clone(),
                    layout,
                    rendered: text.clone(),
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

    // -- Editing the text ----------------------------------------------------

    /// Whether the user can edit the text now: the raw text is shown, it
    /// holds every loaded record and renders, and the rows can be edited,
    /// with no page being read.
    pub(super) fn is_text_editable(&self) -> bool {
        let shows_every_loaded_record = self
            .rendered_text()
            .is_some_and(|layout| !matches!(layout.end, TextEnd::Capped { .. }));

        self.text.view == DelimitedView::Text
            && self.text.mode == TextMode::Raw
            && shows_every_loaded_record
            && self.can_edit()
            && !self.is_loading_more()
            && !self.is_loading_rest()
    }

    /// Follows an edit in the editor: the document is dirty while the text
    /// differs from the one the editor was given, and a reason the text
    /// could not be applied no longer holds once it changed.
    fn follow_text_change(&mut self, cx: &mut Context<Self>) {
        let Some(view) = &self.text.shown else {
            return;
        };

        let edited = *view.input.read(cx).text() != *view.rendered.as_ref();

        self.text.edited = edited;
        self.text.apply_error = None;

        self.refresh_dirty(cx);
        cx.notify();
    }

    /// Applies an edit of the raw text to the pending state, through the
    /// operations the table makes, and has the text rendered again from it.
    /// Returns whether the pending state now holds the text: true when the
    /// text was not edited.
    ///
    /// Text that cannot be applied changes nothing, stays in the editor, is
    /// reported, and says why below the text. That is text the reader would
    /// not read as it reads, a change the table cannot make, and a pending
    /// state a save would refuse to write, which the writer's own check finds
    /// before anything is applied. A pending state that changed since the
    /// text was rendered, which nothing does while the text is edited, is
    /// refused the same way rather than mapped onto rows it does not
    /// describe.
    pub(super) fn apply_text(&mut self, cx: &mut Context<Self>) -> bool {
        if !self.text.edited {
            return true;
        }

        let result = self
            .text_edits_now(cx)
            .and_then(|edits| {
                self.check_text_edits(&edits, cx)?;
                Ok(edits)
            })
            .and_then(|edits| {
                self.apply_edits_of_text(&edits, cx)
                    .map_err(|error| error.to_string())
            });

        match result {
            Ok(()) => {
                self.text.edited = false;
                self.text.apply_error = None;

                self.refresh_dirty(cx);
                self.mark_text_stale(cx);
                cx.notify();

                true
            }

            Err(cause) => {
                let summary = crate::labels::delimited_text_apply_failed_message(&self.title());

                report_error(
                    UserFacingError::new(ErrorKind::User, summary).with_cause(cause.clone()),
                    cx,
                );

                self.text.apply_error = Some(cause);
                cx.notify();

                false
            }
        }
    }

    /// Throws an edit of the text away without reading it: the document no
    /// longer counts it as a change, and the text is rendered again from the
    /// pending state the next time it is drawn.
    pub(super) fn drop_text_edit(&mut self, cx: &mut Context<Self>) {
        self.text.edited = false;
        self.text.apply_error = None;

        self.mark_text_stale(cx);
    }

    /// The changes the editor's text makes to the pending state, or why it
    /// cannot be applied, as the user is told.
    ///
    /// The text is mapped back through the rows the rendered text shows, so
    /// the pending state must still be the one it was rendered from. Every
    /// change of the pending state marks the text stale, so only a stale text
    /// is rendered again to compare: an edit applied right after the text was
    /// rendered costs no render.
    fn text_edits_now(&self, cx: &App) -> Result<TextEdits, String> {
        let (Some(loaded), Some(view)) = (self.loaded(), self.text.shown.as_ref()) else {
            return Ok(TextEdits::default());
        };

        let edited = view.input.read(cx).text().to_string();

        if edited == view.rendered.as_ref() {
            return Ok(TextEdits::default());
        }

        let unmapped = || TextEditError::Unmapped.cause();

        let current;

        let (layout, rendered) = if self.text.stale {
            current = match self.render_text(cx) {
                Ok(rendered) if rendered.text == view.rendered.as_ref() => rendered,
                _ => return Err(unmapped()),
            };

            (&current.layout, current.text.as_str())
        } else {
            let layout = view.layout.as_ref().map_err(|_| unmapped())?;

            (layout, view.rendered.as_ref())
        };

        let buffer = loaded.table_state.read(cx).edit_buffer();

        text_edits(
            layout,
            rendered,
            &edited,
            &loaded.dialect,
            &loaded.page_model,
            buffer,
        )
        .map_err(|error| error.cause())
    }

    /// Refuses `edits` when a save of the pending state they make would be
    /// refused, with the writer's own reason: the check the raw text makes
    /// before it renders, run on a copy of the pending state with the edits
    /// made, so nothing is applied when it fails.
    fn check_text_edits(&self, edits: &TextEdits, cx: &App) -> Result<(), String> {
        let Some(loaded) = self.loaded() else {
            return Ok(());
        };

        if edits.is_empty() {
            return Ok(());
        }

        let mut buffer = loaded.table_state.read(cx).edit_buffer().clone();

        let mut model = Cow::Borrowed(&loaded.page_model);

        apply_to_pending(edits, &mut model, &mut buffer).map_err(|error| error.to_string())?;

        check_pending(&model, &loaded.source_span, &buffer, &loaded.dialect)
            .map_err(|error| error.to_string())
    }

    /// Applies `edits` to the page model and the table of the loaded file.
    fn apply_edits_of_text(
        &mut self,
        edits: &TextEdits,
        cx: &mut Context<Self>,
    ) -> Result<(), PageModelError> {
        if edits.is_empty() {
            return Ok(());
        }

        let Some(loaded) = self.loaded_mut() else {
            return Ok(());
        };

        let table_state = loaded.table_state.clone();
        let page_model = &mut loaded.page_model;

        table_state.update(cx, |state, cx| {
            apply_text_edits(edits, page_model, state, cx)
        })
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

    /// The text view: the editor, read-only unless the user can edit the
    /// text now, and below it the notes of [`Self::text_notes`].
    pub(super) fn render_text_body(&self, cx: &Context<Self>) -> AnyElement {
        let editable = self.is_text_editable();

        let editor = self.text_input().map(|input| {
            Editor::new(input)
                .readonly(!editable)
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
    /// it shows only the first loaded records, why an edit of the text could
    /// not be applied, that a capped raw text cannot be edited, what an edit
    /// of the raw text keeps from the file, and how aligned text marks what
    /// it changes.
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

        if let Some(cause) = &self.text.apply_error {
            notes.push(
                Text::caption(dbflux_i18n::t!(
                    "document.delimited.text.apply_failed",
                    cause = cause
                ))
                .danger(),
            );
        }

        let is_capped = self
            .rendered_text()
            .is_some_and(|layout| matches!(layout.end, TextEnd::Capped { .. }));

        if self.text.mode == TextMode::Raw && is_capped {
            notes.push(
                Text::caption(dbflux_i18n::t!("document.delimited.text.read_only_capped"))
                    .warning(),
            );
        }

        if self.is_text_editable() {
            notes.push(
                Text::caption(dbflux_i18n::t!("document.delimited.text.raw_edit_hint"))
                    .muted_foreground(),
            );
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

/// Applies `edits`, worked out from an edit of the raw text, to the page
/// model and to the table of `state`, through the operations the table and
/// the page model make for the user: the column renames and appended
/// columns first, with the table rebuilt for them and every pending edit
/// kept, then the cell edits, the deletions, the removed inserts, from the
/// last, and the new inserts, padded to the columns. Each of the table's
/// operations is a step of its undo history, as when the user makes it.
///
/// # Errors
///
/// A column change the page model refuses. `text_edits` refuses those
/// first, so the table is not reached.
pub(super) fn apply_text_edits(
    edits: &TextEdits,
    page_model: &mut PageModel,
    state: &mut DataTableState,
    cx: &mut Context<DataTableState>,
) -> Result<(), PageModelError> {
    for (column, name) in &edits.renames {
        page_model.rename_column(*column, name.clone())?;
    }

    for name in &edits.appended_columns {
        page_model.append_column(name.clone())?;
    }

    if !edits.renames.is_empty() || !edits.appended_columns.is_empty() {
        install_page_model(state, page_model, cx);
    }

    for (row, column, value) in &edits.base_cells {
        state.stage_base_cell_value(*row, *column, CellValue::text(value));
    }

    let buffer = state.edit_buffer_mut();

    for (insert, column, value) in &edits.insert_cells {
        buffer.set_insert_cell(*insert, *column, CellValue::text(value));
    }

    for row in &edits.deleted_rows {
        buffer.mark_for_delete(*row);
    }

    let mut removed = edits.removed_inserts.clone();
    removed.sort_unstable();
    removed.dedup();

    for insert in removed.into_iter().rev() {
        buffer.remove_pending_insert_by_idx(insert);
    }

    let column_count = state.col_count();

    for (anchor, fields) in &edits.added_inserts {
        let mut row: Vec<CellValue> = fields.iter().map(|field| CellValue::text(field)).collect();

        if row.len() < column_count {
            row.resize(column_count, CellValue::text(""));
        }

        state.edit_buffer_mut().add_pending_insert_at(*anchor, row);
    }

    cx.notify();

    Ok(())
}
