//! Adding and renaming columns of `DelimitedDocument`, the prompt that asks
//! for a column name, and the offer to load the rest of the file first.
//!
//! A new column goes after the last one, and only once every record of the
//! file is loaded, which the page model enforces. Asking for one on a file
//! that is not fully loaded offers to read the rest of it first. That read
//! puts every remaining record in memory, which the offer says, so it runs
//! in one background task that can be cancelled between two pages; the
//! records read until then stay loaded. Once the file is fully loaded, the
//! name is asked for.
//!
//! A file without a header row has no column names to write: a new column
//! keeps its positional name, which the prompt says, and a rename is refused
//! with how to turn the header row on.
//!
//! Both changes rebuild the table with the pending edits carried over, which
//! is sound because every row keeps its index and every column its position.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dbflux_components::controls::{Button, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{Modal, ModalFocus};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::DocumentMetrics;
use dbflux_core::LogErr;
use dbflux_delimited::{PagedReader, RecordCount};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;

use super::document::{
    DelimitedDocument, OpenError, ReadPage, open_error_to_user_facing, read_page,
};
use super::page_model::{PageModelError, positional_name};
use super::text::MAX_TEXT_BYTES;
use crate::file_source::LocationSource;
use crate::pane::PaneAction;

/// The width of the column prompt and of the offer to load the rest.
const COLUMN_DIALOG_WIDTH: Pixels = px(440.0);

/// What the column prompt does with the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnPromptKind {
    /// Appends a column with the name after the last one.
    Add,

    /// Renames the table column at `column`.
    Rename { column: usize },
}

/// A column prompt asked for, opened on the next render.
pub(super) struct ColumnPromptRequest {
    kind: ColumnPromptKind,

    /// The name the field starts with.
    name: String,

    /// What the prompt says about the name, when it is not written.
    note: Option<String>,
}

/// The open prompt for the name of a new or renamed column.
pub(super) struct ColumnPrompt {
    pub(super) kind: ColumnPromptKind,
    pub(super) input: Entity<InputState>,
    pub(super) note: Option<String>,

    /// Moves the keyboard into the name field, and back to the table when
    /// the prompt closes.
    focus: ModalFocus,

    /// The confirm control follows the typed name, so every edit renders the
    /// prompt again.
    _input_observation: Subscription,
}

/// The open offer to load the rest of the file before a column is added.
pub(super) struct LoadRestPrompt {
    focus: ModalFocus,
}

/// The read of every remaining page while it runs. Dropping it cancels the
/// read before its next page, which is how a cancel, a new reading of the
/// file and a closed tab all stop it.
pub(super) struct LoadingRest {
    pub(super) cancel: Arc<AtomicBool>,
}

impl Drop for LoadingRest {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

/// What the background read of the remaining pages hands to the foreground:
/// the pages read in order, and the failure that stopped it, if one did.
struct RestRead {
    pages: Vec<ReadPage>,
    failure: Option<OpenError>,
}

impl DelimitedDocument {
    // -- Adding and renaming -------------------------------------------------

    /// Asks for the name of a new column after the last one. A file that is
    /// not fully loaded first gets the offer to load the rest of it. Does
    /// nothing while the rows cannot be edited or a page is being read, the
    /// rest of the file included.
    pub fn add_column(&mut self, cx: &mut Context<Self>) {
        if !self.can_change_rows() || self.is_loading_more() {
            return;
        }

        let Some(loaded) = self.loaded() else {
            return;
        };

        if !loaded.page_model.is_fully_loaded() {
            let mut focus = ModalFocus::new(cx);
            focus.focus_on_next_render();

            self.load_rest_prompt = Some(LoadRestPrompt { focus });
            cx.notify();
            return;
        }

        self.request_add_column_prompt(cx);
    }

    /// Asks for a new name for the column the cursor is in, or the first
    /// column when there is no cursor.
    pub fn rename_active_column(&mut self, cx: &mut Context<Self>) {
        let Some(table_state) = self.loaded().map(|loaded| loaded.table_state.clone()) else {
            return;
        };

        let column = table_state
            .read(cx)
            .selection()
            .active
            .map_or(0, |cell| cell.col);

        self.request_rename_column(column, cx);
    }

    /// Asks for a new name for the table column at `column`. A file without
    /// a header record is refused and told how to turn the header row on.
    /// Does nothing while the rows cannot be edited or a dialog is open.
    pub(super) fn request_rename_column(&mut self, column: usize, cx: &mut Context<Self>) {
        if !self.can_change_rows() {
            return;
        }

        let Some(loaded) = self.loaded() else {
            return;
        };

        if loaded.page_model.header().is_none() {
            let error = rename_column_error(
                &self.title(),
                &PageModelError::NoHeader,
                !loaded.dialect.has_header,
            );

            report_error(error, cx);
            return;
        }

        let Some(name) = loaded.page_model.column_names().get(column).cloned() else {
            return;
        };

        self.pending_column_prompt = Some(ColumnPromptRequest {
            kind: ColumnPromptKind::Rename { column },
            name,
            note: None,
        });
        cx.notify();
    }

    /// Asks for the name of a new column of a fully loaded file. The field
    /// starts with the column's positional name, which is the name a file
    /// without a header record keeps, and the prompt says so for such a file.
    fn request_add_column_prompt(&mut self, cx: &mut Context<Self>) {
        let Some(loaded) = self.loaded() else {
            return;
        };

        let name = positional_name(loaded.page_model.column_count());
        let note = loaded
            .page_model
            .header()
            .is_none()
            .then(|| crate::labels::delimited_no_header_column_note(&name));

        self.pending_column_prompt = Some(ColumnPromptRequest {
            kind: ColumnPromptKind::Add,
            name,
            note,
        });
        cx.notify();
    }

    /// Appends a column named `name` after the last one and shows it in the
    /// table with every pending edit kept. Refused and reported unless every
    /// record is loaded, and while a save or a reread runs. Does nothing while
    /// a dialog is open. An edit of the text view's text is applied first,
    /// because the text is mapped back through the columns it was rendered
    /// with, and nothing changes when it cannot be applied.
    pub fn append_column(&mut self, name: String, cx: &mut Context<Self>) {
        let title = self.title();

        if !self.can_edit() {
            let summary = crate::labels::delimited_add_column_failed_message(&title);
            report_error(read_only_column_error(summary), cx);
            return;
        }

        if self.has_open_dialog() || !self.apply_text(cx) {
            return;
        }

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        match loaded.page_model.append_column(name) {
            Ok(()) => loaded.show_page_model(cx),
            Err(error) => report_error(add_column_error(&title, &error), cx),
        }

        self.refresh_dirty(cx);
        cx.notify();
    }

    /// Renames the table column at `column`, a column of the file or one
    /// appended this session, and shows the name in the table with every
    /// pending edit kept. Refused and reported for a file without a header
    /// record, and while a save or a reread runs. Does nothing while a dialog
    /// is open. An edit of the text view's text is applied first, because
    /// the text is mapped back through the columns it was rendered with, and
    /// nothing changes when it cannot be applied.
    pub fn rename_column(&mut self, column: usize, name: String, cx: &mut Context<Self>) {
        let title = self.title();

        if !self.can_edit() {
            let summary = crate::labels::delimited_rename_column_failed_message(&title);
            report_error(read_only_column_error(summary), cx);
            return;
        }

        if self.has_open_dialog() || !self.apply_text(cx) {
            return;
        }

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        let headerless_dialect = !loaded.dialect.has_header;

        match loaded.page_model.rename_column(column, name) {
            Ok(()) => loaded.show_page_model(cx),

            Err(error) => {
                report_error(rename_column_error(&title, &error, headerless_dialect), cx);
            }
        }

        self.refresh_dirty(cx);
        cx.notify();
    }

    // -- The column prompt ---------------------------------------------------

    /// Whether the prompt for a column name is open or asked for.
    pub fn is_column_prompt_open(&self) -> bool {
        self.column_prompt.is_some() || self.pending_column_prompt.is_some()
    }

    /// What the open column prompt says about the name, when it is not
    /// written to the file.
    pub fn column_prompt_note(&self) -> Option<String> {
        match (&self.column_prompt, &self.pending_column_prompt) {
            (Some(prompt), _) => prompt.note.clone(),
            (None, Some(request)) => request.note.clone(),
            (None, None) => None,
        }
    }

    /// Opens the column prompt asked for since the last render, with the
    /// keyboard in its name field.
    pub(super) fn open_pending_column_prompt(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(request) = self.pending_column_prompt.take() else {
            return;
        };

        let input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.delimited.column.name_placeholder"
            ))
        });

        input.update(cx, |state, cx| state.set_value(&request.name, window, cx));

        let input_observation = cx.observe(&input, |_, _, cx| cx.notify());

        let mut focus = ModalFocus::new(cx);
        let input_focus = input.read(cx).focus_handle(cx);
        focus.focus(Some(&input_focus), window, cx);

        self.column_prompt = Some(ColumnPrompt {
            kind: request.kind,
            input,
            note: request.note,
            focus,
            _input_observation: input_observation,
        });
        cx.notify();
    }

    /// Whether the typed name may be confirmed: it is not blank.
    fn column_prompt_has_name(&self, cx: &App) -> bool {
        self.column_prompt
            .as_ref()
            .is_some_and(|prompt| !prompt.input.read(cx).value().trim().is_empty())
    }

    /// Adds or renames the column with the typed name, which is kept as it
    /// was typed. Ignored while the name is blank.
    pub(super) fn confirm_column_prompt(&mut self, cx: &mut Context<Self>) {
        if !self.column_prompt_has_name(cx) {
            return;
        }

        let Some(mut prompt) = self.column_prompt.take() else {
            return;
        };

        let name = prompt.input.read(cx).value().to_string();
        prompt.focus.restore(cx);

        match prompt.kind {
            ColumnPromptKind::Add => self.append_column(name, cx),
            ColumnPromptKind::Rename { column } => self.rename_column(column, name, cx),
        }

        cx.notify();
    }

    /// Closes the column prompt without changing anything.
    pub(super) fn cancel_column_prompt(&mut self, cx: &mut Context<Self>) {
        if let Some(mut prompt) = self.column_prompt.take() {
            prompt.focus.restore(cx);
        }

        cx.notify();
    }

    // -- Loading the rest ----------------------------------------------------

    /// Whether the offer to load the rest of the file is open.
    pub fn is_load_rest_prompt_open(&self) -> bool {
        self.load_rest_prompt.is_some()
    }

    /// Closes the offer and loads the rest of the file, after which the name
    /// of the new column is asked for.
    pub(super) fn confirm_load_rest(&mut self, cx: &mut Context<Self>) {
        if let Some(mut prompt) = self.load_rest_prompt.take() {
            prompt.focus.restore(cx);
        }

        self.load_rest(cx);
        cx.notify();
    }

    /// Closes the offer without reading anything.
    pub(super) fn dismiss_load_rest_prompt(&mut self, cx: &mut Context<Self>) {
        if let Some(mut prompt) = self.load_rest_prompt.take() {
            prompt.focus.restore(cx);
        }

        cx.notify();
    }

    /// Whether the rest of the file is being loaded.
    pub fn is_loading_rest(&self) -> bool {
        self.loaded()
            .is_some_and(|loaded| loaded.loading_rest.is_some())
    }

    /// Reads every page after the loaded ones on the background executor,
    /// in one task, and shows them once it ends. When it read every record
    /// and was not cancelled, the name of the new column is asked for.
    ///
    /// Every record goes into memory, which is why the user is told before
    /// it starts. Cancelling stops the read before its next page and keeps
    /// the pages read until then. A failed page stops the read, is reported,
    /// and keeps the pages before it. Does nothing when a page cannot be
    /// asked for now ([`Self::can_load_more`]), or when an edit of the text
    /// view's text, which is applied first, cannot be applied. An object is
    /// read through the live connection of its profile, as a further page
    /// is.
    pub fn load_rest(&mut self, cx: &mut Context<Self>) {
        if !self.can_load_more() || !self.apply_text(cx) {
            return;
        }

        let summary = crate::labels::delimited_load_more_failed_message(&self.title());

        let Ok(connection) = self.use_live_connection(summary, cx) else {
            return;
        };

        let page_size = self.reader_options.page_size;

        let Some(loaded) = self.loaded_mut() else {
            return;
        };
        let Some(mut reader) = loaded.reader.take() else {
            return;
        };

        let first_page = loaded.page_model.next_page();
        let reader_epoch = loaded.reader_epoch;
        let keep_budget = loaded.source_span.budget(MAX_TEXT_BYTES);

        if let Some(connection) = connection {
            reader.source_mut().use_connection(connection);
        }

        let cancel = Arc::new(AtomicBool::new(false));
        loaded.loading_rest = Some(LoadingRest {
            cancel: cancel.clone(),
        });

        let task = cx.background_executor().spawn(async move {
            let read =
                read_remaining_pages(&mut reader, first_page, page_size, keep_budget, &cancel);

            (reader, read)
        });

        cx.spawn(async move |this, cx| {
            let (reader, read) = task.await;

            cx.update(|cx| {
                this.update(cx, |document, cx| {
                    document.apply_rest_outcome(reader_epoch, reader, read, cx);
                })
                .ok();
            });
        })
        .detach();

        cx.notify();
    }

    /// Stops loading the rest of the file before its next page. The pages
    /// read until then are shown when the read ends, and no column is asked
    /// for.
    pub fn cancel_load_rest(&mut self, cx: &mut Context<Self>) {
        if let Some(loaded) = self.loaded_mut() {
            loaded.loading_rest = None;
        }

        cx.notify();
    }

    /// Takes the reader back, shows the pages the background read returned
    /// and, when it read the whole file and was not cancelled, asks for the
    /// name of the new column. This is the first place a failed page is
    /// caught, so it is reported here and only here.
    ///
    /// The pages of a reader that a new reading of the file replaced belong
    /// to a version no longer shown, and are dropped with it.
    fn apply_rest_outcome(
        &mut self,
        reader_epoch: u64,
        reader: PagedReader<LocationSource>,
        read: RestRead,
        cx: &mut Context<Self>,
    ) {
        let title = self.title();

        let Some(loaded) = self.loaded_mut() else {
            return;
        };

        if loaded.reader_epoch != reader_epoch {
            return;
        }

        loaded.reader = Some(reader);

        let still_wanted = loaded.loading_rest.take().is_some();
        let has_pages = !read.pages.is_empty();
        let mut failure = read.failure;

        for page in read.pages {
            if let Err(error) = loaded.append_read(page) {
                failure = Some(OpenError::from(error));
                break;
            }
        }

        if has_pages {
            loaded.show_page_model(cx);
        }

        let is_fully_loaded = loaded.page_model.is_fully_loaded();

        match failure {
            Some(error) => {
                let summary = crate::labels::delimited_load_more_failed_message(&title);

                report_error(open_error_to_user_facing(&error, summary), cx);
            }

            None if still_wanted && is_fully_loaded => self.request_add_column_prompt(cx),

            None => {}
        }

        cx.notify();
    }

    // -- Pane actions --------------------------------------------------------

    /// Add a column and rename the column the cursor is in, enabled while
    /// the rows can be edited and no dialog is open. Adding is also disabled while a page is being
    /// read, the rest of the file included.
    pub(super) fn column_pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        let run =
            |id: &'static str,
             label: String,
             run: fn(&mut DelimitedDocument, &mut Context<DelimitedDocument>)| {
                let target = this.downgrade();

                PaneAction::callback(id, label, move |_window, cx| {
                    if let Some(document) = target.upgrade() {
                        document.update(cx, run);
                    }
                })
            };

        vec![
            run(
                "delimited-add-column",
                dbflux_i18n::t!("document.delimited.action.add_column"),
                Self::add_column,
            )
            .icon(AppIcon::Columns)
            .enabled(self.can_change_rows() && !self.is_loading_more()),
            run(
                "delimited-rename-column",
                dbflux_i18n::t!("document.delimited.action.rename_column"),
                Self::rename_active_column,
            )
            .icon(AppIcon::Pencil)
            .enabled(self.can_change_rows()),
        ]
    }

    /// The cancel of the running load of the rest, while it runs.
    pub(super) fn load_rest_pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        if !self.is_loading_rest() {
            return Vec::new();
        }

        let target = this.downgrade();

        vec![
            PaneAction::callback(
                "delimited-load-rest-cancel",
                dbflux_i18n::t!("document.delimited.load_rest.cancel"),
                move |_window, cx| {
                    if let Some(document) = target.upgrade() {
                        document.update(cx, Self::cancel_load_rest);
                    }
                },
            )
            .icon(AppIcon::CircleX),
        ]
    }

    // -- Rendering -----------------------------------------------------------

    /// The control that cancels the running load of the rest, for the
    /// footer. `None` while it does not run.
    pub(super) fn render_load_rest_cancel(&self, cx: &Context<Self>) -> Option<Button> {
        if !self.is_loading_rest() {
            return None;
        }

        Some(
            Button::new(
                "delimited-load-rest-cancel",
                dbflux_i18n::t!("document.delimited.load_rest.cancel"),
            )
            .inline()
            .icon(AppIcon::CircleX)
            .on_click(cx.listener(|this, _, _, cx| {
                this.cancel_load_rest(cx);
            })),
        )
    }

    /// The open column prompt: the name field, the note about a name that
    /// is not written, and cancel and confirm. Enter confirms a name that is
    /// not blank and Escape cancels.
    pub(super) fn render_column_prompt(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let prompt = self.column_prompt.as_ref()?;
        let has_name = self.column_prompt_has_name(cx);

        let (title, confirm_label) = match prompt.kind {
            ColumnPromptKind::Add => (
                dbflux_i18n::t!("document.delimited.column.add_title"),
                dbflux_i18n::t!("document.delimited.column.confirm_add"),
            ),

            ColumnPromptKind::Rename { .. } => (
                dbflux_i18n::t!("document.delimited.column.rename_title"),
                dbflux_i18n::t!("document.delimited.column.confirm_rename"),
            ),
        };

        let body = div()
            .flex()
            .flex_col()
            .gap(DocumentMetrics::GAP)
            .child(Input::new(&prompt.input).small().w_full())
            .when_some(prompt.note.clone(), |body, note| {
                body.child(Text::caption(note).muted_foreground())
            });

        let footer = div()
            .flex()
            .gap(DocumentMetrics::GAP)
            .child(
                Button::new(
                    "delimited-column-name-cancel",
                    dbflux_i18n::t!("document.delimited.action.cancel"),
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.cancel_column_prompt(cx);
                })),
            )
            .child(
                Button::new("delimited-column-name-confirm", confirm_label)
                    .primary()
                    .icon(AppIcon::Columns)
                    .disabled(!has_name)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.confirm_column_prompt(cx);
                    })),
            );

        let document = cx.weak_entity();
        let confirm_target = document.clone();

        Some(
            Modal::new(title)
                .id("delimited-column-prompt")
                .icon(AppIcon::Columns)
                .width(COLUMN_DIALOG_WIDTH)
                .focus_handle(prompt.focus.handle())
                .on_close(move |_window, cx| {
                    document
                        .update(cx, |this, cx| this.cancel_column_prompt(cx))
                        .log_err();
                })
                .on_confirm(move |_window, cx| {
                    confirm_target
                        .update(cx, |this, cx| this.confirm_column_prompt(cx))
                        .log_err();
                })
                .confirm_enabled(has_name)
                .body(body)
                .footer(footer)
                .into_any_element(),
        )
    }

    /// The open offer to load the rest of the file: what it costs, and
    /// dismiss and confirm. Enter confirms and Escape dismisses.
    pub(super) fn render_load_rest_prompt(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let loaded_records = self.loaded()?.page_model.records().len();
        let source_length = self.loaded()?.source_length;

        let prompt = self.load_rest_prompt.as_mut()?;
        prompt.focus.apply_pending(window, cx);

        let body = Text::body(crate::labels::delimited_load_rest_body(
            loaded_records,
            source_length,
        ));

        let footer = div()
            .flex()
            .gap(DocumentMetrics::GAP)
            .child(
                Button::new(
                    "delimited-load-rest-dismiss",
                    dbflux_i18n::t!("document.delimited.action.cancel"),
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.dismiss_load_rest_prompt(cx);
                })),
            )
            .child(
                Button::new(
                    "delimited-load-rest-confirm",
                    dbflux_i18n::t!("document.delimited.load_rest.confirm"),
                )
                .primary()
                .icon(AppIcon::ChevronDown)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.confirm_load_rest(cx);
                })),
            );

        let document = cx.weak_entity();
        let confirm_target = document.clone();

        Some(
            Modal::new(dbflux_i18n::t!("document.delimited.load_rest.title"))
                .id("delimited-load-rest-prompt")
                .icon(AppIcon::ChevronDown)
                .width(COLUMN_DIALOG_WIDTH)
                .focus_handle(prompt.focus.handle())
                .on_close(move |_window, cx| {
                    document
                        .update(cx, |this, cx| this.dismiss_load_rest_prompt(cx))
                        .log_err();
                })
                .on_confirm(move |_window, cx| {
                    confirm_target
                        .update(cx, |this, cx| this.confirm_load_rest(cx))
                        .log_err();
                })
                .body(body)
                .footer(footer)
                .into_any_element(),
        )
    }
}

/// Reads pages from `first_page` on through `reader` until the last record,
/// a failed read, or `cancel`, which is checked before every page, keeping
/// for the text view the bytes of the leading records that fit in
/// `keep_budget` bytes. Blocks on file or network I/O.
fn read_remaining_pages(
    reader: &mut PagedReader<LocationSource>,
    first_page: usize,
    page_size: NonZeroUsize,
    mut keep_budget: usize,
    cancel: &AtomicBool,
) -> RestRead {
    let mut pages = Vec::new();
    let mut page_index = first_page;

    while !cancel.load(Ordering::Relaxed) {
        match read_page(reader, page_index, page_size, keep_budget) {
            Ok(read) => {
                keep_budget = remaining_budget(keep_budget, &read);

                let reaches_end = reaches_end(&read);
                pages.push(read);

                if reaches_end {
                    break;
                }

                page_index += 1;
            }

            Err(error) => {
                return RestRead {
                    pages,
                    failure: Some(error),
                };
            }
        }
    }

    RestRead {
        pages,
        failure: None,
    }
}

/// What is left of `budget` after `read` kept its bytes: nothing once a
/// record of the page was not kept, because the kept bytes cannot skip it.
pub(super) fn remaining_budget(budget: usize, read: &ReadPage) -> usize {
    let page_length = match (read.page.records.first(), read.page.records.last()) {
        (Some(first), Some(last)) => last.byte_range.end - first.byte_range.start,
        _ => return budget,
    };

    match &read.bytes {
        Some(bytes) if bytes.len() as u64 == page_length => budget.saturating_sub(bytes.len()),
        _ => 0,
    }
}

/// Whether `read` holds the last record of the file, which is when its
/// count is a total that ends with it.
fn reaches_end(read: &ReadPage) -> bool {
    let end = read.page.first_record + read.page.records.len() as u64;

    matches!(read.record_count, RecordCount::Total(total) if total == end)
}

/// The user-facing error of a column the file named `file_name` did not
/// get.
fn add_column_error(file_name: &str, error: &PageModelError) -> UserFacingError {
    let summary = crate::labels::delimited_add_column_failed_message(file_name);

    let cause = match error {
        PageModelError::FullLoadRequired => {
            dbflux_i18n::t!("document.delimited.error.add_column_needs_full_load")
        }

        other => other.to_string(),
    };

    UserFacingError::new(ErrorKind::User, summary).with_cause(cause)
}

/// The user-facing error of a column change asked for while a save or a
/// reread runs, under `summary`.
fn read_only_column_error(summary: String) -> UserFacingError {
    let cause = dbflux_i18n::t!("document.delimited.error.column_while_read_only");

    UserFacingError::new(ErrorKind::User, summary).with_cause(cause)
}

/// The user-facing error of a rename the file named `file_name` refused.
/// `headerless_dialect` says whether the file is read without a header row,
/// which the toolbar turns on, rather than being a file without records.
fn rename_column_error(
    file_name: &str,
    error: &PageModelError,
    headerless_dialect: bool,
) -> UserFacingError {
    let summary = crate::labels::delimited_rename_column_failed_message(file_name);

    let cause = match error {
        PageModelError::NoHeader => rename_refused_cause(headerless_dialect),
        other => other.to_string(),
    };

    UserFacingError::new(ErrorKind::User, summary).with_cause(cause)
}

/// Why a column of a file without a header record cannot be renamed, and
/// what to do about it. `headerless_dialect` is a file read without a header
/// row, which the header row control of the toolbar turns on; otherwise the
/// file has no record at all.
pub(super) fn rename_refused_cause(headerless_dialect: bool) -> String {
    if headerless_dialect {
        dbflux_i18n::t!("document.delimited.error.rename_no_header")
    } else {
        dbflux_i18n::t!("document.delimited.error.rename_no_header_record")
    }
}
