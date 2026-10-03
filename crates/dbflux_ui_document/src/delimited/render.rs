//! Rendering of `DelimitedDocument`.
//!
//! Layout, top to bottom: the toolbar, the warnings of the opened file, the
//! table of the loaded records or their text, and a footer with the delimiter, the
//! encoding, the record count and, while the file has more records, the
//! control that loads the next page. The toolbar holds the switch between
//! the table and the text (and between raw and aligned text), the dialect controls,
//! the control that reads the file again from its source and, for a file
//! that can be saved in place, the controls that insert a row above the
//! cursor, add a column, discard the changes and save them. The toolbar wraps when the tab
//! is narrow; the footer is one fixed row. While the file is read again under an
//! override the footer says so and that control takes no click. While the
//! first page is read, and when opening failed, a centered notice takes the
//! place of all four. The modal cell editor, the column prompt and the offer
//! to load the rest of the file are drawn over all of it.

use dbflux_components::components::data_table::DataTable;
use dbflux_components::composites::EmptyState;
use dbflux_components::controls::{Button, Checkbox, Dropdown};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::DocumentMetrics;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

use super::document::DelimitedDocument;
use super::text_view::DelimitedView;
use crate::chrome::document_footer;

/// The width of a select of the dialect toolbar: the longest item, a value
/// marked as detected, fits without being cut.
const DIALECT_SELECT_WIDTH: Pixels = px(190.0);

/// The keys that save this document from the keyboard, as the Save button
/// shows them: the save key the editors use, when the table answers it with
/// a save, which is the key a user presses. Otherwise the first key the
/// table binds to its save.
pub(super) fn save_shortcut_label() -> Option<SharedString> {
    use dbflux_app::keymap::{Command, ContextId};

    let keymap = dbflux_ui_base::keymap::effective_keymap();

    let document_save = keymap
        .keys_for_command(ContextId::Editor, Command::SaveQuery)
        .filter(|keys| {
            keymap.resolve_sequence(ContextId::DataTable, keys) == Some(Command::SaveRow)
        });

    match document_save {
        Some(keys) => Some(dbflux_ui_base::keymap::key_sequence_label(keys)),
        None => dbflux_ui_base::keymap::shortcut_label(ContextId::DataTable, Command::SaveRow),
    }
}

impl DelimitedDocument {
    fn render_notice(&self, notice: EmptyState, cx: &Context<Self>) -> AnyElement {
        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .p(DocumentMetrics::PADDING_X)
            .bg(cx.theme().background)
            .child(notice)
            .into_any_element()
    }

    /// The toolbar: three groups, the view switch, the dialect controls
    /// (the delimiter, quote and encoding selects, the header checkbox and
    /// the control that drops every override) and the file actions (the
    /// reload and the edit controls). `None` until the file is loaded. When the tab is narrow the
    /// toolbar wraps by group first, so each group stays together, and a
    /// group wraps within itself only when it is wider than the tab.
    fn render_dialect_toolbar(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let controls = self.dialect_controls()?;
        let requested = self.requested_dialect()?;
        let theme = cx.theme();

        let select = |label: String, dropdown: &Entity<Dropdown>| {
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .child(Text::caption(label))
                .child(div().w(DIALECT_SELECT_WIDTH).child(dropdown.clone()))
        };

        let document = cx.entity().downgrade();

        let header = Checkbox::new("delimited-header")
            .checked(requested.has_header)
            .label(self.header_label())
            .on_click(move |has_header, _window, cx| {
                if let Some(document) = document.upgrade() {
                    document.update(cx, |document, cx| {
                        document.override_has_header(*has_header, cx);
                    });
                }
            });

        let reset = Button::new(
            "delimited-dialect-reset",
            dbflux_i18n::t!("document.delimited.toolbar.reset"),
        )
        .inline()
        .icon(AppIcon::RotateCcw)
        .disabled(!self.has_dialect_overrides() || self.has_open_dialog())
        .on_click(cx.listener(|this, _, _, cx| {
            this.reset_dialect(cx);
        }));

        let dialect_controls = div()
            .id("delimited-dialect-controls")
            .flex()
            .flex_wrap()
            .items_center()
            .max_w_full()
            .gap(DocumentMetrics::GAP)
            .child(select(
                dbflux_i18n::t!("document.delimited.toolbar.delimiter"),
                &controls.delimiter,
            ))
            .child(select(
                dbflux_i18n::t!("document.delimited.toolbar.quote"),
                &controls.quote,
            ))
            .child(div().flex_shrink_0().child(header))
            .child(select(
                dbflux_i18n::t!("document.delimited.toolbar.encoding"),
                &controls.encoding,
            ))
            .child(reset);

        let file_actions = div()
            .id("delimited-file-actions")
            .flex()
            .flex_wrap()
            .items_center()
            .max_w_full()
            .gap(DocumentMetrics::GAP)
            .child(self.render_reload(cx))
            .children(self.render_edit_controls(cx));

        Some(
            div()
                .id("delimited-toolbar")
                .flex()
                .flex_wrap()
                .flex_shrink_0()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .px(DocumentMetrics::PADDING_X)
                .py(DocumentMetrics::GAP)
                .border_b_1()
                .border_color(theme.border)
                .children(self.render_view_controls(cx))
                .child(dialect_controls)
                .child(div().flex_1())
                .child(file_actions)
                .into_any_element(),
        )
    }

    fn render_warnings(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let warnings = self.warning_items();

        if warnings.is_empty() {
            return None;
        }

        Some(
            div()
                .flex()
                .flex_col()
                .flex_shrink_0()
                .gap(DocumentMetrics::GAP)
                .px(DocumentMetrics::PADDING_X)
                .py(DocumentMetrics::GAP)
                .border_b_1()
                .border_color(cx.theme().border)
                .children(
                    warnings
                        .iter()
                        .map(|warning| Text::caption(warning.clone()).warning()),
                )
                .into_any_element(),
        )
    }

    /// The control that loads the next page. `None` once every record is
    /// loaded, and while the rest of the file is loaded. While a page is
    /// being read it says so and takes no click, and it takes none while the
    /// file is read again under an override.
    fn render_load_more(&self, cx: &Context<Self>) -> Option<Button> {
        // The load of the rest shows its own label and cancel instead.
        if !self.has_more_records() || self.is_loading_rest() {
            return None;
        }

        let is_loading = self.is_loading_more();

        let label = if is_loading {
            dbflux_i18n::t!("document.delimited.footer.loading_more")
        } else {
            dbflux_i18n::t!("document.delimited.footer.load_more")
        };

        Some(
            Button::new("delimited-load-more", label)
                .inline()
                .icon(AppIcon::ChevronDown)
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(
                        dbflux_app::keymap::ContextId::Results,
                        dbflux_app::keymap::Command::ResultsNextPage,
                    ),
                    Button::kbd,
                )
                .disabled(!self.can_load_more())
                .on_click(cx.listener(|this, _, _, cx| {
                    this.load_more(cx);
                })),
        )
    }

    /// The control that reads the file again from its source. It takes a
    /// click while there are changes too, and then says why it refuses.
    fn render_reload(&self, cx: &Context<Self>) -> Button {
        Button::new(
            "delimited-reload",
            dbflux_i18n::t!("document.delimited.action.reload"),
        )
        .inline()
        .icon(AppIcon::RefreshCcw)
        .when_some(
            dbflux_ui_base::keymap::shortcut_label(
                dbflux_app::keymap::ContextId::Results,
                dbflux_app::keymap::Command::RefreshSchema,
            ),
            Button::kbd,
        )
        .disabled(self.has_open_dialog())
        .on_click(cx.listener(|this, _, _, cx| {
            this.reload(cx);
        }))
    }

    /// The controls that insert a row above the cursor, add a column,
    /// discard the changes and save them. Empty for a file that cannot be
    /// saved in place, whose table is read-only. Save takes a click only in
    /// the states [`DelimitedDocument::save`] writes in, discard only while
    /// there are changes it can drop, and insert and add a column only while
    /// the rows can be changed.
    fn render_edit_controls(&self, cx: &Context<Self>) -> Vec<Button> {
        if !self.shows_edit_controls() {
            return Vec::new();
        }

        let insert_above = Button::new(
            "delimited-insert-above",
            dbflux_i18n::t!("document.delimited.action.insert_above"),
        )
        .inline()
        .icon(AppIcon::Plus)
        .disabled(!self.can_change_rows())
        .on_click(cx.listener(|this, _, _, cx| {
            this.insert_row_above(cx);
        }));

        let add_column = Button::new(
            "delimited-add-column",
            dbflux_i18n::t!("document.delimited.action.add_column"),
        )
        .inline()
        .icon(AppIcon::Columns)
        .disabled(!self.can_change_rows() || self.is_loading_more())
        .on_click(cx.listener(|this, _, _, cx| {
            this.add_column(cx);
        }));

        let discard = Button::new(
            "delimited-discard",
            dbflux_i18n::t!("document.delimited.action.discard"),
        )
        .inline()
        .icon(AppIcon::RotateCcw)
        .disabled(!self.can_discard())
        .on_click(cx.listener(|this, _, _, cx| {
            this.discard_changes(cx);
        }));

        let (save_label, save_icon) = if self.is_saving() {
            (
                dbflux_i18n::t!("document.delimited.action.saving"),
                AppIcon::Loader,
            )
        } else {
            (
                dbflux_i18n::t!("document.delimited.action.save"),
                AppIcon::Save,
            )
        };

        // A disabled primary button keeps its fill, which reads as a live
        // control, so Save takes the primary style only while it saves.
        let can_save = self.can_save();

        let save = Button::new("delimited-save", save_label)
            .inline()
            .when(can_save, Button::primary)
            .icon(save_icon)
            .when_some(save_shortcut_label(), Button::kbd)
            .disabled(!can_save)
            .on_click(cx.listener(|this, _, _, cx| {
                this.save(cx);
            }));

        vec![insert_above, add_column, discard, save]
    }

    fn render_loaded(&self, table: Entity<DataTable>, cx: &Context<Self>) -> AnyElement {
        let body = match self.view() {
            DelimitedView::Table => div().flex_1().min_h_0().child(table).into_any_element(),
            DelimitedView::Text => self.render_text_body(cx),
        };

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .children(self.render_dialect_toolbar(cx))
            .children(self.render_warnings(cx))
            .child(body)
            .child(
                document_footer(cx)
                    .id("delimited-footer")
                    .overflow_hidden()
                    .child(
                        // The text gives way, truncated, so the controls after it
                        // stay inside a narrow pane.
                        div()
                            .flex()
                            .flex_1()
                            .min_w_0()
                            .overflow_hidden()
                            .items_center()
                            .gap(DocumentMetrics::GAP)
                            .children(
                                self.status_items()
                                    .iter()
                                    .map(|item| div().min_w_0().truncate().child(item.clone())),
                            )
                            .children(
                                self.progress_item()
                                    .map(|item| div().min_w_0().truncate().child(item)),
                            ),
                    )
                    .children(self.render_load_rest_cancel(cx))
                    .children(self.render_load_more(cx)),
            )
            .into_any_element()
    }
}

impl Render for DelimitedDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // The loading notice held the keyboard while the file was read; the
        // table takes it over, which needs the window the load did not have.
        if self.take_pending_table_focus() && self.focus_handle().is_focused(window) {
            self.focus(window, cx);
        }

        // The modal cell editor and the column prompt need the window, which
        // the requests for them did not have.
        self.open_pending_cell_editor(window, cx);
        self.open_pending_column_prompt(window, cx);

        // The text view needs the window to build and refresh its editor,
        // and a switched view takes the keyboard.
        self.prepare_view(window, cx);

        let cell_editor = self
            .cell_editor
            .clone()
            .filter(|editor| editor.read(cx).is_visible());
        let column_prompt = self.render_column_prompt(cx);
        let load_rest_prompt = self.render_load_rest_prompt(window, cx);

        let body = match (self.table().cloned(), self.failure()) {
            (Some(table), _) => self.render_loaded(table, cx),

            (None, Some(cause)) => self.render_notice(
                EmptyState::new(AppIcon::TriangleAlert, cause.to_string())
                    .title(crate::labels::delimited_open_failed_message(&self.title()))
                    .danger(),
                cx,
            ),

            (None, None) => self.render_notice(
                EmptyState::new(
                    AppIcon::Loader,
                    crate::labels::delimited_loading_label(&self.title()),
                ),
                cx,
            ),
        };

        div()
            .id("delimited-document")
            .track_focus(self.focus_handle())
            .capture_action(cx.listener(Self::take_save_key))
            .when(self.view() == DelimitedView::Text, |root| {
                root.capture_key_down(cx.listener(Self::take_text_view_save_key))
            })
            .relative()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .child(body)
            .children(cell_editor)
            .children(column_prompt)
            .children(load_rest_prompt)
    }
}
