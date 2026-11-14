//! Value panel content for the workspace-level inspector rail.
//!
//! # `ValuePanelContent`
//!
//! Shows one cell's full value in a code editor, read as JSON, XML, or plain
//! text. All chrome (title bar, close button, resize grip) is owned by
//! `WorkspaceInspector`, the same as the row inspector.
//!
//! # Editing
//!
//! The panel is a real editor, not a viewer: Save writes through
//! `DataGridPanel::handle_cell_editor_save`, which is the same edit-buffer path
//! the inline grid editor and the cell-editor modal use. A saved value is
//! therefore a pending change like any other — the row still needs Save Row to
//! reach the database, and Revert All undoes it.
//!
//! What is written is exactly what stands in the editor. Pretty-printing is an
//! explicit button rather than something Save does silently, so formatting a
//! value to read it never rewrites what is stored.
//!
//! # Following the cursor
//!
//! `DataGridPanel` reloads the panel as the selection moves, but only while the
//! editor is unmodified — see `is_modified`. Otherwise moving the cursor would
//! silently discard whatever the user had typed.

use super::{DataGridEvent, DataGridPanel};
use dbflux_components::components::value_format::{
    self, ValueFormat, compact_value, format_value, validate_value,
};
use dbflux_components::controls::{
    Button, ButtonVariant, GpuiInput as Input, InputEvent, InputState,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_components::vim::{VimBinding, VimHost};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::EditorState;
use gpui_component::{ActiveTheme, Sizable};

/// Everything the panel needs to show one cell.
#[derive(Clone, Debug)]
pub(crate) struct ValuePanelTarget {
    /// Visual row index, matching `CellCoord::row`.
    pub row: usize,
    pub col: usize,
    pub column_name: String,
    /// The cell's text exactly as the edit buffer holds it.
    pub value: String,
    /// False for read-only results and read-only columns; hides the footer.
    pub editable: bool,
}

/// A button of the panel, run from the keyboard through the grid's Toolbar
/// submenu (see `ValuePanelContent::buttons`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValuePanelButton {
    /// A format of the header's selector.
    Format(ValueFormat),
    /// The header's word-wrap toggle.
    Wrap,
    /// The footer's Format: pretty-prints the value.
    PrettyPrint,
    Compact,
    Revert,
    Save,
}

/// Emitted when the user saves. `DataGridPanel` routes it into the shared
/// cell-save path.
#[derive(Clone, Debug)]
pub(crate) struct ValuePanelSaveEvent {
    pub row: usize,
    pub col: usize,
    pub value: String,
}

pub(crate) struct ValuePanelContent {
    target: ValuePanelTarget,
    format: ValueFormat,
    word_wrap: bool,
    /// The text last written into the editor. `is_modified` compares against
    /// this rather than against the cell, so pretty-printing alone does not
    /// count as an edit the user must resolve.
    loaded_text: String,
    input: Entity<EditorState>,
    /// Vim mode for the editor; rebuilt with it.
    vim: VimBinding,
    /// Whether the editor holds the keyboard. The results keymap binds bare
    /// letters to grid commands, so `DataGridPanel::active_context` has to
    /// hand the keyboard to the text layer while the user is typing here.
    editor_focused: bool,
    _input_subscription: Subscription,
    error: Option<String>,
    /// Holds the keyboard while the user reads the panel without editing;
    /// Enter moves it on into the editor.
    focus_handle: FocusHandle,
}

impl EventEmitter<ValuePanelSaveEvent> for ValuePanelContent {}

impl VimHost for ValuePanelContent {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.vim.for_input(input)
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.vim.for_input_mut(input)
    }
}

impl ValuePanelContent {
    pub(crate) fn new(
        target: ValuePanelTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let format = value_format::detect_format(&target.value);
        let word_wrap = true;
        let text = initial_text(&target.value, format);
        let (input, subscription) = build_input(text.clone(), format, word_wrap, window, cx);
        let vim = VimBinding::new(input.clone(), window, cx);

        let mut panel = Self {
            target,
            format,
            word_wrap,
            loaded_text: text,
            input,
            vim,
            editor_focused: false,
            _input_subscription: subscription,
            error: None,
            focus_handle: cx.focus_handle(),
        };

        let input = panel.vim.input_id();
        VimBinding::follow_setting(&mut panel, input, cx);
        panel
    }

    /// Replaces the editor, with a fresh Vim binding for it.
    fn install_input(
        &mut self,
        input: Entity<EditorState>,
        subscription: Subscription,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.vim = VimBinding::new(input.clone(), window, cx);
        self.input = input;
        self._input_subscription = subscription;
        let input = self.vim.input_id();
        VimBinding::follow_setting(self, input, cx);
    }

    /// The panel's own focus handle, which keyboard focus takes when it moves
    /// into the panel from the grid.
    pub(crate) fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// Hand the keyboard to the editor, to change the value.
    ///
    /// Marks the editor as focused right away rather than on its `Focus`
    /// event, which arrives only with the next frame: until then the grid
    /// would keep reporting the panel's keys and claim the letters typed.
    pub(crate) fn focus_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.input.update(cx, |state, cx| state.focus(window, cx));
        self.editor_focused = true;
        cx.notify();
    }

    /// Take the keyboard back from the editor to the panel, keeping the
    /// text as typed. Mirrors `focus_editor`.
    pub(crate) fn leave_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window, cx);
        self.editor_focused = false;
        cx.notify();
    }

    /// Scroll the value by a line, a page, or to either end. The editor
    /// clamps the offset to its content when it lays out.
    pub(crate) fn scroll(
        &self,
        step: crate::data_grid_panel::side_island::IslandScroll,
        cx: &mut Context<Self>,
    ) {
        use crate::data_grid_panel::side_island::IslandScroll;

        self.input.update(cx, |state, cx| {
            let Some(line_height) = state.line_height() else {
                return;
            };

            let viewport = state.input_bounds().size.height;
            let page = (viewport - line_height).max(line_height);
            let offset = state.scroll_offset();

            let target = match step {
                IslandScroll::LineUp => offset.y + line_height,
                IslandScroll::LineDown => offset.y - line_height,
                IslandScroll::PageUp => offset.y + page,
                IslandScroll::PageDown => offset.y - page,
                IslandScroll::Top => Pixels::ZERO,
                IslandScroll::Bottom => Pixels::MIN,
            };

            state.set_scroll_offset(point(offset.x, target.min(Pixels::ZERO)), cx);
        });
    }

    /// Whether the panel's editor currently owns the keyboard.
    pub(crate) fn editor_has_focus(&self) -> bool {
        self.editor_focused
    }

    /// Point the panel at a different cell, re-detecting the format.
    pub(crate) fn open(
        &mut self,
        target: ValuePanelTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.format = value_format::detect_format(&target.value);
        self.target = target;
        self.error = None;
        self.reload(window, cx);
    }

    /// The cell the panel is currently showing.
    pub(crate) fn target_cell(&self) -> (usize, usize) {
        (self.target.row, self.target.col)
    }

    /// Whether the editor holds something other than what was loaded into it.
    ///
    /// Drives whether the panel may follow the cursor: an unsaved edit pins it
    /// to its cell until the user saves or reverts.
    pub(crate) fn is_modified(&self, cx: &App) -> bool {
        self.input.read(cx).value().as_ref() != self.loaded_text
    }

    /// Rebuild the editor from the target value in the current format.
    fn reload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let text = initial_text(&self.target.value, self.format);
        let (input, subscription) =
            build_input(text.clone(), self.format, self.word_wrap, window, cx);
        self.install_input(input, subscription, window, cx);
        self.editor_focused = false;
        self.loaded_text = text;
        cx.notify();
    }

    fn set_format(&mut self, format: ValueFormat, window: &mut Window, cx: &mut Context<Self>) {
        if self.format == format {
            return;
        }

        // Carry the user's edits across the switch rather than dropping back to
        // the stored value; only the presentation is changing.
        let current = self.input.read(cx).value().to_string();
        let was_modified = current != self.loaded_text;

        self.format = format;
        self.error = None;

        let text = if was_modified {
            current
        } else {
            initial_text(&self.target.value, format)
        };

        let (input, subscription) = build_input(text.clone(), format, self.word_wrap, window, cx);
        self.install_input(input, subscription, window, cx);
        self.editor_focused = false;
        // A modified buffer stays modified across the switch, so `loaded_text`
        // must keep pointing at the stored value's rendering.
        self.loaded_text = if was_modified {
            initial_text(&self.target.value, format)
        } else {
            text
        };
        cx.notify();
    }

    fn toggle_word_wrap(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.word_wrap = !self.word_wrap;
        let wrap = self.word_wrap;
        self.input.update(cx, |state, cx| {
            state.set_soft_wrap(wrap, window, cx);
        });
        cx.notify();
    }

    fn apply_transform(
        &mut self,
        transform: fn(&str, ValueFormat) -> Result<String, String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current = self.input.read(cx).value().to_string();
        match transform(&current, self.format) {
            Ok(next) => {
                self.error = None;
                self.input.update(cx, |state, cx| {
                    state.set_value(next, window, cx);
                });
            }
            Err(error) => self.error = Some(error),
        }
        cx.notify();
    }

    fn revert(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.error = None;
        let text = self.loaded_text.clone();
        self.input.update(cx, |state, cx| {
            state.set_value(text, window, cx);
        });
        cx.notify();
    }

    /// The buttons the panel shows enabled right now, the format shown
    /// excepted: the other formats and the wrap toggle, then for an editable
    /// value Format and Compact (structured formats) and, once the value
    /// changed, Revert and Save.
    pub(crate) fn buttons(&self, cx: &App) -> Vec<ValuePanelButton> {
        let mut buttons: Vec<ValuePanelButton> = ValueFormat::ALL
            .into_iter()
            .filter(|format| *format != self.format)
            .map(ValuePanelButton::Format)
            .collect();
        buttons.push(ValuePanelButton::Wrap);

        if !self.target.editable {
            return buttons;
        }

        if self.format.is_structured() {
            buttons.push(ValuePanelButton::PrettyPrint);
            buttons.push(ValuePanelButton::Compact);
        }

        if self.is_modified(cx) {
            buttons.push(ValuePanelButton::Revert);
            buttons.push(ValuePanelButton::Save);
        }

        buttons
    }

    /// Runs what `button` runs when clicked.
    pub(crate) fn press(
        &mut self,
        button: ValuePanelButton,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match button {
            ValuePanelButton::Format(format) => self.set_format(format, window, cx),
            ValuePanelButton::Wrap => self.toggle_word_wrap(window, cx),
            ValuePanelButton::PrettyPrint => self.apply_transform(format_value, window, cx),
            ValuePanelButton::Compact => self.apply_transform(compact_value, window, cx),
            ValuePanelButton::Revert => self.revert(window, cx),
            ValuePanelButton::Save => self.save(cx),
        }
    }

    pub(crate) fn save(&mut self, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();

        if let Err(error) = validate_value(&value, self.format) {
            self.error = Some(error);
            cx.notify();
            return;
        }

        self.error = None;
        // The saved text becomes the new baseline: the panel is no longer
        // modified and may follow the cursor again.
        self.loaded_text = value.clone();
        self.target.value = value.clone();

        cx.emit(ValuePanelSaveEvent {
            row: self.target.row,
            col: self.target.col,
            value,
        });
        cx.notify();
    }
}

/// Pretty-print on open when the value parses, otherwise show it untouched so
/// a malformed value is still readable and fixable.
fn initial_text(value: &str, format: ValueFormat) -> String {
    format_value(value, format).unwrap_or_else(|_| value.to_string())
}

fn build_input(
    text: String,
    format: ValueFormat,
    word_wrap: bool,
    window: &mut Window,
    cx: &mut Context<ValuePanelContent>,
) -> (Entity<EditorState>, Subscription) {
    // The editor language is fixed at construction in `gpui-component`, so a
    // format switch replaces the whole input rather than mutating it.
    let input = cx.new(|cx| {
        EditorState::new(window, cx)
            .language(format.editor_language())
            .line_number(true)
            .soft_wrap(word_wrap)
    });

    input.update(cx, |state, cx| {
        state.set_value(text, window, cx);
    });

    let subscription = cx.subscribe(&input, |this, _input, event: &InputEvent, cx| match event {
        InputEvent::Focus => {
            this.editor_focused = true;
            cx.notify();
        }
        InputEvent::Blur => {
            this.editor_focused = false;
            cx.notify();
        }
        // A keystroke can change what Save and Revert are allowed to do.
        InputEvent::Change => cx.notify(),
        _ => {}
    });

    (input, subscription)
}

impl Render for ValuePanelContent {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Cloned rather than borrowed: `cx.listener` below needs `cx` mutably.
        let theme = cx.theme().clone();
        let editable = self.target.editable;

        div()
            .id("value-panel-content")
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_header(cx))
            .child(
                VimBinding::capture_run_command(
                    VimBinding::wire(self.vim.leader_scope(div(), cx), self.vim.input_id(), cx),
                    self.vim.input_id(),
                    cx,
                )
                .id("value-panel-editor")
                .flex_1()
                .min_h_0()
                .overflow_hidden()
                .child(self.vim.editor(false).h_full()),
            )
            .children(self.vim.render_indicator(cx))
            .when_some(self.error.clone(), |d, error| {
                d.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .bg(theme.danger.opacity(0.1))
                        .border_t_1()
                        .border_color(theme.danger.opacity(0.3))
                        .child(
                            Icon::new(AppIcon::CircleAlert)
                                .size(Heights::ICON_SM)
                                .danger(),
                        )
                        .child(Text::caption(error).font_size(FontSizes::XS).danger()),
                )
            })
            .when(editable, |d| d.child(self.render_footer(cx)))
            .when(!editable, |d| {
                d.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .border_t_1()
                        .border_color(theme.border)
                        .child(
                            Text::caption(dbflux_i18n::t!("components.value_panel.read_only"))
                                .font_size(FontSizes::XS)
                                .color(theme.muted_foreground),
                        ),
                )
            })
    }
}

impl ValuePanelContent {
    /// Column name, format selector, and the word-wrap toggle.
    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let current = self.format;
        let word_wrap = self.word_wrap;

        div()
            .flex()
            .flex_row()
            .flex_wrap()
            .items_center()
            .justify_between()
            .gap(Spacing::XS)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary.opacity(0.5))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .min_w_0()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(Text::body_sm(self.target.column_name.clone())),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .children(ValueFormat::ALL.map(|format| {
                        let is_active = format == current;
                        div()
                            .id(SharedString::from(format!(
                                "value-panel-format-{}",
                                format.editor_language()
                            )))
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .text_size(FontSizes::XS)
                            .when(is_active, |d| d.bg(theme.accent.opacity(0.15)))
                            .when(!is_active, |d| d.hover(|d| d.bg(theme.secondary)))
                            .child(Text::body(format.label()).font_size(FontSizes::XS).color(
                                if is_active {
                                    theme.foreground
                                } else {
                                    theme.muted_foreground
                                },
                            ))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.set_format(format, window, cx);
                            }))
                    }))
                    .child(
                        div()
                            .id("value-panel-wrap")
                            .px(Spacing::XS)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .text_size(FontSizes::XS)
                            .when(word_wrap, |d| d.bg(theme.accent.opacity(0.15)))
                            .when(!word_wrap, |d| d.hover(|d| d.bg(theme.secondary)))
                            .child(
                                Text::body(dbflux_i18n::t!("components.value_panel.wrap"))
                                    .font_size(FontSizes::XS)
                                    .color(if word_wrap {
                                        theme.foreground
                                    } else {
                                        theme.muted_foreground
                                    }),
                            )
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.toggle_word_wrap(window, cx);
                            })),
                    ),
            )
    }

    /// Format / Compact on the left, Revert / Save on the right — the same
    /// arrangement as the cell-editor modal.
    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let structured = self.format.is_structured();
        // Save and Revert only mean something while the editor differs from
        // what is stored, and `save` resets that baseline — so both go quiet
        // again the moment the value is committed.
        let is_modified = self.is_modified(cx);

        div()
            .flex()
            .flex_row()
            .flex_wrap()
            .items_center()
            .justify_between()
            .gap(Spacing::XS)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .when(structured, |d| {
                        d.child(
                            Button::new(
                                "value-panel-format",
                                dbflux_i18n::t!("components.json_editor.format"),
                            )
                            .variant(ButtonVariant::Ghost)
                            .on_click(cx.listener(
                                |this, _, window, cx| {
                                    this.apply_transform(format_value, window, cx);
                                },
                            )),
                        )
                        .child(
                            Button::new(
                                "value-panel-compact",
                                dbflux_i18n::t!("components.json_editor.compact"),
                            )
                            .variant(ButtonVariant::Ghost)
                            .on_click(cx.listener(
                                |this, _, window, cx| {
                                    this.apply_transform(compact_value, window, cx);
                                },
                            )),
                        )
                    }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(
                        Button::new(
                            "value-panel-revert",
                            dbflux_i18n::t!("components.value_panel.revert"),
                        )
                        .variant(ButtonVariant::Ghost)
                        .disabled(!is_modified)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.revert(window, cx);
                        })),
                    )
                    .child(
                        Button::new(
                            "value-panel-save",
                            dbflux_i18n::t!("components.json_editor.save"),
                        )
                        .variant(if is_modified {
                            ButtonVariant::Primary
                        } else {
                            ButtonVariant::Ghost
                        })
                        .disabled(!is_modified)
                        .on_click(cx.listener(|this, _, _window, cx| {
                            this.save(cx);
                        })),
                    ),
            )
    }
}

impl DataGridPanel {
    /// Whether the value panel currently owns the shared inspector rail.
    pub fn value_panel_is_open(&self) -> bool {
        self.inspector.value_panel_open
    }

    /// Open the value panel on the active cell, or close it if already open.
    pub fn toggle_value_panel(&mut self, cx: &mut Context<Self>) {
        if self.inspector.value_panel_open {
            self.close_value_panel(cx);
            return;
        }

        let active = self
            .grid_table
            .table_state
            .as_ref()
            .and_then(|state| state.read(cx).selection().active);

        if let Some(coord) = active {
            self.request_value_panel(coord.row, coord.col, cx);
        }
    }

    /// Queue the value panel to open on `(row, col)`.
    ///
    /// The builder and the row inspector share this rail, so taking it means
    /// the row inspector must stop following the cursor — otherwise the two
    /// would replace each other on every selection change.
    pub(crate) fn request_value_panel(&mut self, row: usize, col: usize, cx: &mut Context<Self>) {
        let Some(target) = self.value_panel_target(row, col, cx) else {
            return;
        };

        self.inspector.follow_selection = false;
        self.inspector.inspector_row = None;
        self.inspector.value_panel_open = true;
        self.mark_document_builder_closed();
        self.pending.value_panel = Some(target);
        cx.notify();
    }

    /// Carry the value panel's open state onto this tab.
    ///
    /// Called when the tab becomes active. Opening re-reads *this* grid's
    /// selection, so the rail shows the new table's cell instead of whatever
    /// the previous tab left there.
    pub fn set_value_panel_open(&mut self, open: bool, _cx: &mut Context<Self>) {
        if open && !self.builder.builder_open {
            self.inspector.value_panel_open = true;
        } else if !open {
            self.inspector.value_panel_open = false;
            self.pending.value_panel = None;
        }
    }

    /// Point the value panel at the active cell of this grid.
    ///
    /// Returns false when there is nothing to show yet — a tab whose grid has
    /// not loaded, or one with no selection.
    pub(super) fn mount_value_panel_for_active_cell(&mut self, cx: &mut Context<Self>) -> bool {
        let active = self
            .grid_table
            .table_state
            .as_ref()
            .and_then(|state| state.read(cx).selection().active);

        let Some(coord) = active else {
            return false;
        };

        let Some(target) = self.value_panel_target(coord.row, coord.col, cx) else {
            return false;
        };

        self.pending.value_panel = Some(target);
        cx.notify();
        true
    }

    /// The value panel when it is open and holds an unsaved edit.
    pub(crate) fn value_panel_pending_save(&self, cx: &App) -> Option<Entity<ValuePanelContent>> {
        if !self.inspector.value_panel_open {
            return None;
        }

        let panel = self.inspector.value_panel.as_ref()?;
        panel.read(cx).is_modified(cx).then(|| panel.clone())
    }

    fn close_value_panel(&mut self, cx: &mut Context<Self>) {
        self.inspector.value_panel_open = false;
        self.pending.value_panel = None;
        cx.emit(DataGridEvent::CloseInspector);
        cx.notify();
    }

    /// Read the cell's current edit-buffer text and editability for the panel.
    fn value_panel_target(&self, row: usize, col: usize, cx: &App) -> Option<ValuePanelTarget> {
        use dbflux_components::components::data_table::model::{CellValue, VisualRowSource};

        let table_state = self.grid_table.table_state.as_ref()?;
        let state = table_state.read(cx);
        let model = state.model();
        let column = model.columns.get(col)?;

        let edit_buffer = state.edit_buffer();
        let null_cell = CellValue::null();

        let value = match edit_buffer.compute_visual_order().get(row).copied()? {
            VisualRowSource::Base(base_idx) => {
                let base = model.cell(base_idx, col).unwrap_or(&null_cell);
                edit_buffer.get_cell(base_idx, col, base).edit_text()
            }
            VisualRowSource::Insert(insert_idx) => edit_buffer
                .get_pending_insert_by_idx(insert_idx)
                .and_then(|data| data.get(col))
                .map(|cell| cell.edit_text())
                .unwrap_or_default(),
        };

        Some(ValuePanelTarget {
            row,
            col,
            column_name: column.title.to_string(),
            value,
            editable: state.is_editable() && !state.readonly_columns().contains(&col),
        })
    }

    /// Mount or update the value panel. Called from render, where a `Window`
    /// is available to build the editor.
    pub(crate) fn apply_pending_value_panel(
        &mut self,
        target: ValuePanelTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let content = match &self.inspector.value_panel {
            Some(existing) => {
                existing.update(cx, |panel, cx| panel.open(target, window, cx));
                existing.clone()
            }
            None => {
                let content = cx.new(|cx| ValuePanelContent::new(target, window, cx));
                self.inspector._value_panel_subscription = Some(cx.subscribe(
                    &content,
                    |this, _, event: &ValuePanelSaveEvent, cx| {
                        // Save in the panel means "write this value to the
                        // database", not "stage it": having to confirm again in
                        // the toolbar after pressing Save reads as the panel
                        // having done nothing. The commit goes through the
                        // normal row-save path, so mutation policy, approval,
                        // and the task list all still apply.
                        this.write_cell_value(event.row, event.col, &event.value, cx);

                        if let Some(table_state) = this.grid_table.table_state.clone() {
                            table_state.update(cx, |state, cx| {
                                state.request_save_row_at(event.row, cx);
                            });
                        }
                    },
                ));
                self.inspector.value_panel = Some(content.clone());
                content
            }
        };

        cx.emit(DataGridEvent::OpenInspector {
            title: SharedString::from(dbflux_i18n::t!("components.value_panel.title")),
            content: AnyView::from(content),
            content_has_header: false,
        });
    }

    /// Whether the panel may be re-pointed at `(row, col)`.
    ///
    /// An edited-but-unsaved panel stays pinned to its cell; following the
    /// cursor there would throw the user's typing away without asking.
    pub(super) fn value_panel_can_follow(&self, row: usize, col: usize, cx: &App) -> bool {
        let Some(panel) = self.inspector.value_panel.as_ref() else {
            return true;
        };

        let panel = panel.read(cx);
        !panel.is_modified(cx) && panel.target_cell() != (row, col)
    }
}

#[cfg(test)]
mod vim_tests {
    use super::{ValuePanelContent, ValuePanelTarget};
    use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
    use dbflux_app::keymap::{Command, ContextId};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

    /// The panel under a keymap host that routes Cancel the way the grid's
    /// inspector does: out of the editor into the panel.
    fn open_panel(
        cx: &mut TestAppContext,
        vim: bool,
    ) -> (Entity<ValuePanelContent>, &mut VisualTestContext) {
        let (_host, panel, window) = open_panel_with_host(cx, vim);
        (panel, window)
    }

    fn open_panel_with_host(
        cx: &mut TestAppContext,
        vim: bool,
    ) -> (
        Entity<KeymapHost<ValuePanelContent>>,
        Entity<ValuePanelContent>,
        &mut VisualTestContext,
    ) {
        init_keyboard_runtime(cx);
        cx.update(|cx| dbflux_components::vim::set_vim_enabled(cx, vim));

        let (host, window) = host_document(
            cx,
            |window, cx| {
                cx.new(|cx| {
                    ValuePanelContent::new(
                        ValuePanelTarget {
                            row: 0,
                            col: 0,
                            column_name: "notes".to_string(),
                            value: "ab\ncd".to_string(),
                            editable: true,
                        },
                        window,
                        cx,
                    )
                })
            },
            |panel, _cx| {
                if panel.editor_has_focus() {
                    ContextId::TextInput
                } else {
                    ContextId::Inspector
                }
            },
            |panel, command, window, cx| {
                if command == Command::Cancel && panel.editor_has_focus() {
                    panel.leave_editor(window, cx);
                    return true;
                }
                false
            },
        );
        let panel = window.update(|_, cx| host.read(cx).document.clone());
        window.update(|window, cx| panel.update(cx, |panel, cx| panel.focus_editor(window, cx)));
        window.run_until_parked();

        (host, panel, window)
    }

    fn text_and_cursor(
        panel: &Entity<ValuePanelContent>,
        window: &mut VisualTestContext,
    ) -> (String, usize) {
        window.update(|_, cx| {
            let state = panel.read(cx).input.read(cx);
            (state.value().to_string(), state.cursor())
        })
    }

    /// With Vim mode on, `j` moves, the first Escape leaves Insert mode and
    /// keeps the editor, and Escape in Normal mode leaves the editor.
    #[gpui::test]
    fn vim_mode_in_the_value_editor(cx: &mut TestAppContext) {
        let (panel, window) = open_panel(cx, true);

        window.simulate_keystrokes("j");
        window.run_until_parked();
        assert_eq!(text_and_cursor(&panel, window), ("ab\ncd".into(), 3));

        window.simulate_keystrokes("i");
        window.simulate_input("X");
        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert_eq!(text_and_cursor(&panel, window).0, "ab\nXcd");
        assert!(window.update(|_, cx| panel.read(cx).editor_has_focus()));

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!window.update(|_, cx| panel.read(cx).editor_has_focus()));
    }

    /// With Vim mode off, keys type and Escape leaves the editor at once.
    #[gpui::test]
    fn without_vim_mode_escape_leaves_the_editor(cx: &mut TestAppContext) {
        let (panel, window) = open_panel(cx, false);

        window.simulate_input("j");
        window.simulate_keystrokes("escape");
        window.run_until_parked();

        assert_eq!(text_and_cursor(&panel, window).0, "jab\ncd");
        assert!(!window.update(|_, cx| panel.read(cx).editor_has_focus()));
    }

    /// The leader works outside the code editor: `space a` in Normal mode
    /// reaches the host as the pane-actions command, and Insert mode types
    /// the space.
    #[gpui::test]
    fn leader_sequences_reach_the_host_from_the_value_editor(cx: &mut TestAppContext) {
        let _keymap_state = dbflux_ui_base::keymap::keymap_state_test_guard();
        let (host, panel, window) = open_panel_with_host(cx, true);
        let host_commands =
            |window: &mut VisualTestContext| window.update(|_, cx| host.read(cx).commands.clone());

        window.simulate_keystrokes("space a");
        window.run_until_parked();
        assert_eq!(host_commands(window), vec![Command::OpenPaneActions]);
        assert_eq!(text_and_cursor(&panel, window), ("ab\ncd".into(), 0));

        window.simulate_keystrokes("i space escape");
        window.run_until_parked();
        assert_eq!(text_and_cursor(&panel, window).0, " ab\ncd");
        assert_eq!(host_commands(window).len(), 1);
    }
}
