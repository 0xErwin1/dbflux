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

use dbflux_components::components::value_format::{
    self, ValueFormat, compact_value, format_value, validate_value,
};
use dbflux_components::controls::{
    Button, ButtonVariant, GpuiInput as Input, InputEvent, InputState,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::{ActiveTheme, Sizable};

/// Everything the panel needs to show one cell.
#[derive(Clone, Debug)]
pub struct ValuePanelTarget {
    /// Visual row index, matching `CellCoord::row`.
    pub row: usize,
    pub col: usize,
    pub column_name: String,
    /// The cell's text exactly as the edit buffer holds it.
    pub value: String,
    /// False for read-only results and read-only columns; hides the footer.
    pub editable: bool,
}

/// Emitted when the user saves. `DataGridPanel` routes it into the shared
/// cell-save path.
#[derive(Clone, Debug)]
pub struct ValuePanelSaveEvent {
    pub row: usize,
    pub col: usize,
    pub value: String,
}

pub struct ValuePanelContent {
    target: ValuePanelTarget,
    format: ValueFormat,
    word_wrap: bool,
    /// The text last written into the editor. `is_modified` compares against
    /// this rather than against the cell, so pretty-printing alone does not
    /// count as an edit the user must resolve.
    loaded_text: String,
    input: Entity<InputState>,
    /// Whether the editor holds the keyboard. The results keymap binds bare
    /// letters to grid commands, so `DataGridPanel::active_context` has to
    /// hand the keyboard to the text layer while the user is typing here.
    editor_focused: bool,
    _input_subscription: Subscription,
    error: Option<String>,
}

impl EventEmitter<ValuePanelSaveEvent> for ValuePanelContent {}

impl ValuePanelContent {
    pub fn new(target: ValuePanelTarget, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let format = value_format::detect_format(&target.value);
        let word_wrap = true;
        let text = initial_text(&target.value, format);
        let (input, subscription) = build_input(text.clone(), format, word_wrap, window, cx);

        Self {
            target,
            format,
            word_wrap,
            loaded_text: text,
            input,
            editor_focused: false,
            _input_subscription: subscription,
            error: None,
        }
    }

    /// Whether the panel's editor currently owns the keyboard.
    pub fn editor_has_focus(&self) -> bool {
        self.editor_focused
    }

    /// Point the panel at a different cell, re-detecting the format.
    pub fn open(&mut self, target: ValuePanelTarget, window: &mut Window, cx: &mut Context<Self>) {
        self.format = value_format::detect_format(&target.value);
        self.target = target;
        self.error = None;
        self.reload(window, cx);
    }

    /// The cell the panel is currently showing.
    pub fn target_cell(&self) -> (usize, usize) {
        (self.target.row, self.target.col)
    }

    /// Whether the editor holds something other than what was loaded into it.
    ///
    /// Drives whether the panel may follow the cursor: an unsaved edit pins it
    /// to its cell until the user saves or reverts.
    pub fn is_modified(&self, cx: &App) -> bool {
        self.input.read(cx).value().as_ref() != self.loaded_text
    }

    /// Rebuild the editor from the target value in the current format.
    fn reload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let text = initial_text(&self.target.value, self.format);
        let (input, subscription) =
            build_input(text.clone(), self.format, self.word_wrap, window, cx);
        self.input = input;
        self._input_subscription = subscription;
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
        self.input = input;
        self._input_subscription = subscription;
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

    pub fn save(&mut self, cx: &mut Context<Self>) {
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
) -> (Entity<InputState>, Subscription) {
    // The editor language is fixed at construction in `gpui-component`, so a
    // format switch replaces the whole input rather than mutating it.
    let input = cx.new(|cx| {
        InputState::new(window, cx)
            .code_editor(format.editor_language())
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
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_header(cx))
            .child(
                div()
                    .id("value-panel-editor")
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(Input::new(&self.input).h_full()),
            )
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
                    .child(Text::label_sm(self.target.column_name.clone())),
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
                            .small()
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
                            .small()
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
                        .small()
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
                        .small()
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
