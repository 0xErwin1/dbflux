use crate::keymap::ContextId;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::input::{Input, InputState};

/// Event emitted when the modal editor saves.
#[derive(Clone)]
pub struct CellEditorSaveEvent {
    pub row: usize,
    pub col: usize,
    pub value: String,
}

/// Modal editor for JSON and long text values.
pub struct CellEditorModal {
    visible: bool,
    row: usize,
    col: usize,
    is_json: bool,
    input: Entity<InputState>,
    focus_handle: FocusHandle,
    validation_error: Option<String>,
}

impl CellEditorModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("json")
                .line_number(true)
                .soft_wrap(true)
        });

        Self {
            visible: false,
            row: 0,
            col: 0,
            is_json: false,
            input,
            focus_handle: cx.focus_handle(),
            validation_error: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        row: usize,
        col: usize,
        value: String,
        is_json: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.row = row;
        self.col = col;
        self.is_json = is_json;
        self.visible = true;
        self.validation_error = None;

        // Format JSON if applicable
        let formatted = if is_json && !value.is_empty() {
            Self::format_json(&value).unwrap_or(value)
        } else {
            value
        };

        self.input.update(cx, |state, cx| {
            state.set_value(&formatted, window, cx);
            state.focus(window, cx);
        });

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.validation_error = None;
        cx.notify();
    }

    fn format_json(s: &str) -> Option<String> {
        let parsed: serde_json::Value = serde_json::from_str(s).ok()?;
        serde_json::to_string_pretty(&parsed).ok()
    }

    fn validate_json(s: &str) -> Result<(), String> {
        if s.is_empty() {
            return Ok(());
        }
        serde_json::from_str::<serde_json::Value>(s)
            .map(|_| ())
            .map_err(|e| e.to_string().replace('\n', " "))
    }

    fn save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();

        // Validate JSON if needed
        if self.is_json {
            if let Err(e) = Self::validate_json(&value) {
                self.validation_error = Some(e);
                cx.notify();
                return;
            }
        }

        cx.emit(CellEditorSaveEvent {
            row: self.row,
            col: self.col,
            value,
        });

        self.close(cx);
    }

    fn compact_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&value) {
            if let Ok(compact) = serde_json::to_string(&parsed) {
                self.input.update(cx, |state, cx| {
                    state.set_value(&compact, window, cx);
                });
                self.validation_error = None;
            }
        }
    }

    fn format(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(formatted) = Self::format_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&formatted, window, cx);
            });
            self.validation_error = None;
        }
    }
}

impl EventEmitter<CellEditorSaveEvent> for CellEditorModal {}

impl Render for CellEditorModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let input = self.input.clone();
        let is_json = self.is_json;
        let has_error = self.validation_error.is_some();
        let error_msg = self.validation_error.clone();

        div()
            .id("cell-editor-modal")
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .track_focus(&self.focus_handle)
            .absolute()
            .inset_0()
            .bg(gpui::black().opacity(0.5))
            .flex()
            .justify_center()
            .items_start()
            .pt(px(80.0))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.close(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &crate::keymap::Cancel, _, cx| {
                this.close(cx);
            }))
            .child(
                div()
                    .w(px(900.0))
                    .h(px(600.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::LG)
                    .shadow_lg()
                    .overflow_hidden()
                    .flex()
                    .flex_col()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    // Header
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        svg()
                                            .path(AppIcon::Pencil.path())
                                            .size_4()
                                            .text_color(theme.primary),
                                    )
                                    .child(
                                        div()
                                            .text_size(FontSizes::SM)
                                            .font_weight(FontWeight::MEDIUM)
                                            .text_color(theme.foreground)
                                            .child(if is_json { "Edit JSON" } else { "Edit Text" }),
                                    ),
                            )
                            .child(
                                div()
                                    .id("close-btn")
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .size(Heights::ICON_SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.secondary))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.close(cx);
                                    }))
                                    .child(
                                        svg()
                                            .path(AppIcon::X.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground),
                                    ),
                            ),
                    )
                    // Editor
                    .child(
                        div()
                            .flex_1()
                            .p(Spacing::MD)
                            .min_h(px(300.0))
                            .overflow_hidden()
                            .child(Input::new(&input).w_full().h_full()),
                    )
                    // Validation error
                    .when(has_error, |d| {
                        d.child(
                            div()
                                .px(Spacing::MD)
                                .py(Spacing::SM)
                                .bg(theme.danger.opacity(0.1))
                                .border_t_1()
                                .border_color(theme.danger.opacity(0.3))
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .child(
                                    svg()
                                        .path(AppIcon::CircleAlert.path())
                                        .size_4()
                                        .text_color(theme.danger),
                                )
                                .child(
                                    div()
                                        .text_size(FontSizes::XS)
                                        .text_color(theme.danger)
                                        .child(error_msg.unwrap_or_default()),
                                ),
                        )
                    })
                    // Footer
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_t_1()
                            .border_color(theme.border)
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(
                                // JSON formatting buttons (only for JSON mode)
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .when(is_json, |d| {
                                        d.child(
                                            Button::new("format")
                                                .label("Format")
                                                .small()
                                                .with_variant(ButtonVariant::Ghost)
                                                .on_click(cx.listener(|this, _, window, cx| {
                                                    this.format(window, cx);
                                                })),
                                        )
                                        .child(
                                            Button::new("compact")
                                                .label("Compact")
                                                .small()
                                                .with_variant(ButtonVariant::Ghost)
                                                .on_click(cx.listener(|this, _, window, cx| {
                                                    this.compact_json(window, cx);
                                                })),
                                        )
                                    }),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .child(
                                        Button::new("cancel")
                                            .label("Cancel")
                                            .small()
                                            .with_variant(ButtonVariant::Ghost)
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.close(cx);
                                            })),
                                    )
                                    .child(
                                        Button::new("save")
                                            .label("Save")
                                            .small()
                                            .with_variant(ButtonVariant::Primary)
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.save(window, cx);
                                            })),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }
}
