use super::{
    PendingSnippetAction, SnippetFocus, SnippetFormField, SnippetsSection, language_label,
};
use crate::settings::layout;
use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_components::controls::{Button, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, Chamfer, Icon, Text};
use dbflux_components::tokens::{ChamferCut, Spacing, ui};
use dbflux_storage::SnippetRecord;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::Editor;
use gpui_component::{ActiveTheme, Theme};

/// Height of the body editor: about eight lines of the editor font.
const BODY_EDITOR_HEIGHT: Rems = ui(180.0);
const NAME_FIELD_WIDTH: Rems = ui(360.0);
const PREFIX_FIELD_WIDTH: Rems = ui(160.0);
const LANGUAGE_FIELD_WIDTH: Rems = ui(180.0);

/// What a single-line text field of the form needs to render.
struct TextField<'a> {
    field: SnippetFormField,
    label: String,
    input: &'a Entity<InputState>,
    element_id: &'static str,
    width: Rems,
    mono: bool,
}

impl SnippetsSection {
    pub(super) fn render_snippets_section(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let show_form = self.new_form_open || self.editing_id.is_some();

        layout::split_section_shell(
            cx.theme().border,
            dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.snippets.header.title"),
                dbflux_i18n::t!("settings.snippets.header.subtitle"),
                cx,
            ),
            self.render_list(cx),
            if show_form {
                self.render_form(cx).into_any_element()
            } else {
                self.render_empty_detail(cx).into_any_element()
            },
        )
    }

    fn render_list(&mut self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let list_focused = self.content_focused && self.focus == SnippetFocus::List;
        let new_button_focused = list_focused && self.list_idx.is_none();

        if let Some(scroll_idx) = self.pending_scroll_idx.take() {
            self.list_scroll_handle.scroll_to_item(scroll_idx);
        }

        let toolbar = layout::master_list_toolbar(vec![
            Button::new("new-snippet", dbflux_i18n::t!("settings.snippets.list.new"))
                .primary()
                .icon(AppIcon::Plus)
                .focused(new_button_focused)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.request_form_action(PendingSnippetAction::New, window, cx);
                    this.focus = SnippetFocus::Form;
                    this.form_field = SnippetFormField::Name;
                }))
                .into_any_element(),
        ]);

        let rows: Vec<AnyElement> = self
            .snippets
            .iter()
            .enumerate()
            .map(|(index, snippet)| {
                let selected = self.editing_id.as_deref() == Some(snippet.id.as_str());
                let focused = list_focused && self.list_idx == Some(index);
                let id = snippet.id.clone();

                layout::master_list_row(
                    SharedString::from(format!("snippet-row-{}", snippet.id)),
                    layout::MasterRow {
                        icon: Some(AppIcon::Braces),
                        title: snippet.name.clone().into(),
                        detail: Some(snippet.prefix.clone().into()),
                        trailing: Some(row_chips(snippet)),
                    },
                    selected,
                    focused,
                    cx,
                )
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.focus = SnippetFocus::Form;
                    this.request_form_action(PendingSnippetAction::Select(id.clone()), window, cx);
                }))
                .into_any_element()
            })
            .collect();

        let empty = rows.is_empty();

        div()
            .w(SettingsMetrics::LIST_WIDTH)
            .h_full()
            .min_h_0()
            .flex_shrink_0()
            .border_r_1()
            .border_color(cx.theme().border)
            .flex()
            .flex_col()
            .child(toolbar)
            .child(layout::scrolled_list_viewport(
                div()
                    .id("snippets-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.list_scroll_handle)
                    .flex()
                    .flex_col()
                    .when(empty, |list| {
                        list.child(layout::master_list_empty(dbflux_i18n::t!(
                            "settings.snippets.list.empty"
                        )))
                    })
                    .children(rows),
                &self.list_scroll_handle,
            ))
    }

    fn render_empty_detail(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let strong = dbflux_components::tokens::ChromeColors::strong(cx.theme());

        div()
            .flex_1()
            .h_full()
            .min_w_0()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(FormMetrics::INLINE_GAP)
            .p(Spacing::XL)
            .child(
                Icon::new(AppIcon::Braces)
                    .size(ui(28.0))
                    .color(cx.theme().muted_foreground),
            )
            .child(
                Text::body(dbflux_i18n::t!("settings.snippets.empty.title"))
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(strong),
            )
            .child(div().max_w(ui(320.0)).child(
                Text::body(dbflux_i18n::t!("settings.snippets.empty.hint")).muted_foreground(),
            ))
    }

    fn is_cursor_on(&self, field: SnippetFormField) -> bool {
        self.content_focused
            && self.focus == SnippetFocus::Form
            && self.form_field == field
            && !self.editing_field
    }

    /// Moves the keyboard cursor to `field` on a press and focuses its input.
    fn field_press(&self, field: SnippetFormField, frame: Div, cx: &mut Context<Self>) -> Div {
        frame.on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, window, cx| {
                // Only a press that takes focus off an input produces the
                // Blur that clears the flag; a press on the input that
                // already has focus would leave it set.
                this.switching_input = this.press_moves_input_focus(field, window, cx);
                this.focus = SnippetFocus::Form;
                this.form_field = field;
                this.focus_form_field(window, cx);
                cx.notify();
            }),
        )
    }

    /// Whether pressing `field` takes focus off the input that has it.
    pub(super) fn press_moves_input_focus(
        &self,
        field: SnippetFormField,
        window: &Window,
        cx: &App,
    ) -> bool {
        let focused = |handle: FocusHandle| handle.is_focused(window);
        let name = focused(self.input_name.read(cx).focus_handle(cx));
        let prefix = focused(self.input_prefix.read(cx).focus_handle(cx));
        let body = focused(self.input_body.read(cx).focus_handle(cx));

        let target_has_focus = match field {
            SnippetFormField::Name => name,
            SnippetFormField::Prefix => prefix,
            SnippetFormField::Body => body,
            _ => false,
        };

        (name || prefix || body) && !target_has_focus
    }

    /// Error under a field: the icon and the message in the danger color.
    fn error_line(message: String, cx: &App) -> Div {
        let danger = cx.theme().danger;

        div()
            .flex()
            .items_start()
            .gap(FormMetrics::INLINE_GAP)
            .child(
                div()
                    .pt(px(2.0))
                    .child(Icon::new(AppIcon::CircleAlert).size(ui(13.0)).color(danger)),
            )
            .child(
                Text::body(message)
                    .font_size(FormMetrics::HELP_FONT)
                    .text_color(danger),
            )
    }

    /// A form row: the label, the control and, under it, the error when the
    /// field has one and its help text otherwise.
    fn field_row(
        label: String,
        control: Div,
        error: Option<String>,
        help: Option<String>,
        cx: &App,
    ) -> Div {
        let column = div()
            .flex()
            .flex_col()
            .gap(FormMetrics::HELP_GAP)
            .child(control)
            .when_some(error.clone(), |column, message| {
                column.child(Self::error_line(message, cx))
            })
            .when(error.is_none(), |column| {
                column.when_some(help, |column, help| column.child(layout::help_text(help)))
            });

        layout::form_row(label, column, None)
    }

    fn text_field(&self, spec: TextField<'_>, cx: &mut Context<Self>) -> Div {
        let control = Input::new(spec.input)
            .id(spec.element_id)
            .aria_label(spec.label);

        self.field_press(
            spec.field,
            layout::field_frame(
                self.is_cursor_on(spec.field),
                Some(spec.width),
                spec.mono,
                control,
                cx,
            ),
            cx,
        )
    }

    /// The body editor with its error and hint: the frame turns danger-colored
    /// while the body has an error.
    fn render_body_block(&self, theme: &Theme, cx: &mut Context<Self>) -> Div {
        let body_editor = self.field_press(
            SnippetFormField::Body,
            layout::cursor_ring(
                self.is_cursor_on(SnippetFormField::Body),
                div()
                    .relative()
                    .w_full()
                    .h(BODY_EDITOR_HEIGHT)
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .child(
                        Chamfer::new(ChamferCut::CONTROL)
                            .fill(theme.background)
                            .border(if self.errors.body.is_some() {
                                theme.danger
                            } else {
                                theme.border
                            }),
                    )
                    .child(Editor::new(&self.input_body).bordered(false).size_full()),
                cx,
            ),
            cx,
        );

        div()
            .py(FormMetrics::ROW_PADDING_Y)
            .flex()
            .flex_col()
            .gap(FormMetrics::HELP_GAP)
            .child(body_editor)
            .when_some(self.errors.body.clone(), |column, message| {
                column.child(Self::error_line(message, cx))
            })
            .child(layout::help_text(dbflux_i18n::t!(
                "settings.snippets.form.body_hint"
            )))
    }

    fn render_form(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme().clone();
        let name_label = dbflux_i18n::t!("settings.snippets.form.name");
        let prefix_label = dbflux_i18n::t!("settings.snippets.form.prefix");
        let language_label_text = dbflux_i18n::t!("settings.snippets.form.language");

        let name_row = Self::field_row(
            name_label.clone(),
            self.text_field(
                TextField {
                    field: SnippetFormField::Name,
                    label: name_label,
                    input: &self.input_name,
                    element_id: "snippet-name",
                    width: NAME_FIELD_WIDTH,
                    mono: false,
                },
                cx,
            ),
            self.errors.name.clone(),
            None,
            cx,
        );

        let prefix_row = Self::field_row(
            prefix_label.clone(),
            self.text_field(
                TextField {
                    field: SnippetFormField::Prefix,
                    label: prefix_label,
                    input: &self.input_prefix,
                    element_id: "snippet-prefix",
                    width: PREFIX_FIELD_WIDTH,
                    mono: true,
                },
                cx,
            ),
            self.errors.prefix.clone(),
            Some(dbflux_i18n::t!("settings.snippets.form.prefix_hint")),
            cx,
        );

        let language_row = Self::field_row(
            language_label_text,
            self.field_press(
                SnippetFormField::Language,
                layout::cursor_ring(
                    self.is_cursor_on(SnippetFormField::Language),
                    div()
                        .w(LANGUAGE_FIELD_WIDTH)
                        .child(self.language_dropdown.clone()),
                    cx,
                ),
                cx,
            ),
            None,
            Some(dbflux_i18n::t!("settings.snippets.form.language_hint")),
            cx,
        );

        let body_block = self.render_body_block(&theme, cx);

        layout::sticky_form_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.snippets.form.group.snippet"),
                Some(AppIcon::Braces.into()),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .child(name_row)
                .child(prefix_row)
                .child(language_row)
                .child(dbflux_components::composites::section_header(
                    dbflux_i18n::t!("settings.snippets.form.group.body"),
                    Some(AppIcon::FileCode.into()),
                    cx,
                ))
                .child(body_block),
            None,
            &theme,
        )
    }

    /// Delete, on the left of the footer, for a saved snippet.
    pub(super) fn render_footer_leading_buttons(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let id = self.editing_id.clone()?;

        Some(
            Button::new(
                "delete-snippet",
                dbflux_i18n::t!("settings.snippets.action.delete"),
            )
            .danger()
            .icon(AppIcon::Delete)
            .focused(self.is_cursor_on(SnippetFormField::DeleteButton))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.request_delete_snippet(id.clone(), cx);
            }))
            .into_any_element(),
        )
    }

    /// Cancel (new snippet only) and Save or Update, on the right of the footer.
    pub(super) fn render_footer_buttons(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        if !self.new_form_open && self.editing_id.is_none() {
            return None;
        }

        let editing = self.editing_id.is_some();

        Some(
            div()
                .flex()
                .items_center()
                .gap(FormMetrics::INLINE_GAP)
                .when(!editing, |footer| {
                    footer.child(
                        Button::new(
                            "cancel-snippet",
                            dbflux_i18n::t!("settings.snippets.action.cancel"),
                        )
                        .secondary()
                        .focused(self.is_cursor_on(SnippetFormField::CancelButton))
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.cancel_new_snippet(window, cx);
                        })),
                    )
                })
                .child(
                    Button::new(
                        "save-snippet",
                        if editing {
                            dbflux_i18n::t!("settings.snippets.action.update")
                        } else {
                            dbflux_i18n::t!("settings.snippets.action.save")
                        },
                    )
                    .primary()
                    .icon(AppIcon::Check)
                    .when_some(crate::settings::save_shortcut(), Button::kbd)
                    .focused(self.is_cursor_on(SnippetFormField::SaveButton))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save_snippet(window, cx);
                    })),
                )
                .into_any_element(),
        )
    }
}

/// Language chip and, for a shipped snippet, the Built-in chip.
fn row_chips(snippet: &SnippetRecord) -> AnyElement {
    div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .child(Badge::new(
            language_label(&snippet.language),
            BadgeTone::Neutral,
        ))
        .when(snippet.builtin_key.is_some(), |chips| {
            chips.child(Badge::new(
                dbflux_i18n::t!("settings.snippets.list.builtin"),
                BadgeTone::Accent,
            ))
        })
        .into_any_element()
}
