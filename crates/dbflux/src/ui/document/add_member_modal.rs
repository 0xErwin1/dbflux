use crate::keymap::{Command, ContextId};
use crate::ui::components::form_navigation::{
    FormEditState, FormField, FormNavigation, focus_ring, subscribe_form_input,
};
use crate::ui::components::modal_frame::ModalFrame;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::KeyType;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::input::{Input, InputState};
use gpui_component::{ActiveTheme, Sizable};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AddMemberEvent {
    pub fields: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// Focus enum
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum AddMemberFocus {
    RowField(usize),
    RowValue(usize),
    RowDelete(usize),
    AddRow,
    Cancel,
    Submit,
}

impl FormField for AddMemberFocus {
    fn is_input(&self) -> bool {
        matches!(
            self,
            AddMemberFocus::RowField(_) | AddMemberFocus::RowValue(_)
        )
    }
}

// ---------------------------------------------------------------------------
// Value row
// ---------------------------------------------------------------------------

struct ValueRow {
    field_input: Entity<InputState>,
    value_input: Entity<InputState>,
}

// ---------------------------------------------------------------------------
// AddMemberModal
// ---------------------------------------------------------------------------

pub struct AddMemberModal {
    visible: bool,
    focus_handle: FocusHandle,
    key_type: KeyType,

    edit_state: FormEditState,
    form_focus: AddMemberFocus,

    value_rows: Vec<ValueRow>,
    error_message: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl AddMemberModal {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            visible: false,
            focus_handle: cx.focus_handle(),
            key_type: KeyType::Hash,
            edit_state: FormEditState::default(),
            form_focus: AddMemberFocus::RowField(0),
            value_rows: Vec::new(),
            error_message: None,
            _subscriptions: Vec::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn active_context(&self) -> ContextId {
        match self.edit_state {
            FormEditState::Navigating => ContextId::FormNavigation,
            FormEditState::Editing => ContextId::TextInput,
            FormEditState::DropdownOpen => ContextId::FormNavigation,
        }
    }

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match self.edit_state {
            FormEditState::Navigating => self.handle_navigating_command(cmd, window, cx),
            FormEditState::Editing => self.handle_editing_command(cmd, window, cx),
            FormEditState::DropdownOpen => false,
        }
    }

    pub fn open(&mut self, key_type: KeyType, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.key_type = key_type;
        self.error_message = None;
        self.value_rows.clear();
        self.edit_state = FormEditState::Navigating;

        self.add_value_row(window, cx);
        self.form_focus = AddMemberFocus::RowField(0);

        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.error_message = None;
        cx.notify();
    }

    // -- Row management -----------------------------------------------------

    fn add_value_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (field_placeholder, value_placeholder) = match self.key_type {
            KeyType::Hash | KeyType::Stream => ("Enter Field", "Enter Value"),
            KeyType::SortedSet => ("Enter Member", "Enter Score"),
            KeyType::List | KeyType::Set => ("Enter Member", ""),
            _ => ("Enter Field", "Enter Value"),
        };

        let field_input = cx.new(|cx| InputState::new(window, cx).placeholder(field_placeholder));
        let value_input = cx.new(|cx| InputState::new(window, cx).placeholder(value_placeholder));

        self._subscriptions
            .push(subscribe_form_input(cx, window, &field_input));

        if self.needs_two_columns() {
            self._subscriptions
                .push(subscribe_form_input(cx, window, &value_input));
        }

        self.value_rows.push(ValueRow {
            field_input,
            value_input,
        });

        cx.notify();
    }

    fn remove_value_row(&mut self, index: usize, cx: &mut Context<Self>) {
        if index >= self.value_rows.len() {
            return;
        }

        self.value_rows.remove(index);

        match self.form_focus {
            AddMemberFocus::RowField(i)
            | AddMemberFocus::RowValue(i)
            | AddMemberFocus::RowDelete(i) => {
                if i == index {
                    if self.value_rows.is_empty() {
                        self.form_focus = AddMemberFocus::AddRow;
                    } else if i >= self.value_rows.len() {
                        self.form_focus = AddMemberFocus::RowField(self.value_rows.len() - 1);
                    }
                } else if i > index {
                    self.form_focus = match self.form_focus {
                        AddMemberFocus::RowField(_) => AddMemberFocus::RowField(i - 1),
                        AddMemberFocus::RowValue(_) => AddMemberFocus::RowValue(i - 1),
                        AddMemberFocus::RowDelete(_) => AddMemberFocus::RowDelete(i - 1),
                        other => other,
                    };
                }
            }
            _ => {}
        }

        cx.notify();
    }

    // -- Submit -------------------------------------------------------------

    fn submit(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let two_cols = self.needs_two_columns();

        let fields: Vec<(String, String)> = self
            .value_rows
            .iter()
            .map(|row| {
                let f = row.field_input.read(cx).value().to_string();
                let v = if two_cols {
                    row.value_input.read(cx).value().to_string()
                } else {
                    String::new()
                };
                (f, v)
            })
            .filter(|(f, _)| !f.trim().is_empty())
            .collect();

        if fields.is_empty() {
            self.error_message = Some("At least one entry is required".to_string());
            cx.notify();
            return;
        }

        cx.emit(AddMemberEvent { fields });
        self.close(cx);
    }

    // -- Helpers ------------------------------------------------------------

    fn input_for_focus(&self, focus: AddMemberFocus) -> Option<&Entity<InputState>> {
        match focus {
            AddMemberFocus::RowField(i) => self.value_rows.get(i).map(|r| &r.field_input),
            AddMemberFocus::RowValue(i) => self.value_rows.get(i).map(|r| &r.value_input),
            _ => None,
        }
    }

    fn needs_two_columns(&self) -> bool {
        matches!(
            self.key_type,
            KeyType::Hash | KeyType::Stream | KeyType::SortedSet
        )
    }

    fn title(&self) -> &'static str {
        match self.key_type {
            KeyType::Hash => "Add Hash Fields",
            KeyType::Stream => "Add Stream Entry",
            KeyType::List => "Add List Members",
            KeyType::Set => "Add Set Members",
            KeyType::SortedSet => "Add Sorted Set Members",
            _ => "Add Member",
        }
    }

    fn section_label(&self) -> &'static str {
        match self.key_type {
            KeyType::Hash | KeyType::Stream => "Fields",
            KeyType::SortedSet => "Members",
            KeyType::List | KeyType::Set => "Members",
            _ => "Fields",
        }
    }
}

// ---------------------------------------------------------------------------
// Navigation graph
// ---------------------------------------------------------------------------

impl AddMemberModal {
    fn focus_down_impl(&self, current: AddMemberFocus) -> AddMemberFocus {
        match current {
            AddMemberFocus::RowField(i)
            | AddMemberFocus::RowValue(i)
            | AddMemberFocus::RowDelete(i) => {
                if i + 1 < self.value_rows.len() {
                    AddMemberFocus::RowField(i + 1)
                } else {
                    AddMemberFocus::AddRow
                }
            }

            AddMemberFocus::AddRow => AddMemberFocus::Cancel,

            AddMemberFocus::Cancel | AddMemberFocus::Submit => {
                if self.value_rows.is_empty() {
                    AddMemberFocus::AddRow
                } else {
                    AddMemberFocus::RowField(0)
                }
            }
        }
    }

    fn focus_up_impl(&self, current: AddMemberFocus) -> AddMemberFocus {
        match current {
            AddMemberFocus::RowField(i)
            | AddMemberFocus::RowValue(i)
            | AddMemberFocus::RowDelete(i) => {
                if i > 0 {
                    AddMemberFocus::RowField(i - 1)
                } else {
                    AddMemberFocus::Cancel
                }
            }

            AddMemberFocus::AddRow => {
                if let Some(last) = self.value_rows.len().checked_sub(1) {
                    AddMemberFocus::RowField(last)
                } else {
                    AddMemberFocus::Cancel
                }
            }

            AddMemberFocus::Cancel | AddMemberFocus::Submit => AddMemberFocus::AddRow,
        }
    }

    fn focus_left_impl(&self, current: AddMemberFocus) -> AddMemberFocus {
        match current {
            AddMemberFocus::RowValue(i) => AddMemberFocus::RowField(i),
            AddMemberFocus::RowDelete(i) => {
                if self.needs_two_columns() {
                    AddMemberFocus::RowValue(i)
                } else {
                    AddMemberFocus::RowField(i)
                }
            }
            AddMemberFocus::Submit => AddMemberFocus::Cancel,
            other => other,
        }
    }

    fn focus_right_impl(&self, current: AddMemberFocus) -> AddMemberFocus {
        match current {
            AddMemberFocus::RowField(i) => {
                if self.needs_two_columns() {
                    AddMemberFocus::RowValue(i)
                } else {
                    AddMemberFocus::RowDelete(i)
                }
            }
            AddMemberFocus::RowValue(i) => AddMemberFocus::RowDelete(i),
            AddMemberFocus::Cancel => AddMemberFocus::Submit,
            other => other,
        }
    }
}

// ---------------------------------------------------------------------------
// FormNavigation trait impl
// ---------------------------------------------------------------------------

impl FormNavigation for AddMemberModal {
    type Focus = AddMemberFocus;

    fn edit_state(&self) -> FormEditState {
        self.edit_state
    }

    fn set_edit_state(&mut self, state: FormEditState) {
        self.edit_state = state;
    }

    fn form_focus(&self) -> AddMemberFocus {
        self.form_focus
    }

    fn set_form_focus(&mut self, focus: AddMemberFocus) {
        self.form_focus = focus;
    }

    fn form_focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    fn focus_down(&mut self, _cx: &App) {
        self.form_focus = self.focus_down_impl(self.form_focus);
    }

    fn focus_up(&mut self, _cx: &App) {
        self.form_focus = self.focus_up_impl(self.form_focus);
    }

    fn focus_left(&mut self, _cx: &App) {
        self.form_focus = self.focus_left_impl(self.form_focus);
    }

    fn focus_right(&mut self, _cx: &App) {
        self.form_focus = self.focus_right_impl(self.form_focus);
    }

    fn activate_focused_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let focus = self.form_focus;

        match focus {
            AddMemberFocus::RowField(_) | AddMemberFocus::RowValue(_) => {
                if let Some(input) = self.input_for_focus(focus).cloned() {
                    self.edit_state = FormEditState::Editing;
                    input.update(cx, |state, cx| state.focus(window, cx));
                }
            }

            AddMemberFocus::RowDelete(i) => {
                self.remove_value_row(i, cx);
            }

            AddMemberFocus::AddRow => {
                self.add_value_row(window, cx);
                let new_index = self.value_rows.len() - 1;
                self.form_focus = AddMemberFocus::RowField(new_index);
            }

            AddMemberFocus::Cancel => {
                self.close(cx);
            }

            AddMemberFocus::Submit => {
                self.submit(window, cx);
            }
        }

        cx.notify();
    }

    fn handle_cancel_navigating(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> bool {
        self.close(cx);
        true
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

impl EventEmitter<AddMemberEvent> for AddMemberModal {}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for AddMemberModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let ring_color = theme.ring;
        let show_ring = self.edit_state == FormEditState::Navigating;
        let focus = self.form_focus;
        let row_count = self.value_rows.len();
        let two_cols = self.needs_two_columns();

        let entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let mut body = div().flex().flex_col().gap(Spacing::LG).p(Spacing::LG);

        // -- Header label ---------------------------------------------------

        body = body.child(
            div()
                .text_size(FontSizes::SM)
                .text_color(theme.muted_foreground)
                .child(self.section_label()),
        );

        // -- Rows -----------------------------------------------------------

        let mut rows_container = div().flex().flex_col().gap(Spacing::SM);

        for index in 0..row_count {
            let row = &self.value_rows[index];

            let mut row_div = div().flex().items_center().gap(Spacing::SM);

            row_div = row_div.child(
                focus_ring(
                    show_ring && focus == AddMemberFocus::RowField(index),
                    ring_color,
                )
                .flex_1()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, window, cx| {
                        this.enter_edit_mode_for_field(AddMemberFocus::RowField(index), window, cx);
                    }),
                )
                .child(Input::new(&row.field_input).small().w_full()),
            );

            if two_cols {
                row_div = row_div.child(
                    focus_ring(
                        show_ring && focus == AddMemberFocus::RowValue(index),
                        ring_color,
                    )
                    .flex_1()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.enter_edit_mode_for_field(
                                AddMemberFocus::RowValue(index),
                                window,
                                cx,
                            );
                        }),
                    )
                    .child(Input::new(&row.value_input).small().w_full()),
                );
            }

            let delete_focused = show_ring && focus == AddMemberFocus::RowDelete(index);
            row_div = row_div.child(
                focus_ring(delete_focused, ring_color).child(
                    div()
                        .w(Heights::ICON_MD)
                        .h(Heights::ICON_MD)
                        .flex()
                        .items_center()
                        .justify_center()
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .hover(|d| d.bg(gpui::red()))
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, _, cx| {
                                this.remove_value_row(index, cx);
                            }),
                        )
                        .child(
                            svg()
                                .path(AppIcon::Delete.path())
                                .size(Heights::ICON_SM)
                                .text_color(theme.muted_foreground),
                        ),
                ),
            );

            rows_container = rows_container.child(row_div);
        }

        // -- Add row button -------------------------------------------------

        let add_focused = show_ring && focus == AddMemberFocus::AddRow;
        rows_container = rows_container.child(
            div().flex().justify_center().child(
                focus_ring(add_focused, ring_color).child(
                    div()
                        .w(Heights::ICON_LG)
                        .h(Heights::ICON_LG)
                        .flex()
                        .items_center()
                        .justify_center()
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .bg(theme.primary)
                        .hover(|d| d.opacity(0.8))
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, window, cx| {
                                this.add_value_row(window, cx);
                            }),
                        )
                        .child(
                            svg()
                                .path(AppIcon::Plus.path())
                                .size(Heights::ICON_SM)
                                .text_color(theme.primary_foreground),
                        ),
                ),
            ),
        );

        body = body.child(rows_container);

        // -- Error message --------------------------------------------------

        if let Some(error) = &self.error_message {
            body = body.child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .child(format!("Error: {}", error)),
            );
        }

        // -- Footer buttons -------------------------------------------------

        let cancel_focused = show_ring && focus == AddMemberFocus::Cancel;
        let submit_focused = show_ring && focus == AddMemberFocus::Submit;

        body = body.child(
            div()
                .flex()
                .justify_end()
                .gap(Spacing::SM)
                .child(
                    focus_ring(cancel_focused, ring_color).child(
                        Button::new("add-member-cancel")
                            .small()
                            .label("Cancel")
                            .with_variant(ButtonVariant::Ghost)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.close(cx);
                            })),
                    ),
                )
                .child(
                    focus_ring(submit_focused, ring_color).child(
                        Button::new("add-member-submit")
                            .small()
                            .label("Add")
                            .with_variant(ButtonVariant::Primary)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit(window, cx);
                            })),
                    ),
                ),
        );

        ModalFrame::new("add-member-modal", &self.focus_handle, close)
            .title(self.title())
            .icon(AppIcon::Plus)
            .width(px(600.0))
            .max_height(px(500.0))
            .child(body.into_any_element())
            .render(cx)
    }
}
