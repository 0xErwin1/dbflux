use crate::keymap::{Command, ContextId};
use crate::ui::components::form_navigation::{
    FormEditState, FormField, FormNavigation, focus_ring, subscribe_form_input,
};
use crate::ui::components::modal_frame::ModalFrame;
use crate::ui::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::button::{Button, ButtonVariant, ButtonVariants};
use gpui_component::input::{Input, InputState};
use gpui_component::{ActiveTheme, Sizable};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct NewKeyCreatedEvent {
    pub key_name: String,
    pub key_type: NewKeyType,
    pub ttl: Option<u64>,
    pub value: NewKeyValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NewKeyType {
    String,
    Hash,
    List,
    Set,
    SortedSet,
    Json,
}

impl NewKeyType {
    pub fn all() -> &'static [NewKeyType] {
        &[
            Self::String,
            Self::Hash,
            Self::List,
            Self::Set,
            Self::SortedSet,
            Self::Json,
        ]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::String => "String",
            Self::Hash => "Hash",
            Self::List => "List",
            Self::Set => "Set",
            Self::SortedSet => "Sorted Set",
            Self::Json => "JSON",
        }
    }
}

#[derive(Clone, Debug)]
pub enum NewKeyValue {
    Simple(String),
    HashFields(Vec<(String, String)>),
    ListMembers(Vec<String>),
    SetMembers(Vec<String>),
    ZSetMembers(Vec<(String, f64)>),
}

// ---------------------------------------------------------------------------
// Focus enum
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) enum ModalFocus {
    KeyType,
    TTL,
    KeyName,
    /// Single-value input for String / JSON types.
    Value,
    /// Primary field input in a row (member for List/Set, field for Hash, member for ZSet).
    RowField(usize),
    /// Secondary value input in a row (value for Hash, score for ZSet).
    RowValue(usize),
    /// Delete button for a row.
    RowDelete(usize),
    AddRow,
    Cancel,
    Create,
}

impl FormField for ModalFocus {
    fn is_input(&self) -> bool {
        matches!(
            self,
            ModalFocus::TTL
                | ModalFocus::KeyName
                | ModalFocus::Value
                | ModalFocus::RowField(_)
                | ModalFocus::RowValue(_)
        )
    }
}

// ---------------------------------------------------------------------------
// Value row (inputs for structured types)
// ---------------------------------------------------------------------------

struct ValueRow {
    field_input: Entity<InputState>,
    value_input: Entity<InputState>,
}

// ---------------------------------------------------------------------------
// NewKeyModal
// ---------------------------------------------------------------------------

pub struct NewKeyModal {
    visible: bool,
    focus_handle: FocusHandle,

    // Form navigation state
    edit_state: FormEditState,
    form_focus: ModalFocus,

    // Fields
    key_type_dropdown: Entity<Dropdown>,
    selected_type: NewKeyType,
    key_name_input: Entity<InputState>,
    ttl_input: Entity<InputState>,
    value_input: Entity<InputState>,
    value_rows: Vec<ValueRow>,
    error_message: Option<String>,

    _subscriptions: Vec<Subscription>,
}

impl NewKeyModal {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let items: Vec<DropdownItem> = NewKeyType::all()
            .iter()
            .map(|t| DropdownItem::new(t.label()))
            .collect();

        let key_type_dropdown = cx.new(|_cx| {
            Dropdown::new("new-key-type")
                .items(items)
                .selected_index(Some(0))
                .placeholder("Select type")
        });

        let key_name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Enter Key Name"));
        let ttl_input = cx.new(|cx| InputState::new(window, cx).placeholder("No limit"));
        let value_input = cx.new(|cx| InputState::new(window, cx).placeholder("Enter Value"));

        let mut subscriptions = Vec::new();

        subscriptions.push(cx.subscribe(
            &key_type_dropdown,
            |this: &mut Self, _, event: &DropdownSelectionChanged, cx| {
                let types = NewKeyType::all();
                if event.index < types.len() {
                    this.selected_type = types[event.index];
                    this.value_rows.clear();
                    this.error_message = None;

                    this.edit_state = FormEditState::Navigating;
                    this.form_focus = ModalFocus::KeyName;

                    cx.notify();
                }
            },
        ));

        subscriptions.push(subscribe_form_input(cx, window, &key_name_input));
        subscriptions.push(subscribe_form_input(cx, window, &ttl_input));
        subscriptions.push(subscribe_form_input(cx, window, &value_input));

        Self {
            visible: false,
            focus_handle: cx.focus_handle(),
            edit_state: FormEditState::default(),
            form_focus: ModalFocus::KeyName,
            key_type_dropdown,
            selected_type: NewKeyType::String,
            key_name_input,
            ttl_input,
            value_input,
            value_rows: Vec::new(),
            error_message: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn active_context(&self) -> ContextId {
        match self.edit_state {
            FormEditState::Navigating => ContextId::FormNavigation,
            FormEditState::Editing => ContextId::TextInput,
            FormEditState::DropdownOpen => ContextId::Dropdown,
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
            FormEditState::DropdownOpen => self.handle_dropdown_command(cmd, window, cx),
        }
    }

    fn handle_dropdown_command(
        &mut self,
        cmd: Command,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::SelectNext => {
                self.key_type_dropdown
                    .update(cx, |dd, cx| dd.select_next_item(cx));
                true
            }
            Command::SelectPrev => {
                self.key_type_dropdown
                    .update(cx, |dd, cx| dd.select_prev_item(cx));
                true
            }
            Command::Execute => {
                self.key_type_dropdown
                    .update(cx, |dd, cx| dd.accept_selection(cx));
                self.edit_state = FormEditState::Navigating;
                self.sync_dropdown_ring(cx);
                cx.notify();
                true
            }
            Command::Cancel => {
                self.key_type_dropdown.update(cx, |dd, cx| dd.close(cx));
                self.edit_state = FormEditState::Navigating;
                self.sync_dropdown_ring(cx);
                cx.notify();
                true
            }
            _ => false,
        }
    }

    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.error_message = None;
        self.value_rows.clear();
        self.edit_state = FormEditState::Navigating;
        self.form_focus = ModalFocus::KeyName;

        self.key_name_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.ttl_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.value_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

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
        let field_placeholder = match self.selected_type {
            NewKeyType::Hash => "Enter Field",
            NewKeyType::SortedSet => "Enter Member",
            _ => "Enter Member",
        };
        let value_placeholder = match self.selected_type {
            NewKeyType::Hash => "Enter Value",
            NewKeyType::SortedSet => "Enter Score",
            _ => "Enter Value",
        };

        let field_input = cx.new(|cx| InputState::new(window, cx).placeholder(field_placeholder));
        let value_input = cx.new(|cx| InputState::new(window, cx).placeholder(value_placeholder));

        self._subscriptions
            .push(subscribe_form_input(cx, window, &field_input));
        self._subscriptions
            .push(subscribe_form_input(cx, window, &value_input));

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
            ModalFocus::RowField(i) | ModalFocus::RowValue(i) | ModalFocus::RowDelete(i) => {
                if i == index {
                    if self.value_rows.is_empty() {
                        self.form_focus = ModalFocus::AddRow;
                    } else if i >= self.value_rows.len() {
                        self.form_focus = ModalFocus::RowField(self.value_rows.len() - 1);
                    }
                } else if i > index {
                    self.form_focus = match self.form_focus {
                        ModalFocus::RowField(_) => ModalFocus::RowField(i - 1),
                        ModalFocus::RowValue(_) => ModalFocus::RowValue(i - 1),
                        ModalFocus::RowDelete(_) => ModalFocus::RowDelete(i - 1),
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
        let key_name = self.key_name_input.read(cx).value().trim().to_string();
        if key_name.is_empty() {
            self.error_message = Some("Key name is required".to_string());
            cx.notify();
            return;
        }

        let ttl_text = self.ttl_input.read(cx).value().trim().to_string();
        let ttl = if ttl_text.is_empty() {
            None
        } else {
            match ttl_text.parse::<u64>() {
                Ok(v) => Some(v),
                Err(_) => {
                    self.error_message = Some("TTL must be a positive integer".to_string());
                    cx.notify();
                    return;
                }
            }
        };

        let value = match self.selected_type {
            NewKeyType::String | NewKeyType::Json => {
                let text = self.value_input.read(cx).value().to_string();
                NewKeyValue::Simple(text)
            }
            NewKeyType::Hash => {
                let fields: Vec<(String, String)> = self
                    .value_rows
                    .iter()
                    .map(|row| {
                        let f = row.field_input.read(cx).value().to_string();
                        let v = row.value_input.read(cx).value().to_string();
                        (f, v)
                    })
                    .filter(|(f, _)| !f.trim().is_empty())
                    .collect();
                NewKeyValue::HashFields(fields)
            }
            NewKeyType::List | NewKeyType::Set => {
                let members: Vec<String> = self
                    .value_rows
                    .iter()
                    .map(|row| row.field_input.read(cx).value().to_string())
                    .filter(|m| !m.trim().is_empty())
                    .collect();
                if self.selected_type == NewKeyType::List {
                    NewKeyValue::ListMembers(members)
                } else {
                    NewKeyValue::SetMembers(members)
                }
            }
            NewKeyType::SortedSet => {
                let members: Vec<(String, f64)> = self
                    .value_rows
                    .iter()
                    .filter_map(|row| {
                        let member = row.field_input.read(cx).value().to_string();
                        let score_text = row.value_input.read(cx).value().to_string();
                        if member.trim().is_empty() {
                            return None;
                        }
                        let score = score_text.parse::<f64>().unwrap_or(0.0);
                        Some((member, score))
                    })
                    .collect();
                NewKeyValue::ZSetMembers(members)
            }
        };

        cx.emit(NewKeyCreatedEvent {
            key_name,
            key_type: self.selected_type,
            ttl,
            value,
        });

        self.close(cx);
    }

    // -- Helpers ------------------------------------------------------------

    fn needs_rows(&self) -> bool {
        matches!(
            self.selected_type,
            NewKeyType::Hash | NewKeyType::List | NewKeyType::Set | NewKeyType::SortedSet
        )
    }

    fn needs_two_columns(&self) -> bool {
        matches!(self.selected_type, NewKeyType::Hash | NewKeyType::SortedSet)
    }

    fn input_for_focus(&self, focus: ModalFocus) -> Option<&Entity<InputState>> {
        match focus {
            ModalFocus::KeyName => Some(&self.key_name_input),
            ModalFocus::TTL => Some(&self.ttl_input),
            ModalFocus::Value => Some(&self.value_input),
            ModalFocus::RowField(i) => self.value_rows.get(i).map(|r| &r.field_input),
            ModalFocus::RowValue(i) => self.value_rows.get(i).map(|r| &r.value_input),
            _ => None,
        }
    }

    fn sync_dropdown_ring(&self, cx: &mut Context<Self>) {
        let show =
            self.edit_state == FormEditState::Navigating && self.form_focus == ModalFocus::KeyType;
        let color = if show { Some(cx.theme().ring) } else { None };

        self.key_type_dropdown
            .update(cx, |dd, cx| dd.set_focus_ring(color, cx));
    }
}

// ---------------------------------------------------------------------------
// Navigation graph
// ---------------------------------------------------------------------------

impl NewKeyModal {
    fn focus_down_impl(&self, current: ModalFocus) -> ModalFocus {
        match current {
            ModalFocus::KeyType | ModalFocus::TTL => ModalFocus::KeyName,

            ModalFocus::KeyName => {
                if self.needs_rows() {
                    if self.value_rows.is_empty() {
                        ModalFocus::AddRow
                    } else {
                        ModalFocus::RowField(0)
                    }
                } else {
                    ModalFocus::Value
                }
            }

            ModalFocus::Value => ModalFocus::Cancel,

            ModalFocus::RowField(i) | ModalFocus::RowValue(i) | ModalFocus::RowDelete(i) => {
                if i + 1 < self.value_rows.len() {
                    ModalFocus::RowField(i + 1)
                } else {
                    ModalFocus::AddRow
                }
            }

            ModalFocus::AddRow => ModalFocus::Cancel,
            ModalFocus::Cancel | ModalFocus::Create => ModalFocus::KeyType,
        }
    }

    fn focus_up_impl(&self, current: ModalFocus) -> ModalFocus {
        match current {
            ModalFocus::KeyType | ModalFocus::TTL => ModalFocus::Cancel,

            ModalFocus::KeyName => ModalFocus::KeyType,

            ModalFocus::Value => ModalFocus::KeyName,

            ModalFocus::RowField(i) | ModalFocus::RowValue(i) | ModalFocus::RowDelete(i) => {
                if i > 0 {
                    ModalFocus::RowField(i - 1)
                } else {
                    ModalFocus::KeyName
                }
            }

            ModalFocus::AddRow => {
                if let Some(last) = self.value_rows.len().checked_sub(1) {
                    ModalFocus::RowField(last)
                } else {
                    ModalFocus::KeyName
                }
            }

            ModalFocus::Cancel | ModalFocus::Create => {
                if self.needs_rows() {
                    ModalFocus::AddRow
                } else {
                    ModalFocus::Value
                }
            }
        }
    }

    fn focus_left_impl(&self, current: ModalFocus) -> ModalFocus {
        match current {
            // KeyType and TTL are on the same visual row
            ModalFocus::TTL => ModalFocus::KeyType,

            ModalFocus::RowValue(i) => ModalFocus::RowField(i),
            ModalFocus::RowDelete(i) => {
                if self.needs_two_columns() {
                    ModalFocus::RowValue(i)
                } else {
                    ModalFocus::RowField(i)
                }
            }

            ModalFocus::Create => ModalFocus::Cancel,

            other => other,
        }
    }

    fn focus_right_impl(&self, current: ModalFocus) -> ModalFocus {
        match current {
            // KeyType and TTL are on the same visual row
            ModalFocus::KeyType => ModalFocus::TTL,

            ModalFocus::RowField(i) => {
                if self.needs_two_columns() {
                    ModalFocus::RowValue(i)
                } else {
                    ModalFocus::RowDelete(i)
                }
            }
            ModalFocus::RowValue(i) => ModalFocus::RowDelete(i),

            ModalFocus::Cancel => ModalFocus::Create,

            other => other,
        }
    }
}

// ---------------------------------------------------------------------------
// FormNavigation trait impl
// ---------------------------------------------------------------------------

impl FormNavigation for NewKeyModal {
    type Focus = ModalFocus;

    fn edit_state(&self) -> FormEditState {
        self.edit_state
    }

    fn set_edit_state(&mut self, state: FormEditState) {
        self.edit_state = state;
    }

    fn form_focus(&self) -> ModalFocus {
        self.form_focus
    }

    fn set_form_focus(&mut self, focus: ModalFocus) {
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
            ModalFocus::KeyName
            | ModalFocus::TTL
            | ModalFocus::Value
            | ModalFocus::RowField(_)
            | ModalFocus::RowValue(_) => {
                if let Some(input) = self.input_for_focus(focus).cloned() {
                    self.edit_state = FormEditState::Editing;
                    input.update(cx, |state, cx| state.focus(window, cx));
                }
            }

            ModalFocus::KeyType => {
                self.key_type_dropdown.update(cx, |dd, cx| dd.open(cx));
                self.edit_state = FormEditState::DropdownOpen;
            }

            ModalFocus::RowDelete(i) => {
                self.remove_value_row(i, cx);
            }

            ModalFocus::AddRow => {
                self.add_value_row(window, cx);
                let new_index = self.value_rows.len() - 1;
                self.form_focus = ModalFocus::RowField(new_index);
            }

            ModalFocus::Cancel => {
                self.close(cx);
            }

            ModalFocus::Create => {
                self.submit(window, cx);
            }
        }

        self.sync_dropdown_ring(cx);
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

impl EventEmitter<NewKeyCreatedEvent> for NewKeyModal {}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for NewKeyModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.sync_dropdown_ring(cx);

        let theme = cx.theme();
        let ring_color = theme.ring;
        let show_ring = self.edit_state == FormEditState::Navigating;

        let entity = cx.entity().downgrade();

        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let needs_rows = self.needs_rows();
        let needs_two_cols = self.needs_two_columns();
        let row_count = self.value_rows.len();
        let focus = self.form_focus;

        let mut body = div().flex().flex_col().gap(Spacing::LG).p(Spacing::LG);

        // -- Row 1: Key Type + TTL ------------------------------------------

        body = body.child(
            div()
                .flex()
                .gap(Spacing::LG)
                .child(
                    div()
                        .flex_1()
                        .flex()
                        .flex_col()
                        .gap(Spacing::XS)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .child("Key Type*"),
                        )
                        .child(self.key_type_dropdown.clone()),
                )
                .child(
                    div()
                        .w(px(200.0))
                        .flex()
                        .flex_col()
                        .gap(Spacing::XS)
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .child("TTL"),
                        )
                        .child(
                            focus_ring(show_ring && focus == ModalFocus::TTL, ring_color)
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, window, cx| {
                                        this.enter_edit_mode_for_field(ModalFocus::TTL, window, cx);
                                    }),
                                )
                                .child(Input::new(&self.ttl_input).small().w_full()),
                        ),
                ),
        );

        // -- Row 2: Key Name ------------------------------------------------

        body = body.child(
            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child("Key Name*"),
                )
                .child(
                    focus_ring(show_ring && focus == ModalFocus::KeyName, ring_color)
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, window, cx| {
                                this.enter_edit_mode_for_field(ModalFocus::KeyName, window, cx);
                            }),
                        )
                        .child(Input::new(&self.key_name_input).small().w_full()),
                ),
        );

        // -- Separator ------------------------------------------------------

        body = body.child(div().h(px(1.0)).bg(theme.border));

        // -- Value section --------------------------------------------------

        if needs_rows {
            let mut rows_container = div().flex().flex_col().gap(Spacing::SM);

            for index in 0..row_count {
                let row = &self.value_rows[index];

                let mut row_div = div().flex().items_center().gap(Spacing::SM);

                row_div = row_div.child(
                    focus_ring(
                        show_ring && focus == ModalFocus::RowField(index),
                        ring_color,
                    )
                    .flex_1()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.enter_edit_mode_for_field(ModalFocus::RowField(index), window, cx);
                        }),
                    )
                    .child(Input::new(&row.field_input).small().w_full()),
                );

                if needs_two_cols {
                    row_div = row_div.child(
                        focus_ring(
                            show_ring && focus == ModalFocus::RowValue(index),
                            ring_color,
                        )
                        .flex_1()
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, window, cx| {
                                this.enter_edit_mode_for_field(
                                    ModalFocus::RowValue(index),
                                    window,
                                    cx,
                                );
                            }),
                        )
                        .child(Input::new(&row.value_input).small().w_full()),
                    );
                }

                let delete_focused = show_ring && focus == ModalFocus::RowDelete(index);
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

            // Add row button
            let add_focused = show_ring && focus == ModalFocus::AddRow;
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
        } else {
            body = body.child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.muted_foreground)
                            .child("Value"),
                    )
                    .child(
                        focus_ring(show_ring && focus == ModalFocus::Value, ring_color)
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.enter_edit_mode_for_field(ModalFocus::Value, window, cx);
                                }),
                            )
                            .child(Input::new(&self.value_input).small().w_full()),
                    ),
            );
        }

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

        let cancel_focused = show_ring && focus == ModalFocus::Cancel;
        let create_focused = show_ring && focus == ModalFocus::Create;

        body = body.child(
            div()
                .flex()
                .justify_end()
                .gap(Spacing::SM)
                .child(
                    focus_ring(cancel_focused, ring_color).child(
                        Button::new("new-key-cancel")
                            .small()
                            .label("Cancel")
                            .with_variant(ButtonVariant::Ghost)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.close(cx);
                            })),
                    ),
                )
                .child(
                    focus_ring(create_focused, ring_color).child(
                        Button::new("new-key-create")
                            .small()
                            .label("Create")
                            .with_variant(ButtonVariant::Primary)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit(window, cx);
                            })),
                    ),
                ),
        );

        ModalFrame::new("new-key-modal", &self.focus_handle, close)
            .title("New Key")
            .icon(AppIcon::Plus)
            .width(px(600.0))
            .max_height(px(500.0))
            .child(body.into_any_element())
            .render(cx)
    }
}
