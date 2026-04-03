use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::FormSection;
use super::section_trait::SectionFocusEvent;
use crate::app::AppState;
use crate::keymap::{key_chord_from_gpui, KeyChord, Modifiers};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::form_renderer::FormRendererState;
use dbflux_core::{
    DriverFormDef, DriverKey, DriverMetadata, FormValues, GeneralSettings, GlobalOverrides,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::{InputEvent, InputState};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct DriverSettingsEntry {
    pub(super) driver_key: DriverKey,
    pub(super) metadata: DriverMetadata,
    pub(super) settings_schema: Option<Arc<DriverFormDef>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum DriversFocus {
    List,
    Editor,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum DriverEditorField {
    OverrideRefreshPolicy,
    RefreshPolicy,
    OverrideRefreshInterval,
    RefreshInterval,
    ConfirmDangerous,
    RequiresWhere,
    RequiresPreview,
    Save,
}

pub(super) struct DriversSection {
    pub(super) app_state: Entity<AppState>,
    pub(super) gen_settings: GeneralSettings,
    pub(super) drv_entries: Vec<DriverSettingsEntry>,
    pub(super) drv_selected_idx: Option<usize>,
    pub(super) drv_overrides: HashMap<DriverKey, GlobalOverrides>,
    pub(super) drv_settings: HashMap<DriverKey, FormValues>,

    pub(super) drv_editor_dirty: bool,
    pub(super) drv_loading_selected_editor: bool,

    pub(super) drv_override_refresh_policy: bool,
    pub(super) drv_override_refresh_interval: bool,

    pub(super) drv_refresh_policy_dropdown: Entity<Dropdown>,
    pub(super) drv_refresh_interval_input: Entity<InputState>,
    pub(super) drv_confirm_dangerous_dropdown: Entity<Dropdown>,
    pub(super) drv_requires_where_dropdown: Entity<Dropdown>,
    pub(super) drv_requires_preview_dropdown: Entity<Dropdown>,

    pub(super) drv_form_state: FormRendererState,
    pub(super) drv_form_subscriptions: Vec<Subscription>,
    pub(super) drv_list_scroll_handle: ScrollHandle,
    pub(super) drv_pending_scroll_idx: Option<usize>,
    pub(super) drv_focus: DriversFocus,
    pub(super) drv_editor_field: DriverEditorField,
    pub(super) drv_editing_field: bool,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    _subscriptions: Vec<Subscription>,
    _blur_subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for DriversSection {}

impl DriversSection {
    pub(super) fn new(
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let drv_refresh_policy_dropdown = cx.new(|_cx| {
            Dropdown::new("drv-refresh-policy")
                .items(vec![
                    DropdownItem::with_value("Manual", "manual"),
                    DropdownItem::with_value("Interval", "interval"),
                ])
                .selected_index(Some(0))
        });

        let drv_refresh_interval_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("5");
            state.set_value("5", window, cx);
            state
        });

        let drv_confirm_dangerous_dropdown = cx.new(|_cx| {
            Dropdown::new("drv-confirm-dangerous")
                .items(vec![
                    DropdownItem::with_value("Use Global", "default"),
                    DropdownItem::with_value("On", "true"),
                    DropdownItem::with_value("Off", "false"),
                ])
                .selected_index(Some(0))
        });

        let drv_requires_where_dropdown = cx.new(|_cx| {
            Dropdown::new("drv-requires-where")
                .items(vec![
                    DropdownItem::with_value("Use Global", "default"),
                    DropdownItem::with_value("On", "true"),
                    DropdownItem::with_value("Off", "false"),
                ])
                .selected_index(Some(0))
        });

        let drv_requires_preview_dropdown = cx.new(|_cx| {
            Dropdown::new("drv-requires-preview")
                .items(vec![
                    DropdownItem::with_value("Use Global", "default"),
                    DropdownItem::with_value("On", "true"),
                    DropdownItem::with_value("Off", "false"),
                ])
                .selected_index(Some(0))
        });

        let drv_refresh_dropdown_sub = cx.subscribe_in(
            &drv_refresh_policy_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, _window, cx| {
                if this.drv_loading_selected_editor {
                    return;
                }

                this.drv_editor_dirty = true;
                cx.notify();
            },
        );

        let drv_refresh_input_sub = cx.subscribe_in(
            &drv_refresh_interval_input,
            window,
            |this, _, event: &gpui_component::input::InputEvent, _window, cx| {
                if matches!(event, gpui_component::input::InputEvent::Change) {
                    if this.drv_loading_selected_editor {
                        return;
                    }

                    this.drv_editor_dirty = true;
                    cx.notify();
                }
            },
        );

        let blur_refresh_interval = cx.subscribe(
            &drv_refresh_interval_input,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        let drv_confirm_dangerous_sub = cx.subscribe_in(
            &drv_confirm_dangerous_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, _window, cx| {
                if this.drv_loading_selected_editor {
                    return;
                }

                this.drv_editor_dirty = true;
                cx.notify();
            },
        );

        let drv_requires_where_sub = cx.subscribe_in(
            &drv_requires_where_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, _window, cx| {
                if this.drv_loading_selected_editor {
                    return;
                }

                this.drv_editor_dirty = true;
                cx.notify();
            },
        );

        let drv_requires_preview_sub = cx.subscribe_in(
            &drv_requires_preview_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, _window, cx| {
                if this.drv_loading_selected_editor {
                    return;
                }

                this.drv_editor_dirty = true;
                cx.notify();
            },
        );

        let (drv_overrides, drv_settings, gen_settings) = {
            let state = app_state.read(cx);
            (
                state.driver_overrides().clone(),
                state.driver_settings().clone(),
                state.general_settings().clone(),
            )
        };

        let mut section = Self {
            app_state,
            gen_settings,
            drv_entries: Vec::new(),
            drv_selected_idx: None,
            drv_overrides,
            drv_settings,
            drv_editor_dirty: false,
            drv_loading_selected_editor: false,
            drv_override_refresh_policy: false,
            drv_override_refresh_interval: false,
            drv_refresh_policy_dropdown,
            drv_refresh_interval_input,
            drv_confirm_dangerous_dropdown,
            drv_requires_where_dropdown,
            drv_requires_preview_dropdown,
            drv_form_state: FormRendererState::default(),
            drv_form_subscriptions: Vec::new(),
            drv_list_scroll_handle: ScrollHandle::new(),
            drv_pending_scroll_idx: None,
            drv_focus: DriversFocus::List,
            drv_editor_field: DriverEditorField::OverrideRefreshPolicy,
            drv_editing_field: false,
            content_focused: false,
            switching_input: false,
            _subscriptions: vec![
                drv_refresh_dropdown_sub,
                drv_refresh_input_sub,
                drv_confirm_dangerous_sub,
                drv_requires_where_sub,
                drv_requires_preview_sub,
            ],
            _blur_subscriptions: vec![blur_refresh_interval],
        };

        section.drv_load_entries(window, cx);
        section
    }

    fn active_open_dropdown(&self, cx: &App) -> Option<Entity<Dropdown>> {
        let core_dropdowns = [
            &self.drv_refresh_policy_dropdown,
            &self.drv_confirm_dangerous_dropdown,
            &self.drv_requires_where_dropdown,
            &self.drv_requires_preview_dropdown,
        ];

        for dropdown in core_dropdowns {
            if dropdown.read(cx).is_open() {
                return Some(dropdown.clone());
            }
        }

        self.drv_form_state
            .dropdowns
            .values()
            .find(|dropdown| dropdown.read(cx).is_open())
            .cloned()
    }

    fn handle_open_dropdown(&mut self, chord: &KeyChord, cx: &mut Context<Self>) -> bool {
        let Some(dropdown_entity) = self.active_open_dropdown(cx) else {
            return false;
        };

        match (chord.key.as_str(), chord.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.select_next_item(cx));
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.select_prev_item(cx));
            }
            ("enter", modifiers) | ("tab", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.accept_selection(cx));
            }
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.close(cx));
            }
            ("h", modifiers) | ("left", modifiers) if modifiers == Modifiers::none() => {
                dropdown_entity.update(cx, |dropdown, cx| dropdown.close(cx));
            }
            _ => return false,
        }

        cx.notify();
        true
    }

    fn drv_move_editor_right(&mut self) {
        match self.drv_editor_field {
            DriverEditorField::OverrideRefreshPolicy if self.drv_override_refresh_policy => {
                self.drv_editor_field = DriverEditorField::RefreshPolicy;
            }
            DriverEditorField::OverrideRefreshInterval if self.drv_override_refresh_interval => {
                self.drv_editor_field = DriverEditorField::RefreshInterval;
            }
            _ => {}
        }
    }

    fn drv_move_editor_left(&mut self) {
        match self.drv_editor_field {
            DriverEditorField::RefreshPolicy => {
                self.drv_editor_field = DriverEditorField::OverrideRefreshPolicy;
            }
            DriverEditorField::RefreshInterval => {
                self.drv_editor_field = DriverEditorField::OverrideRefreshInterval;
            }
            _ => {}
        }
    }
}

impl SettingsSection for DriversSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Drivers
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.content_focused {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        if self.handle_open_dropdown(&chord, cx) {
            return;
        }

        match self.drv_focus {
            DriversFocus::List => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    if let Some(current) = self.drv_selected_idx
                        && current + 1 < self.drv_entries.len()
                    {
                        self.drv_select_driver(current + 1, window, cx);
                    }
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    if let Some(current) = self.drv_selected_idx
                        && current > 0
                    {
                        self.drv_select_driver(current - 1, window, cx);
                    }
                }
                ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                    if modifiers == Modifiers::none() =>
                {
                    self.enter_form(window, cx);
                    cx.notify();
                }
                ("g", modifiers)
                    if modifiers == Modifiers::none() && !self.drv_entries.is_empty() =>
                {
                    self.drv_select_driver(0, window, cx);
                }
                ("g", modifiers)
                    if modifiers == Modifiers::shift() && !self.drv_entries.is_empty() =>
                {
                    self.drv_select_driver(self.drv_entries.len() - 1, window, cx);
                }
                _ => {}
            },
            DriversFocus::Editor => match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("h", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("h", modifiers) | ("left", modifiers) if modifiers == Modifiers::none() => {
                    self.move_left();
                    cx.notify();
                }
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.move_down();
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.move_up();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.move_first();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    self.move_last();
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) if modifiers == Modifiers::none() => {
                    let previous_field = self.drv_editor_field;
                    self.move_right();

                    if previous_field != self.drv_editor_field {
                        cx.notify();
                        return;
                    }

                    if !matches!(
                        self.drv_editor_field,
                        DriverEditorField::OverrideRefreshPolicy
                            | DriverEditorField::OverrideRefreshInterval
                    ) {
                        self.activate_current_field(window, cx);
                    }
                    cx.notify();
                }
                ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                    self.activate_current_field(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.tab_next();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.tab_prev();
                    cx.notify();
                }
                _ => {}
            },
        }
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.drv_focus = DriversFocus::List;
        self.drv_editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_driver_changes(cx)
    }
}

impl Render for DriversSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_drivers_section(cx)
    }
}

impl FormSection for DriversSection {
    type Focus = DriversFocus;
    type FormField = DriverEditorField;

    fn focus_area(&self) -> Self::Focus {
        self.drv_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.drv_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.drv_editor_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.drv_editor_field = field;
    }

    fn editing_field(&self) -> bool {
        self.drv_editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.drv_editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        DriversFocus::List
    }

    fn form_focus() -> Self::Focus {
        DriversFocus::Editor
    }

    fn first_form_field() -> Self::FormField {
        DriverEditorField::OverrideRefreshPolicy
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        let mut rows = vec![
            vec![
                DriverEditorField::OverrideRefreshPolicy,
                DriverEditorField::RefreshPolicy,
            ],
            vec![
                DriverEditorField::OverrideRefreshInterval,
                DriverEditorField::RefreshInterval,
            ],
            vec![DriverEditorField::ConfirmDangerous],
            vec![DriverEditorField::RequiresWhere],
            vec![DriverEditorField::RequiresPreview],
            vec![DriverEditorField::Save],
        ];

        if !self.drv_override_refresh_policy {
            rows[0].retain(|f| *f != DriverEditorField::RefreshPolicy);
        }
        if !self.drv_override_refresh_interval {
            rows[1].retain(|f| *f != DriverEditorField::RefreshInterval);
        }

        rows
    }

    fn is_input_field(field: Self::FormField) -> bool {
        matches!(field, DriverEditorField::RefreshInterval)
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.drv_editor_field {
            DriverEditorField::RefreshInterval => {
                self.drv_editing_field = true;
                self.drv_refresh_interval_input.update(cx, |input, cx| {
                    input.focus(window, cx);
                });
            }
            _ => {
                self.drv_editing_field = false;
            }
        }
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.drv_activate_editor_field(window, cx);
    }

    fn move_down(&mut self) {
        self.drv_move_editor_down();
    }

    fn move_up(&mut self) {
        self.drv_move_editor_up();
    }

    fn move_left(&mut self) {
        self.drv_move_editor_left();
    }

    fn move_right(&mut self) {
        self.drv_move_editor_right();
    }

    fn move_first(&mut self) {
        self.drv_editor_field = DriverEditorField::OverrideRefreshPolicy;
    }

    fn move_last(&mut self) {
        self.drv_editor_field = DriverEditorField::Save;
    }

    fn tab_next(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();
        let mut found_current = false;

        for row in &rows {
            for field in row {
                if found_current {
                    self.set_form_field(*field);
                    return;
                }
                if *field == current {
                    found_current = true;
                }
            }
        }

        self.move_first();
    }

    fn tab_prev(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();
        let mut prev_field: Option<Self::FormField> = None;

        for row in &rows {
            for field in row {
                if *field == current {
                    if let Some(prev) = prev_field {
                        self.set_form_field(prev);
                    } else {
                        self.move_last();
                    }
                    return;
                }
                prev_field = Some(*field);
            }
        }
    }

    fn validate_form_field(&mut self) {
        let rows = self.form_rows();
        let current = self.form_field();

        for row in &rows {
            if row.contains(&current) {
                return;
            }
        }

        self.set_form_field(Self::first_form_field());
    }
}
