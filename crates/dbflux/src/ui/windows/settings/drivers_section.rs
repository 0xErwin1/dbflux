use super::SettingsSection;
use super::SettingsSectionId;
use crate::app::AppState;
use crate::keymap::{KeyChord, Modifiers};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::form_renderer::FormRendererState;
use dbflux_core::{
    DriverFormDef, DriverKey, DriverMetadata, FormValues, GeneralSettings, GlobalOverrides,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::InputState;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct DriverSettingsEntry {
    pub(super) driver_key: DriverKey,
    pub(super) metadata: DriverMetadata,
    pub(super) settings_schema: Option<Arc<DriverFormDef>>,
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
    pub(super) content_focused: bool,
    _subscriptions: Vec<Subscription>,
}

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
            content_focused: false,
            _subscriptions: vec![
                drv_refresh_dropdown_sub,
                drv_refresh_input_sub,
                drv_confirm_dangerous_sub,
                drv_requires_where_sub,
                drv_requires_preview_sub,
            ],
        };

        section.drv_load_entries(window, cx);
        section
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

        let chord = KeyChord::from_gpui(&event.keystroke);

        match (chord.key.as_str(), chord.modifiers) {
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
            _ => {}
        }
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
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
