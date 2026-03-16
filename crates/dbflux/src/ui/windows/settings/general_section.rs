use super::SettingsSection;
use super::SettingsSectionId;
use super::section_trait::SectionFocusEvent;
use crate::app::AppState;
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_core::{GeneralSettings, RefreshPolicySetting, StartupFocus, ThemeSetting};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::{InputEvent, InputState};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum GeneralFormRow {
    Theme,
    RestoreSession,
    ReopenConnections,
    DefaultFocus,
    MaxHistory,
    AutoSaveInterval,
    DefaultRefreshPolicy,
    DefaultRefreshInterval,
    MaxBackgroundTasks,
    PauseRefreshOnError,
    RefreshOnlyIfVisible,
    ConfirmDangerous,
    RequiresWhere,
    RequiresPreview,
    SaveButton,
}

pub(super) struct GeneralSection {
    pub(super) app_state: Entity<AppState>,
    pub(super) gen_settings: GeneralSettings,
    pub(super) gen_form_cursor: usize,
    pub(super) gen_editing_field: bool,
    pub(super) dropdown_theme: Entity<Dropdown>,
    pub(super) dropdown_default_focus: Entity<Dropdown>,
    pub(super) dropdown_refresh_policy: Entity<Dropdown>,
    pub(super) input_max_history: Entity<InputState>,
    pub(super) input_auto_save: Entity<InputState>,
    pub(super) input_refresh_interval: Entity<InputState>,
    pub(super) input_max_bg_tasks: Entity<InputState>,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for GeneralSection {}

impl GeneralSection {
    pub(super) fn new(
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let settings = app_state.read(cx).general_settings().clone();
        let theme_index = Self::theme_index(settings.theme);
        let startup_focus_index = Self::startup_focus_index(settings.default_focus_on_startup);
        let refresh_policy_index = Self::refresh_policy_index(settings.default_refresh_policy);
        let max_history = settings.max_history_entries.to_string();
        let auto_save_interval = settings.auto_save_interval_ms.to_string();
        let refresh_interval = settings.default_refresh_interval_secs.to_string();
        let max_background_tasks = settings.max_concurrent_background_tasks.to_string();

        let dropdown_theme = cx.new(move |_cx| {
            Dropdown::new("general-theme")
                .placeholder("Theme")
                .items(Self::theme_items())
                .selected_index(Some(theme_index))
        });
        let dropdown_default_focus = cx.new(move |_cx| {
            Dropdown::new("general-default-focus")
                .placeholder("Default focus")
                .items(Self::startup_focus_items())
                .selected_index(Some(startup_focus_index))
        });
        let dropdown_refresh_policy = cx.new(move |_cx| {
            Dropdown::new("general-refresh-policy")
                .placeholder("Refresh policy")
                .items(Self::refresh_policy_items())
                .selected_index(Some(refresh_policy_index))
        });

        let input_max_history = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("1000")
                .default_value(max_history.clone())
        });
        let input_auto_save = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("2000")
                .default_value(auto_save_interval.clone())
        });
        let input_refresh_interval = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("5")
                .default_value(refresh_interval.clone())
        });
        let input_max_bg_tasks = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("8")
                .default_value(max_background_tasks.clone())
        });

        let theme_subscription = cx.subscribe(
            &dropdown_theme,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.theme = Self::theme_for_index(event.index);
                cx.notify();
            },
        );

        let focus_subscription = cx.subscribe(
            &dropdown_default_focus,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.default_focus_on_startup =
                    Self::startup_focus_for_index(event.index);
                cx.notify();
            },
        );

        let refresh_policy_subscription = cx.subscribe(
            &dropdown_refresh_policy,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.default_refresh_policy =
                    Self::refresh_policy_for_index(event.index);
                cx.notify();
            },
        );

        let blur_max_history =
            cx.subscribe(&input_max_history, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        let blur_auto_save = cx.subscribe(&input_auto_save, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_refresh_interval = cx.subscribe(
            &input_refresh_interval,
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

        let blur_max_bg_tasks =
            cx.subscribe(&input_max_bg_tasks, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        Self {
            app_state,
            gen_settings: settings,
            gen_form_cursor: 0,
            gen_editing_field: false,
            dropdown_theme,
            dropdown_default_focus,
            dropdown_refresh_policy,
            input_max_history,
            input_auto_save,
            input_refresh_interval,
            input_max_bg_tasks,
            content_focused: false,
            switching_input: false,
            _subscriptions: vec![
                theme_subscription,
                focus_subscription,
                refresh_policy_subscription,
                blur_max_history,
                blur_auto_save,
                blur_refresh_interval,
                blur_max_bg_tasks,
            ],
        }
    }

    fn theme_items() -> Vec<DropdownItem> {
        vec![DropdownItem::new("Dark"), DropdownItem::new("Light")]
    }

    fn startup_focus_items() -> Vec<DropdownItem> {
        vec![DropdownItem::new("Sidebar"), DropdownItem::new("Last Tab")]
    }

    fn refresh_policy_items() -> Vec<DropdownItem> {
        vec![DropdownItem::new("Manual"), DropdownItem::new("Interval")]
    }

    fn theme_index(theme: ThemeSetting) -> usize {
        match theme {
            ThemeSetting::Dark => 0,
            ThemeSetting::Light => 1,
        }
    }

    fn theme_for_index(index: usize) -> ThemeSetting {
        match index {
            1 => ThemeSetting::Light,
            _ => ThemeSetting::Dark,
        }
    }

    fn startup_focus_index(focus: StartupFocus) -> usize {
        match focus {
            StartupFocus::Sidebar => 0,
            StartupFocus::LastTab => 1,
        }
    }

    fn startup_focus_for_index(index: usize) -> StartupFocus {
        match index {
            1 => StartupFocus::LastTab,
            _ => StartupFocus::Sidebar,
        }
    }

    fn refresh_policy_index(policy: RefreshPolicySetting) -> usize {
        match policy {
            RefreshPolicySetting::Manual => 0,
            RefreshPolicySetting::Interval => 1,
        }
    }

    fn refresh_policy_for_index(index: usize) -> RefreshPolicySetting {
        match index {
            1 => RefreshPolicySetting::Interval,
            _ => RefreshPolicySetting::Manual,
        }
    }
}

impl SettingsSection for GeneralSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::General
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        GeneralSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.gen_editing_field = false;
        self.close_open_dropdown(cx);
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_general_changes(cx)
    }
}

impl Render for GeneralSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_general_section(cx)
    }
}
