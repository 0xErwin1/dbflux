use super::*;
use crate::keymap::{KeyChord, Modifiers};
use crate::ui::components::tree_nav::TreeNavAction;

impl SettingsCoordinator {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let active_section = SettingsSectionId::General;
        let mut sidebar_tree = Self::build_sidebar_tree();
        sidebar_tree.select_by_id(Self::tree_id_for_section(active_section));

        let focus_handle = cx.focus_handle();
        focus_handle.focus(window);

        let (active_section_entity, section_subscription) =
            Self::new_section_entity(active_section, app_state.clone(), window, cx);
        let active_section_view = active_section_entity.as_view();

        Self {
            app_state,
            sidebar_tree,
            focus_area: SettingsFocus::Sidebar,
            focus_handle,
            active_section,
            active_section_entity,
            active_section_view,
            pending_section_confirm: None,
            _section_subscription: section_subscription,
        }
    }

    fn new_section_entity(
        section_id: SettingsSectionId,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (ActiveSettingsSection, Option<Subscription>) {
        match section_id {
            SettingsSectionId::General => (
                ActiveSettingsSection::General(
                    cx.new(|cx| GeneralSection::new(app_state, window, cx)),
                ),
                None,
            ),
            SettingsSectionId::Keybindings => (
                ActiveSettingsSection::Keybindings(
                    cx.new(|cx| KeybindingsSection::new(window, cx)),
                ),
                None,
            ),
            SettingsSectionId::Proxies => (
                ActiveSettingsSection::Proxies(
                    cx.new(|cx| ProxiesSection::new(app_state, window, cx)),
                ),
                None,
            ),
            SettingsSectionId::AuthProfiles => (
                ActiveSettingsSection::AuthProfiles(
                    cx.new(|cx| AuthProfilesSection::new(app_state, window, cx)),
                ),
                None,
            ),
            SettingsSectionId::SshTunnels => (
                ActiveSettingsSection::SshTunnels(
                    cx.new(|cx| SshTunnelsSection::new(app_state, window, cx)),
                ),
                None,
            ),
            SettingsSectionId::Services => (
                ActiveSettingsSection::Services(cx.new(|cx| ServicesSection::new(window, cx))),
                None,
            ),
            SettingsSectionId::Hooks => {
                let section = cx.new(|cx| HooksSection::new(app_state, window, cx));
                let subscription = cx.subscribe(&section, |this, _, event: &SettingsEvent, cx| {
                    cx.emit(event.clone());
                    this.focus_area = SettingsFocus::Content;
                    cx.notify();
                });
                (ActiveSettingsSection::Hooks(section), Some(subscription))
            }
            SettingsSectionId::Drivers => (
                ActiveSettingsSection::Drivers(
                    cx.new(|cx| DriversSection::new(app_state, window, cx)),
                ),
                None,
            ),
            _ => (
                ActiveSettingsSection::Empty(cx.new(|_cx| EmptySettingsSection::new(section_id))),
                None,
            ),
        }
    }

    pub(super) fn set_active_section(
        &mut self,
        section: SettingsSectionId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.active_section == section {
            return;
        }

        self.active_section_entity.focus_out(window, cx);
        self.active_section = section;
        let (next_section_entity, section_subscription) =
            Self::new_section_entity(section, self.app_state.clone(), window, cx);
        self.active_section_entity = next_section_entity;
        self.active_section_view = self.active_section_entity.as_view();
        self._section_subscription = section_subscription;

        if self.focus_area == SettingsFocus::Content {
            self.active_section_entity.focus_in(window, cx);
        }

        self.sidebar_tree
            .select_by_id(Self::tree_id_for_section(section));
        self.pending_section_confirm = None;
    }

    pub(super) fn request_section_transition(
        &mut self,
        section: SettingsSectionId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if section == self.active_section {
            self.focus_area = SettingsFocus::Content;
            self.active_section_entity.focus_in(window, cx);
            cx.notify();
            return;
        }

        if self.active_section_entity.is_dirty(cx) {
            self.pending_section_confirm = Some(section);
            cx.notify();
            return;
        }

        self.focus_area = SettingsFocus::Content;
        self.set_active_section(section, window, cx);
        cx.notify();
    }

    pub(super) fn confirm_section_transition(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(section) = self.pending_section_confirm.take() else {
            return;
        };

        self.focus_area = SettingsFocus::Content;
        self.set_active_section(section, window, cx);
        cx.notify();
    }

    pub(super) fn cancel_section_transition(&mut self, cx: &mut Context<Self>) {
        self.pending_section_confirm = None;
        self.sidebar_tree
            .select_by_id(Self::tree_id_for_section(self.active_section));
        cx.notify();
    }

    pub(super) fn try_close(&mut self, window: &mut Window) {
        window.remove_window();
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.pending_section_confirm.is_some() {
            return;
        }

        let chord = KeyChord::from_gpui(&event.keystroke);

        match (chord.key.as_str(), chord.modifiers) {
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                self.try_close(window);
                return;
            }
            ("tab", modifiers)
                if modifiers == Modifiers::none() || modifiers == Modifiers::shift() =>
            {
                self.focus_area = match self.focus_area {
                    SettingsFocus::Sidebar => SettingsFocus::Content,
                    SettingsFocus::Content => SettingsFocus::Sidebar,
                };

                if self.focus_area == SettingsFocus::Content {
                    self.active_section_entity.focus_in(window, cx);
                } else {
                    self.active_section_entity.focus_out(window, cx);
                }

                cx.notify();
                return;
            }
            _ => {}
        }

        if self.focus_area != SettingsFocus::Sidebar {
            self.active_section_entity
                .handle_key_event(event, window, cx);
            return;
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                self.sidebar_tree.move_next();
                cx.notify();
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                self.sidebar_tree.move_prev();
                cx.notify();
            }
            ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                self.activate_sidebar_cursor(window, cx);
            }
            ("right", modifiers) if modifiers == Modifiers::none() => {
                if self.cursor_is_collapsed_group() {
                    self.activate_sidebar_cursor(window, cx);
                } else {
                    self.focus_area = SettingsFocus::Content;
                    self.active_section_entity.focus_in(window, cx);
                    cx.notify();
                }
            }
            ("left", modifiers) if modifiers == Modifiers::none() => {
                if self.cursor_is_expanded_group() {
                    self.activate_sidebar_cursor(window, cx);
                }
            }
            _ => {}
        }
    }

    fn activate_sidebar_cursor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.sidebar_tree.activate() {
            TreeNavAction::Selected(id) => {
                if let Some(section) = Self::section_for_tree_id(id.as_ref()) {
                    self.request_section_transition(section, window, cx);
                }
            }
            TreeNavAction::Toggled { .. } => {
                self.persist_collapse_state();
                cx.notify();
            }
            TreeNavAction::None => {}
        }
    }

    fn cursor_is_collapsed_group(&self) -> bool {
        self.sidebar_tree
            .cursor_item()
            .is_some_and(|row| row.has_children && !row.selectable && !row.expanded)
    }

    fn cursor_is_expanded_group(&self) -> bool {
        self.sidebar_tree
            .cursor_item()
            .is_some_and(|row| row.has_children && !row.selectable && row.expanded)
    }
}
