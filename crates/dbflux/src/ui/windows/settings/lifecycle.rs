use super::*;
use crate::keymap::{KeyChord, Modifiers};
use crate::ui::components::tree_nav::TreeNavAction;
use section_trait::SectionFocusEvent;

impl SettingsCoordinator {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self::new_with_section(app_state, SettingsSectionId::General, window, cx)
    }

    pub fn new_with_section(
        app_state: Entity<AppState>,
        initial_section: SettingsSectionId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let active_section = initial_section;
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
            pending_focus_return: false,
            sidebar_width: SETTINGS_SIDEBAR_DEFAULT_WIDTH,
            sidebar_is_resizing: false,
            sidebar_resize_start_x: None,
            sidebar_resize_start_width: None,
            _subscriptions: section_subscription,
        }
    }

    fn new_section_entity(
        section_id: SettingsSectionId,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> (ActiveSettingsSection, Vec<Subscription>) {
        match section_id {
            SettingsSectionId::General => {
                let section = cx.new(|cx| GeneralSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::General(section), vec![focus_sub])
            }
            SettingsSectionId::Audit => {
                let section = cx.new(|cx| AuditSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::Audit(section), vec![focus_sub])
            }
            SettingsSectionId::Keybindings => (
                ActiveSettingsSection::Keybindings(
                    cx.new(|cx| KeybindingsSection::new(window, cx)),
                ),
                vec![],
            ),
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpClients => {
                let section =
                    cx.new(|cx| McpSection::new(app_state, McpSectionVariant::Clients, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::McpClients(section), vec![focus_sub])
            }
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpRoles => {
                let section =
                    cx.new(|cx| McpSection::new(app_state, McpSectionVariant::Roles, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::McpRoles(section), vec![focus_sub])
            }
            #[cfg(feature = "mcp")]
            SettingsSectionId::McpPolicies => {
                let section = cx
                    .new(|cx| McpSection::new(app_state, McpSectionVariant::Policies, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::McpPolicies(section), vec![focus_sub])
            }

            SettingsSectionId::Proxies => {
                let section = cx.new(|cx| ProxiesSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::Proxies(section), vec![focus_sub])
            }
            SettingsSectionId::AuthProfiles => {
                let section = cx.new(|cx| AuthProfilesSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (
                    ActiveSettingsSection::AuthProfiles(section),
                    vec![focus_sub],
                )
            }
            SettingsSectionId::SshTunnels => {
                let section = cx.new(|cx| SshTunnelsSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::SshTunnels(section), vec![focus_sub])
            }
            SettingsSectionId::Services => {
                let section = cx.new(|cx| ServicesSection::new(app_state, window, cx));
                (ActiveSettingsSection::Services(section), vec![])
            }
            SettingsSectionId::Hooks => {
                let section = cx.new(|cx| HooksSection::new(app_state, window, cx));
                let subscription = cx.subscribe(&section, |this, _, event: &SettingsEvent, cx| {
                    cx.emit(event.clone());
                    this.focus_area = SettingsFocus::Content;
                    cx.notify();
                });
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (
                    ActiveSettingsSection::Hooks(section),
                    vec![subscription, focus_sub],
                )
            }
            SettingsSectionId::Drivers => {
                let section = cx.new(|cx| DriversSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::Drivers(section), vec![focus_sub])
            }
            SettingsSectionId::About => (
                ActiveSettingsSection::About(cx.new(AboutSection::new)),
                vec![],
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
        self._subscriptions = section_subscription;

        if self.focus_area == SettingsFocus::Content {
            self.active_section_entity.focus_in(window, cx);
        }

        self.sidebar_tree
            .select_by_id(Self::tree_id_for_section(section));
        self.pending_section_confirm = None;

        #[cfg(feature = "mcp")]
        self.app_state.update(cx, |state, cx| {
            if let Err(e) = state.persist_mcp_governance() {
                log::error!("Failed to persist MCP governance: {}", e);
            }
            cx.emit(crate::app::McpRuntimeEventRaised {
                event: dbflux_mcp::McpRuntimeEvent::TrustedClientsUpdated,
            });
        });
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
            ("w", modifiers) if modifiers == Modifiers::ctrl() => {
                self.try_close(window);
                return;
            }
            ("q", modifiers) if modifiers == Modifiers::ctrl() => {
                self.try_close(window);
                return;
            }
            ("h", modifiers) if modifiers == Modifiers::ctrl() => {
                if self.focus_area == SettingsFocus::Content {
                    self.focus_area = SettingsFocus::Sidebar;
                    self.active_section_entity.focus_out(window, cx);
                    self.sidebar_tree
                        .select_by_id(Self::tree_id_for_section(self.active_section));
                    cx.notify();
                }
                return;
            }
            ("l", modifiers) if modifiers == Modifiers::ctrl() => {
                if self.focus_area == SettingsFocus::Sidebar {
                    self.focus_area = SettingsFocus::Content;
                    self.active_section_entity.focus_in(window, cx);
                    cx.notify();
                }
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
            ("left", modifiers) if modifiers == Modifiers::none() => {
                self.collapse_sidebar_group(cx);
            }
            ("right", modifiers) if modifiers == Modifiers::none() => {
                self.expand_sidebar_group(cx);
            }
            ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                self.activate_sidebar_cursor(window, cx);
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
                cx.notify();
            }
            TreeNavAction::None => {}
        }
    }

    fn collapse_sidebar_group(&mut self, cx: &mut Context<Self>) {
        let Some(row) = self.sidebar_tree.cursor_item() else {
            return;
        };

        if !row.has_children || row.selectable || !row.expanded {
            return;
        }

        let _ = self.sidebar_tree.activate();
        cx.notify();
    }

    fn expand_sidebar_group(&mut self, cx: &mut Context<Self>) {
        let Some(row) = self.sidebar_tree.cursor_item() else {
            return;
        };

        if !row.has_children || row.selectable || row.expanded {
            return;
        }

        let _ = self.sidebar_tree.activate();
        cx.notify();
    }
}
