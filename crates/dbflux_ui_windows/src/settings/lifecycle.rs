use super::*;
use auth_profiles_section::AuthProfilesSectionEvent;
use dbflux_app::keymap::{Command, ContextId, Modifiers};
use dbflux_components::components::tree_nav::TreeNavAction;
use dbflux_ui_base::keymap::{SETTINGS_WINDOW_KEY_CONTEXT, key_chord_from_gpui, root_key_context};
#[cfg(feature = "mcp")]
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use section_trait::{SectionFocusEvent, SectionPortabilityEvent};

impl SettingsCoordinator {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::new_with_section(app_state, SettingsSectionId::General, window, cx)
    }

    pub fn new_with_section(
        app_state: Entity<AppStateEntity>,
        initial_section: SettingsSectionId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let active_section = initial_section;
        let mut sidebar_tree = Self::build_sidebar_tree("");
        sidebar_tree.select_by_id(Self::tree_id_for_section(active_section));

        let focus_handle = cx.focus_handle();
        focus_handle.focus(window, cx);

        let (active_section_entity, section_subscription) =
            Self::new_section_entity(active_section, app_state.clone(), window, cx);
        let active_section_view = active_section_entity.as_view();

        let export_modal = cx.new(|cx| ExportBundleModal::new(app_state.clone(), window, cx));
        let import_panel = cx.new(|cx| ImportConnectionsPanel::new(app_state.clone(), window, cx));

        let nav_search = cx.new(|cx| {
            dbflux_components::controls::InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("settings.nav.search_placeholder"))
        });
        let nav_search_sub = cx.subscribe_in(
            &nav_search,
            window,
            |this, _, event: &dbflux_components::controls::InputEvent, window, cx| match event {
                dbflux_components::controls::InputEvent::Change => {
                    this.apply_nav_search(cx);
                }
                dbflux_components::controls::InputEvent::PressEnter { .. } => {
                    this.focus_handle.focus(window, cx);
                    this.activate_sidebar_cursor(window, cx);
                }
                _ => {}
            },
        );

        let import_sub = cx.subscribe(
            &import_panel,
            |this, _, event: &ImportConnectionsPanelEvent, cx| match event {
                ImportConnectionsPanelEvent::Cancelled | ImportConnectionsPanelEvent::Completed => {
                    this.import_visible = false;
                    cx.notify();
                }
            },
        );

        Self {
            app_state,
            sidebar_tree,
            nav_search,
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
            export_modal,
            import_panel,
            import_visible: false,
            pending_export_target: None,
            pending_import_open: false,
            _subscriptions: section_subscription,
            _portability_subscriptions: vec![import_sub, nav_search_sub],
        }
    }

    /// Subscribe to a profile section's portability events, deferring the actual
    /// overlay open to `render` (where a `Window` is in scope).
    fn subscribe_portability<S>(section: &Entity<S>, cx: &mut Context<Self>) -> Subscription
    where
        S: EventEmitter<SectionPortabilityEvent> + 'static,
    {
        cx.subscribe(section, |this, _, event: &SectionPortabilityEvent, cx| {
            match event {
                SectionPortabilityEvent::OpenExport(target) => {
                    this.pending_export_target = Some(*target);
                }
                SectionPortabilityEvent::OpenImport => {
                    this.pending_import_open = true;
                }
            }
            cx.notify();
        })
    }

    fn new_section_entity(
        section_id: SettingsSectionId,
        app_state: Entity<AppStateEntity>,
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
            SettingsSectionId::Keybindings => {
                let section = cx.new(|cx| KeybindingsSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::Keybindings(section), vec![focus_sub])
            }
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
                let portability_sub = Self::subscribe_portability(&section, cx);
                (
                    ActiveSettingsSection::Proxies(section),
                    vec![focus_sub, portability_sub],
                )
            }
            SettingsSectionId::AuthProfiles => {
                let section = cx.new(|cx| AuthProfilesSection::new(app_state, window, cx));

                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });

                // Forward login-URL events from the auth profiles section up to
                // the workspace, which owns the login modal.
                let login_sub =
                    cx.subscribe(&section, |this, _, event: &AuthProfilesSectionEvent, cx| {
                        match event {
                            AuthProfilesSectionEvent::OpenLoginModal {
                                provider_name,
                                profile_name,
                                url,
                            } => {
                                cx.emit(SettingsEvent::OpenLoginModal {
                                    provider_name: provider_name.clone(),
                                    profile_name: profile_name.clone(),
                                    url: url.clone(),
                                });
                                let _ = this;
                            }
                        }
                    });

                let portability_sub = Self::subscribe_portability(&section, cx);
                (
                    ActiveSettingsSection::AuthProfiles(section),
                    vec![focus_sub, login_sub, portability_sub],
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
                let portability_sub = Self::subscribe_portability(&section, cx);
                (
                    ActiveSettingsSection::SshTunnels(section),
                    vec![focus_sub, portability_sub],
                )
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
            SettingsSectionId::Updates => {
                let section = cx.new(|cx| UpdatesSection::new(app_state, window, cx));
                let focus_sub = cx.subscribe(&section, |this, _, event: &SectionFocusEvent, cx| {
                    if matches!(event, SectionFocusEvent::RequestFocusReturn) {
                        this.pending_focus_return = true;
                        cx.notify();
                    }
                });
                (ActiveSettingsSection::Updates(section), vec![focus_sub])
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
                report_error(
                    UserFacingError::new(
                        ErrorKind::Config,
                        dbflux_i18n::t!("settings.mcp_governance.persist_error", error = e),
                    ),
                    cx,
                );
            }
            cx.emit(dbflux_ui_base::McpRuntimeEventRaised {
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

    fn dispatch_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.pending_section_confirm.is_some() {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        if self.nav_search_focused(window, cx) {
            self.handle_nav_search_key(&chord, window, cx);
            return;
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
            ("/", modifiers) if modifiers == Modifiers::none() => {
                self.focus_nav_search(window, cx);
            }
            _ => {}
        }
    }

    /// The key context of the settings window root: `SettingsWindow`, the
    /// `Settings` context of the window-level keys, `FormNavigation` for the
    /// navigation and section keys, the active section and whether the
    /// navigation or the section has the keyboard.
    pub(super) fn root_key_context(&self) -> gpui::KeyContext {
        let focus = match self.focus_area {
            SettingsFocus::Sidebar => "navigation",
            SettingsFocus::Content => "section",
        };

        let mut key_context = root_key_context(
            SETTINGS_WINDOW_KEY_CONTEXT,
            ContextId::FormNavigation,
            &[
                (
                    "section".into(),
                    Self::tree_id_for_section(self.active_section).into(),
                ),
                ("focus".into(), focus.into()),
            ],
        );
        key_context.add(ContextId::Settings.as_gpui_context());
        key_context
    }

    /// Runs a keymap command in the settings window: a window command, or a
    /// FormNavigation command, which reaches the navigation or the active
    /// section as the key their handlers take for it (see
    /// [`form_navigation_key`]). Returns whether the command applied.
    pub(super) fn handle_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.handle_window_command(command, window, cx) {
            return true;
        }

        let Some(keystroke) = form_navigation_key(command) else {
            return false;
        };

        if self.pending_section_confirm.is_some() {
            return false;
        }

        let event = KeyDownEvent {
            keystroke,
            is_held: false,
            prefer_character_input: false,
        };
        self.dispatch_key_event(&event, window, cx);
        true
    }

    /// A key typed while the settings window root itself holds focus (no
    /// field is being edited). The keys the keymap's FormNavigation context
    /// binds by default arrive as commands instead (see
    /// [`Self::handle_command`]); one the user unbound is ignored here, so
    /// removing it in the keybindings editor really removes it.
    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let root_focused = self.focus_handle.is_focused(window);

        if root_focused && is_form_navigation_default(&key_chord_from_gpui(&event.keystroke)) {
            return;
        }

        self.dispatch_key_event(event, window, cx);
    }

    /// Runs a command of the keymap's `Settings` context. Returns whether it
    /// applied; nothing does while the discard-changes prompt is open.
    pub(super) fn handle_window_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.pending_section_confirm.is_some() {
            return false;
        }

        match command {
            Command::CloseWindow => {
                self.try_close(window);
                true
            }
            Command::SaveQuery => {
                self.active_section_entity.save_from_shortcut(window, cx);
                cx.notify();
                true
            }
            Command::FocusLeft => {
                if self.focus_area == SettingsFocus::Content {
                    self.focus_area = SettingsFocus::Sidebar;
                    self.active_section_entity.focus_out(window, cx);
                    self.sidebar_tree
                        .select_by_id(Self::tree_id_for_section(self.active_section));
                    cx.notify();
                }
                true
            }
            Command::FocusRight => {
                if self.focus_area == SettingsFocus::Sidebar {
                    self.focus_area = SettingsFocus::Content;
                    self.active_section_entity.focus_in(window, cx);
                    cx.notify();
                }
                true
            }
            _ => false,
        }
    }

    fn nav_search_focused(&self, window: &Window, cx: &App) -> bool {
        self.nav_search
            .read(cx)
            .focus_handle(cx)
            .contains_focused(window, cx)
    }

    pub(super) fn focus_nav_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.focus_area == SettingsFocus::Content {
            self.active_section_entity.focus_out(window, cx);
        }

        self.focus_area = SettingsFocus::Sidebar;
        self.nav_search
            .update(cx, |input, cx| input.focus(window, cx));
        cx.notify();
    }

    /// Keys typed while the navigation search holds focus: the input edits
    /// the text itself; Escape and the arrows hand focus back to the tree.
    /// Enter arrives as `InputEvent::PressEnter` and opens the entry under the
    /// tree cursor.
    fn handle_nav_search_key(
        &mut self,
        chord: &dbflux_app::keymap::KeyChord,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match (chord.key.as_str(), chord.modifiers) {
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                self.focus_handle.focus(window, cx);
                cx.notify();
            }
            ("down", modifiers) if modifiers == Modifiers::none() => {
                self.sidebar_tree.move_next();
                self.focus_handle.focus(window, cx);
                cx.notify();
            }
            ("up", modifiers) if modifiers == Modifiers::none() => {
                self.sidebar_tree.move_prev();
                self.focus_handle.focus(window, cx);
                cx.notify();
            }
            _ => {}
        }
    }

    /// Rebuilds the navigation tree for the current search text, keeping the
    /// active section under the cursor when it is still listed, else the
    /// first entry.
    pub(super) fn apply_nav_search(&mut self, cx: &mut Context<Self>) {
        let query = self.nav_search.read(cx).value().to_string();
        self.sidebar_tree = Self::build_sidebar_tree(&query);

        let active_id = Self::tree_id_for_section(self.active_section);
        let active_listed = self
            .sidebar_tree
            .rows()
            .iter()
            .any(|row| row.id.as_ref() == active_id);

        if active_listed {
            self.sidebar_tree.select_by_id(active_id);
        } else if let Some(first_leaf) = self
            .sidebar_tree
            .rows()
            .iter()
            .find(|row| row.selectable)
            .map(|row| row.id.clone())
        {
            self.sidebar_tree.select_by_id(first_leaf.as_ref());
        }

        cx.notify();
    }

    pub(super) fn activate_sidebar_cursor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
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

/// The key the settings navigation and sections handle for a FormNavigation
/// command. Their handlers were written against keys; this is the one place
/// that maps the keymap's commands onto them.
fn form_navigation_key(command: Command) -> Option<gpui::Keystroke> {
    let key = match command {
        Command::SelectNext => "down",
        Command::SelectPrev => "up",
        Command::FocusLeft => "h",
        Command::ColumnLeft => "left",
        Command::FocusRight => "l",
        Command::ColumnRight => "right",
        Command::SelectFirst => "g",
        Command::SelectLast => "shift-g",
        Command::CycleFocusForward => "tab",
        Command::CycleFocusBackward => "shift-tab",
        Command::ExpandCollapse => "space",
        Command::Execute => "enter",
        Command::Cancel => "escape",
        Command::FocusSearch => "/",
        _ => return None,
    };

    gpui::Keystroke::parse(key).ok()
}

/// Whether `chord` is one of the keys the FormNavigation context binds by
/// default, which the settings window only takes as commands.
fn is_form_navigation_default(chord: &dbflux_app::keymap::KeyChord) -> bool {
    dbflux_ui_base::keymap::default_keymap()
        .layer(ContextId::FormNavigation)
        .is_some_and(|layer| {
            layer.ordered_bindings().any(|(keys, command)| {
                keys.is_single() && keys.first() == chord && form_navigation_key(command).is_some()
            })
        })
}
