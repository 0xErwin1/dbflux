use super::*;

impl SettingsWindow {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();

        let keybindings_filter =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter keybindings..."));

        let input_tunnel_name = cx.new(|cx| InputState::new(window, cx).placeholder("Tunnel name"));
        let input_ssh_host = cx.new(|cx| InputState::new(window, cx).placeholder("hostname"));
        let input_ssh_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("22")
                .default_value("22")
        });
        let input_ssh_user = cx.new(|cx| InputState::new(window, cx).placeholder("username"));
        let input_ssh_key_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("~/.ssh/id_rsa"));
        let input_ssh_key_passphrase = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("passphrase")
                .masked(true)
        });
        let input_ssh_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("password")
                .masked(true)
        });

        // Proxy inputs
        let input_proxy_name = cx.new(|cx| InputState::new(window, cx).placeholder("Proxy name"));
        let input_proxy_host =
            cx.new(|cx| InputState::new(window, cx).placeholder("proxy.example.com"));
        let input_proxy_port = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("8080")
                .default_value("8080")
        });
        let input_proxy_username = cx.new(|cx| InputState::new(window, cx).placeholder("username"));
        let input_proxy_password = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("password")
                .masked(true)
        });
        let input_proxy_no_proxy =
            cx.new(|cx| InputState::new(window, cx).placeholder("localhost, 127.0.0.1, .internal"));

        let subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.editing_tunnel_id = None;
            this.editing_proxy_id = None;
            cx.notify();
        });

        // Start with Global context expanded
        let mut keybindings_expanded = HashSet::new();
        keybindings_expanded.insert(ContextId::Global);

        // General dropdowns
        let gen_settings = app_state.read(cx).general_settings().clone();

        let theme_selected = match gen_settings.theme {
            dbflux_core::ThemeSetting::Dark => 0,
            dbflux_core::ThemeSetting::Light => 1,
        };
        let dropdown_theme = cx.new(|_cx| {
            Dropdown::new("gen-theme")
                .items(vec![DropdownItem::new("Dark"), DropdownItem::new("Light")])
                .selected_index(Some(theme_selected))
        });

        let focus_selected = match gen_settings.default_focus_on_startup {
            dbflux_core::StartupFocus::Sidebar => 0,
            dbflux_core::StartupFocus::LastTab => 1,
        };
        let dropdown_default_focus = cx.new(|_cx| {
            Dropdown::new("gen-default-focus")
                .items(vec![
                    DropdownItem::new("Sidebar"),
                    DropdownItem::new("Last Tab"),
                ])
                .selected_index(Some(focus_selected))
        });

        let refresh_selected = match gen_settings.default_refresh_policy {
            dbflux_core::RefreshPolicySetting::Manual => 0,
            dbflux_core::RefreshPolicySetting::Interval => 1,
        };
        let dropdown_refresh_policy = cx.new(|_cx| {
            Dropdown::new("gen-refresh-policy")
                .items(vec![
                    DropdownItem::new("Manual"),
                    DropdownItem::new("Interval"),
                ])
                .selected_index(Some(refresh_selected))
        });

        let theme_sub = cx.subscribe_in(
            &dropdown_theme,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                this.gen_settings.theme = match event.index {
                    0 => dbflux_core::ThemeSetting::Dark,
                    _ => dbflux_core::ThemeSetting::Light,
                };
                crate::ui::theme::apply_theme(this.gen_settings.theme, Some(window), cx);
                cx.notify();
            },
        );

        let focus_sub = cx.subscribe_in(
            &dropdown_default_focus,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.gen_settings.default_focus_on_startup = match event.index {
                    0 => dbflux_core::StartupFocus::Sidebar,
                    _ => dbflux_core::StartupFocus::LastTab,
                };
                cx.notify();
            },
        );

        let refresh_sub = cx.subscribe_in(
            &dropdown_refresh_policy,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.gen_settings.default_refresh_policy = match event.index {
                    0 => dbflux_core::RefreshPolicySetting::Manual,
                    _ => dbflux_core::RefreshPolicySetting::Interval,
                };
                cx.notify();
            },
        );

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

        // General inputs
        let input_max_history = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("1000");
            s.set_value(gen_settings.max_history_entries.to_string(), window, cx);
            s
        });
        let input_auto_save = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("2000");
            s.set_value(gen_settings.auto_save_interval_ms.to_string(), window, cx);
            s
        });
        let input_refresh_interval = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("5");
            s.set_value(
                gen_settings.default_refresh_interval_secs.to_string(),
                window,
                cx,
            );
            s
        });
        let input_max_bg_tasks = cx.new(|cx| {
            let mut s = InputState::new(window, cx).placeholder("8");
            s.set_value(
                gen_settings.max_concurrent_background_tasks.to_string(),
                window,
                cx,
            );
            s
        });

        // Services inputs
        let input_socket_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("my-driver.sock"));
        let input_svc_command =
            cx.new(|cx| InputState::new(window, cx).placeholder("dbflux-driver-host"));
        let input_svc_timeout = cx.new(|cx| InputState::new(window, cx).placeholder("5000"));

        let input_hook_id = cx.new(|cx| InputState::new(window, cx).placeholder("hook-id"));
        let input_hook_command = cx.new(|cx| InputState::new(window, cx).placeholder("command"));
        let input_hook_args = cx.new(|cx| InputState::new(window, cx).placeholder("arg1 arg2 ..."));
        let input_hook_cwd =
            cx.new(|cx| InputState::new(window, cx).placeholder("/path/to/working-dir"));
        let input_hook_env =
            cx.new(|cx| InputState::new(window, cx).placeholder("KEY=value, OTHER=value"));
        let input_hook_timeout = cx.new(|cx| InputState::new(window, cx).placeholder("30000"));

        let hook_failure_dropdown = cx.new(|_cx| {
            Dropdown::new("hook-failure-mode")
                .items(vec![
                    DropdownItem::with_value("Disconnect", "disconnect"),
                    DropdownItem::with_value("Warn", "warn"),
                    DropdownItem::with_value("Ignore", "ignore"),
                ])
                .selected_index(Some(0))
        });

        let (drv_overrides, drv_settings) = {
            let state = app_state.read(cx);
            (
                state.driver_overrides().clone(),
                state.driver_settings().clone(),
            )
        };

        let hook_definitions = app_state.read(cx).hook_definitions().clone();

        // Focus the window on creation
        focus_handle.focus(window);

        let mut this = Self {
            app_state,
            active_section: SettingsSection::General,
            focus_area: SettingsFocus::Sidebar,
            focus_handle,

            sidebar_tree: Self::build_sidebar_tree(),

            keybindings_filter,
            keybindings_expanded,
            keybindings_selection: KeybindingsSelection::Context(0),
            keybindings_editing_filter: false,
            keybindings_scroll_handle: ScrollHandle::new(),
            keybindings_pending_scroll: None,

            editing_tunnel_id: None,
            input_tunnel_name,
            input_ssh_host,
            input_ssh_port,
            input_ssh_user,
            input_ssh_key_path,
            input_ssh_key_passphrase,
            input_ssh_password,
            ssh_auth_method: SshAuthSelection::PrivateKey,
            form_save_secret: true,
            show_ssh_passphrase: false,
            show_ssh_password: false,

            ssh_focus: SshFocus::ProfileList,
            ssh_selected_idx: None,
            ssh_form_field: SshFormField::Name,
            ssh_editing_field: false,

            ssh_test_status: SshTestStatus::None,
            ssh_test_error: None,

            pending_ssh_key_path: None,
            pending_delete_tunnel_id: None,

            editing_proxy_id: None,
            input_proxy_name,
            input_proxy_host,
            input_proxy_port,
            input_proxy_username,
            input_proxy_password,
            input_proxy_no_proxy,
            proxy_kind: dbflux_core::ProxyKind::Http,
            proxy_auth_selection: ProxyAuthSelection::None,
            proxy_save_secret: false,
            proxy_enabled: true,
            show_proxy_password: false,

            proxy_focus: ProxyFocus::ProfileList,
            proxy_selected_idx: None,
            proxy_form_field: ProxyFormField::Name,
            proxy_editing_field: false,
            pending_delete_proxy_id: None,

            svc_services: Vec::new(),
            svc_config_store: None,
            svc_focus: ServiceFocus::List,
            svc_selected_idx: None,
            svc_form_cursor: 0,
            svc_env_col: 0,
            svc_editing_field: false,
            input_socket_id,
            input_svc_command,
            input_svc_timeout,
            svc_enabled: true,
            svc_arg_inputs: Vec::new(),
            svc_env_key_inputs: Vec::new(),
            svc_env_value_inputs: Vec::new(),
            editing_svc_idx: None,
            pending_delete_svc_idx: None,

            hook_definitions,
            hook_selected_id: None,
            editing_hook_id: None,
            pending_delete_hook_id: None,
            input_hook_id,
            input_hook_command,
            input_hook_args,
            input_hook_cwd,
            input_hook_env,
            input_hook_timeout,
            hook_enabled: true,
            hook_inherit_env: true,
            hook_failure_dropdown,

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

            gen_settings,
            gen_form_cursor: 0,
            gen_editing_field: false,
            dropdown_theme,
            dropdown_default_focus,
            dropdown_refresh_policy,
            input_max_history,
            input_auto_save,
            input_refresh_interval,
            input_max_bg_tasks,

            pending_close_confirm: false,

            _subscriptions: vec![
                subscription,
                theme_sub,
                focus_sub,
                refresh_sub,
                drv_refresh_dropdown_sub,
                drv_refresh_input_sub,
                drv_confirm_dangerous_sub,
                drv_requires_where_sub,
                drv_requires_preview_sub,
            ],
        };

        this.load_services();
        this.drv_load_entries(window, cx);

        let entity = cx.entity().clone();
        window.on_window_should_close(cx, move |_window, cx| {
            let has_changes = entity.read(cx).has_unsaved_changes(cx);
            if has_changes {
                entity.update(cx, |this, cx| {
                    this.pending_close_confirm = true;
                    cx.notify();
                });
                false
            } else {
                true
            }
        });

        this
    }

    pub(super) fn try_close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_unsaved_changes(cx) {
            self.pending_close_confirm = true;
            cx.notify();
        } else {
            window.remove_window();
        }
    }

    pub(super) fn save_all_and_close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_unsaved_general_changes(cx) {
            self.save_general_settings(window, cx);

            // save_general_settings returns early on validation failure — check
            // whether it's still dirty to detect that case.
            if self.has_unsaved_general_changes(cx) {
                return;
            }
        }

        if self.has_unsaved_ssh_changes(cx) {
            self.save_tunnel(window, cx);
            if self.has_unsaved_ssh_changes(cx) {
                return;
            }
        }

        if self.has_unsaved_svc_changes(cx) {
            self.save_service(window, cx);
            if self.has_unsaved_svc_changes(cx) {
                return;
            }
        }

        if self.has_unsaved_hook_changes(cx) {
            self.save_hook(window, cx);
            if self.has_unsaved_hook_changes(cx) {
                return;
            }
        }

        if self.has_unsaved_driver_changes(cx) {
            self.save_driver_settings(window, cx);
            if self.has_unsaved_driver_changes(cx) {
                return;
            }
        }

        window.remove_window();
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let chord = KeyChord::from_gpui(&event.keystroke);

        if self.keybindings_editing_filter {
            if chord.key == "escape" && chord.modifiers == Modifiers::none() {
                self.keybindings_editing_filter = false;
                self.focus_handle.focus(window);
                cx.notify();
            }
            return;
        }

        if self.pending_delete_tunnel_id.is_some()
            || self.pending_delete_proxy_id.is_some()
            || self.pending_delete_svc_idx.is_some()
            || self.pending_delete_hook_id.is_some()
            || self.pending_close_confirm
        {
            return;
        }

        // General: editing input mode
        if self.active_section == SettingsSection::General && self.gen_editing_field {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    self.gen_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    self.gen_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.gen_editing_field = false;
                    self.focus_handle.focus(window);
                    self.gen_move_up();
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        // General: form navigation
        if self.active_section == SettingsSection::General
            && self.focus_area == SettingsFocus::Content
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("j", m) | ("down", m) if m == Modifiers::none() => {
                    self.gen_move_down();
                    cx.notify();
                    return;
                }
                ("k", m) | ("up", m) if m == Modifiers::none() => {
                    self.gen_move_up();
                    cx.notify();
                    return;
                }
                ("enter", m) | ("space", m) if m == Modifiers::none() => {
                    self.gen_activate_current_field(window, cx);
                    cx.notify();
                    return;
                }
                ("h", m) | ("left", m) if m == Modifiers::none() => {
                    self.focus_sidebar();
                    cx.notify();
                    return;
                }
                ("escape", m) if m == Modifiers::none() => {
                    self.focus_sidebar();
                    cx.notify();
                    return;
                }
                ("g", m) if m == Modifiers::none() => {
                    self.gen_form_cursor = 0;
                    cx.notify();
                    return;
                }
                ("g", m) if m == Modifiers::shift() => {
                    let count = self.gen_form_rows().len();
                    if count > 0 {
                        self.gen_form_cursor = count - 1;
                    }
                    cx.notify();
                    return;
                }
                _ => {}
            }
        }

        // Services: editing input mode
        if self.active_section == SettingsSection::Services
            && self.svc_focus == ServiceFocus::Form
            && self.svc_editing_field
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    self.svc_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    self.svc_tab_next();
                    self.svc_focus_current_field(window, cx);
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.svc_editing_field = false;
                    self.focus_handle.focus(window);
                    self.svc_tab_prev();
                    self.svc_focus_current_field(window, cx);
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        // Services: list and form navigation
        if self.active_section == SettingsSection::Services
            && self.focus_area == SettingsFocus::Content
        {
            match self.svc_focus {
                ServiceFocus::List => match (chord.key.as_str(), chord.modifiers) {
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.svc_move_next_profile();
                        self.svc_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.svc_move_prev_profile();
                        self.svc_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) | ("enter", m) if m == Modifiers::none() => {
                        self.svc_enter_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("d", m) if m == Modifiers::none() => {
                        if let Some(idx) = self.svc_selected_idx {
                            self.request_delete_service(idx, cx);
                        }
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.svc_selected_idx = None;
                        self.svc_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        if !self.svc_services.is_empty() {
                            self.svc_selected_idx = Some(self.svc_services.len() - 1);
                            self.svc_load_selected_profile(window, cx);
                        }
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.focus_sidebar();
                        cx.notify();
                        return;
                    }
                    ("escape", m) if m == Modifiers::none() => {
                        self.focus_sidebar();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
                ServiceFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                    ("escape", m) if m == Modifiers::none() => {
                        self.svc_exit_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.svc_move_down();
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.svc_move_up();
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.svc_move_left();
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) if m == Modifiers::none() => {
                        self.svc_move_right();
                        cx.notify();
                        return;
                    }
                    ("enter", m) | ("space", m) if m == Modifiers::none() => {
                        self.svc_activate_current_field(window, cx);
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::none() => {
                        self.svc_tab_next();
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::shift() => {
                        self.svc_tab_prev();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.svc_move_first();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        self.svc_move_last();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
            }
        }

        if self.active_section == SettingsSection::Drivers
            && self.focus_area == SettingsFocus::Content
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("j", m) | ("down", m) if m == Modifiers::none() => {
                    if let Some(current) = self.drv_selected_idx
                        && current + 1 < self.drv_entries.len()
                    {
                        self.drv_select_driver(current + 1, window, cx);
                    }
                    return;
                }
                ("k", m) | ("up", m) if m == Modifiers::none() => {
                    if let Some(current) = self.drv_selected_idx
                        && current > 0
                    {
                        self.drv_select_driver(current - 1, window, cx);
                    }
                    return;
                }
                ("h", m) | ("left", m) if m == Modifiers::none() => {
                    self.focus_sidebar();
                    cx.notify();
                    return;
                }
                ("escape", m) if m == Modifiers::none() => {
                    self.focus_sidebar();
                    cx.notify();
                    return;
                }
                _ => {}
            }
        }

        // Proxy: editing input mode
        if self.active_section == SettingsSection::Proxies
            && self.proxy_focus == ProxyFocus::Form
            && self.proxy_editing_field
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.proxy_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.proxy_editing_field = false;
                    self.focus_handle.focus(window);
                    self.proxy_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.proxy_editing_field = false;
                    self.focus_handle.focus(window);
                    self.proxy_tab_next();
                    self.proxy_focus_current_field(window, cx);
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.proxy_editing_field = false;
                    self.focus_handle.focus(window);
                    self.proxy_tab_prev();
                    self.proxy_focus_current_field(window, cx);
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        // Proxy: list and form navigation
        if self.active_section == SettingsSection::Proxies
            && self.focus_area == SettingsFocus::Content
        {
            match self.proxy_focus {
                ProxyFocus::ProfileList => match (chord.key.as_str(), chord.modifiers) {
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.proxy_move_next_profile(cx);
                        self.proxy_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.proxy_move_prev_profile(cx);
                        self.proxy_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) | ("enter", m) if m == Modifiers::none() => {
                        self.proxy_enter_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("d", m) if m == Modifiers::none() => {
                        if let Some(idx) = self.proxy_selected_idx {
                            let proxies = self.app_state.read(cx).proxies().to_vec();
                            if let Some(proxy) = proxies.get(idx) {
                                self.request_delete_proxy(proxy.id, cx);
                            }
                        }
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.proxy_selected_idx = None;
                        self.proxy_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        let count = self.proxy_count(cx);
                        if count > 0 {
                            self.proxy_selected_idx = Some(count - 1);
                            self.proxy_load_selected_profile(window, cx);
                        }
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.focus_sidebar();
                        cx.notify();
                        return;
                    }
                    ("escape", m) if m == Modifiers::none() => {
                        self.focus_sidebar();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
                ProxyFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                    ("escape", m) if m == Modifiers::none() => {
                        self.proxy_exit_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.proxy_move_down();
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.proxy_move_up();
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.proxy_move_left();
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) if m == Modifiers::none() => {
                        self.proxy_move_right();
                        cx.notify();
                        return;
                    }
                    ("enter", m) | ("space", m) if m == Modifiers::none() => {
                        self.proxy_activate_current_field(window, cx);
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::none() => {
                        self.proxy_tab_next();
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::shift() => {
                        self.proxy_tab_prev();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.proxy_move_first();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        self.proxy_move_last();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
            }
        }

        // SSH: editing input mode
        if self.active_section == SettingsSection::SshTunnels
            && self.ssh_focus == SshFocus::Form
            && self.ssh_editing_field
        {
            match (chord.key.as_str(), chord.modifiers) {
                ("escape", m) if m == Modifiers::none() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    cx.notify();
                }
                ("enter", m) if m == Modifiers::none() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    self.ssh_move_down();
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::none() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    self.ssh_tab_next();
                    self.ssh_focus_current_field(window, cx);
                    cx.notify();
                }
                ("tab", m) if m == Modifiers::shift() => {
                    self.ssh_editing_field = false;
                    self.focus_handle.focus(window);
                    self.ssh_tab_prev();
                    self.ssh_focus_current_field(window, cx);
                    cx.notify();
                }
                _ => {}
            }
            return;
        }

        if self.active_section == SettingsSection::SshTunnels
            && self.focus_area == SettingsFocus::Content
        {
            match self.ssh_focus {
                SshFocus::ProfileList => match (chord.key.as_str(), chord.modifiers) {
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.ssh_move_next_profile(cx);
                        self.ssh_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.ssh_move_prev_profile(cx);
                        self.ssh_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) | ("enter", m) if m == Modifiers::none() => {
                        self.ssh_enter_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("d", m) if m == Modifiers::none() => {
                        if let Some(idx) = self.ssh_selected_idx {
                            let tunnels = {
                                let state = self.app_state.read(cx);
                                state.ssh_tunnels().to_vec()
                            };
                            if let Some(tunnel) = tunnels.get(idx) {
                                self.request_delete_tunnel(tunnel.id, cx);
                            }
                        }
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.ssh_selected_idx = None;
                        self.ssh_load_selected_profile(window, cx);
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        let count = self.ssh_tunnel_count(cx);
                        if count > 0 {
                            self.ssh_selected_idx = Some(count - 1);
                            self.ssh_load_selected_profile(window, cx);
                        }
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.focus_sidebar();
                        cx.notify();
                        return;
                    }
                    ("escape", m) if m == Modifiers::none() => {
                        self.focus_sidebar();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
                SshFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                    ("escape", m) if m == Modifiers::none() => {
                        self.ssh_exit_form(window, cx);
                        cx.notify();
                        return;
                    }
                    ("j", m) | ("down", m) if m == Modifiers::none() => {
                        self.ssh_move_down();
                        cx.notify();
                        return;
                    }
                    ("k", m) | ("up", m) if m == Modifiers::none() => {
                        self.ssh_move_up();
                        cx.notify();
                        return;
                    }
                    ("h", m) | ("left", m) if m == Modifiers::none() => {
                        self.ssh_move_left();
                        cx.notify();
                        return;
                    }
                    ("l", m) | ("right", m) if m == Modifiers::none() => {
                        self.ssh_move_right();
                        cx.notify();
                        return;
                    }
                    ("enter", m) | ("space", m) if m == Modifiers::none() => {
                        self.ssh_activate_current_field(window, cx);
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::none() => {
                        self.ssh_tab_next();
                        cx.notify();
                        return;
                    }
                    ("tab", m) if m == Modifiers::shift() => {
                        self.ssh_tab_prev();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::none() => {
                        self.ssh_move_first();
                        cx.notify();
                        return;
                    }
                    ("g", m) if m == Modifiers::shift() => {
                        self.ssh_move_last();
                        cx.notify();
                        return;
                    }
                    _ => {}
                },
            }
        }

        match (chord.key.as_str(), chord.modifiers) {
            ("h", m) | ("left", m) if m == Modifiers::none() => {
                self.focus_sidebar();
                cx.notify();
            }
            ("l", m) | ("right", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Sidebar {
                    if let Some(row) = self.sidebar_tree.cursor_item() {
                        if row.has_children && !row.expanded {
                            let action = self.sidebar_tree.activate();
                            if let TreeNavAction::Toggled { .. } = action {
                                self.persist_collapse_state();
                            }
                        } else if row.selectable {
                            self.focus_area = SettingsFocus::Content;
                        }
                    }
                } else {
                    self.focus_area = SettingsFocus::Content;
                }
                cx.notify();
            }
            ("j", m) | ("down", m) if m == Modifiers::none() => {
                match self.focus_area {
                    SettingsFocus::Sidebar => {
                        self.sidebar_tree.move_next();
                        self.sync_active_section_from_cursor();
                    }
                    SettingsFocus::Content => {
                        if self.active_section == SettingsSection::Keybindings {
                            self.keybindings_move_next(cx);
                            self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                        }
                    }
                }
                cx.notify();
            }
            ("k", m) | ("up", m) if m == Modifiers::none() => {
                match self.focus_area {
                    SettingsFocus::Sidebar => {
                        self.sidebar_tree.move_prev();
                        self.sync_active_section_from_cursor();
                    }
                    SettingsFocus::Content => {
                        if self.active_section == SettingsSection::Keybindings {
                            self.keybindings_move_prev(cx);
                            self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                        }
                    }
                }
                cx.notify();
            }
            ("g", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Content
                    && self.active_section == SettingsSection::Keybindings
                {
                    let first = self.first_visible_context(cx);
                    self.keybindings_selection = KeybindingsSelection::Context(first);
                    self.keybindings_pending_scroll = Some(0);
                    cx.notify();
                }
            }
            ("g", m) if m == Modifiers::shift() => {
                if self.focus_area == SettingsFocus::Content
                    && self.active_section == SettingsSection::Keybindings
                {
                    let last = self.last_visible_context(cx);
                    let binding_count = self.get_visible_binding_count(last, cx);
                    if binding_count > 0 {
                        self.keybindings_selection =
                            KeybindingsSelection::Binding(last, binding_count - 1);
                    } else {
                        self.keybindings_selection = KeybindingsSelection::Context(last);
                    }
                    self.keybindings_pending_scroll = Some(self.keybindings_flat_index(cx));
                    cx.notify();
                }
            }
            ("enter", m) | ("space", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Sidebar {
                    let action = self.sidebar_tree.activate();

                    match action {
                        TreeNavAction::Selected(id) => {
                            if let Some(section) = Self::section_for_tree_id(id.as_ref()) {
                                self.active_section = section;
                                self.focus_area = SettingsFocus::Content;
                            }
                        }
                        TreeNavAction::Toggled { .. } => {
                            self.persist_collapse_state();
                        }
                        TreeNavAction::None => {}
                    }
                } else if self.active_section == SettingsSection::Keybindings
                    && let KeybindingsSelection::Context(ctx_idx) = self.keybindings_selection
                    && let Some(context) = ContextId::all_variants().get(ctx_idx)
                {
                    if self.keybindings_expanded.contains(context) {
                        self.keybindings_expanded.remove(context);
                    } else {
                        self.keybindings_expanded.insert(*context);
                    }
                }
                cx.notify();
            }
            ("/", m) | ("f", m) if m == Modifiers::none() => {
                if self.active_section == SettingsSection::Keybindings {
                    self.keybindings_editing_filter = true;
                    self.keybindings_filter.update(cx, |state, cx| {
                        state.focus(window, cx);
                    });
                    cx.notify();
                }
            }
            ("escape", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Content {
                    self.focus_sidebar();
                    cx.notify();
                }
            }

            _ => {}
        }
    }
}
