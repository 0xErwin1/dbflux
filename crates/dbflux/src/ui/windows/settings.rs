use crate::app::{AppState, AppStateChanged};
use crate::keymap::{ContextId, KeyChord, Modifiers, default_keymap};
use dbflux_core::{SshAuthMethod, SshTunnelConfig, SshTunnelProfile};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::dialog::Dialog;
use gpui_component::input::{Input, InputState};
use gpui_component::{Icon, IconName};

use std::collections::HashSet;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq)]
enum SettingsSection {
    Keybindings,
    SshTunnels,
}

#[derive(Clone, Copy, PartialEq)]
enum SettingsFocus {
    Sidebar,
    Content,
}

/// Represents the currently selected item in the keybindings list
#[derive(Clone, Copy, PartialEq, Debug)]
enum KeybindingsSelection {
    /// A context header row (e.g., "Global", "Sidebar")
    Context(usize),
    /// A binding row within an expanded context (context_idx, binding_idx)
    Binding(usize, usize),
}

impl KeybindingsSelection {
    fn context_idx(&self) -> usize {
        match self {
            Self::Context(idx) | Self::Binding(idx, _) => *idx,
        }
    }
}

enum KeybindingsListItem {
    ContextHeader {
        context: ContextId,
        ctx_idx: usize,
        is_expanded: bool,
        is_selected: bool,
        binding_count: usize,
    },
    Binding {
        chord: KeyChord,
        cmd_name: String,
        is_inherited: bool,
        is_selected: bool,
        ctx_idx: usize,
        binding_idx: usize,
    },
}

#[derive(Clone, Copy, PartialEq)]
enum SshAuthSelection {
    PrivateKey,
    Password,
}

pub struct SettingsWindow {
    app_state: Entity<AppState>,
    active_section: SettingsSection,
    focus_area: SettingsFocus,
    focus_handle: FocusHandle,

    // Keybindings section state
    keybindings_filter: Entity<InputState>,
    keybindings_expanded: HashSet<ContextId>,
    keybindings_selection: KeybindingsSelection,
    keybindings_editing_filter: bool,
    keybindings_scroll_handle: ScrollHandle,
    keybindings_pending_scroll: Option<usize>,

    // SSH Tunnels section state
    editing_tunnel_id: Option<Uuid>,
    input_tunnel_name: Entity<InputState>,
    input_ssh_host: Entity<InputState>,
    input_ssh_port: Entity<InputState>,
    input_ssh_user: Entity<InputState>,
    input_ssh_key_path: Entity<InputState>,
    input_ssh_key_passphrase: Entity<InputState>,
    input_ssh_password: Entity<InputState>,
    ssh_auth_method: SshAuthSelection,
    form_save_secret: bool,

    pending_ssh_key_path: Option<String>,
    pending_delete_tunnel_id: Option<Uuid>,
    _subscriptions: Vec<Subscription>,
}

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
        let input_ssh_key_passphrase =
            cx.new(|cx| InputState::new(window, cx).placeholder("passphrase"));
        let input_ssh_password = cx.new(|cx| InputState::new(window, cx).placeholder("password"));

        let subscription = cx.subscribe(&app_state, |this, _app_state, _event, cx| {
            this.editing_tunnel_id = None;
            cx.notify();
        });

        // Start with Global context expanded
        let mut keybindings_expanded = HashSet::new();
        keybindings_expanded.insert(ContextId::Global);

        // Focus the window on creation
        focus_handle.focus(window);

        Self {
            app_state,
            active_section: SettingsSection::Keybindings,
            focus_area: SettingsFocus::Sidebar,
            focus_handle,

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
            form_save_secret: false,
            pending_ssh_key_path: None,
            pending_delete_tunnel_id: None,
            _subscriptions: vec![subscription],
        }
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_tunnel_id = None;
        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_secret = false;

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_port
            .update(cx, |s, cx| s.set_value("22", window, cx));
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_path
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_key_passphrase
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_ssh_password
            .update(cx, |s, cx| s.set_value("", window, cx));

        cx.notify();
    }

    fn edit_tunnel(
        &mut self,
        tunnel: &SshTunnelProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editing_tunnel_id = Some(tunnel.id);

        self.input_tunnel_name
            .update(cx, |s, cx| s.set_value(&tunnel.name, window, cx));
        self.input_ssh_host
            .update(cx, |s, cx| s.set_value(&tunnel.config.host, window, cx));
        self.input_ssh_port.update(cx, |s, cx| {
            s.set_value(tunnel.config.port.to_string(), window, cx)
        });
        self.input_ssh_user
            .update(cx, |s, cx| s.set_value(&tunnel.config.user, window, cx));

        match &tunnel.config.auth_method {
            SshAuthMethod::PrivateKey { key_path } => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                let path_str = key_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                self.input_ssh_key_path
                    .update(cx, |s, cx| s.set_value(&path_str, window, cx));

                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    self.input_ssh_key_passphrase
                        .update(cx, |s, cx| s.set_value(&secret, window, cx));
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(secret) = self.app_state.read(cx).get_ssh_tunnel_secret(tunnel) {
                    self.input_ssh_password
                        .update(cx, |s, cx| s.set_value(&secret, window, cx));
                }
            }
        }

        self.form_save_secret = tunnel.save_secret;
        cx.notify();
    }

    fn save_tunnel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_tunnel_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }

        let host = self.input_ssh_host.read(cx).value().to_string();
        let port = self.input_ssh_port.read(cx).value().parse().unwrap_or(22);
        let user = self.input_ssh_user.read(cx).value().to_string();

        let auth_method = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                let key_path_str = self.input_ssh_key_path.read(cx).value().to_string();
                let key_path = if key_path_str.trim().is_empty() {
                    None
                } else {
                    Some(Self::expand_path(&key_path_str))
                };
                SshAuthMethod::PrivateKey { key_path }
            }
            SshAuthSelection::Password => SshAuthMethod::Password,
        };

        let secret = match self.ssh_auth_method {
            SshAuthSelection::PrivateKey => {
                self.input_ssh_key_passphrase.read(cx).value().to_string()
            }
            SshAuthSelection::Password => self.input_ssh_password.read(cx).value().to_string(),
        };

        let config = SshTunnelConfig {
            host,
            port,
            user,
            auth_method,
        };

        let tunnel = SshTunnelProfile {
            id: self.editing_tunnel_id.unwrap_or_else(Uuid::new_v4),
            name,
            config,
            save_secret: self.form_save_secret,
        };

        let is_edit = self.editing_tunnel_id.is_some();

        self.app_state.update(cx, |state, cx| {
            if tunnel.save_secret && !secret.is_empty() {
                state.save_ssh_tunnel_secret(&tunnel, &secret);
            }

            if is_edit {
                state.update_ssh_tunnel(tunnel);
            } else {
                state.add_ssh_tunnel(tunnel);
            }

            cx.emit(AppStateChanged);
        });

        self.clear_form(window, cx);
    }

    fn request_delete_tunnel(&mut self, tunnel_id: Uuid, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = Some(tunnel_id);
        cx.notify();
    }

    fn confirm_delete_tunnel(&mut self, cx: &mut Context<Self>) {
        let Some(tunnel_id) = self.pending_delete_tunnel_id.take() else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Some(idx) = state.ssh_tunnels.iter().position(|t| t.id == tunnel_id) {
                state.remove_ssh_tunnel(idx);
            }
            cx.emit(AppStateChanged);
        });

        if self.editing_tunnel_id == Some(tunnel_id) {
            self.editing_tunnel_id = None;
        }
        cx.notify();
    }

    fn cancel_delete_tunnel(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_tunnel_id = None;
        cx.notify();
    }

    fn browse_ssh_key(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();

        let start_dir = dirs::home_dir().map(|h| h.join(".ssh")).unwrap_or_default();

        let task = cx.background_executor().spawn(async move {
            let dialog = rfd::FileDialog::new()
                .set_title("Select SSH Private Key")
                .set_directory(&start_dir);

            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let path = task.await;

            if let Some(path) = path {
                cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_ssh_key_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
                .ok();
            }
        })
        .detach();
    }

    fn expand_path(path: &str) -> PathBuf {
        if let Some(rest) = path.strip_prefix("~/")
            && let Some(home) = dirs::home_dir()
        {
            return home.join(rest);
        }
        PathBuf::from(path)
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let active = self.active_section;
        let focused = self.focus_area == SettingsFocus::Sidebar;

        div()
            .w(px(180.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar)
            .flex()
            .flex_col()
            .p_2()
            .gap_1()
            .child(self.render_sidebar_item(
                "section-keybindings",
                "Keybindings",
                SettingsSection::Keybindings,
                active,
                focused && self.sidebar_index_for_section(active) == 0,
                cx,
            ))
            .child(self.render_sidebar_item(
                "section-ssh-tunnels",
                "SSH Tunnels",
                SettingsSection::SshTunnels,
                active,
                focused && self.sidebar_index_for_section(active) == 1,
                cx,
            ))
    }

    fn render_sidebar_item(
        &self,
        id: &'static str,
        label: &'static str,
        section: SettingsSection,
        active: SettingsSection,
        is_focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let is_active = active == section;

        div()
            .id(id)
            .px_3()
            .py_2()
            .rounded(px(4.0))
            .text_sm()
            .cursor_pointer()
            .border_1()
            .border_color(if is_focused && !is_active {
                theme.primary
            } else {
                gpui::transparent_black()
            })
            .when(is_active, |d| {
                d.bg(theme.secondary).font_weight(FontWeight::MEDIUM)
            })
            .hover(|d| d.bg(theme.secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.active_section = section;
                this.focus_area = SettingsFocus::Content;
                cx.notify();
            }))
            .child(label)
    }

    fn sidebar_index_for_section(&self, section: SettingsSection) -> usize {
        match section {
            SettingsSection::Keybindings => 0,
            SettingsSection::SshTunnels => 1,
        }
    }

    fn section_for_sidebar_index(&self, idx: usize) -> SettingsSection {
        match idx {
            0 => SettingsSection::Keybindings,
            1 => SettingsSection::SshTunnels,
            _ => SettingsSection::Keybindings,
        }
    }

    fn sidebar_section_count(&self) -> usize {
        2
    }

    fn render_keybindings_section(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let keymap = default_keymap();
        let filter_text = self.keybindings_filter.read(cx).value().to_lowercase();
        let has_filter = !filter_text.is_empty();

        // Validate selection when filter is active
        if has_filter {
            self.validate_selection_for_filter(cx);
        }

        // Extract theme colors before closures to avoid borrow issues
        let border = theme.border;
        let muted_foreground = theme.muted_foreground;
        let secondary = theme.secondary;

        let current_selection = self.keybindings_selection;
        let is_content_focused =
            self.focus_area == SettingsFocus::Content && !self.keybindings_editing_filter;

        // Flat list required for scroll_to_item to work correctly
        let mut flat_items: Vec<KeybindingsListItem> = Vec::new();

        for (idx, context) in ContextId::all_variants().iter().enumerate() {
            let is_expanded = has_filter || self.keybindings_expanded.contains(context);
            let bindings = keymap.bindings_for_context(*context);

            let filtered_bindings: Vec<_> = if has_filter {
                bindings
                    .iter()
                    .filter(|(chord, cmd, _)| {
                        let chord_str = chord.to_string().to_lowercase();
                        let cmd_name = cmd.display_name().to_lowercase();
                        chord_str.contains(&filter_text) || cmd_name.contains(&filter_text)
                    })
                    .cloned()
                    .collect()
            } else {
                bindings
            };

            // Skip contexts with no matching bindings when filtering
            if has_filter && filtered_bindings.is_empty() {
                continue;
            }

            let is_context_selected = is_content_focused
                && matches!(current_selection, KeybindingsSelection::Context(i) if i == idx);

            // Add context header
            flat_items.push(KeybindingsListItem::ContextHeader {
                context: *context,
                ctx_idx: idx,
                is_expanded,
                is_selected: is_context_selected,
                binding_count: filtered_bindings.len(),
            });

            // Add bindings if expanded
            if is_expanded {
                for (binding_idx, (chord, cmd, source_ctx)) in filtered_bindings.iter().enumerate()
                {
                    let is_inherited = *source_ctx != *context;
                    let is_binding_selected = is_content_focused
                        && matches!(
                            current_selection,
                            KeybindingsSelection::Binding(ci, bi) if ci == idx && bi == binding_idx
                        );

                    flat_items.push(KeybindingsListItem::Binding {
                        chord: chord.clone(),
                        cmd_name: cmd.display_name().to_string(),
                        is_inherited,
                        is_selected: is_binding_selected,
                        ctx_idx: idx,
                        binding_idx,
                    });
                }
            }
        }

        if let Some(scroll_idx) = self.keybindings_pending_scroll.take() {
            self.keybindings_scroll_handle.scroll_to_item(scroll_idx);
        }

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Keyboard Shortcuts"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(muted_foreground)
                            .child("View all keyboard shortcuts by context"),
                    ),
            )
            .child(
                div().p_4().border_b_1().border_color(border).child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            Icon::new(IconName::Search)
                                .size(px(16.0))
                                .text_color(muted_foreground),
                        )
                        .child(
                            div()
                                .flex_1()
                                .child(Input::new(&self.keybindings_filter).small()),
                        ),
                ),
            )
            .child(
                div()
                    .id("keybindings-scroll-container")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.keybindings_scroll_handle)
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_0()
                    .children(flat_items.into_iter().map(|item| match item {
                        KeybindingsListItem::ContextHeader {
                            context,
                            ctx_idx,
                            is_expanded,
                            is_selected,
                            binding_count,
                        } => {
                            let has_parent = context.parent().is_some();
                            let parent_name = context
                                .parent()
                                .map(|p| p.display_name())
                                .unwrap_or("");

                            div()
                                .id(SharedString::from(format!(
                                    "context-{}",
                                    context.as_gpui_context()
                                )))
                                .flex()
                                .items_center()
                                .gap_2()
                                .px_3()
                                .py_2()
                                .mt_1()
                                .rounded(px(4.0))
                                .cursor_pointer()
                                .bg(if is_selected {
                                    secondary
                                } else {
                                    gpui::transparent_black()
                                })
                                .hover(|d| d.bg(secondary))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.keybindings_selection =
                                        KeybindingsSelection::Context(ctx_idx);
                                    this.focus_area = SettingsFocus::Content;

                                    if this.keybindings_expanded.contains(&context) {
                                        this.keybindings_expanded.remove(&context);
                                    } else {
                                        this.keybindings_expanded.insert(context);
                                    }
                                    cx.notify();
                                }))
                                // Chevron icon
                                .child(
                                    div()
                                        .w(px(16.0))
                                        .flex()
                                        .items_center()
                                        .justify_center()
                                        .child(
                                            Icon::new(if is_expanded {
                                                IconName::ChevronDown
                                            } else {
                                                IconName::ChevronRight
                                            })
                                            .size(px(16.0))
                                            .text_color(muted_foreground),
                                        ),
                                )
                                // Context name and bindings count
                                .child(
                                    div()
                                        .flex_1()
                                        .flex()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            div()
                                                .font_weight(FontWeight::MEDIUM)
                                                .child(context.display_name()),
                                        )
                                        .child(
                                            div()
                                                .text_sm()
                                                .text_color(muted_foreground)
                                                .child(format!("({} bindings)", binding_count)),
                                        ),
                                )
                                // Inherits info
                                .when(has_parent, |d| {
                                    d.child(
                                        div()
                                            .text_xs()
                                            .text_color(muted_foreground)
                                            .child(format!("inherits from {}", parent_name)),
                                    )
                                })
                        }

                        KeybindingsListItem::Binding {
                            chord,
                            cmd_name,
                            is_inherited,
                            is_selected,
                            ctx_idx,
                            binding_idx,
                        } => self.render_binding_row(
                            &chord,
                            &cmd_name,
                            is_inherited,
                            is_selected,
                            ctx_idx,
                            binding_idx,
                            muted_foreground,
                            secondary,
                            border,
                            cx,
                        ),
                    })),
            )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_binding_row(
        &self,
        chord: &KeyChord,
        cmd_name: &str,
        is_inherited: bool,
        is_selected: bool,
        ctx_idx: usize,
        binding_idx: usize,
        muted_foreground: Hsla,
        secondary: Hsla,
        border: Hsla,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        div()
            .id(SharedString::from(format!(
                "binding-{}-{}",
                ctx_idx, binding_idx
            )))
            .ml(px(28.0))
            .pl_4()
            .border_l_2()
            .border_color(border)
            .flex()
            .items_center()
            .py_1()
            .px_2()
            .rounded_r(px(4.0))
            .gap_4()
            .cursor_pointer()
            .bg(if is_selected {
                secondary
            } else {
                gpui::transparent_black()
            })
            .hover(|d| d.bg(secondary))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.keybindings_selection = KeybindingsSelection::Binding(ctx_idx, binding_idx);
                this.focus_area = SettingsFocus::Content;
                cx.notify();
            }))
            .child(
                div()
                    .w(px(140.0))
                    .child(self.render_key_badge(chord, muted_foreground, secondary)),
            )
            .child(
                div()
                    .flex_1()
                    .text_sm()
                    .when(is_inherited, |d| d.text_color(muted_foreground))
                    .child(cmd_name.to_string()),
            )
            .when(is_inherited, |d| {
                d.child(
                    div()
                        .text_xs()
                        .text_color(muted_foreground)
                        .px_2()
                        .py(px(2.0))
                        .rounded(px(4.0))
                        .bg(secondary)
                        .child("inherited"),
                )
            })
    }

    fn render_key_badge(
        &self,
        chord: &KeyChord,
        muted_foreground: Hsla,
        secondary: Hsla,
    ) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_1()
            .children(self.chord_to_badges(chord, muted_foreground, secondary))
    }

    fn chord_to_badges(
        &self,
        chord: &KeyChord,
        muted_foreground: Hsla,
        secondary: Hsla,
    ) -> Vec<Div> {
        let mut badges = Vec::new();

        if chord.modifiers.ctrl {
            badges.push(self.render_single_key_badge("Ctrl", muted_foreground, secondary));
        }
        if chord.modifiers.alt {
            badges.push(self.render_single_key_badge("Alt", muted_foreground, secondary));
        }
        if chord.modifiers.shift {
            badges.push(self.render_single_key_badge("Shift", muted_foreground, secondary));
        }
        if chord.modifiers.platform {
            badges.push(self.render_single_key_badge("Cmd", muted_foreground, secondary));
        }

        let key_display = self.format_key(&chord.key);
        badges.push(self.render_single_key_badge(&key_display, muted_foreground, secondary));

        badges
    }

    fn render_single_key_badge(&self, key: &str, muted_foreground: Hsla, secondary: Hsla) -> Div {
        div()
            .px_2()
            .py(px(2.0))
            .rounded(px(4.0))
            .bg(secondary)
            .border_1()
            .border_color(muted_foreground.opacity(0.3))
            .text_xs()
            .font_weight(FontWeight::MEDIUM)
            .child(key.to_string())
    }

    fn format_key(&self, key: &str) -> String {
        match key {
            "down" => "↓".to_string(),
            "up" => "↑".to_string(),
            "left" => "←".to_string(),
            "right" => "→".to_string(),
            "enter" => "Enter".to_string(),
            "escape" => "Esc".to_string(),
            "backspace" => "⌫".to_string(),
            "delete" => "Del".to_string(),
            "tab" => "Tab".to_string(),
            "space" => "Space".to_string(),
            "home" => "Home".to_string(),
            "end" => "End".to_string(),
            "pageup" => "PgUp".to_string(),
            "pagedown" => "PgDn".to_string(),
            _ => key.to_uppercase(),
        }
    }

    fn render_ssh_tunnels_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let tunnels = self.app_state.read(cx).ssh_tunnels.clone();
        let editing_id = self.editing_tunnel_id;
        let keyring_available = self.app_state.read(cx).secret_store_available();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("SSH Tunnels"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Manage reusable SSH tunnel configurations"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_tunnel_list(&tunnels, editing_id, cx))
                    .child(self.render_tunnel_form(editing_id, keyring_available, cx)),
            )
    }

    fn render_tunnel_list(
        &self,
        tunnels: &[SshTunnelProfile],
        editing_id: Option<Uuid>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .w(px(250.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    Button::new("new-tunnel")
                        .icon(Icon::new(IconName::Plus))
                        .label("New Tunnel")
                        .small()
                        .w_full()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.clear_form(window, cx);
                        })),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(tunnels.is_empty(), |d: Div| {
                        d.child(
                            div()
                                .p_4()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No saved tunnels"),
                        )
                    })
                    .children(tunnels.iter().map(|tunnel| {
                        let tunnel_id = tunnel.id;
                        let is_selected = editing_id == Some(tunnel_id);
                        let tunnel_clone = tunnel.clone();

                        div()
                            .id(SharedString::from(format!("tunnel-item-{}", tunnel_id)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .when(is_selected, |d| d.bg(theme.secondary))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.edit_tunnel(&tunnel_clone, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        Icon::new(IconName::SquareTerminal)
                                            .size(px(14.0))
                                            .text_color(theme.muted_foreground)
                                            .mt(px(2.0)),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child(tunnel.name.clone()),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child(format!(
                                                        "{}@{}:{}",
                                                        tunnel.config.user,
                                                        tunnel.config.host,
                                                        tunnel.config.port
                                                    )),
                                            ),
                                    ),
                            )
                    })),
            )
    }

    fn render_tunnel_form(
        &self,
        editing_id: Option<Uuid>,
        keyring_available: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let auth_method = self.ssh_auth_method;
        let save_secret = self.form_save_secret;

        let title = if editing_id.is_some() {
            "Edit Tunnel"
        } else {
            "New Tunnel"
        };

        let auth_selector = self
            .render_auth_selector(auth_method, cx)
            .into_any_element();
        let auth_fields = self
            .render_auth_fields(auth_method, keyring_available, save_secret, cx)
            .into_any_element();

        let theme = cx.theme();

        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div().p_4().border_b_1().border_color(theme.border).child(
                    div()
                        .text_base()
                        .font_weight(FontWeight::MEDIUM)
                        .child(title),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(self.render_form_field("Name", &self.input_tunnel_name, true))
                    .child(
                        div()
                            .flex()
                            .gap_3()
                            .child(div().flex_1().child(self.render_form_field(
                                "Host",
                                &self.input_ssh_host,
                                true,
                            )))
                            .child(div().w(px(80.0)).child(self.render_form_field(
                                "Port",
                                &self.input_ssh_port,
                                false,
                            ))),
                    )
                    .child(self.render_form_field("Username", &self.input_ssh_user, true))
                    .child(auth_selector)
                    .child(auth_fields),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .gap_2()
                    .justify_end()
                    .when(editing_id.is_some(), |d| {
                        let tunnel_id = editing_id.unwrap();
                        d.child(
                            Button::new("delete-tunnel")
                                .label("Delete")
                                .small()
                                .danger()
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.request_delete_tunnel(tunnel_id, cx);
                                })),
                        )
                        .child(div().flex_1())
                    })
                    .child(
                        Button::new("save-tunnel")
                            .label(if editing_id.is_some() {
                                "Update"
                            } else {
                                "Create"
                            })
                            .small()
                            .primary()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_tunnel(window, cx);
                            })),
                    ),
            )
    }

    fn render_form_field(
        &self,
        label: &str,
        input: &Entity<InputState>,
        _required: bool,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(label.to_string()),
            )
            .child(Input::new(input).small())
    }

    fn render_auth_selector(
        &self,
        current: SshAuthSelection,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;
        let border = theme.border;

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Authentication"),
            )
            .child(
                div()
                    .flex()
                    .gap_4()
                    .child(
                        div()
                            .id("auth-key")
                            .flex()
                            .items_center()
                            .gap_2()
                            .cursor_pointer()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::PrivateKey;
                                cx.notify();
                            }))
                            .child(self.render_radio(
                                current == SshAuthSelection::PrivateKey,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Private Key")),
                    )
                    .child(
                        div()
                            .id("auth-pw")
                            .flex()
                            .items_center()
                            .gap_2()
                            .cursor_pointer()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.ssh_auth_method = SshAuthSelection::Password;
                                cx.notify();
                            }))
                            .child(self.render_radio(
                                current == SshAuthSelection::Password,
                                primary,
                                border,
                            ))
                            .child(div().text_sm().child("Password")),
                    ),
            )
    }

    fn render_radio(&self, selected: bool, primary: Hsla, border: Hsla) -> impl IntoElement {
        div()
            .w(px(16.0))
            .h(px(16.0))
            .rounded_full()
            .border_2()
            .border_color(if selected { primary } else { border })
            .when(selected, |d| {
                d.child(
                    div()
                        .absolute()
                        .top(px(3.0))
                        .left(px(3.0))
                        .w(px(6.0))
                        .h(px(6.0))
                        .rounded_full()
                        .bg(primary),
                )
            })
    }

    fn render_auth_fields(
        &self,
        auth_method: SshAuthSelection,
        keyring_available: bool,
        save_secret: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let save_checkbox = if keyring_available {
            Some(
                Checkbox::new("save-secret")
                    .checked(save_secret)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.form_save_secret = *checked;
                        cx.notify();
                    })),
            )
        } else {
            None
        };

        match auth_method {
            SshAuthSelection::PrivateKey => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::MEDIUM)
                                .child("Private Key Path"),
                        )
                        .child(
                            div()
                                .flex()
                                .gap_2()
                                .child(
                                    div()
                                        .flex_1()
                                        .child(Input::new(&self.input_ssh_key_path).small()),
                                )
                                .child(
                                    Button::new("browse-key")
                                        .label("Browse")
                                        .small()
                                        .ghost()
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.browse_ssh_key(window, cx);
                                        })),
                                ),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("Leave empty to use SSH agent"),
                        ),
                )
                .child(
                    div()
                        .flex()
                        .items_end()
                        .gap_3()
                        .child(div().flex_1().child(self.render_form_field(
                            "Key Passphrase",
                            &self.input_ssh_key_passphrase,
                            false,
                        )))
                        .when_some(save_checkbox, |d, checkbox| {
                            d.child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .pb(px(2.0))
                                    .child(checkbox)
                                    .child(div().text_sm().child("Save")),
                            )
                        }),
                )
                .into_any_element(),

            SshAuthSelection::Password => div()
                .flex()
                .flex_col()
                .gap_3()
                .child(
                    div()
                        .flex()
                        .items_end()
                        .gap_3()
                        .child(div().flex_1().child(self.render_form_field(
                            "Password",
                            &self.input_ssh_password,
                            false,
                        )))
                        .when_some(save_checkbox, |d, checkbox| {
                            d.child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .pb(px(2.0))
                                    .child(checkbox)
                                    .child(div().text_sm().child("Save")),
                            )
                        }),
                )
                .into_any_element(),
        }
    }
}

pub struct DismissEvent;

impl EventEmitter<DismissEvent> for SettingsWindow {}

impl SettingsWindow {
    /// Returns the current filter text, lowercased.
    fn get_filter_text(&self, cx: &Context<Self>) -> String {
        self.keybindings_filter.read(cx).value().to_lowercase()
    }

    /// Check if a binding matches the current filter.
    fn binding_matches_filter(chord: &KeyChord, cmd_name: &str, filter: &str) -> bool {
        if filter.is_empty() {
            return true;
        }
        let chord_str = chord.to_string().to_lowercase();
        let cmd_lower = cmd_name.to_lowercase();
        chord_str.contains(filter) || cmd_lower.contains(filter)
    }

    /// Get filtered bindings for a context.
    fn get_filtered_bindings(
        &self,
        context: ContextId,
        filter: &str,
    ) -> Vec<(KeyChord, crate::keymap::Command, ContextId)> {
        let keymap = default_keymap();
        let bindings = keymap.bindings_for_context(context);

        if filter.is_empty() {
            bindings
        } else {
            bindings
                .into_iter()
                .filter(|(chord, cmd, _)| {
                    Self::binding_matches_filter(chord, cmd.display_name(), filter)
                })
                .collect()
        }
    }

    /// Check if a context is visible (has matching bindings when filtering).
    fn is_context_visible(&self, ctx_idx: usize, filter: &str) -> bool {
        if filter.is_empty() {
            return true;
        }
        if let Some(context) = ContextId::all_variants().get(ctx_idx) {
            !self.get_filtered_bindings(*context, filter).is_empty()
        } else {
            false
        }
    }

    /// Check if a context is expanded (always true when filtering).
    fn is_context_expanded(&self, context: &ContextId, has_filter: bool) -> bool {
        has_filter || self.keybindings_expanded.contains(context)
    }

    /// Get the number of visible bindings for a context.
    fn get_visible_binding_count(&self, ctx_idx: usize, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        let has_filter = !filter.is_empty();

        if let Some(context) = ContextId::all_variants().get(ctx_idx) {
            if !self.is_context_expanded(context, has_filter) {
                return 0;
            }
            self.get_filtered_bindings(*context, &filter).len()
        } else {
            0
        }
    }

    fn first_visible_context(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        (0..ContextId::all_variants().len())
            .find(|&idx| self.is_context_visible(idx, &filter))
            .unwrap_or(0)
    }

    fn last_visible_context(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        (0..ContextId::all_variants().len())
            .rev()
            .find(|&idx| self.is_context_visible(idx, &filter))
            .unwrap_or(0)
    }

    fn next_visible_context(&self, after_idx: usize, cx: &Context<Self>) -> Option<usize> {
        let filter = self.get_filter_text(cx);
        ((after_idx + 1)..ContextId::all_variants().len())
            .find(|&idx| self.is_context_visible(idx, &filter))
    }

    fn prev_visible_context(&self, before_idx: usize, cx: &Context<Self>) -> Option<usize> {
        let filter = self.get_filter_text(cx);
        (0..before_idx)
            .rev()
            .find(|&idx| self.is_context_visible(idx, &filter))
    }

    /// Validate and reset selection if it points to a filtered-out item.
    fn validate_selection_for_filter(&mut self, cx: &Context<Self>) {
        let filter = self.get_filter_text(cx);
        if filter.is_empty() {
            return;
        }

        let ctx_idx = self.keybindings_selection.context_idx();

        if !self.is_context_visible(ctx_idx, &filter) {
            self.keybindings_selection =
                KeybindingsSelection::Context(self.first_visible_context(cx));
            return;
        }

        if let KeybindingsSelection::Binding(_, binding_idx) = self.keybindings_selection {
            let visible_count = self.get_visible_binding_count(ctx_idx, cx);
            if binding_idx >= visible_count {
                if visible_count > 0 {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(ctx_idx, visible_count - 1);
                } else {
                    self.keybindings_selection = KeybindingsSelection::Context(ctx_idx);
                }
            }
        }
    }

    fn keybindings_move_next(&mut self, cx: &Context<Self>) {
        let binding_count = self.get_visible_binding_count(
            self.keybindings_selection.context_idx(),
            cx,
        );

        match self.keybindings_selection {
            KeybindingsSelection::Context(ctx_idx) => {
                if binding_count > 0 {
                    self.keybindings_selection = KeybindingsSelection::Binding(ctx_idx, 0);
                } else if let Some(next) = self.next_visible_context(ctx_idx, cx) {
                    self.keybindings_selection = KeybindingsSelection::Context(next);
                }
            }
            KeybindingsSelection::Binding(ctx_idx, binding_idx) => {
                if binding_idx + 1 < binding_count {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(ctx_idx, binding_idx + 1);
                } else if let Some(next) = self.next_visible_context(ctx_idx, cx) {
                    self.keybindings_selection = KeybindingsSelection::Context(next);
                }
            }
        }
    }

    fn keybindings_move_prev(&mut self, cx: &Context<Self>) {
        match self.keybindings_selection {
            KeybindingsSelection::Context(ctx_idx) => {
                if let Some(prev) = self.prev_visible_context(ctx_idx, cx) {
                    let prev_count = self.get_visible_binding_count(prev, cx);
                    if prev_count > 0 {
                        self.keybindings_selection =
                            KeybindingsSelection::Binding(prev, prev_count - 1);
                    } else {
                        self.keybindings_selection = KeybindingsSelection::Context(prev);
                    }
                }
            }
            KeybindingsSelection::Binding(ctx_idx, binding_idx) => {
                if binding_idx > 0 {
                    self.keybindings_selection =
                        KeybindingsSelection::Binding(ctx_idx, binding_idx - 1);
                } else {
                    self.keybindings_selection = KeybindingsSelection::Context(ctx_idx);
                }
            }
        }
    }

    fn keybindings_flat_index(&self, cx: &Context<Self>) -> usize {
        let filter = self.get_filter_text(cx);
        let has_filter = !filter.is_empty();
        let mut flat_idx = 0;

        for (ctx_idx, context) in ContextId::all_variants().iter().enumerate() {
            if !self.is_context_visible(ctx_idx, &filter) {
                continue;
            }

            match self.keybindings_selection {
                KeybindingsSelection::Context(sel) if sel == ctx_idx => return flat_idx,
                KeybindingsSelection::Binding(sel, bi) if sel == ctx_idx => {
                    return flat_idx + 1 + bi;
                }
                _ => {}
            }

            flat_idx += 1;
            if self.is_context_expanded(context, has_filter) {
                flat_idx += self.get_filtered_bindings(*context, &filter).len();
            }
        }
        flat_idx
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let chord = KeyChord::from_gpui(&event.keystroke);

        // Handle filter input mode
        if self.keybindings_editing_filter {
            if chord.key == "escape" && chord.modifiers == Modifiers::none() {
                self.keybindings_editing_filter = false;
                self.focus_handle.focus(window);
                cx.notify();
            }
            return;
        }

        match (chord.key.as_str(), chord.modifiers) {
            // Navigation between sidebar and content
            ("h", m) | ("left", m) if m == Modifiers::none() => {
                self.focus_area = SettingsFocus::Sidebar;
                cx.notify();
            }
            ("l", m) | ("right", m) if m == Modifiers::none() => {
                self.focus_area = SettingsFocus::Content;
                cx.notify();
            }

            // Vertical navigation
            ("j", m) | ("down", m) if m == Modifiers::none() => {
                match self.focus_area {
                    SettingsFocus::Sidebar => {
                        let current_idx = self.sidebar_index_for_section(self.active_section);
                        let next_idx = (current_idx + 1) % self.sidebar_section_count();
                        self.active_section = self.section_for_sidebar_index(next_idx);
                    }
                    SettingsFocus::Content => {
                        if self.active_section == SettingsSection::Keybindings {
                            self.keybindings_move_next(cx);
                            self.keybindings_pending_scroll =
                                Some(self.keybindings_flat_index(cx));
                        }
                    }
                }
                cx.notify();
            }
            ("k", m) | ("up", m) if m == Modifiers::none() => {
                match self.focus_area {
                    SettingsFocus::Sidebar => {
                        let current_idx = self.sidebar_index_for_section(self.active_section);
                        let prev_idx = if current_idx == 0 {
                            self.sidebar_section_count() - 1
                        } else {
                            current_idx - 1
                        };
                        self.active_section = self.section_for_sidebar_index(prev_idx);
                    }
                    SettingsFocus::Content => {
                        if self.active_section == SettingsSection::Keybindings {
                            self.keybindings_move_prev(cx);
                            self.keybindings_pending_scroll =
                                Some(self.keybindings_flat_index(cx));
                        }
                    }
                }
                cx.notify();
            }

            // Go to first (g) / last (G)
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

            // Expand/collapse in keybindings (only when on a context header)
            ("enter", m) | ("space", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Sidebar {
                    self.focus_area = SettingsFocus::Content;
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

            // Focus filter
            ("/", m) | ("f", m) if m == Modifiers::none() => {
                if self.active_section == SettingsSection::Keybindings {
                    self.keybindings_editing_filter = true;
                    self.keybindings_filter.update(cx, |state, cx| {
                        state.focus(window, cx);
                    });
                    cx.notify();
                }
            }

            // Escape to go back
            ("escape", m) if m == Modifiers::none() => {
                if self.focus_area == SettingsFocus::Content {
                    self.focus_area = SettingsFocus::Sidebar;
                    cx.notify();
                }
            }

            _ => {}
        }
    }
}

impl Render for SettingsWindow {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_ssh_key_path.take() {
            self.input_ssh_key_path.update(cx, |state, cx| {
                state.set_value(path, window, cx);
            });
        }

        let theme = cx.theme();
        let show_delete_confirm = self.pending_delete_tunnel_id.is_some();

        let tunnel_name = self
            .pending_delete_tunnel_id
            .and_then(|id| {
                self.app_state
                    .read(cx)
                    .ssh_tunnels
                    .iter()
                    .find(|t| t.id == id)
                    .map(|t| t.name.clone())
            })
            .unwrap_or_default();

        div()
            .size_full()
            .bg(theme.background)
            .flex()
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                this.handle_key_event(event, window, cx);
            }))
            .child(self.render_sidebar(cx))
            .child(match self.active_section {
                SettingsSection::Keybindings => {
                    self.render_keybindings_section(cx).into_any_element()
                }
                SettingsSection::SshTunnels => {
                    self.render_ssh_tunnels_section(cx).into_any_element()
                }
            })
            .when(show_delete_confirm, |el| {
                let this = cx.entity().clone();
                let this_cancel = this.clone();

                el.child(
                    Dialog::new(window, cx)
                        .title("Delete SSH Tunnel")
                        .confirm()
                        .on_ok(move |_, _, cx| {
                            this.update(cx, |settings, cx| {
                                settings.confirm_delete_tunnel(cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            this_cancel.update(cx, |settings, cx| {
                                settings.cancel_delete_tunnel(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(format!(
                            "Are you sure you want to delete \"{}\"?",
                            tunnel_name
                        ))),
                )
            })
    }
}
