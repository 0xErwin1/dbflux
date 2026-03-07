use crate::app::AppStateChanged;
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{ProxyAuth, ProxyKind, ProxyProfile};
use gpui::*;
use uuid::Uuid;

use super::form_nav::FormGridNav;
use super::{ProxyAuthSelection, ProxyFocus, ProxyFormField, SettingsWindow};

/// Proxy-specific form navigation state, built on top of `FormGridNav`.
///
/// Holds the extra context (`auth_selection`, `editing_id`) needed to compute
/// which rows are visible, then delegates all movement to the generic grid nav.
#[derive(Clone)]
pub(super) struct ProxyFormNav {
    pub(super) auth_selection: ProxyAuthSelection,
    pub(super) editing_id: Option<Uuid>,
    nav: FormGridNav<ProxyFormField>,
}

impl ProxyFormNav {
    pub(super) fn new(
        auth_selection: ProxyAuthSelection,
        editing_id: Option<Uuid>,
        field: ProxyFormField,
    ) -> Self {
        Self {
            auth_selection,
            editing_id,
            nav: FormGridNav::new(field),
        }
    }

    pub(super) fn field(&self) -> ProxyFormField {
        self.nav.focused
    }

    #[cfg(test)]
    pub(super) fn set_field(&mut self, field: ProxyFormField) {
        self.nav.focused = field;
    }

    pub(super) fn form_rows(&self) -> Vec<Vec<ProxyFormField>> {
        let mut rows = vec![
            vec![ProxyFormField::Name],
            vec![
                ProxyFormField::KindHttp,
                ProxyFormField::KindHttps,
                ProxyFormField::KindSocks5,
            ],
            vec![ProxyFormField::Host, ProxyFormField::Port],
            vec![ProxyFormField::AuthNone, ProxyFormField::AuthBasic],
        ];

        if self.auth_selection == ProxyAuthSelection::Basic {
            rows.push(vec![ProxyFormField::Username]);
            rows.push(vec![ProxyFormField::Password, ProxyFormField::SaveSecret]);
        }

        rows.push(vec![ProxyFormField::NoProxy]);
        rows.push(vec![ProxyFormField::Enabled]);

        if self.editing_id.is_some() {
            rows.push(vec![
                ProxyFormField::DeleteButton,
                ProxyFormField::SaveButton,
            ]);
        } else {
            rows.push(vec![ProxyFormField::SaveButton]);
        }

        rows
    }

    pub(super) fn move_down(&mut self) {
        let rows = self.form_rows();
        self.nav.move_down(&rows);
    }

    pub(super) fn move_up(&mut self) {
        let rows = self.form_rows();
        self.nav.move_up(&rows);
    }

    pub(super) fn move_right(&mut self) {
        let rows = self.form_rows();
        self.nav.move_right(&rows);
    }

    pub(super) fn move_left(&mut self) {
        let rows = self.form_rows();
        self.nav.move_left(&rows);
    }

    pub(super) fn move_first(&mut self) {
        let rows = self.form_rows();
        self.nav.move_first(&rows);
    }

    pub(super) fn move_last(&mut self) {
        let rows = self.form_rows();
        self.nav.move_last(&rows);
    }

    pub(super) fn tab_next(&mut self) {
        let rows = self.form_rows();
        self.nav.tab_next(&rows);
    }

    pub(super) fn tab_prev(&mut self) {
        let rows = self.form_rows();
        self.nav.tab_prev(&rows);
    }

    pub(super) fn validate_field(&mut self) {
        let rows = self.form_rows();
        self.nav.validate(&rows, ProxyFormField::Name);
    }

    pub(super) fn is_input_field(field: ProxyFormField) -> bool {
        matches!(
            field,
            ProxyFormField::Name
                | ProxyFormField::Host
                | ProxyFormField::Port
                | ProxyFormField::Username
                | ProxyFormField::Password
                | ProxyFormField::NoProxy
        )
    }
}

impl SettingsWindow {
    pub(super) fn clear_proxy_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_proxy_id = None;
        self.proxy_kind = ProxyKind::Http;
        self.proxy_auth_selection = ProxyAuthSelection::None;
        self.proxy_save_secret = false;
        self.proxy_enabled = true;
        self.show_proxy_password = false;

        self.input_proxy_name
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_proxy_host
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_proxy_port
            .update(cx, |s, cx| s.set_value("8080", window, cx));
        self.input_proxy_username
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_proxy_password
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_proxy_no_proxy
            .update(cx, |s, cx| s.set_value("", window, cx));

        cx.notify();
    }

    pub(super) fn edit_proxy(
        &mut self,
        proxy: &ProxyProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editing_proxy_id = Some(proxy.id);

        self.input_proxy_name
            .update(cx, |s, cx| s.set_value(&proxy.name, window, cx));
        self.input_proxy_host
            .update(cx, |s, cx| s.set_value(&proxy.host, window, cx));
        self.input_proxy_port
            .update(cx, |s, cx| s.set_value(proxy.port.to_string(), window, cx));

        self.proxy_kind = proxy.kind;
        self.proxy_enabled = proxy.enabled;
        self.proxy_save_secret = proxy.save_secret;

        match &proxy.auth {
            ProxyAuth::None => {
                self.proxy_auth_selection = ProxyAuthSelection::None;
                self.input_proxy_username
                    .update(cx, |s, cx| s.set_value("", window, cx));
                self.input_proxy_password
                    .update(cx, |s, cx| s.set_value("", window, cx));
            }
            ProxyAuth::Basic { username } => {
                self.proxy_auth_selection = ProxyAuthSelection::Basic;
                self.input_proxy_username
                    .update(cx, |s, cx| s.set_value(username, window, cx));

                if let Some(secret) = self.app_state.read(cx).get_proxy_secret(proxy) {
                    let secret = secret.expose_secret().to_string();
                    self.input_proxy_password
                        .update(cx, |s, cx| s.set_value(secret.clone(), window, cx));
                } else {
                    self.input_proxy_password
                        .update(cx, |s, cx| s.set_value("", window, cx));
                }
            }
        }

        let no_proxy_str = proxy.no_proxy.clone().unwrap_or_default();
        self.input_proxy_no_proxy
            .update(cx, |s, cx| s.set_value(&no_proxy_str, window, cx));

        cx.notify();
    }

    pub(super) fn save_proxy(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_proxy_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }

        let host = self.input_proxy_host.read(cx).value().to_string();
        let port_str = self.input_proxy_port.read(cx).value().to_string();
        let port: u16 = port_str
            .trim()
            .parse()
            .unwrap_or(self.proxy_kind.default_port());

        let auth = match self.proxy_auth_selection {
            ProxyAuthSelection::None => ProxyAuth::None,
            ProxyAuthSelection::Basic => ProxyAuth::Basic {
                username: self.input_proxy_username.read(cx).value().to_string(),
            },
        };

        let no_proxy_str = self.input_proxy_no_proxy.read(cx).value().to_string();
        let no_proxy = if no_proxy_str.trim().is_empty() {
            None
        } else {
            Some(no_proxy_str)
        };

        let password = self.input_proxy_password.read(cx).value().to_string();

        let proxy = ProxyProfile {
            id: self.editing_proxy_id.unwrap_or_else(Uuid::new_v4),
            name,
            kind: self.proxy_kind,
            host,
            port,
            auth,
            no_proxy,
            enabled: self.proxy_enabled,
            save_secret: self.proxy_save_secret,
        };

        let is_edit = self.editing_proxy_id.is_some();

        self.app_state.update(cx, |state, cx| {
            if proxy.save_secret
                && matches!(proxy.auth, ProxyAuth::Basic { .. })
                && !password.is_empty()
            {
                state.save_proxy_secret(&proxy, &SecretString::from(password.clone()));
            } else if is_edit {
                state.delete_proxy_secret(&proxy);
            }

            if is_edit {
                state.update_proxy(proxy);
            } else {
                state.add_proxy(proxy);
            }

            cx.emit(AppStateChanged);
        });

        self.clear_proxy_form(window, cx);
    }

    pub(super) fn request_delete_proxy(&mut self, proxy_id: Uuid, cx: &mut Context<Self>) {
        self.pending_delete_proxy_id = Some(proxy_id);
        cx.notify();
    }

    pub(super) fn confirm_delete_proxy(&mut self, cx: &mut Context<Self>) {
        let Some(proxy_id) = self.pending_delete_proxy_id.take() else {
            return;
        };

        let deleted_idx = self.app_state.update(cx, |state, cx| {
            let affected: Vec<_> = state
                .profiles()
                .iter()
                .filter(|p| p.proxy_profile_id == Some(proxy_id))
                .cloned()
                .collect();

            for mut profile in affected {
                profile.proxy_profile_id = None;
                state.update_profile(profile);
            }

            let idx = state.proxies().iter().position(|p| p.id == proxy_id);
            if let Some(i) = idx {
                state.remove_proxy(i);
            }

            cx.emit(AppStateChanged);
            idx
        });

        if self.editing_proxy_id == Some(proxy_id) {
            self.editing_proxy_id = None;
        }

        if let Some(deleted) = deleted_idx {
            let new_count = self.proxy_count(cx);
            if new_count == 0 {
                self.proxy_selected_idx = None;
            } else if let Some(sel) = self.proxy_selected_idx {
                if sel >= new_count {
                    self.proxy_selected_idx = Some(new_count - 1);
                } else if sel > deleted {
                    self.proxy_selected_idx = Some(sel - 1);
                }
            }
        }

        cx.notify();
    }

    pub(super) fn cancel_delete_proxy(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_proxy_id = None;
        cx.notify();
    }

    pub(super) fn proxy_count(&self, cx: &Context<Self>) -> usize {
        self.app_state.read(cx).proxies().len()
    }

    pub(super) fn profiles_using_proxy(&self, proxy_id: Uuid, cx: &Context<Self>) -> usize {
        self.app_state
            .read(cx)
            .profiles()
            .iter()
            .filter(|p| p.proxy_profile_id == Some(proxy_id))
            .count()
    }

    // --- Navigation ---

    pub(super) fn proxy_move_next_profile(&mut self, cx: &Context<Self>) {
        let count = self.proxy_count(cx);
        if count == 0 {
            self.proxy_selected_idx = None;
            return;
        }

        match self.proxy_selected_idx {
            None => self.proxy_selected_idx = Some(0),
            Some(idx) if idx + 1 < count => self.proxy_selected_idx = Some(idx + 1),
            _ => {}
        }
    }

    pub(super) fn proxy_move_prev_profile(&mut self, cx: &Context<Self>) {
        let count = self.proxy_count(cx);
        if count == 0 {
            self.proxy_selected_idx = None;
            return;
        }

        match self.proxy_selected_idx {
            Some(idx) if idx > 0 => self.proxy_selected_idx = Some(idx - 1),
            Some(0) => self.proxy_selected_idx = None,
            _ => {}
        }
    }

    pub(super) fn proxy_load_selected_profile(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let proxies = self.app_state.read(cx).proxies().to_vec();

        if let Some(idx) = self.proxy_selected_idx
            && idx >= proxies.len()
        {
            self.proxy_selected_idx = if proxies.is_empty() {
                None
            } else {
                Some(proxies.len() - 1)
            };
        }

        if let Some(idx) = self.proxy_selected_idx
            && let Some(proxy) = proxies.get(idx)
        {
            self.edit_proxy(proxy, window, cx);
            return;
        }

        self.clear_proxy_form(window, cx);
    }

    pub(super) fn proxy_enter_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.proxy_focus = ProxyFocus::Form;
        self.proxy_form_field = ProxyFormField::Name;
        self.proxy_editing_field = false;

        self.proxy_load_selected_profile(window, cx);
    }

    pub(super) fn proxy_exit_form(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.proxy_focus = ProxyFocus::ProfileList;
        self.proxy_editing_field = false;
        self.focus_handle.focus(window);
    }

    fn proxy_nav(&self) -> ProxyFormNav {
        ProxyFormNav::new(
            self.proxy_auth_selection,
            self.editing_proxy_id,
            self.proxy_form_field,
        )
    }

    fn apply_proxy_nav(&mut self, nav: ProxyFormNav) {
        self.proxy_form_field = nav.field();
    }

    pub(super) fn proxy_move_down(&mut self) {
        let mut nav = self.proxy_nav();
        nav.move_down();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_move_up(&mut self) {
        let mut nav = self.proxy_nav();
        nav.move_up();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_move_right(&mut self) {
        let mut nav = self.proxy_nav();
        nav.move_right();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_move_left(&mut self) {
        let mut nav = self.proxy_nav();
        nav.move_left();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_move_first(&mut self) {
        let mut nav = self.proxy_nav();
        nav.move_first();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_move_last(&mut self) {
        let mut nav = self.proxy_nav();
        nav.move_last();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_tab_next(&mut self) {
        let mut nav = self.proxy_nav();
        nav.tab_next();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_tab_prev(&mut self) {
        let mut nav = self.proxy_nav();
        nav.tab_prev();
        self.apply_proxy_nav(nav);
    }

    pub(super) fn proxy_focus_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.proxy_editing_field = true;

        match self.proxy_form_field {
            ProxyFormField::Name => {
                self.input_proxy_name
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            ProxyFormField::Host => {
                self.input_proxy_host
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            ProxyFormField::Port => {
                self.input_proxy_port
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            ProxyFormField::Username => {
                self.input_proxy_username
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            ProxyFormField::Password => {
                self.input_proxy_password
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            ProxyFormField::NoProxy => {
                self.input_proxy_no_proxy
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            _ => {
                self.proxy_editing_field = false;
            }
        }
    }

    pub(super) fn proxy_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.proxy_form_field {
            ProxyFormField::KindHttp => {
                self.proxy_kind = ProxyKind::Http;
                self.input_proxy_port
                    .update(cx, |s, cx| s.set_value("8080", window, cx));
                self.validate_proxy_form_field();
                cx.notify();
            }
            ProxyFormField::KindHttps => {
                self.proxy_kind = ProxyKind::Https;
                self.input_proxy_port
                    .update(cx, |s, cx| s.set_value("8080", window, cx));
                self.validate_proxy_form_field();
                cx.notify();
            }
            ProxyFormField::KindSocks5 => {
                self.proxy_kind = ProxyKind::Socks5;
                self.input_proxy_port
                    .update(cx, |s, cx| s.set_value("1080", window, cx));
                self.validate_proxy_form_field();
                cx.notify();
            }
            ProxyFormField::AuthNone => {
                self.proxy_auth_selection = ProxyAuthSelection::None;
                self.validate_proxy_form_field();
                cx.notify();
            }
            ProxyFormField::AuthBasic => {
                self.proxy_auth_selection = ProxyAuthSelection::Basic;
                self.validate_proxy_form_field();
                cx.notify();
            }
            ProxyFormField::Enabled => {
                self.proxy_enabled = !self.proxy_enabled;
                cx.notify();
            }
            ProxyFormField::SaveSecret => {
                self.proxy_save_secret = !self.proxy_save_secret;
                cx.notify();
            }
            ProxyFormField::SaveButton => {
                self.save_proxy(window, cx);
            }
            ProxyFormField::DeleteButton => {
                if let Some(id) = self.editing_proxy_id {
                    self.request_delete_proxy(id, cx);
                }
            }
            field if ProxyFormNav::is_input_field(field) => {
                self.proxy_focus_current_field(window, cx);
            }
            _ => {}
        }
    }

    pub(super) fn validate_proxy_form_field(&mut self) {
        let mut nav = self.proxy_nav();
        nav.validate_field();
        self.apply_proxy_nav(nav);
    }
}

#[cfg(test)]
mod tests {
    use super::{ProxyAuthSelection, ProxyFormField, ProxyFormNav};
    use uuid::Uuid;

    fn nav_no_auth_new() -> ProxyFormNav {
        ProxyFormNav::new(ProxyAuthSelection::None, None, ProxyFormField::Name)
    }

    fn nav_basic_auth_new() -> ProxyFormNav {
        ProxyFormNav::new(ProxyAuthSelection::Basic, None, ProxyFormField::Name)
    }

    fn nav_basic_auth_editing() -> ProxyFormNav {
        ProxyFormNav::new(
            ProxyAuthSelection::Basic,
            Some(Uuid::new_v4()),
            ProxyFormField::Name,
        )
    }

    #[test]
    fn form_rows_no_auth_excludes_credentials() {
        let nav = nav_no_auth_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(!all_fields.contains(&&ProxyFormField::Username));
        assert!(!all_fields.contains(&&ProxyFormField::Password));
        assert!(!all_fields.contains(&&ProxyFormField::SaveSecret));
    }

    #[test]
    fn form_rows_basic_auth_includes_credentials() {
        let nav = nav_basic_auth_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(all_fields.contains(&&ProxyFormField::Username));
        assert!(all_fields.contains(&&ProxyFormField::Password));
        assert!(all_fields.contains(&&ProxyFormField::SaveSecret));
    }

    #[test]
    fn form_rows_new_proxy_no_delete_button() {
        let nav = nav_no_auth_new();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(!all_fields.contains(&&ProxyFormField::DeleteButton));
        assert!(all_fields.contains(&&ProxyFormField::SaveButton));
    }

    #[test]
    fn form_rows_editing_has_delete_button() {
        let nav = nav_basic_auth_editing();
        let rows = nav.form_rows();
        let all_fields: Vec<_> = rows.iter().flatten().collect();
        assert!(all_fields.contains(&&ProxyFormField::DeleteButton));
        assert!(all_fields.contains(&&ProxyFormField::SaveButton));
    }

    #[test]
    fn move_down_from_name_to_kind() {
        let mut nav = nav_no_auth_new();
        nav.set_field(ProxyFormField::Name);
        nav.move_down();
        assert_eq!(nav.field(), ProxyFormField::KindHttp);
    }

    #[test]
    fn move_right_in_kind_row() {
        let mut nav = nav_no_auth_new();
        nav.set_field(ProxyFormField::KindHttp);
        nav.move_right();
        assert_eq!(nav.field(), ProxyFormField::KindHttps);
        nav.move_right();
        assert_eq!(nav.field(), ProxyFormField::KindSocks5);
        nav.move_right();
        assert_eq!(nav.field(), ProxyFormField::KindSocks5);
    }

    #[test]
    fn tab_next_crosses_row_boundary() {
        let mut nav = nav_no_auth_new();
        nav.set_field(ProxyFormField::Port);
        nav.tab_next();
        assert_eq!(nav.field(), ProxyFormField::AuthNone);
    }

    #[test]
    fn validate_resets_orphaned_field() {
        let mut nav = ProxyFormNav::new(ProxyAuthSelection::None, None, ProxyFormField::Username);
        nav.validate_field();
        assert_eq!(nav.field(), ProxyFormField::Name);
    }

    #[test]
    fn is_proxy_input_field_correctness() {
        assert!(ProxyFormNav::is_input_field(ProxyFormField::Name));
        assert!(ProxyFormNav::is_input_field(ProxyFormField::Host));
        assert!(ProxyFormNav::is_input_field(ProxyFormField::Port));
        assert!(ProxyFormNav::is_input_field(ProxyFormField::Username));
        assert!(ProxyFormNav::is_input_field(ProxyFormField::Password));
        assert!(ProxyFormNav::is_input_field(ProxyFormField::NoProxy));

        assert!(!ProxyFormNav::is_input_field(ProxyFormField::KindHttp));
        assert!(!ProxyFormNav::is_input_field(ProxyFormField::AuthNone));
        assert!(!ProxyFormNav::is_input_field(ProxyFormField::Enabled));
        assert!(!ProxyFormNav::is_input_field(ProxyFormField::SaveButton));
        assert!(!ProxyFormNav::is_input_field(ProxyFormField::DeleteButton));
    }
}
