use crate::app::AppStateChanged;
use dbflux_core::{AuthProfile, AuthProfileConfig};
use gpui::*;
use uuid::Uuid;

use super::form_nav::FormGridNav;
use super::{AuthProfileFocus, AuthProfileFormField, SettingsWindow};

/// Which provider type is currently selected in the form.
#[derive(Clone, Copy, PartialEq, Debug, Default)]
#[allow(clippy::enum_variant_names)]
pub(super) enum AuthProviderSelection {
    #[default]
    AwsSso,
    AwsSharedCredentials,
    AwsStaticCredentials,
}

impl AuthProviderSelection {
    #[allow(dead_code)]
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::AwsSso => "AWS SSO",
            Self::AwsSharedCredentials => "AWS Shared Credentials",
            Self::AwsStaticCredentials => "AWS Static Credentials",
        }
    }
}

/// Auth-profile-specific form navigation state, built on top of `FormGridNav`.
#[derive(Clone)]
pub(super) struct AuthProfileFormNav {
    pub(super) provider: AuthProviderSelection,
    pub(super) editing_id: Option<Uuid>,
    nav: FormGridNav<AuthProfileFormField>,
}

impl AuthProfileFormNav {
    pub(super) fn new(
        provider: AuthProviderSelection,
        editing_id: Option<Uuid>,
        field: AuthProfileFormField,
    ) -> Self {
        Self {
            provider,
            editing_id,
            nav: FormGridNav::new(field),
        }
    }

    pub(super) fn field(&self) -> AuthProfileFormField {
        self.nav.focused
    }

    #[cfg(test)]
    pub(super) fn set_field(&mut self, field: AuthProfileFormField) {
        self.nav.focused = field;
    }

    pub(super) fn form_rows(&self) -> Vec<Vec<AuthProfileFormField>> {
        let mut rows = vec![
            vec![AuthProfileFormField::Name],
            vec![
                AuthProfileFormField::ProviderAwsSso,
                AuthProfileFormField::ProviderAwsSharedCredentials,
                AuthProfileFormField::ProviderAwsStaticCredentials,
            ],
        ];

        match self.provider {
            AuthProviderSelection::AwsSso => {
                rows.push(vec![AuthProfileFormField::SsoProfileName]);
                rows.push(vec![AuthProfileFormField::SsoRegion]);
                rows.push(vec![AuthProfileFormField::SsoStartUrl]);
                rows.push(vec![AuthProfileFormField::SsoAccountId]);
                rows.push(vec![AuthProfileFormField::SsoRoleName]);
            }
            AuthProviderSelection::AwsSharedCredentials => {
                rows.push(vec![AuthProfileFormField::SharedCredentialsProfileName]);
                rows.push(vec![AuthProfileFormField::SharedCredentialsRegion]);
            }
            AuthProviderSelection::AwsStaticCredentials => {
                rows.push(vec![AuthProfileFormField::StaticRegion]);
            }
        }

        rows.push(vec![AuthProfileFormField::Enabled]);
        rows.push(vec![AuthProfileFormField::TestButton]);

        if self.editing_id.is_some() {
            rows.push(vec![
                AuthProfileFormField::DeleteButton,
                AuthProfileFormField::SaveButton,
            ]);
        } else {
            rows.push(vec![AuthProfileFormField::SaveButton]);
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
        self.nav.validate(&rows, AuthProfileFormField::Name);
    }

    pub(super) fn is_input_field(field: AuthProfileFormField) -> bool {
        matches!(
            field,
            AuthProfileFormField::Name
                | AuthProfileFormField::SsoProfileName
                | AuthProfileFormField::SsoRegion
                | AuthProfileFormField::SsoStartUrl
                | AuthProfileFormField::SsoAccountId
                | AuthProfileFormField::SsoRoleName
                | AuthProfileFormField::SharedCredentialsProfileName
                | AuthProfileFormField::SharedCredentialsRegion
                | AuthProfileFormField::StaticRegion
        )
    }
}

impl SettingsWindow {
    pub(super) fn clear_auth_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.auth_editing_id = None;
        self.auth_provider_selection = AuthProviderSelection::AwsSso;
        self.auth_enabled = true;

        self.input_auth_name
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_sso_profile
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_sso_region
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_sso_start_url
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_sso_account_id
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_sso_role_name
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_shared_profile
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_shared_region
            .update(cx, |s, cx| s.set_value("", window, cx));
        self.input_auth_static_region
            .update(cx, |s, cx| s.set_value("", window, cx));

        cx.notify();
    }

    pub(super) fn edit_auth_profile(
        &mut self,
        profile: &AuthProfile,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.auth_editing_id = Some(profile.id);
        self.auth_enabled = profile.enabled;

        self.input_auth_name
            .update(cx, |s, cx| s.set_value(&profile.name, window, cx));

        match &profile.config {
            AuthProfileConfig::AwsSso {
                profile_name,
                region,
                sso_start_url,
                sso_account_id,
                sso_role_name,
            } => {
                self.auth_provider_selection = AuthProviderSelection::AwsSso;
                self.input_auth_sso_profile
                    .update(cx, |s, cx| s.set_value(profile_name, window, cx));
                self.input_auth_sso_region
                    .update(cx, |s, cx| s.set_value(region, window, cx));
                self.input_auth_sso_start_url
                    .update(cx, |s, cx| s.set_value(sso_start_url, window, cx));
                self.input_auth_sso_account_id
                    .update(cx, |s, cx| s.set_value(sso_account_id, window, cx));
                self.input_auth_sso_role_name
                    .update(cx, |s, cx| s.set_value(sso_role_name, window, cx));
            }
            AuthProfileConfig::AwsSharedCredentials {
                profile_name,
                region,
            } => {
                self.auth_provider_selection = AuthProviderSelection::AwsSharedCredentials;
                self.input_auth_shared_profile
                    .update(cx, |s, cx| s.set_value(profile_name, window, cx));
                self.input_auth_shared_region
                    .update(cx, |s, cx| s.set_value(region, window, cx));
            }
            AuthProfileConfig::AwsStaticCredentials { region } => {
                self.auth_provider_selection = AuthProviderSelection::AwsStaticCredentials;
                self.input_auth_static_region
                    .update(cx, |s, cx| s.set_value(region, window, cx));
            }
        }

        cx.notify();
    }

    pub(super) fn save_auth_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_auth_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }

        let config = match self.auth_provider_selection {
            AuthProviderSelection::AwsSso => AuthProfileConfig::AwsSso {
                profile_name: self.input_auth_sso_profile.read(cx).value().to_string(),
                region: self.input_auth_sso_region.read(cx).value().to_string(),
                sso_start_url: self.input_auth_sso_start_url.read(cx).value().to_string(),
                sso_account_id: self.input_auth_sso_account_id.read(cx).value().to_string(),
                sso_role_name: self.input_auth_sso_role_name.read(cx).value().to_string(),
            },
            AuthProviderSelection::AwsSharedCredentials => {
                AuthProfileConfig::AwsSharedCredentials {
                    profile_name: self.input_auth_shared_profile.read(cx).value().to_string(),
                    region: self.input_auth_shared_region.read(cx).value().to_string(),
                }
            }
            AuthProviderSelection::AwsStaticCredentials => {
                AuthProfileConfig::AwsStaticCredentials {
                    region: self.input_auth_static_region.read(cx).value().to_string(),
                }
            }
        };

        let provider_id = match self.auth_provider_selection {
            AuthProviderSelection::AwsSso => "aws-sso",
            AuthProviderSelection::AwsSharedCredentials => "aws-shared-credentials",
            AuthProviderSelection::AwsStaticCredentials => "aws-static-credentials",
        };

        let profile = AuthProfile {
            id: self.auth_editing_id.unwrap_or_else(Uuid::new_v4),
            name,
            provider_id: provider_id.to_string(),
            config,
            enabled: self.auth_enabled,
        };

        let is_edit = self.auth_editing_id.is_some();

        self.app_state.update(cx, |state, cx| {
            if is_edit {
                state.update_auth_profile(profile);
            } else {
                state.add_auth_profile(profile);
            }

            cx.emit(AppStateChanged);
        });

        self.clear_auth_form(window, cx);
    }

    pub(super) fn request_delete_auth_profile(&mut self, profile_id: Uuid, cx: &mut Context<Self>) {
        self.pending_delete_auth_id = Some(profile_id);
        cx.notify();
    }

    pub(super) fn confirm_delete_auth_profile(&mut self, cx: &mut Context<Self>) {
        let Some(profile_id) = self.pending_delete_auth_id.take() else {
            return;
        };

        let deleted_idx = self.app_state.update(cx, |state, cx| {
            // Clear auth_profile_id from any connection profiles referencing this
            let affected: Vec<_> = state
                .profiles()
                .iter()
                .filter(|p| p.auth_profile_id == Some(profile_id))
                .cloned()
                .collect();

            for mut profile in affected {
                profile.auth_profile_id = None;
                state.update_profile(profile);
            }

            let idx = state
                .auth_profiles()
                .iter()
                .position(|p| p.id == profile_id);
            if let Some(i) = idx {
                state.remove_auth_profile(i);
            }

            cx.emit(AppStateChanged);
            idx
        });

        if self.auth_editing_id == Some(profile_id) {
            self.auth_editing_id = None;
        }

        if let Some(deleted) = deleted_idx {
            let new_count = self.auth_profile_count(cx);
            if new_count == 0 {
                self.auth_selected_idx = None;
            } else if let Some(sel) = self.auth_selected_idx {
                if sel >= new_count {
                    self.auth_selected_idx = Some(new_count - 1);
                } else if sel > deleted {
                    self.auth_selected_idx = Some(sel - 1);
                }
            }
        }

        cx.notify();
    }

    pub(super) fn cancel_delete_auth_profile(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_auth_id = None;
        cx.notify();
    }

    pub(super) fn auth_profile_count(&self, cx: &Context<Self>) -> usize {
        self.app_state.read(cx).auth_profiles().len()
    }

    pub(super) fn profiles_using_auth(&self, auth_id: Uuid, cx: &Context<Self>) -> usize {
        self.app_state
            .read(cx)
            .profiles()
            .iter()
            .filter(|p| p.auth_profile_id == Some(auth_id))
            .count()
    }

    // --- Navigation ---

    pub(super) fn auth_move_next_profile(&mut self, cx: &Context<Self>) {
        let count = self.auth_profile_count(cx);
        if count == 0 {
            self.auth_selected_idx = None;
            return;
        }

        match self.auth_selected_idx {
            None => self.auth_selected_idx = Some(0),
            Some(idx) if idx + 1 < count => self.auth_selected_idx = Some(idx + 1),
            _ => {}
        }
    }

    pub(super) fn auth_move_prev_profile(&mut self, cx: &Context<Self>) {
        let count = self.auth_profile_count(cx);
        if count == 0 {
            self.auth_selected_idx = None;
            return;
        }

        match self.auth_selected_idx {
            Some(idx) if idx > 0 => self.auth_selected_idx = Some(idx - 1),
            Some(0) => self.auth_selected_idx = None,
            _ => {}
        }
    }

    pub(super) fn auth_load_selected_profile(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profiles = self.app_state.read(cx).auth_profiles().to_vec();

        if let Some(idx) = self.auth_selected_idx
            && idx >= profiles.len()
        {
            self.auth_selected_idx = if profiles.is_empty() {
                None
            } else {
                Some(profiles.len() - 1)
            };
        }

        if let Some(idx) = self.auth_selected_idx
            && let Some(profile) = profiles.get(idx)
        {
            self.edit_auth_profile(profile, window, cx);
            return;
        }

        self.clear_auth_form(window, cx);
    }

    pub(super) fn auth_enter_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.auth_focus = AuthProfileFocus::Form;
        self.auth_form_field = AuthProfileFormField::Name;
        self.auth_editing_field = false;

        self.auth_load_selected_profile(window, cx);
    }

    pub(super) fn auth_exit_form(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.auth_focus = AuthProfileFocus::ProfileList;
        self.auth_editing_field = false;
        self.focus_handle.focus(window);
    }

    fn auth_nav(&self) -> AuthProfileFormNav {
        AuthProfileFormNav::new(
            self.auth_provider_selection,
            self.auth_editing_id,
            self.auth_form_field,
        )
    }

    fn apply_auth_nav(&mut self, nav: AuthProfileFormNav) {
        self.auth_form_field = nav.field();
    }

    pub(super) fn auth_move_down(&mut self) {
        let mut nav = self.auth_nav();
        nav.move_down();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_move_up(&mut self) {
        let mut nav = self.auth_nav();
        nav.move_up();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_move_right(&mut self) {
        let mut nav = self.auth_nav();
        nav.move_right();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_move_left(&mut self) {
        let mut nav = self.auth_nav();
        nav.move_left();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_move_first(&mut self) {
        let mut nav = self.auth_nav();
        nav.move_first();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_move_last(&mut self) {
        let mut nav = self.auth_nav();
        nav.move_last();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_tab_next(&mut self) {
        let mut nav = self.auth_nav();
        nav.tab_next();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_tab_prev(&mut self) {
        let mut nav = self.auth_nav();
        nav.tab_prev();
        self.apply_auth_nav(nav);
    }

    pub(super) fn auth_focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.auth_editing_field = true;

        match self.auth_form_field {
            AuthProfileFormField::Name => {
                self.input_auth_name.update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SsoProfileName => {
                self.input_auth_sso_profile
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SsoRegion => {
                self.input_auth_sso_region
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SsoStartUrl => {
                self.input_auth_sso_start_url
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SsoAccountId => {
                self.input_auth_sso_account_id
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SsoRoleName => {
                self.input_auth_sso_role_name
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SharedCredentialsProfileName => {
                self.input_auth_shared_profile
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::SharedCredentialsRegion => {
                self.input_auth_shared_region
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            AuthProfileFormField::StaticRegion => {
                self.input_auth_static_region
                    .update(cx, |s, cx| s.focus(window, cx));
            }
            _ => {
                self.auth_editing_field = false;
            }
        }
    }

    pub(super) fn auth_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.auth_form_field {
            AuthProfileFormField::ProviderAwsSso => {
                self.auth_provider_selection = AuthProviderSelection::AwsSso;
                self.validate_auth_form_field();
                cx.notify();
            }
            AuthProfileFormField::ProviderAwsSharedCredentials => {
                self.auth_provider_selection = AuthProviderSelection::AwsSharedCredentials;
                self.validate_auth_form_field();
                cx.notify();
            }
            AuthProfileFormField::ProviderAwsStaticCredentials => {
                self.auth_provider_selection = AuthProviderSelection::AwsStaticCredentials;
                self.validate_auth_form_field();
                cx.notify();
            }
            AuthProfileFormField::Enabled => {
                self.auth_enabled = !self.auth_enabled;
                cx.notify();
            }
            AuthProfileFormField::SaveButton => {
                self.save_auth_profile(window, cx);
            }
            AuthProfileFormField::DeleteButton => {
                if let Some(id) = self.auth_editing_id {
                    self.request_delete_auth_profile(id, cx);
                }
            }
            AuthProfileFormField::TestButton => {
                // TODO: Wire test_auth_session when auth providers are integrated
            }
            field if AuthProfileFormNav::is_input_field(field) => {
                self.auth_focus_current_field(window, cx);
            }
            _ => {}
        }
    }

    pub(super) fn validate_auth_form_field(&mut self) {
        let mut nav = self.auth_nav();
        nav.validate_field();
        self.apply_auth_nav(nav);
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthProfileFormNav, AuthProviderSelection};
    use crate::ui::windows::settings::AuthProfileFormField;
    use uuid::Uuid;

    fn nav_sso_new() -> AuthProfileFormNav {
        AuthProfileFormNav::new(
            AuthProviderSelection::AwsSso,
            None,
            AuthProfileFormField::Name,
        )
    }

    fn nav_shared_new() -> AuthProfileFormNav {
        AuthProfileFormNav::new(
            AuthProviderSelection::AwsSharedCredentials,
            None,
            AuthProfileFormField::Name,
        )
    }

    fn nav_static_new() -> AuthProfileFormNav {
        AuthProfileFormNav::new(
            AuthProviderSelection::AwsStaticCredentials,
            None,
            AuthProfileFormField::Name,
        )
    }

    fn nav_sso_editing() -> AuthProfileFormNav {
        AuthProfileFormNav::new(
            AuthProviderSelection::AwsSso,
            Some(Uuid::new_v4()),
            AuthProfileFormField::Name,
        )
    }

    #[test]
    fn sso_form_includes_sso_fields() {
        let nav = nav_sso_new();
        let rows = nav.form_rows();
        let all: Vec<_> = rows.iter().flatten().collect();

        assert!(all.contains(&&AuthProfileFormField::SsoProfileName));
        assert!(all.contains(&&AuthProfileFormField::SsoRegion));
        assert!(all.contains(&&AuthProfileFormField::SsoStartUrl));
        assert!(all.contains(&&AuthProfileFormField::SsoAccountId));
        assert!(all.contains(&&AuthProfileFormField::SsoRoleName));
        assert!(!all.contains(&&AuthProfileFormField::SharedCredentialsProfileName));
        assert!(!all.contains(&&AuthProfileFormField::StaticRegion));
    }

    #[test]
    fn shared_form_includes_shared_fields() {
        let nav = nav_shared_new();
        let rows = nav.form_rows();
        let all: Vec<_> = rows.iter().flatten().collect();

        assert!(all.contains(&&AuthProfileFormField::SharedCredentialsProfileName));
        assert!(all.contains(&&AuthProfileFormField::SharedCredentialsRegion));
        assert!(!all.contains(&&AuthProfileFormField::SsoProfileName));
        assert!(!all.contains(&&AuthProfileFormField::StaticRegion));
    }

    #[test]
    fn static_form_includes_static_fields() {
        let nav = nav_static_new();
        let rows = nav.form_rows();
        let all: Vec<_> = rows.iter().flatten().collect();

        assert!(all.contains(&&AuthProfileFormField::StaticRegion));
        assert!(!all.contains(&&AuthProfileFormField::SsoProfileName));
        assert!(!all.contains(&&AuthProfileFormField::SharedCredentialsProfileName));
    }

    #[test]
    fn new_profile_no_delete_button() {
        let nav = nav_sso_new();
        let rows = nav.form_rows();
        let all: Vec<_> = rows.iter().flatten().collect();

        assert!(!all.contains(&&AuthProfileFormField::DeleteButton));
        assert!(all.contains(&&AuthProfileFormField::SaveButton));
    }

    #[test]
    fn editing_has_delete_button() {
        let nav = nav_sso_editing();
        let rows = nav.form_rows();
        let all: Vec<_> = rows.iter().flatten().collect();

        assert!(all.contains(&&AuthProfileFormField::DeleteButton));
        assert!(all.contains(&&AuthProfileFormField::SaveButton));
    }

    #[test]
    fn move_down_from_name_to_provider() {
        let mut nav = nav_sso_new();
        nav.set_field(AuthProfileFormField::Name);
        nav.move_down();
        assert_eq!(nav.field(), AuthProfileFormField::ProviderAwsSso);
    }

    #[test]
    fn move_right_in_provider_row() {
        let mut nav = nav_sso_new();
        nav.set_field(AuthProfileFormField::ProviderAwsSso);
        nav.move_right();
        assert_eq!(
            nav.field(),
            AuthProfileFormField::ProviderAwsSharedCredentials
        );
        nav.move_right();
        assert_eq!(
            nav.field(),
            AuthProfileFormField::ProviderAwsStaticCredentials
        );
        nav.move_right();
        assert_eq!(
            nav.field(),
            AuthProfileFormField::ProviderAwsStaticCredentials
        );
    }

    #[test]
    fn tab_next_crosses_row_boundary() {
        let mut nav = nav_sso_new();
        nav.set_field(AuthProfileFormField::ProviderAwsStaticCredentials);
        nav.tab_next();
        assert_eq!(nav.field(), AuthProfileFormField::SsoProfileName);
    }

    #[test]
    fn validate_resets_orphaned_field() {
        let mut nav = AuthProfileFormNav::new(
            AuthProviderSelection::AwsSharedCredentials,
            None,
            AuthProfileFormField::SsoProfileName,
        );
        nav.validate_field();
        assert_eq!(nav.field(), AuthProfileFormField::Name);
    }

    #[test]
    fn is_input_field_correctness() {
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::Name
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SsoProfileName
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SsoRegion
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SsoStartUrl
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SsoAccountId
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SsoRoleName
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SharedCredentialsProfileName
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SharedCredentialsRegion
        ));
        assert!(AuthProfileFormNav::is_input_field(
            AuthProfileFormField::StaticRegion
        ));

        assert!(!AuthProfileFormNav::is_input_field(
            AuthProfileFormField::ProviderAwsSso
        ));
        assert!(!AuthProfileFormNav::is_input_field(
            AuthProfileFormField::Enabled
        ));
        assert!(!AuthProfileFormNav::is_input_field(
            AuthProfileFormField::SaveButton
        ));
        assert!(!AuthProfileFormNav::is_input_field(
            AuthProfileFormField::DeleteButton
        ));
        assert!(!AuthProfileFormNav::is_input_field(
            AuthProfileFormField::TestButton
        ));
    }
}
