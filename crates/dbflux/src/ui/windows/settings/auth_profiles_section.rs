use super::SettingsSection;
use super::SettingsSectionId;
use crate::app::{AppState, AppStateChanged};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_core::{AccessKind, AuthProfile, AuthProfileConfig};
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::Button;
use gpui_component::button::ButtonVariants;
use gpui_component::checkbox::Checkbox;
use gpui_component::dialog::Dialog;
use gpui_component::input::{Input, InputState};
use gpui_component::{ActiveTheme, Disableable, Icon, IconName, Sizable};
use std::collections::HashSet;
use uuid::Uuid;

#[cfg(feature = "aws")]
use dbflux_aws::CachedAwsConfig;
#[cfg(feature = "aws")]
use dbflux_aws::{
    AwsSsoAccount, append_aws_shared_credentials_profile, append_aws_sso_profile,
    list_sso_account_roles_blocking, list_sso_accounts_blocking, login_sso_blocking,
};

#[derive(Clone)]
struct DetectedAwsProfile {
    name: String,
    region: String,
    is_sso: bool,
    sso_start_url: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Debug, Default)]
#[allow(clippy::enum_variant_names)]
enum AuthProviderSelection {
    #[default]
    AwsSso,
    AwsSharedCredentials,
    AwsStaticCredentials,
}

pub(super) struct AuthProfilesSection {
    app_state: Entity<AppState>,
    selected_profile_id: Option<Uuid>,
    editing_profile_id: Option<Uuid>,
    provider_selection: AuthProviderSelection,
    profile_enabled: bool,
    pending_delete_profile_id: Option<Uuid>,
    pending_sync_from_app_state: bool,
    input_name: Entity<InputState>,
    input_sso_profile_name: Entity<InputState>,
    input_sso_region: Entity<InputState>,
    input_sso_start_url: Entity<InputState>,
    input_sso_account_id: Entity<InputState>,
    input_sso_role_name: Entity<InputState>,
    sso_account_dropdown: Entity<Dropdown>,
    sso_role_dropdown: Entity<Dropdown>,
    input_shared_profile_name: Entity<InputState>,
    input_shared_region: Entity<InputState>,
    input_static_region: Entity<InputState>,
    #[cfg(feature = "aws")]
    aws_config_cache: CachedAwsConfig,
    #[cfg(feature = "aws")]
    sso_accounts: Vec<AwsSsoAccount>,
    #[cfg(feature = "aws")]
    sso_roles: Vec<String>,
    #[cfg(feature = "aws")]
    sso_accounts_loading: bool,
    #[cfg(feature = "aws")]
    sso_roles_loading: bool,
    #[cfg(feature = "aws")]
    sso_accounts_error: Option<String>,
    #[cfg(feature = "aws")]
    sso_roles_error: Option<String>,
    #[cfg(feature = "aws")]
    sso_login_loading: bool,
    #[cfg(feature = "aws")]
    sso_login_status: Option<(String, bool)>,
    #[cfg(feature = "aws")]
    sso_accounts_context_key: Option<String>,
    #[cfg(feature = "aws")]
    sso_roles_context_key: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl AuthProfilesSection {
    pub(super) fn new(
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let selected_profile_id = app_state.read(cx).auth_profiles().first().map(|p| p.id);

        let input_name = cx.new(|cx| InputState::new(window, cx).placeholder("Profile name"));
        let input_sso_profile_name =
            cx.new(|cx| InputState::new(window, cx).placeholder("default"));
        let input_sso_region = cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1"));
        let input_sso_start_url = cx
            .new(|cx| InputState::new(window, cx).placeholder("https://d-xxxx.awsapps.com/start"));
        let input_sso_account_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("123456789012"));
        let input_sso_role_name =
            cx.new(|cx| InputState::new(window, cx).placeholder("ReadOnlyRole"));
        let sso_account_dropdown =
            cx.new(|_cx| Dropdown::new("auth-sso-account-dropdown").placeholder("Select account"));
        let sso_role_dropdown =
            cx.new(|_cx| Dropdown::new("auth-sso-role-dropdown").placeholder("Select role"));

        let input_shared_profile_name =
            cx.new(|cx| InputState::new(window, cx).placeholder("default"));
        let input_shared_region = cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1"));

        let input_static_region = cx.new(|cx| InputState::new(window, cx).placeholder("us-east-1"));

        let subscription = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.pending_sync_from_app_state = true;
            cx.notify();
        });

        let sso_account_dropdown_sub = cx.subscribe_in(
            &sso_account_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                Self::set_input_value(
                    &this.input_sso_account_id,
                    event.item.value.to_string(),
                    window,
                    cx,
                );
                Self::set_input_value(&this.input_sso_role_name, "", window, cx);

                #[cfg(feature = "aws")]
                {
                    this.sso_roles.clear();
                    this.sso_roles_error = None;
                    this.sso_roles_loading = false;
                    this.sso_roles_context_key = None;

                    this.sso_role_dropdown.update(cx, |dropdown, cx| {
                        dropdown.set_items(Vec::new(), cx);
                        dropdown.set_selected_index(None, cx);
                    });
                }

                cx.notify();
            },
        );

        let sso_role_dropdown_sub = cx.subscribe_in(
            &sso_role_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                Self::set_input_value(
                    &this.input_sso_role_name,
                    event.item.value.to_string(),
                    window,
                    cx,
                );
                cx.notify();
            },
        );

        let mut section = Self {
            app_state,
            selected_profile_id,
            editing_profile_id: None,
            provider_selection: AuthProviderSelection::AwsSso,
            profile_enabled: true,
            pending_delete_profile_id: None,
            pending_sync_from_app_state: false,
            input_name,
            input_sso_profile_name,
            input_sso_region,
            input_sso_start_url,
            input_sso_account_id,
            input_sso_role_name,
            sso_account_dropdown,
            sso_role_dropdown,
            input_shared_profile_name,
            input_shared_region,
            input_static_region,
            #[cfg(feature = "aws")]
            aws_config_cache: CachedAwsConfig::new(),
            #[cfg(feature = "aws")]
            sso_accounts: Vec::new(),
            #[cfg(feature = "aws")]
            sso_roles: Vec::new(),
            #[cfg(feature = "aws")]
            sso_accounts_loading: false,
            #[cfg(feature = "aws")]
            sso_roles_loading: false,
            #[cfg(feature = "aws")]
            sso_accounts_error: None,
            #[cfg(feature = "aws")]
            sso_roles_error: None,
            #[cfg(feature = "aws")]
            sso_login_loading: false,
            #[cfg(feature = "aws")]
            sso_login_status: None,
            #[cfg(feature = "aws")]
            sso_accounts_context_key: None,
            #[cfg(feature = "aws")]
            sso_roles_context_key: None,
            _subscriptions: vec![
                subscription,
                sso_account_dropdown_sub,
                sso_role_dropdown_sub,
            ],
        };

        if let Some(profile_id) = section.selected_profile_id {
            section.load_profile_into_form(profile_id, window, cx);
        }

        section
    }

    fn set_input_value(
        input: &Entity<InputState>,
        value: impl Into<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let value = value.into();
        input.update(cx, |state, cx| {
            state.set_value(value, window, cx);
        });
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_profile_id = None;
        self.provider_selection = AuthProviderSelection::AwsSso;
        self.profile_enabled = true;

        Self::set_input_value(&self.input_name, "", window, cx);
        Self::set_input_value(&self.input_sso_profile_name, "", window, cx);
        Self::set_input_value(&self.input_sso_region, "", window, cx);
        Self::set_input_value(&self.input_sso_start_url, "", window, cx);
        Self::set_input_value(&self.input_sso_account_id, "", window, cx);
        Self::set_input_value(&self.input_sso_role_name, "", window, cx);
        Self::set_input_value(&self.input_shared_profile_name, "", window, cx);
        Self::set_input_value(&self.input_shared_region, "", window, cx);
        Self::set_input_value(&self.input_static_region, "", window, cx);

        #[cfg(feature = "aws")]
        self.reset_sso_listing_state(cx);
    }

    #[cfg(feature = "aws")]
    fn reset_sso_listing_state(&mut self, cx: &mut Context<Self>) {
        self.sso_accounts.clear();
        self.sso_roles.clear();
        self.sso_accounts_loading = false;
        self.sso_roles_loading = false;
        self.sso_accounts_error = None;
        self.sso_roles_error = None;
        self.sso_login_loading = false;
        self.sso_login_status = None;
        self.sso_accounts_context_key = None;
        self.sso_roles_context_key = None;

        self.sso_account_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(Vec::new(), cx);
            dropdown.set_selected_index(None, cx);
        });

        self.sso_role_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(Vec::new(), cx);
            dropdown.set_selected_index(None, cx);
        });
    }

    #[cfg(feature = "aws")]
    fn sso_listing_context_from_form(
        &self,
        cx: &Context<Self>,
    ) -> Option<(String, String, String)> {
        let profile_name = self
            .input_sso_profile_name
            .read(cx)
            .value()
            .trim()
            .to_string();
        let region = self.input_sso_region.read(cx).value().trim().to_string();
        let start_url = self.input_sso_start_url.read(cx).value().trim().to_string();

        if profile_name.is_empty() || region.is_empty() || start_url.is_empty() {
            return None;
        }

        Some((profile_name, region, start_url))
    }

    #[cfg(feature = "aws")]
    fn sync_account_dropdown_selection(&self, cx: &mut Context<Self>) {
        let selected = self
            .input_sso_account_id
            .read(cx)
            .value()
            .trim()
            .to_string();
        let selected_index = self
            .sso_accounts
            .iter()
            .position(|account| account.account_id == selected);

        self.sso_account_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(selected_index, cx);
        });
    }

    #[cfg(feature = "aws")]
    fn sync_role_dropdown_selection(&self, cx: &mut Context<Self>) {
        let selected = self.input_sso_role_name.read(cx).value().trim().to_string();
        let selected_index = self
            .sso_roles
            .iter()
            .position(|role_name| role_name == &selected);

        self.sso_role_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(selected_index, cx);
        });
    }

    #[cfg(feature = "aws")]
    fn ensure_sso_listing(&mut self, cx: &mut Context<Self>) {
        let Some((profile_name, region, start_url)) = self.sso_listing_context_from_form(cx) else {
            self.reset_sso_listing_state(cx);
            return;
        };

        let accounts_context_key = format!("{}|{}|{}", profile_name, region, start_url);

        if self.sso_accounts_context_key.as_deref() != Some(accounts_context_key.as_str()) {
            self.sso_accounts_context_key = Some(accounts_context_key.clone());
            self.sso_accounts_loading = true;
            self.sso_accounts_error = None;
            self.sso_accounts.clear();
            self.sso_roles.clear();
            self.sso_roles_loading = false;
            self.sso_roles_error = None;
            self.sso_roles_context_key = None;

            self.sso_account_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_items(Vec::new(), cx);
                dropdown.set_selected_index(None, cx);
            });

            self.sso_role_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_items(Vec::new(), cx);
                dropdown.set_selected_index(None, cx);
            });

            self.fetch_sso_accounts(
                profile_name.clone(),
                region.clone(),
                start_url.clone(),
                accounts_context_key.clone(),
                cx,
            );
        }

        let account_id = self
            .input_sso_account_id
            .read(cx)
            .value()
            .trim()
            .to_string();
        if account_id.is_empty() {
            self.sso_roles.clear();
            self.sso_roles_loading = false;
            self.sso_roles_error = None;
            self.sso_roles_context_key = None;
            self.sso_role_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_items(Vec::new(), cx);
                dropdown.set_selected_index(None, cx);
            });
            return;
        }

        let roles_context_key = format!("{}|{}", accounts_context_key, account_id);
        if self.sso_roles_context_key.as_deref() != Some(roles_context_key.as_str()) {
            self.sso_roles_context_key = Some(roles_context_key.clone());
            self.sso_roles_loading = true;
            self.sso_roles_error = None;
            self.sso_roles.clear();

            self.sso_role_dropdown.update(cx, |dropdown, cx| {
                dropdown.set_items(Vec::new(), cx);
                dropdown.set_selected_index(None, cx);
            });

            self.fetch_sso_roles(
                profile_name,
                region,
                start_url,
                account_id,
                roles_context_key,
                cx,
            );
        }
    }

    #[cfg(feature = "aws")]
    fn login_sso_profile(&mut self, cx: &mut Context<Self>) {
        let Some((profile_name, region, start_url)) = self.sso_listing_context_from_form(cx) else {
            self.sso_login_loading = false;
            self.sso_login_status = Some((
                "Provide AWS Profile Name, Region, and SSO Start URL before login".to_string(),
                false,
            ));
            cx.notify();
            return;
        };

        let sso_account_id = self
            .input_sso_account_id
            .read(cx)
            .value()
            .trim()
            .to_string();
        let sso_role_name = self.input_sso_role_name.read(cx).value().trim().to_string();

        self.sso_login_loading = true;
        self.sso_login_status = Some(("Running AWS SSO login...".to_string(), false));
        cx.notify();

        let profile_id = self.editing_profile_id.unwrap_or_else(Uuid::new_v4);

        let this = cx.entity().clone();

        // `login_sso_blocking` uses std::process::Command + polling — no Tokio
        // runtime required. Safe to call directly from a GPUI background task.
        let task = cx.background_executor().spawn({
            let profile_name = profile_name.clone();
            let start_url = start_url.clone();
            async move {
                login_sso_blocking(
                    profile_id,
                    &profile_name,
                    &start_url,
                    &region,
                    &sso_account_id,
                    &sso_role_name,
                )
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(err) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.sso_login_loading = false;

                    match result {
                        Ok(_) => {
                            this.sso_login_status = Some((
                                format!(
                                    "AWS SSO login succeeded for profile '{}'. Refreshing accounts and roles...",
                                    profile_name
                                ),
                                true,
                            ));
                            this.sso_accounts_context_key = None;
                            this.sso_roles_context_key = None;
                            this.sso_accounts_error = None;
                            this.sso_roles_error = None;
                            this.ensure_sso_listing(cx);
                        }
                        Err(error) => {
                            this.sso_login_status = Some((
                                format!(
                                    "AWS SSO login failed for profile '{}': {}",
                                    profile_name, error
                                ),
                                false,
                            ));
                        }
                    }

                    cx.notify();
                });
            }) {
                log::warn!("Failed to apply AWS SSO login result: {:?}", err);
            }
        })
        .detach();
    }

    /// Attempts to write the profile's AWS configuration block to `~/.aws/config`.
    ///
    /// Called only for newly created profiles, not edits. Failures are logged as
    /// warnings but do not prevent the profile from being saved to DBFlux's own
    /// store — the write-back is best-effort.
    #[cfg(feature = "aws")]
    fn write_back_aws_profile(&mut self, config: &AuthProfileConfig) {
        let result = match config {
            AuthProfileConfig::AwsSso {
                profile_name,
                region,
                sso_start_url,
                sso_account_id,
                sso_role_name,
            } => append_aws_sso_profile(
                profile_name,
                sso_start_url,
                region,
                sso_account_id,
                sso_role_name,
                region,
            ),

            AuthProfileConfig::AwsSharedCredentials {
                profile_name,
                region,
            } => append_aws_shared_credentials_profile(profile_name, region),

            // Static credentials have no representation in ~/.aws/config.
            AuthProfileConfig::AwsStaticCredentials { .. } => return,
        };

        match result {
            Ok(true) => {
                log::info!(
                    "[auth_profiles] wrote profile block to ~/.aws/config"
                );
                // Invalidate the mtime cache so the next read picks up the new entry.
                self.aws_config_cache = CachedAwsConfig::new();
            }
            Ok(false) => {
                log::debug!(
                    "[auth_profiles] profile already exists in ~/.aws/config — skipped write-back"
                );
            }
            Err(err) => {
                log::warn!(
                    "[auth_profiles] failed to write profile to ~/.aws/config: {}",
                    err
                );
            }
        }
    }

    #[cfg(feature = "aws")]
    fn fetch_sso_accounts(
        &mut self,
        profile_name: String,
        region: String,
        start_url: String,
        context_key: String,
        cx: &mut Context<Self>,
    ) {
        let this = cx.entity().clone();

        let profile_name_for_error = profile_name.clone();

        let task = cx
            .background_executor()
            .spawn(async move { list_sso_accounts_blocking(&profile_name, &region, &start_url) });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(err) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    if this.sso_accounts_context_key.as_deref() != Some(context_key.as_str()) {
                        return;
                    }

                    this.sso_accounts_loading = false;

                    match result {
                        Ok(accounts) => {
                            this.sso_accounts = accounts;
                            this.sso_accounts_error = None;

                            let items = this
                                .sso_accounts
                                .iter()
                                .map(|account| {
                                    let label = if account.account_name.trim().is_empty() {
                                        account.account_id.clone()
                                    } else {
                                        format!("{} ({})", account.account_name, account.account_id)
                                    };

                                    DropdownItem::with_value(label, account.account_id.clone())
                                })
                                .collect::<Vec<_>>();

                            this.sso_account_dropdown.update(cx, |dropdown, cx| {
                                dropdown.set_items(items, cx);
                            });
                            this.sync_account_dropdown_selection(cx);
                        }
                        Err(error) => {
                            this.sso_accounts.clear();
                            this.sso_accounts_error =
                                Some(format!("profile '{}': {}", profile_name_for_error, error));

                            this.sso_account_dropdown.update(cx, |dropdown, cx| {
                                dropdown.set_items(Vec::new(), cx);
                                dropdown.set_selected_index(None, cx);
                            });
                        }
                    }

                    cx.notify();
                });
            }) {
                log::warn!("Failed to apply AWS SSO accounts listing result: {:?}", err);
            }
        })
        .detach();
    }

    #[cfg(feature = "aws")]
    fn fetch_sso_roles(
        &mut self,
        profile_name: String,
        region: String,
        start_url: String,
        account_id: String,
        context_key: String,
        cx: &mut Context<Self>,
    ) {
        let this = cx.entity().clone();

        let profile_name_for_error = profile_name.clone();

        let task = cx.background_executor().spawn(async move {
            list_sso_account_roles_blocking(&profile_name, &region, &start_url, &account_id)
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(err) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    if this.sso_roles_context_key.as_deref() != Some(context_key.as_str()) {
                        return;
                    }

                    this.sso_roles_loading = false;

                    match result {
                        Ok(roles) => {
                            this.sso_roles = roles;
                            this.sso_roles_error = None;

                            let items = this
                                .sso_roles
                                .iter()
                                .map(|role_name| {
                                    DropdownItem::with_value(role_name.clone(), role_name.clone())
                                })
                                .collect::<Vec<_>>();

                            this.sso_role_dropdown.update(cx, |dropdown, cx| {
                                dropdown.set_items(items, cx);
                            });
                            this.sync_role_dropdown_selection(cx);
                        }
                        Err(error) => {
                            this.sso_roles.clear();
                            this.sso_roles_error =
                                Some(format!("profile '{}': {}", profile_name_for_error, error));

                            this.sso_role_dropdown.update(cx, |dropdown, cx| {
                                dropdown.set_items(Vec::new(), cx);
                                dropdown.set_selected_index(None, cx);
                            });
                        }
                    }

                    cx.notify();
                });
            }) {
                log::warn!("Failed to apply AWS SSO roles listing result: {:?}", err);
            }
        })
        .detach();
    }

    fn sync_from_app_state(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let profiles = self.app_state.read(cx).auth_profiles().to_vec();

        if profiles.is_empty() {
            self.selected_profile_id = None;
            self.clear_form(window, cx);
            return;
        }

        if let Some(selected_id) = self.selected_profile_id
            && profiles.iter().any(|profile| profile.id == selected_id)
        {
            return;
        }

        self.selected_profile_id = profiles.first().map(|profile| profile.id);
        if let Some(profile_id) = self.selected_profile_id {
            self.load_profile_into_form(profile_id, window, cx);
        }
    }

    fn imported_aws_profile_names(profiles: &[AuthProfile]) -> HashSet<String> {
        profiles
            .iter()
            .filter_map(|profile| match &profile.config {
                AuthProfileConfig::AwsSso { profile_name, .. }
                | AuthProfileConfig::AwsSharedCredentials { profile_name, .. } => {
                    Some(profile_name.clone())
                }
                AuthProfileConfig::AwsStaticCredentials { .. } => None,
            })
            .collect()
    }

    fn detected_unimported_aws_profiles(&mut self, cx: &App) -> Vec<DetectedAwsProfile> {
        let imported = Self::imported_aws_profile_names(self.app_state.read(cx).auth_profiles());

        #[cfg(feature = "aws")]
        {
            self.aws_config_cache
                .profiles()
                .iter()
                .filter(|profile| !imported.contains(profile.name.as_str()))
                .map(|profile| DetectedAwsProfile {
                    name: profile.name.clone(),
                    region: profile.region.clone().unwrap_or_default(),
                    is_sso: profile.is_sso,
                    sso_start_url: profile.sso_start_url.clone(),
                })
                .collect()
        }

        #[cfg(not(feature = "aws"))]
        {
            let _ = imported;
            Vec::new()
        }
    }

    fn import_detected_aws_profiles(&mut self, cx: &mut Context<Self>) {
        let detected = self.detected_unimported_aws_profiles(cx);
        if detected.is_empty() {
            return;
        }

        let imported_count = self.app_state.update(cx, |state, cx| {
            let mut existing_names = Self::imported_aws_profile_names(state.auth_profiles());
            let mut imported_count = 0;

            for profile in detected {
                if existing_names.contains(profile.name.as_str()) {
                    continue;
                }

                let config = if profile.is_sso {
                    AuthProfileConfig::AwsSso {
                        profile_name: profile.name.clone(),
                        region: profile.region.clone(),
                        sso_start_url: profile.sso_start_url.unwrap_or_default(),
                        sso_account_id: String::new(),
                        sso_role_name: String::new(),
                    }
                } else {
                    AuthProfileConfig::AwsSharedCredentials {
                        profile_name: profile.name.clone(),
                        region: profile.region.clone(),
                    }
                };

                state.add_auth_profile(AuthProfile {
                    id: Uuid::new_v4(),
                    name: format!("AWS {}", profile.name),
                    provider_id: if profile.is_sso {
                        "aws-sso".to_string()
                    } else {
                        "aws-shared-credentials".to_string()
                    },
                    config,
                    enabled: true,
                });

                existing_names.insert(profile.name);
                imported_count += 1;
            }

            if imported_count > 0 {
                cx.emit(AppStateChanged);
            }

            imported_count
        });

        if imported_count > 0 {
            if self.selected_profile_id.is_none() {
                self.selected_profile_id = self
                    .app_state
                    .read(cx)
                    .auth_profiles()
                    .first()
                    .map(|p| p.id);
            }
            cx.notify();
        }
    }

    fn load_profile_into_form(
        &mut self,
        profile_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let profile = self
            .app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .find(|p| p.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            return;
        };

        self.selected_profile_id = Some(profile.id);
        self.editing_profile_id = Some(profile.id);
        self.profile_enabled = profile.enabled;

        Self::set_input_value(&self.input_name, profile.name, window, cx);

        match profile.config {
            AuthProfileConfig::AwsSso {
                profile_name,
                region,
                sso_start_url,
                sso_account_id,
                sso_role_name,
            } => {
                self.provider_selection = AuthProviderSelection::AwsSso;
                #[cfg(feature = "aws")]
                {
                    self.sso_login_loading = false;
                    self.sso_login_status = None;
                }
                Self::set_input_value(&self.input_sso_profile_name, profile_name, window, cx);
                Self::set_input_value(&self.input_sso_region, region, window, cx);
                Self::set_input_value(&self.input_sso_start_url, sso_start_url, window, cx);
                Self::set_input_value(&self.input_sso_account_id, sso_account_id, window, cx);
                Self::set_input_value(&self.input_sso_role_name, sso_role_name, window, cx);
            }
            AuthProfileConfig::AwsSharedCredentials {
                profile_name,
                region,
            } => {
                self.provider_selection = AuthProviderSelection::AwsSharedCredentials;
                #[cfg(feature = "aws")]
                {
                    self.sso_login_loading = false;
                    self.sso_login_status = None;
                }
                Self::set_input_value(&self.input_shared_profile_name, profile_name, window, cx);
                Self::set_input_value(&self.input_shared_region, region, window, cx);
            }
            AuthProfileConfig::AwsStaticCredentials { region } => {
                self.provider_selection = AuthProviderSelection::AwsStaticCredentials;
                #[cfg(feature = "aws")]
                {
                    self.sso_login_loading = false;
                    self.sso_login_status = None;
                }
                Self::set_input_value(&self.input_static_region, region, window, cx);
            }
        }

        #[cfg(feature = "aws")]
        {
            self.sso_accounts_context_key = None;
            self.sso_roles_context_key = None;
            self.sso_accounts_error = None;
            self.sso_roles_error = None;
        }

        cx.notify();
    }

    fn begin_create_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_profile_id = None;
        self.clear_form(window, cx);
        cx.notify();
    }

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_name.read(cx).value().to_string();
        if name.trim().is_empty() {
            return;
        }

        let (provider_id, config) = match self.provider_selection {
            AuthProviderSelection::AwsSso => (
                "aws-sso",
                AuthProfileConfig::AwsSso {
                    profile_name: self.input_sso_profile_name.read(cx).value().to_string(),
                    region: self.input_sso_region.read(cx).value().to_string(),
                    sso_start_url: self.input_sso_start_url.read(cx).value().to_string(),
                    sso_account_id: self.input_sso_account_id.read(cx).value().to_string(),
                    sso_role_name: self.input_sso_role_name.read(cx).value().to_string(),
                },
            ),
            AuthProviderSelection::AwsSharedCredentials => (
                "aws-shared-credentials",
                AuthProfileConfig::AwsSharedCredentials {
                    profile_name: self.input_shared_profile_name.read(cx).value().to_string(),
                    region: self.input_shared_region.read(cx).value().to_string(),
                },
            ),
            AuthProviderSelection::AwsStaticCredentials => (
                "aws-static-credentials",
                AuthProfileConfig::AwsStaticCredentials {
                    region: self.input_static_region.read(cx).value().to_string(),
                },
            ),
        };

        let profile_id = self.editing_profile_id.unwrap_or_else(Uuid::new_v4);

        let profile = AuthProfile {
            id: profile_id,
            name,
            provider_id: provider_id.to_string(),
            config,
            enabled: self.profile_enabled,
        };

        let is_edit = self.editing_profile_id.is_some();
        self.app_state.update(cx, |state, cx| {
            if is_edit {
                state.update_auth_profile(profile.clone());
            } else {
                state.add_auth_profile(profile.clone());
            }

            cx.emit(AppStateChanged);
        });

        #[cfg(feature = "aws")]
        if !is_edit {
            self.write_back_aws_profile(&profile.config);
        }

        self.selected_profile_id = Some(profile_id);
        self.load_profile_into_form(profile_id, window, cx);
    }

    fn request_delete_selected_profile(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_profile_id = self.editing_profile_id;
        cx.notify();
    }

    fn confirm_delete_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(profile_id) = self.pending_delete_profile_id.take() else {
            return;
        };

        self.app_state.update(cx, |state, cx| {
            let affected: Vec<_> = state
                .profiles()
                .iter()
                .filter(|profile| {
                    profile.auth_profile_id == Some(profile_id)
                        || matches!(
                            profile.access_kind,
                            Some(AccessKind::Ssm {
                                auth_profile_id: Some(id),
                                ..
                            }) if id == profile_id
                        )
                })
                .cloned()
                .collect();

            for mut profile in affected {
                if profile.auth_profile_id == Some(profile_id) {
                    profile.auth_profile_id = None;
                }

                if let Some(AccessKind::Ssm {
                    auth_profile_id, ..
                }) = profile.access_kind.as_mut()
                    && *auth_profile_id == Some(profile_id)
                {
                    *auth_profile_id = None;
                }

                state.update_profile(profile);
            }

            if let Some(index) = state
                .auth_profiles()
                .iter()
                .position(|profile| profile.id == profile_id)
            {
                state.remove_auth_profile(index);
            }

            cx.emit(AppStateChanged);
        });

        self.editing_profile_id = None;
        self.selected_profile_id = self
            .app_state
            .read(cx)
            .auth_profiles()
            .first()
            .map(|p| p.id);

        if let Some(selected_id) = self.selected_profile_id {
            self.load_profile_into_form(selected_id, window, cx);
        } else {
            self.clear_form(window, cx);
        }

        cx.notify();
    }

    fn cancel_delete_profile(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_profile_id = None;
        cx.notify();
    }

    fn profiles_using_auth(&self, auth_id: Uuid, cx: &Context<Self>) -> usize {
        self.app_state
            .read(cx)
            .profiles()
            .iter()
            .filter(|profile| {
                profile.auth_profile_id == Some(auth_id)
                    || matches!(
                        profile.access_kind,
                        Some(AccessKind::Ssm {
                            auth_profile_id: Some(id),
                            ..
                        }) if id == auth_id
                    )
            })
            .count()
    }

    fn render_import_banner(
        &self,
        detected_profiles: &[DetectedAwsProfile],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let names_preview = detected_profiles
            .iter()
            .take(3)
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let has_more = detected_profiles.len() > 3;
        let preview_suffix = if has_more { ", ..." } else { "" };

        div()
            .m_4()
            .p_3()
            .rounded(px(8.0))
            .border_1()
            .border_color(theme.primary)
            .bg(theme.secondary)
            .flex()
            .items_center()
            .justify_between()
            .gap_3()
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap_2()
                    .child(Icon::new(IconName::Info).size_4().text_color(theme.primary))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child(format!(
                                        "Detected {} AWS profile{} not imported in DBFlux",
                                        detected_profiles.len(),
                                        if detected_profiles.len() == 1 {
                                            ""
                                        } else {
                                            "s"
                                        }
                                    )),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(format!("{}{}", names_preview, preview_suffix)),
                            ),
                    ),
            )
            .child(
                Button::new("import-detected-aws-auth-profiles")
                    .label("Import")
                    .small()
                    .primary()
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.import_detected_aws_profiles(cx);
                    })),
            )
    }

    fn render_input_row(&self, label: &str, input: &Entity<InputState>) -> impl IntoElement {
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

    fn render_dropdown_row(&self, label: &str, dropdown: &Entity<Dropdown>) -> impl IntoElement {
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
            .child(dropdown.clone())
    }

    fn render_provider_selector(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let primary = theme.primary;

        let button = |id: &'static str,
                      label: &'static str,
                      selected: bool,
                      selection: AuthProviderSelection,
                      cx: &mut Context<Self>| {
            div()
                .rounded(px(6.0))
                .border_1()
                .border_color(if selected {
                    primary
                } else {
                    transparent_black()
                })
                .child(
                    Button::new(id)
                        .label(label)
                        .small()
                        .ghost()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.provider_selection = selection;
                            #[cfg(feature = "aws")]
                            {
                                this.sso_login_loading = false;
                                this.sso_login_status = None;
                            }
                            cx.notify();
                        })),
                )
        };

        div()
            .flex()
            .items_center()
            .gap_2()
            .child(button(
                "auth-provider-sso",
                "AWS SSO",
                self.provider_selection == AuthProviderSelection::AwsSso,
                AuthProviderSelection::AwsSso,
                cx,
            ))
            .child(button(
                "auth-provider-shared",
                "AWS Shared Credentials",
                self.provider_selection == AuthProviderSelection::AwsSharedCredentials,
                AuthProviderSelection::AwsSharedCredentials,
                cx,
            ))
            .child(button(
                "auth-provider-static",
                "AWS Static Credentials",
                self.provider_selection == AuthProviderSelection::AwsStaticCredentials,
                AuthProviderSelection::AwsStaticCredentials,
                cx,
            ))
    }

    fn render_profile_list(
        &self,
        profiles: &[AuthProfile],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .w(px(280.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div()
                    .p_3()
                    .border_b_1()
                    .border_color(theme.border)
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Profiles"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("{} total", profiles.len())),
                    )
                    .child(
                        Button::new("new-auth-profile")
                            .icon(Icon::new(IconName::Plus))
                            .label("New Auth Profile")
                            .small()
                            .w_full()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.begin_create_profile(window, cx);
                            })),
                    ),
            )
            .child(
                div()
                    .id("auth-profiles-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(profiles.is_empty(), |root| {
                        root.child(
                            div()
                                .p_3()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No auth profiles yet"),
                        )
                    })
                    .children(profiles.iter().map(|profile| {
                        let profile_id = profile.id;
                        let is_selected = self.selected_profile_id == Some(profile_id);
                        let provider = match profile.provider_id.as_str() {
                            "aws-sso" => "AWS SSO",
                            "aws-shared-credentials" => "AWS Shared Credentials",
                            "aws-static-credentials" => "AWS Static Credentials",
                            _ => profile.provider_id.as_str(),
                        }
                        .to_string();

                        div()
                            .id(SharedString::from(format!(
                                "auth-profile-item-{}",
                                profile_id
                            )))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .when(is_selected, |div| div.bg(theme.secondary))
                            .hover({
                                let secondary = theme.secondary;
                                move |div| div.bg(secondary)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.load_profile_into_form(profile_id, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::MEDIUM)
                                            .child(profile.name.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(provider),
                                    )
                                    .when(!profile.enabled, |container| {
                                        container.child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.warning)
                                                .child("Disabled"),
                                        )
                                    }),
                            )
                    })),
            )
    }

    fn render_editor_panel(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_editing = self.editing_profile_id.is_some();

        #[cfg(feature = "aws")]
        if self.provider_selection == AuthProviderSelection::AwsSso {
            self.ensure_sso_listing(cx);
            self.sync_account_dropdown_selection(cx);
            self.sync_role_dropdown_selection(cx);
        }

        div()
            .flex_1()
            .h_full()
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
                            .text_base()
                            .font_weight(FontWeight::MEDIUM)
                            .child(if is_editing {
                                "Edit Auth Profile"
                            } else {
                                "New Auth Profile"
                            }),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.muted_foreground)
                            .child("Reusable authentication profile for SSM access"),
                    ),
            )
            .child(
                div()
                    .id("auth-profiles-editor-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(self.render_input_row("Name", &self.input_name))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Provider"),
                            )
                            .child(self.render_provider_selector(cx)),
                    )
                    .when(
                        self.provider_selection == AuthProviderSelection::AwsSso,
                        |container| {
                            #[cfg(feature = "aws")]
                            let account_fallback_to_input =
                                !self.sso_accounts_loading
                                    && (self.sso_accounts_error.is_some()
                                        || self.sso_accounts.is_empty());
                            #[cfg(feature = "aws")]
                            let role_fallback_to_input = !self.sso_roles_loading
                                && (self.sso_roles_error.is_some() || self.sso_roles.is_empty());

                            #[cfg(not(feature = "aws"))]
                            let account_fallback_to_input = true;
                            #[cfg(not(feature = "aws"))]
                            let role_fallback_to_input = true;

                            let content = container
                                .child(self.render_input_row(
                                    "AWS Profile Name",
                                    &self.input_sso_profile_name,
                                ))
                                .child(self.render_input_row("Region", &self.input_sso_region))
                                .child(
                                    self.render_input_row(
                                        "SSO Start URL",
                                        &self.input_sso_start_url,
                                    ),
                                );

                            #[cfg(feature = "aws")]
                            let content = content.child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        Button::new("auth-sso-login")
                                            .label(if self.sso_login_loading {
                                                "Logging in..."
                                            } else {
                                                "Login"
                                            })
                                            .small()
                                            .primary()
                                            .disabled(self.sso_login_loading)
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.login_sso_profile(cx);
                                            })),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child("Runs AWS SSO login for the selected profile"),
                                    ),
                            );

                            content
                                .when(!account_fallback_to_input, |content| {
                                    content.child(self.render_dropdown_row(
                                        "SSO Account ID",
                                        &self.sso_account_dropdown,
                                    ))
                                })
                                .when(account_fallback_to_input, |content| {
                                    content.child(self.render_input_row(
                                        "SSO Account ID",
                                        &self.input_sso_account_id,
                                    ))
                                })
                                .when(!role_fallback_to_input, |content| {
                                    content.child(self.render_dropdown_row(
                                        "SSO Role Name",
                                        &self.sso_role_dropdown,
                                    ))
                                })
                                .when(role_fallback_to_input, |content| {
                                    content.child(self.render_input_row(
                                        "SSO Role Name",
                                        &self.input_sso_role_name,
                                    ))
                                })
                                .when(self.provider_selection == AuthProviderSelection::AwsSso, |content| {
                                    #[cfg(feature = "aws")]
                                    {
                                        let account_status = if self.sso_accounts_loading {
                                            Some(("Loading SSO accounts...".to_string(), false))
                                        } else if let Some(error) = &self.sso_accounts_error {
                                            Some((format!("Account listing failed: {}", error), true))
                                        } else if self.sso_accounts.is_empty() {
                                            if self.sso_listing_context_from_form(cx).is_some() {
                                                Some((
                                                    "No SSO accounts found for the current session"
                                                        .to_string(),
                                                    false,
                                                ))
                                            } else {
                                                Some((
                                                    "Provide AWS Profile Name, Region, and SSO Start URL to load accounts"
                                                        .to_string(),
                                                    false,
                                                ))
                                            }
                                        } else {
                                            None
                                        };

                                        let role_status = if self.sso_roles_loading {
                                            Some(("Loading account roles...".to_string(), false))
                                        } else if let Some(error) = &self.sso_roles_error {
                                            Some((format!("Role listing failed: {}", error), true))
                                        } else if self.input_sso_account_id.read(cx).value().trim().is_empty() {
                                            Some((
                                                "Select an account to load available roles"
                                                    .to_string(),
                                                false,
                                            ))
                                        } else if self.sso_roles.is_empty() {
                                            Some((
                                                "No roles found for the selected account"
                                                    .to_string(),
                                                false,
                                            ))
                                        } else {
                                            None
                                        };

                                        let mut content = content;

                                        if let Some((text, is_error)) = account_status {
                                            content = content.child(
                                                div()
                                                    .text_xs()
                                                    .text_color(if is_error {
                                                        theme.warning
                                                    } else {
                                                        theme.muted_foreground
                                                    })
                                                    .child(text),
                                            );
                                        }

                                        if let Some((text, is_error)) = role_status {
                                            content = content.child(
                                                div()
                                                    .text_xs()
                                                    .text_color(if is_error {
                                                        theme.warning
                                                    } else {
                                                        theme.muted_foreground
                                                    })
                                                    .child(text),
                                            );
                                        }

                                        if let Some((text, is_success)) = &self.sso_login_status {
                                            content = content.child(
                                                div()
                                                    .text_xs()
                                                    .text_color(if *is_success {
                                                        theme.success
                                                    } else if self.sso_login_loading {
                                                        theme.muted_foreground
                                                    } else {
                                                        theme.warning
                                                    })
                                                    .child(text.clone()),
                                            );
                                        }

                                        content
                                    }

                                    #[cfg(not(feature = "aws"))]
                                    {
                                        content
                                    }
                                })
                        },
                    )
                    .when(
                        self.provider_selection == AuthProviderSelection::AwsSharedCredentials,
                        |container| {
                            container
                                .child(self.render_input_row(
                                    "AWS Profile Name",
                                    &self.input_shared_profile_name,
                                ))
                                .child(self.render_input_row("Region", &self.input_shared_region))
                        },
                    )
                    .when(
                        self.provider_selection == AuthProviderSelection::AwsStaticCredentials,
                        |container| {
                            container
                                .child(self.render_input_row("Region", &self.input_static_region))
                        },
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Checkbox::new("auth-profile-enabled")
                                    .checked(self.profile_enabled)
                                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                        this.profile_enabled = *checked;
                                        cx.notify();
                                    })),
                            )
                            .child(div().text_sm().child("Enabled")),
                    ),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .gap_2()
                    .justify_end()
                    .when(is_editing, |root| {
                        root.child(
                            Button::new("delete-auth-profile")
                                .label("Delete")
                                .small()
                                .danger()
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.request_delete_selected_profile(cx);
                                })),
                        )
                    })
                    .child(
                        Button::new("cancel-auth-profile")
                            .label("Cancel")
                            .small()
                            .on_click(cx.listener(|this, _, window, cx| {
                                if let Some(selected_id) = this.selected_profile_id {
                                    this.load_profile_into_form(selected_id, window, cx);
                                } else {
                                    this.clear_form(window, cx);
                                    cx.notify();
                                }
                            })),
                    )
                    .child(
                        Button::new("save-auth-profile")
                            .label(if is_editing { "Update" } else { "Create" })
                            .small()
                            .primary()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_profile(window, cx);
                            })),
                    ),
            )
    }
}

impl SettingsSection for AuthProfilesSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::AuthProfiles
    }
}

impl Render for AuthProfilesSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_app_state {
            self.pending_sync_from_app_state = false;
            self.sync_from_app_state(window, cx);
        }

        let profiles = self.app_state.read(cx).auth_profiles().to_vec();
        let detected_profiles = self.detected_unimported_aws_profiles(cx);
        let show_delete_dialog = self.pending_delete_profile_id.is_some();

        let (delete_name, affected_connections) = self
            .pending_delete_profile_id
            .and_then(|profile_id| {
                profiles
                    .iter()
                    .find(|profile| profile.id == profile_id)
                    .map(|profile| {
                        (
                            profile.name.clone(),
                            self.profiles_using_auth(profile_id, cx),
                        )
                    })
            })
            .unwrap_or_else(|| (String::new(), 0));

        div()
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Auth Profiles"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child("Manage reusable authentication profiles for connection access"),
                    ),
            )
            .when(!detected_profiles.is_empty(), |root| {
                root.child(self.render_import_banner(&detected_profiles, cx))
            })
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_profile_list(&profiles, cx))
                    .child(self.render_editor_panel(cx)),
            )
            .when(show_delete_dialog, |element| {
                let entity = cx.entity().clone();
                let entity_cancel = entity.clone();

                let body = if affected_connections > 0 {
                    format!(
                        "Are you sure you want to delete \"{}\"? {} connection{} using this auth profile will be updated.",
                        delete_name,
                        affected_connections,
                        if affected_connections == 1 { "" } else { "s" }
                    )
                } else {
                    format!("Are you sure you want to delete \"{}\"?", delete_name)
                };

                element.child(
                    Dialog::new(window, cx)
                        .title("Delete Auth Profile")
                        .confirm()
                        .on_ok(move |_, window, cx| {
                            entity.update(cx, |section, cx| {
                                section.confirm_delete_profile(window, cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            entity_cancel.update(cx, |section, cx| {
                                section.cancel_delete_profile(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(body)),
                )
            })
    }
}
