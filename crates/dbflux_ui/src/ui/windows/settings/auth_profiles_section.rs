use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::{FormSection, create_blur_subscription};
use super::layout;
use super::section_trait::SectionFocusEvent;
use crate::app::{AppStateChanged, AppStateEntity};
use crate::keymap::{Modifiers, key_chord_from_gpui};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::InputState;
use dbflux_components::controls::{Button, Checkbox, Input};
use dbflux_components::primitives::focus_frame;
use dbflux_components::primitives::{Icon as FluxIcon, Label, Text};
use dbflux_core::{AccessKind, AuthProfile, FormFieldKind, ImportableProfile};
use gpui::prelude::*;
use gpui::*;
use gpui_component::dialog::Dialog;
use gpui_component::{ActiveTheme, Icon, IconName};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(feature = "aws")]
use dbflux_aws::{
    AwsSsoAccount, list_sso_account_roles_blocking, list_sso_accounts_blocking, login_sso_blocking,
};

pub(super) struct AuthProfilesSection {
    app_state: Entity<AppStateEntity>,
    selected_profile_id: Option<Uuid>,
    editing_profile_id: Option<Uuid>,
    selected_provider_id: Option<String>,
    provider_entries_cache: Vec<(String, String)>,
    profile_enabled: bool,
    pending_delete_profile_id: Option<Uuid>,
    pending_sync_from_app_state: bool,

    input_name: Entity<InputState>,
    form_inputs: HashMap<String, Entity<InputState>>,
    provider_field_order: Vec<String>,

    #[cfg(feature = "aws")]
    sso_account_dropdown: Entity<Dropdown>,
    #[cfg(feature = "aws")]
    sso_role_dropdown: Entity<Dropdown>,
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

    auth_focus: AuthFocus,
    auth_form_field: AuthFormField,
    auth_editing_field: bool,
    content_focused: bool,
    profile_list_scroll_handle: ScrollHandle,
    pending_profile_scroll_idx: Option<usize>,
    switching_input: bool,

    _subscriptions: Vec<Subscription>,
    _blur_subscriptions: Vec<Subscription>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum AuthFocus {
    ProfileList,
    Form,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum AuthFormField {
    Name,
    Provider(usize),
    DynamicField(usize),
    #[cfg(feature = "aws")]
    SsoLogin,
    #[cfg(feature = "aws")]
    SsoAccount,
    #[cfg(feature = "aws")]
    SsoRole,
    Enabled,
    DeleteButton,
    SaveButton,
}

impl EventEmitter<SectionFocusEvent> for AuthProfilesSection {}

impl AuthProfilesSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let selected_profile_id = app_state.read(cx).auth_profiles().first().map(|p| p.id);
        let selected_provider_id = app_state
            .read(cx)
            .auth_provider_registry()
            .providers()
            .next()
            .map(|provider| provider.provider_id().to_string());

        let provider_entries_cache = app_state
            .read(cx)
            .auth_provider_registry()
            .providers()
            .map(|provider| {
                (
                    provider.provider_id().to_string(),
                    provider.display_name().to_string(),
                )
            })
            .collect();

        let input_name = cx.new(|cx| InputState::new(window, cx).placeholder("Profile name"));

        #[cfg(feature = "aws")]
        let sso_account_dropdown =
            cx.new(|_cx| Dropdown::new("auth-sso-account-dropdown").placeholder("Select account"));
        #[cfg(feature = "aws")]
        let sso_role_dropdown =
            cx.new(|_cx| Dropdown::new("auth-sso-role-dropdown").placeholder("Select role"));

        let app_state_subscription =
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.pending_sync_from_app_state = true;
                cx.notify();
            });

        #[cfg(feature = "aws")]
        let sso_account_dropdown_sub = cx.subscribe_in(
            &sso_account_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                this.set_form_value_in_window(
                    "sso_account_id",
                    event.item.value.to_string(),
                    window,
                    cx,
                );
                this.set_form_value_in_window("sso_role_name", "", window, cx);

                this.sso_roles.clear();
                this.sso_roles_error = None;
                this.sso_roles_loading = false;
                this.sso_roles_context_key = None;

                this.sso_role_dropdown.update(cx, |dropdown, cx| {
                    dropdown.set_items(Vec::new(), cx);
                    dropdown.set_selected_index(None, cx);
                });

                cx.notify();
            },
        );

        #[cfg(feature = "aws")]
        let sso_role_dropdown_sub = cx.subscribe_in(
            &sso_role_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                this.set_form_value_in_window(
                    "sso_role_name",
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
            selected_provider_id,
            provider_entries_cache,
            profile_enabled: true,
            pending_delete_profile_id: None,
            pending_sync_from_app_state: false,
            input_name,
            form_inputs: HashMap::new(),
            provider_field_order: Vec::new(),

            #[cfg(feature = "aws")]
            sso_account_dropdown,
            #[cfg(feature = "aws")]
            sso_role_dropdown,
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

            auth_focus: AuthFocus::ProfileList,
            auth_form_field: AuthFormField::Name,
            auth_editing_field: false,
            content_focused: false,
            profile_list_scroll_handle: ScrollHandle::new(),
            pending_profile_scroll_idx: None,
            switching_input: false,

            _subscriptions: vec![
                app_state_subscription,
                #[cfg(feature = "aws")]
                sso_account_dropdown_sub,
                #[cfg(feature = "aws")]
                sso_role_dropdown_sub,
            ],
            _blur_subscriptions: Vec::new(),
        };

        section.rebuild_form_inputs(window, cx);

        if let Some(profile_id) = section.selected_profile_id {
            section.load_profile_into_form(profile_id, window, cx);
        }

        section
    }

    fn provider_entries(&self, cx: &App) -> Vec<(String, String)> {
        self.app_state
            .read(cx)
            .auth_provider_registry()
            .providers()
            .map(|provider| {
                (
                    provider.provider_id().to_string(),
                    provider.display_name().to_string(),
                )
            })
            .collect()
    }

    fn selected_provider(&self, cx: &App) -> Option<Arc<dyn dbflux_core::DynAuthProvider>> {
        self.selected_provider_id
            .as_deref()
            .and_then(|provider_id| self.app_state.read(cx).auth_provider_by_id(provider_id))
    }

    fn rebuild_form_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(provider) = self.selected_provider(cx) else {
            self.form_inputs.clear();
            self.provider_field_order.clear();
            self.rebuild_blur_subscriptions(cx);
            return;
        };

        let field_defs = provider
            .form_def()
            .tabs
            .iter()
            .flat_map(|tab| tab.sections.iter())
            .flat_map(|section| section.fields.iter())
            .map(|field| {
                (
                    field.id.clone(),
                    field.placeholder.clone(),
                    field.kind.clone(),
                )
            })
            .collect::<Vec<_>>();

        let expected_ids = field_defs
            .iter()
            .map(|(field_id, _, _)| field_id.clone())
            .collect::<HashSet<_>>();

        self.form_inputs
            .retain(|field_id, _| expected_ids.contains(field_id));

        self.provider_field_order = field_defs
            .iter()
            .map(|(field_id, _, _)| field_id.clone())
            .collect();

        for (field_id, placeholder, kind) in field_defs {
            if self.form_inputs.contains_key(&field_id) {
                continue;
            }

            let input = cx.new(|cx| {
                let state = InputState::new(window, cx).placeholder(placeholder);
                match kind {
                    FormFieldKind::Password => state.masked(true),
                    _ => state,
                }
            });

            self.form_inputs.insert(field_id, input);
        }

        self.rebuild_blur_subscriptions(cx);
    }

    fn rebuild_blur_subscriptions(&mut self, cx: &mut Context<Self>) {
        let mut subs = Vec::new();

        subs.push(create_blur_subscription(cx, &self.input_name.clone()));

        for input in self.form_inputs.values() {
            subs.push(create_blur_subscription(cx, input));
        }

        self._blur_subscriptions = subs;
    }

    fn form_value(&self, field_id: &str, cx: &App) -> String {
        self.form_inputs
            .get(field_id)
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default()
    }

    fn set_form_value_in_window(
        &mut self,
        field_id: &str,
        value: impl Into<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(input) = self.form_inputs.get(field_id) {
            input.update(cx, |state, cx| {
                state.set_value(value.into(), window, cx);
            });
        }
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_profile_id = None;
        self.profile_enabled = true;

        self.input_name.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        if self.selected_provider_id.is_none() {
            self.selected_provider_id = self
                .provider_entries(cx)
                .first()
                .map(|(provider_id, _)| provider_id.clone());
        }

        self.rebuild_form_inputs(window, cx);

        for input in self.form_inputs.values() {
            input.update(cx, |state, cx| {
                state.set_value("", window, cx);
            });
        }

        #[cfg(feature = "aws")]
        self.reset_sso_listing_state(cx);
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

    fn profile_ids(&self, cx: &App) -> Vec<Uuid> {
        self.app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .map(|profile| profile.id)
            .collect()
    }

    fn selected_profile_index(&self, profile_ids: &[Uuid]) -> Option<usize> {
        self.selected_profile_id
            .and_then(|profile_id| profile_ids.iter().position(|id| *id == profile_id))
    }

    fn load_profile_at_index(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let profile_ids = self.profile_ids(cx);
        if let Some(profile_id) = profile_ids.get(index).copied() {
            self.pending_profile_scroll_idx = Some(index);
            self.load_profile_into_form(profile_id, window, cx);
        }
    }

    fn move_profile_selection(&mut self, step: isize, window: &mut Window, cx: &mut Context<Self>) {
        let profile_ids = self.profile_ids(cx);
        if profile_ids.is_empty() {
            return;
        }

        let current_index = self.selected_profile_index(&profile_ids).unwrap_or(0);
        let current_index = current_index as isize;
        let next_index = (current_index + step).clamp(0, profile_ids.len() as isize - 1) as usize;

        self.load_profile_at_index(next_index, window, cx);
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.content_focused || self.pending_delete_profile_id.is_some() {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match self.auth_focus {
            AuthFocus::ProfileList => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.move_profile_selection(1, window, cx);
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.move_profile_selection(-1, window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.load_profile_at_index(0, window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    let profile_count = self.profile_ids(cx).len();
                    if profile_count > 0 {
                        self.load_profile_at_index(profile_count - 1, window, cx);
                        cx.notify();
                    }
                }
                ("n", modifiers) if modifiers == Modifiers::none() => {
                    self.begin_create_profile(window, cx);
                }
                ("d", modifiers) if modifiers == Modifiers::none() => {
                    self.request_delete_selected_profile(cx);
                }
                ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                    if modifiers == Modifiers::none() =>
                {
                    self.enter_form(window, cx);
                    cx.notify();
                }
                _ => {}
            },
            AuthFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.move_down();
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.move_up();
                    cx.notify();
                }
                ("h", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("left", modifiers) if modifiers == Modifiers::none() => {
                    self.move_left();
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) if modifiers == Modifiers::none() => {
                    self.move_right();
                    cx.notify();
                }
                ("enter", modifiers) if modifiers == Modifiers::none() => {
                    self.activate_current_field(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.tab_next();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.tab_prev();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.move_first();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    self.move_last();
                    cx.notify();
                }
                _ => {}
            },
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
            .find(|profile| profile.id == profile_id)
            .cloned();

        let Some(profile) = profile else {
            return;
        };

        self.selected_profile_id = Some(profile.id);
        self.editing_profile_id = Some(profile.id);
        self.profile_enabled = profile.enabled;
        self.selected_provider_id = Some(profile.provider_id.clone());

        self.input_name.update(cx, |state, cx| {
            state.set_value(profile.name.clone(), window, cx);
        });

        self.rebuild_form_inputs(window, cx);

        for (field_id, input) in &self.form_inputs {
            let value = profile.fields.get(field_id).cloned().unwrap_or_default();
            input.update(cx, |state, cx| {
                state.set_value(value.clone(), window, cx);
            });
        }

        #[cfg(feature = "aws")]
        {
            self.sso_accounts_context_key = None;
            self.sso_roles_context_key = None;
            self.sso_accounts_error = None;
            self.sso_roles_error = None;
            self.sso_login_loading = false;
            self.sso_login_status = None;
        }

        cx.notify();
    }

    fn begin_create_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_profile_id = None;
        self.clear_form(window, cx);
        self.enter_form(window, cx);
        cx.notify();
    }

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let name = self.input_name.read(cx).value().trim().to_string();
        if name.is_empty() {
            return;
        }

        let Some(provider_id) = self.selected_provider_id.clone() else {
            return;
        };

        let profile_id = self.editing_profile_id.unwrap_or_else(Uuid::new_v4);
        let fields = self
            .form_inputs
            .iter()
            .map(|(field_id, input)| (field_id.clone(), input.read(cx).value().to_string()))
            .collect::<HashMap<_, _>>();

        let profile = AuthProfile {
            id: profile_id,
            name,
            provider_id,
            fields,
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

        if !is_edit
            && let Some(provider) = self
                .app_state
                .read(cx)
                .auth_provider_by_id(&profile.provider_id)
        {
            provider.after_profile_saved(&profile);
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
                            &profile.access_kind,
                            Some(AccessKind::Managed { params, .. })
                                if params.get("auth_profile_id")
                                    .and_then(|s| s.parse::<uuid::Uuid>().ok())
                                    == Some(profile_id)
                        )
                })
                .cloned()
                .collect();

            for mut profile in affected {
                if profile.auth_profile_id == Some(profile_id) {
                    profile.auth_profile_id = None;
                }

                if let Some(AccessKind::Managed { params, .. }) = profile.access_kind.as_mut()
                    && params
                        .get("auth_profile_id")
                        .and_then(|s| s.parse::<uuid::Uuid>().ok())
                        == Some(profile_id)
                {
                    params.remove("auth_profile_id");
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
            .map(|profile| profile.id);

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
                        &profile.access_kind,
                        Some(AccessKind::Managed { params, .. })
                            if params.get("auth_profile_id")
                                .and_then(|s| s.parse::<uuid::Uuid>().ok())
                                == Some(auth_id)
                    )
            })
            .count()
    }

    fn imported_profile_keys(profiles: &[AuthProfile]) -> HashSet<String> {
        profiles
            .iter()
            .map(|profile| {
                let name = profile
                    .fields
                    .get("profile_name")
                    .cloned()
                    .unwrap_or_else(|| profile.name.clone());
                format!("{}::{}", profile.provider_id, name)
            })
            .collect()
    }

    fn detected_unimported_profiles(&self, cx: &App) -> Vec<ImportableProfile> {
        let state = self.app_state.read(cx);
        let imported = Self::imported_profile_keys(state.auth_profiles());

        state
            .auth_provider_registry()
            .providers()
            .flat_map(|provider| provider.detect_importable_profiles())
            .filter(|profile| {
                let name = profile
                    .fields
                    .get("profile_name")
                    .cloned()
                    .unwrap_or_else(|| profile.display_name.clone());
                let key = format!("{}::{}", profile.provider_id, name);
                !imported.contains(&key)
            })
            .collect()
    }

    fn import_detected_profiles(&mut self, cx: &mut Context<Self>) {
        let detected = self.detected_unimported_profiles(cx);
        if detected.is_empty() {
            return;
        }

        let imported_count = self.app_state.update(cx, |state, cx| {
            let mut existing = Self::imported_profile_keys(state.auth_profiles());
            let mut imported_count = 0;

            for profile in detected {
                let key_name = profile
                    .fields
                    .get("profile_name")
                    .cloned()
                    .unwrap_or_else(|| profile.display_name.clone());
                let key = format!("{}::{}", profile.provider_id, key_name);
                if existing.contains(&key) {
                    continue;
                }

                state.add_auth_profile(AuthProfile {
                    id: Uuid::new_v4(),
                    name: profile.display_name,
                    provider_id: profile.provider_id,
                    fields: profile.fields,
                    enabled: true,
                });

                existing.insert(key);
                imported_count += 1;
            }

            if imported_count > 0 {
                cx.emit(AppStateChanged);
            }

            imported_count
        });

        if imported_count > 0 {
            cx.notify();
        }
    }

    fn render_import_banner(
        &self,
        detected_profiles: &[ImportableProfile],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let names_preview = detected_profiles
            .iter()
            .take(3)
            .map(|profile| profile.display_name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

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
                    .child(
                        FluxIcon::new(IconName::Info)
                            .size(px(16.0))
                            .color(theme.primary),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(Label::new(format!(
                                "Detected {} importable profile{}",
                                detected_profiles.len(),
                                if detected_profiles.len() == 1 {
                                    ""
                                } else {
                                    "s"
                                }
                            )))
                            .child(Text::caption(names_preview)),
                    ),
            )
            .child(
                Button::new("import-detected-auth-profiles", "Import")
                    .small()
                    .primary()
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.import_detected_profiles(cx);
                    })),
            )
    }

    fn render_input_row(
        &self,
        label: &str,
        input: &Entity<InputState>,
        field: AuthFormField,
        is_focused: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let primary = cx.theme().primary;

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()))
            .child(
                focus_frame(
                    is_focused,
                    Some(primary),
                    layout::compact_input_shell(Input::new(input).small()),
                    cx,
                )
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, window, cx| {
                        this.switching_input = true;
                        this.auth_focus = AuthFocus::Form;
                        this.auth_form_field = field;
                        this.focus_current_field(window, cx);
                        cx.notify();
                    }),
                ),
            )
    }

    #[cfg(feature = "aws")]
    fn render_dropdown_row(&self, label: &str, dropdown: &Entity<Dropdown>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()))
            .child(dropdown.clone())
    }

    fn render_provider_selector(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let providers = self.provider_entries(cx);

        div()
            .flex()
            .items_center()
            .gap_2()
            .children(providers.into_iter().map(|(provider_id, label)| {
                let selected = self.selected_provider_id.as_deref() == Some(provider_id.as_str());

                div()
                    .rounded(px(6.0))
                    .border_1()
                    .border_color(if selected {
                        theme.primary
                    } else {
                        transparent_black()
                    })
                    .child(
                        Button::new(
                            SharedString::from(format!("auth-provider-{}", provider_id)),
                            label,
                        )
                        .small()
                        .ghost()
                        .on_click(cx.listener(
                            move |this, _, window, cx| {
                                this.selected_provider_id = Some(provider_id.clone());
                                this.rebuild_form_inputs(window, cx);

                                for input in this.form_inputs.values() {
                                    input.update(cx, |state, cx| {
                                        state.set_value("", window, cx);
                                    });
                                }

                                #[cfg(feature = "aws")]
                                this.reset_sso_listing_state(cx);

                                cx.notify();
                            },
                        )),
                    )
            }))
    }

    fn render_profile_list(
        &mut self,
        profiles: &[AuthProfile],
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let list_focused = self.content_focused && self.auth_focus == AuthFocus::ProfileList;

        if let Some(scroll_idx) = self.pending_profile_scroll_idx.take() {
            self.profile_list_scroll_handle.scroll_to_item(scroll_idx);
        }

        div()
            .w(px(280.0))
            .h_full()
            .min_h_0()
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
                    .child(Label::new("Profiles"))
                    .child(
                        Button::new("new-auth-profile", "New Auth Profile")
                            .icon(Icon::new(IconName::Plus))
                            .small()
                            .w_full()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.auth_focus = AuthFocus::Form;
                                this.begin_create_profile(window, cx);
                            })),
                    ),
            )
            .child(
                div()
                    .id("auth-profile-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.profile_list_scroll_handle)
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .children(profiles.iter().map(|profile| {
                        let profile_id = profile.id;
                        let is_selected = self.selected_profile_id == Some(profile_id);
                        let is_focused = list_focused && is_selected;
                        let provider_label = self
                            .app_state
                            .read(cx)
                            .auth_provider_by_id(&profile.provider_id)
                            .map(|provider| provider.display_name().to_string())
                            .unwrap_or_else(|| profile.provider_id.clone());

                        div()
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if is_focused {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .when(is_selected, |div| div.bg(theme.secondary))
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    this.switching_input = true;
                                    this.load_profile_into_form(profile_id, window, cx);
                                }),
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(Label::new(profile.name.clone()))
                                    .child(Text::caption(provider_label)),
                            )
                    })),
            )
    }

    #[cfg(feature = "aws")]
    fn is_aws_sso_selected(&self) -> bool {
        self.selected_provider_id.as_deref() == Some("aws-sso")
    }

    #[cfg(feature = "aws")]
    fn sso_listing_context_from_form(
        &self,
        cx: &Context<Self>,
    ) -> Option<(String, String, String)> {
        let profile_name = self.form_value("profile_name", cx).trim().to_string();
        let region = self.form_value("region", cx).trim().to_string();
        let start_url = self.form_value("sso_start_url", cx).trim().to_string();

        if profile_name.is_empty() || region.is_empty() || start_url.is_empty() {
            return None;
        }

        Some((profile_name, region, start_url))
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
    fn sync_account_dropdown_selection(&self, cx: &mut Context<Self>) {
        let selected = self.form_value("sso_account_id", cx);
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
        let selected = self.form_value("sso_role_name", cx);
        let selected_index = self
            .sso_roles
            .iter()
            .position(|role_name| role_name == &selected);

        self.sso_role_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(selected_index, cx);
        });
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

        let account_id = self.form_value("sso_account_id", cx).trim().to_string();
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

        let sso_account_id = self.form_value("sso_account_id", cx);
        let sso_role_name = self.form_value("sso_role_name", cx);

        self.sso_login_loading = true;
        self.sso_login_status = Some((
            format!("Starting AWS SSO login for profile '{}'...", profile_name),
            false,
        ));
        cx.notify();

        let this = cx.entity().clone();
        let profile_name_for_result = profile_name.clone();
        let task = cx.background_executor().spawn(async move {
            login_sso_blocking(
                Uuid::new_v4(),
                &profile_name,
                &start_url,
                &region,
                &sso_account_id,
                &sso_role_name,
            )
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
                                    profile_name_for_result
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
                                    profile_name_for_result, error
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

    fn render_editor_panel(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_editing = self.editing_profile_id.is_some();

        #[cfg(feature = "aws")]
        if self.is_aws_sso_selected() {
            self.ensure_sso_listing(cx);
            self.sync_account_dropdown_selection(cx);
            self.sync_role_dropdown_selection(cx);
        }

        let dynamic_fields = self
            .selected_provider(cx)
            .map(|provider| {
                provider
                    .form_def()
                    .tabs
                    .iter()
                    .flat_map(|tab| tab.sections.iter())
                    .flat_map(|section| section.fields.iter())
                    .enumerate()
                    .map(|(idx, field)| {
                        if let Some(input) = self.form_inputs.get(&field.id) {
                            let form_field = AuthFormField::DynamicField(idx);
                            let is_focused = self.auth_form_field == form_field
                                && self.auth_focus == AuthFocus::Form
                                && self.content_focused;
                            self.render_input_row(&field.label, input, form_field, is_focused, cx)
                                .into_any_element()
                        } else {
                            div().into_any_element()
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        layout::sticky_form_shell(
            div()
                .child(Label::new(layout::editor_panel_title(
                    "Auth Profile",
                    is_editing,
                )))
                .child(Text::muted(
                    "Reusable authentication profile for access and value resolution",
                )),
            div()
                .flex()
                .flex_col()
                .gap_4()
                .child({
                    let is_focused = self.auth_form_field == AuthFormField::Name
                        && self.auth_focus == AuthFocus::Form
                        && self.content_focused;
                    self.render_input_row(
                        "Name",
                        &self.input_name,
                        AuthFormField::Name,
                        is_focused,
                        cx,
                    )
                })
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(Label::new("Provider"))
                        .child(self.render_provider_selector(window, cx)),
                )
                .children(dynamic_fields)
                .when(
                    cfg!(feature = "aws")
                        && self.selected_provider_id.as_deref() == Some("aws-sso"),
                    |content| {
                        #[cfg(feature = "aws")]
                        {
                            content
                                .child(
                                    div()
                                        .flex()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            Button::new(
                                                "auth-sso-login",
                                                if self.sso_login_loading {
                                                    "Logging in..."
                                                } else {
                                                    "Login"
                                                },
                                            )
                                            .small()
                                            .primary()
                                            .disabled(self.sso_login_loading)
                                            .on_click(
                                                cx.listener(|this, _, _, cx| {
                                                    this.login_sso_profile(cx);
                                                }),
                                            ),
                                        )
                                        .child(Text::caption(
                                            "Runs AWS SSO login for this profile",
                                        )),
                                )
                                .child(self.render_dropdown_row(
                                    "SSO Account ID",
                                    &self.sso_account_dropdown,
                                ))
                                .child(
                                    self.render_dropdown_row(
                                        "SSO Role Name",
                                        &self.sso_role_dropdown,
                                    ),
                                )
                                .when_some(self.sso_accounts_error.as_ref(), |content, error| {
                                    content.child(
                                        Text::caption(format!("Account listing failed: {}", error))
                                            .warning(),
                                    )
                                })
                                .when_some(self.sso_roles_error.as_ref(), |content, error| {
                                    content.child(
                                        Text::caption(format!("Role listing failed: {}", error))
                                            .warning(),
                                    )
                                })
                                .when_some(self.sso_login_status.as_ref(), |content, status| {
                                    content.child(if status.1 {
                                        Text::caption(status.0.clone()).success()
                                    } else {
                                        Text::caption(status.0.clone()).warning()
                                    })
                                })
                        }

                        #[cfg(not(feature = "aws"))]
                        {
                            content
                        }
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
                        .child(Text::body("Enabled")),
                ),
            div()
                .flex()
                .gap_2()
                .justify_end()
                .when(is_editing, |root| {
                    root.child(
                        Button::new("delete-auth-profile", "Delete")
                            .small()
                            .danger()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.request_delete_selected_profile(cx);
                            })),
                    )
                })
                .child(
                    Button::new("cancel-auth-profile", "Cancel")
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
                    Button::new(
                        "save-auth-profile",
                        if is_editing { "Update" } else { "Create" },
                    )
                    .small()
                    .primary()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save_profile(window, cx);
                    })),
                ),
            &theme,
        )
    }
}

impl FormSection for AuthProfilesSection {
    type Focus = AuthFocus;
    type FormField = AuthFormField;

    fn focus_area(&self) -> Self::Focus {
        self.auth_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.auth_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.auth_form_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.auth_form_field = field;
    }

    fn editing_field(&self) -> bool {
        self.auth_editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.auth_editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        AuthFocus::ProfileList
    }

    fn form_focus() -> Self::Focus {
        AuthFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        AuthFormField::Name
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        let providers = &self.provider_entries_cache;
        let mut rows = vec![vec![AuthFormField::Name]];

        let provider_row: Vec<AuthFormField> = providers
            .iter()
            .enumerate()
            .map(|(idx, _)| AuthFormField::Provider(idx))
            .collect();
        if !provider_row.is_empty() {
            rows.push(provider_row);
        }

        for idx in 0..self.provider_field_order.len() {
            rows.push(vec![AuthFormField::DynamicField(idx)]);
        }

        #[cfg(feature = "aws")]
        if self.selected_provider_id.as_deref() == Some("aws-sso") {
            rows.push(vec![AuthFormField::SsoLogin]);
            rows.push(vec![AuthFormField::SsoAccount]);
            rows.push(vec![AuthFormField::SsoRole]);
        }

        rows.push(vec![AuthFormField::Enabled]);

        if self.editing_profile_id.is_some() {
            rows.push(vec![AuthFormField::DeleteButton, AuthFormField::SaveButton]);
        } else {
            rows.push(vec![AuthFormField::SaveButton]);
        }

        rows
    }

    fn is_input_field(field: Self::FormField) -> bool {
        matches!(field, AuthFormField::Name | AuthFormField::DynamicField(_))
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.auth_editing_field = true;

        match self.auth_form_field {
            AuthFormField::Name => {
                self.input_name
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            AuthFormField::DynamicField(idx) => {
                if let Some(field_id) = self.provider_field_order.get(idx).cloned()
                    && let Some(input) = self.form_inputs.get(&field_id)
                {
                    input.update(cx, |state, cx| state.focus(window, cx));
                    return;
                }
                self.auth_editing_field = false;
            }
            _ => {
                self.auth_editing_field = false;
            }
        }
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.auth_form_field {
            AuthFormField::Name | AuthFormField::DynamicField(_) => {
                self.focus_current_field(window, cx);
            }
            AuthFormField::Provider(idx) => {
                let providers = self.provider_entries(cx);
                if let Some((provider_id, _)) = providers.get(idx) {
                    let provider_id = provider_id.clone();
                    self.selected_provider_id = Some(provider_id.clone());
                    self.rebuild_form_inputs(window, cx);

                    for input in self.form_inputs.values() {
                        input.update(cx, |state, cx| {
                            state.set_value("", window, cx);
                        });
                    }

                    #[cfg(feature = "aws")]
                    self.reset_sso_listing_state(cx);

                    self.validate_form_field();
                }
            }
            AuthFormField::Enabled => {
                self.profile_enabled = !self.profile_enabled;
            }
            #[cfg(feature = "aws")]
            AuthFormField::SsoLogin => {
                self.login_sso_profile(cx);
            }
            AuthFormField::SaveButton => {
                self.save_profile(window, cx);
            }
            AuthFormField::DeleteButton => {
                self.request_delete_selected_profile(cx);
            }
            #[cfg(feature = "aws")]
            AuthFormField::SsoAccount | AuthFormField::SsoRole => {}
        }
    }

    fn validate_form_field(&mut self) {
        let rows = self.form_rows();
        let current = self.auth_form_field;

        for row in &rows {
            if row.contains(&current) {
                return;
            }
        }

        self.auth_form_field = AuthFormField::Name;
    }
}

impl SettingsSection for AuthProfilesSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::AuthProfiles
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        AuthProfilesSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.auth_editing_field = false;
        cx.notify();
    }
}

impl Render for AuthProfilesSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_app_state {
            self.pending_sync_from_app_state = false;
            self.sync_from_app_state(window, cx);
        }

        let profiles = self.app_state.read(cx).auth_profiles().to_vec();
        let detected_profiles = self.detected_unimported_profiles(cx);
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

        layout::section_container(
            div()
                .flex_1()
                .min_h_0()
                .flex()
                .flex_col()
                .overflow_hidden()
                .child(dbflux_components::composites::section_header(
                    "Auth Profiles",
                    "Manage reusable authentication profiles for connection access",
                    cx,
                ))
                .when(!detected_profiles.is_empty(), |root| {
                    root.child(self.render_import_banner(&detected_profiles, cx))
                })
                .child(
                    div()
                        .flex_1()
                        .min_h_0()
                        .flex()
                        .overflow_hidden()
                        .child(self.render_profile_list(&profiles, cx))
                        .child(self.render_editor_panel(window, cx)),
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
                        .child(Text::body(body)),
                )
            }),
        )
    }
}
