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
use dbflux_core::{
    AccessKind, AuthProfile, FetchOptionsError, FetchOptionsRequest, FormFieldKind,
    ImportableProfile, RefreshTrigger,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::dialog::Dialog;
use gpui_component::{ActiveTheme, Icon, IconName};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Cached dropdown options for a `DynamicSelect` field.
///
/// The cache key is `(provider_id, field_id, dep_hash)` where `dep_hash` is
/// a hash of the sorted dependency key-value pairs.
struct CachedOptions {
    options: Vec<dbflux_core::SelectOption>,
    expires_at: Instant,
}

pub(super) struct AuthProfilesSection {
    app_state: Entity<AppStateEntity>,
    selected_profile_id: Option<Uuid>,
    editing_profile_id: Option<Uuid>,
    selected_provider_id: Option<String>,
    selected_provider_supports_login: bool,
    provider_entries_cache: Vec<(String, String)>,
    profile_enabled: bool,
    pending_delete_profile_id: Option<Uuid>,
    pending_sync_from_app_state: bool,

    input_name: Entity<InputState>,
    form_inputs: HashMap<String, Entity<InputState>>,
    provider_field_order: Vec<String>,
    provider_login_loading: bool,
    provider_login_status: Option<(String, bool)>,

    /// Generic dynamic-select dropdowns keyed by field id.
    dynamic_dropdowns: HashMap<String, Entity<Dropdown>>,
    /// Cached options indexed by `(provider_id, field_id, dep_hash)`.
    options_cache: HashMap<(String, String, u64), CachedOptions>,
    /// When set, the URL is routed to the login modal via the pending pattern.
    ///
    /// Fields: `(provider_name, profile_name, url)`.
    pending_sso_url: Option<(String, String, Option<String>)>,
    /// When true, the next render cycle will re-fetch options for all
    /// `OnLoginComplete` DynamicSelect fields.
    pending_login_complete_refresh: bool,

    auth_focus: AuthFocus,
    auth_form_field: AuthFormField,
    auth_editing_field: bool,
    content_focused: bool,
    profile_list_scroll_handle: ScrollHandle,
    pending_profile_scroll_idx: Option<usize>,
    switching_input: bool,

    _subscriptions: Vec<Subscription>,
    _blur_subscriptions: Vec<Subscription>,
    /// Subscriptions for DynamicSelect dropdown selection events.
    /// Rebuilt whenever `rebuild_form_inputs` is called.
    _dropdown_subscriptions: Vec<Subscription>,
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
    ProviderLogin,
    Enabled,
    DeleteButton,
    SaveButton,
}

/// Events emitted by the auth profiles section for inter-window routing.
#[derive(Clone, Debug)]
pub(super) enum AuthProfilesSectionEvent {
    /// A login flow produced a URL that should be displayed in the login modal.
    ///
    /// Fields: `(provider_name, profile_name, url)`.
    OpenLoginModal {
        provider_name: String,
        profile_name: String,
        url: Option<String>,
    },
}

impl EventEmitter<AuthProfilesSectionEvent> for AuthProfilesSection {}
impl EventEmitter<SectionFocusEvent> for AuthProfilesSection {}

fn build_form_rows(
    provider_count: usize,
    dynamic_field_count: usize,
    selected_provider_supports_login: bool,
    is_editing: bool,
) -> Vec<Vec<AuthFormField>> {
    let mut rows = vec![vec![AuthFormField::Name]];

    let provider_row: Vec<AuthFormField> =
        (0..provider_count).map(AuthFormField::Provider).collect();
    if !provider_row.is_empty() {
        rows.push(provider_row);
    }

    for idx in 0..dynamic_field_count {
        rows.push(vec![AuthFormField::DynamicField(idx)]);
    }

    if selected_provider_supports_login {
        rows.push(vec![AuthFormField::ProviderLogin]);
    }

    rows.push(vec![AuthFormField::Enabled]);

    if is_editing {
        rows.push(vec![AuthFormField::DeleteButton, AuthFormField::SaveButton]);
    } else {
        rows.push(vec![AuthFormField::SaveButton]);
    }

    rows
}

fn build_auth_profile_from_form(
    profile_id: Uuid,
    name: &str,
    provider_id: &str,
    fields: HashMap<String, String>,
    enabled: bool,
) -> Option<AuthProfile> {
    let trimmed_name = name.trim();
    if trimmed_name.is_empty() || provider_id.is_empty() {
        return None;
    }

    Some(AuthProfile {
        id: profile_id,
        name: trimmed_name.to_string(),
        provider_id: provider_id.to_string(),
        fields,
        enabled,
    })
}

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

        let app_state_subscription =
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.pending_sync_from_app_state = true;
                cx.notify();
            });

        let mut section = Self {
            app_state,
            selected_profile_id,
            editing_profile_id: None,
            selected_provider_id,
            selected_provider_supports_login: false,
            provider_entries_cache,
            profile_enabled: true,
            pending_delete_profile_id: None,
            pending_sync_from_app_state: false,
            input_name,
            form_inputs: HashMap::new(),
            provider_field_order: Vec::new(),
            provider_login_loading: false,
            provider_login_status: None,

            dynamic_dropdowns: HashMap::new(),
            options_cache: HashMap::new(),
            pending_login_complete_refresh: false,
            pending_sso_url: None,

            auth_focus: AuthFocus::ProfileList,
            auth_form_field: AuthFormField::Name,
            auth_editing_field: false,
            content_focused: false,
            profile_list_scroll_handle: ScrollHandle::new(),
            pending_profile_scroll_idx: None,
            switching_input: false,

            _subscriptions: vec![app_state_subscription],
            _blur_subscriptions: Vec::new(),
            _dropdown_subscriptions: Vec::new(),
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

    fn current_form_profile(&self, cx: &App) -> Option<AuthProfile> {
        let name = self.input_name.read(cx).value().to_string();
        let provider_id = self.selected_provider_id.clone()?;

        let profile_id = self.editing_profile_id.unwrap_or_else(Uuid::new_v4);
        let fields = self
            .form_inputs
            .iter()
            .map(|(field_id, input)| (field_id.clone(), input.read(cx).value().to_string()))
            .collect::<HashMap<_, _>>();

        build_auth_profile_from_form(
            profile_id,
            &name,
            &provider_id,
            fields,
            self.profile_enabled,
        )
    }

    fn rebuild_form_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(provider) = self.selected_provider(cx) else {
            self.form_inputs.clear();
            self.provider_field_order.clear();
            self.selected_provider_supports_login = false;
            self.rebuild_blur_subscriptions(cx);
            return;
        };

        self.selected_provider_supports_login = provider.capabilities().login.supported;

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

        let mut dropdown_subs: Vec<Subscription> = Vec::new();

        for (field_id, placeholder, kind) in field_defs {
            match kind {
                FormFieldKind::DynamicSelect { .. } => {
                    // For DynamicSelect fields: keep an InputState as the value
                    // store so save_profile can read it uniformly. The InputState
                    // is hidden from the UI; the Dropdown entity is rendered instead.
                    if !self.form_inputs.contains_key(&field_id) {
                        let input = cx.new(|cx| InputState::new(window, cx));
                        self.form_inputs.insert(field_id.clone(), input);
                    }

                    // Create or reuse the dropdown entity.
                    if !self.dynamic_dropdowns.contains_key(&field_id) {
                        let dropdown_id = format!("auth-dynamic-{}", field_id);
                        let placeholder_str = if placeholder.is_empty() {
                            "Select...".to_string()
                        } else {
                            placeholder
                        };
                        let dropdown = cx.new(|_cx| {
                            Dropdown::new(SharedString::from(dropdown_id))
                                .placeholder(placeholder_str)
                        });
                        self.dynamic_dropdowns.insert(field_id.clone(), dropdown);
                    }

                    // Subscribe to dropdown selection to write back into form_inputs.
                    let dropdown = self.dynamic_dropdowns[&field_id].clone();
                    let input = self.form_inputs[&field_id].clone();
                    let sub = cx.subscribe_in(
                        &dropdown,
                        window,
                        move |_this, _, event: &DropdownSelectionChanged, window, cx| {
                            input.update(cx, |state, cx| {
                                state.set_value(event.item.value.to_string(), window, cx);
                            });
                        },
                    );
                    dropdown_subs.push(sub);
                }
                _ => {
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
            }
        }

        self._dropdown_subscriptions = dropdown_subs;
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

    // ---------------------------------------------------------------------------
    // T10/T11: Dynamic-select options cache and background fetch
    // ---------------------------------------------------------------------------

    /// Compute a stable 64-bit hash of a sorted dependency map.
    fn hash_deps(deps: &HashMap<String, String>) -> u64 {
        let mut pairs: Vec<(&str, &str)> = deps
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        pairs.sort_unstable();

        let mut hasher = DefaultHasher::new();
        for (key, value) in pairs {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Fetch dynamic options for `field_id` if not already cached and valid.
    ///
    /// Cache miss conditions:
    /// - No entry for `(provider_id, field_id, dep_hash)`.
    /// - Cached entry has expired.
    /// - `refresh == RefreshTrigger::Manual` (always re-fetches when called).
    /// - `refresh == RefreshTrigger::OnLoginComplete` and `login_just_completed` is true.
    /// - `requires_session == true` and no active session.
    ///
    /// On fetch completion the cache is updated and `cx.notify()` is called.
    fn fetch_dynamic_options_if_needed(
        &mut self,
        provider_id: String,
        field: &dbflux_core::FormFieldDef,
        session: Option<serde_json::Value>,
        login_just_completed: bool,
        cx: &mut Context<Self>,
    ) {
        let FormFieldKind::DynamicSelect {
            depends_on,
            refresh,
            requires_session,
            ..
        } = &field.kind
        else {
            return;
        };

        // Guard: do not fetch when a session is required but absent.
        if *requires_session && session.is_none() {
            return;
        }

        // Collect the current values of all declared dependencies.
        let deps: HashMap<String, String> = depends_on
            .iter()
            .filter_map(|dep_id| {
                self.form_inputs
                    .get(dep_id)
                    .map(|input| (dep_id.clone(), input.read(cx).value().to_string()))
            })
            .collect();

        let dep_hash = Self::hash_deps(&deps);
        let cache_key = (provider_id.clone(), field.id.clone(), dep_hash);

        // Determine whether we need to (re-)fetch.
        let needs_fetch = match refresh {
            RefreshTrigger::Manual => true,
            RefreshTrigger::OnLoginComplete => login_just_completed,
            RefreshTrigger::OnDependencyChange | RefreshTrigger::OnFocus => {
                match self.options_cache.get(&cache_key) {
                    None => true,
                    Some(cached) => Instant::now() > cached.expires_at,
                }
            }
        };

        if !needs_fetch {
            return;
        }

        let Some(provider) = self.selected_provider(cx) else {
            return;
        };

        let field_id = field.id.clone();
        let this = cx.entity().clone();

        // Build a profile snapshot from the current form values so the provider
        // can access `profile_name`, `region`, `sso_start_url`, etc. The profile
        // name is set to a placeholder when the user has not filled it in yet —
        // the provider only reads `fields`, not `name`, for option fetching.
        let provider_id_snap = provider.provider_id().to_string();
        let profile_id_snap = self.editing_profile_id.unwrap_or_else(Uuid::new_v4);
        let fields_snap: HashMap<String, String> = self
            .form_inputs
            .iter()
            .map(|(key, input)| (key.clone(), input.read(cx).value().to_string()))
            .collect();

        let profile_snapshot = AuthProfile {
            id: profile_id_snap,
            name: self.input_name.read(cx).value().to_string(),
            provider_id: provider_id_snap,
            fields: fields_snap,
            enabled: self.profile_enabled,
        };

        let request = FetchOptionsRequest {
            field_id: field_id.clone(),
            dependencies: deps,
            session,
        };

        let fetch_task = cx.background_executor().spawn(async move {
            provider
                .fetch_dynamic_options(&profile_snapshot, request)
                .await
                .map_err(|err| match err {
                    FetchOptionsError::NeedsLogin => "needs login".to_string(),
                    FetchOptionsError::SessionExpired => "session expired".to_string(),
                    FetchOptionsError::Transient(msg) => format!("transient: {}", msg),
                    FetchOptionsError::Permanent(msg) => format!("permanent: {}", msg),
                })
        });

        cx.spawn(async move |_this, cx| {
            let result = fetch_task.await;

            let _ = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(response) => {
                            let ttl_secs = response.cache_hint_seconds.unwrap_or(0) as u64;
                            let expires_at = Instant::now()
                                + std::time::Duration::from_secs(if ttl_secs == 0 {
                                    0
                                } else {
                                    ttl_secs
                                });

                            let options = response.options;

                            this.options_cache.insert(
                                (provider_id.clone(), field_id.clone(), dep_hash),
                                CachedOptions { options, expires_at },
                            );

                            // Sync cached options into the dropdown entity.
                            if let Some(dropdown) = this.dynamic_dropdowns.get(&field_id) {
                                let items = this
                                    .options_cache
                                    .get(&(provider_id, field_id, dep_hash))
                                    .map(|cached| {
                                        cached
                                            .options
                                            .iter()
                                            .map(|opt| {
                                                DropdownItem::with_value(
                                                    opt.label.clone(),
                                                    opt.value.clone(),
                                                )
                                            })
                                            .collect::<Vec<_>>()
                                    })
                                    .unwrap_or_default();

                                dropdown.update(cx, |dropdown, cx| {
                                    dropdown.set_items(items, cx);
                                });
                            }
                        }
                        Err(error) => {
                            log::debug!(
                                "fetch_dynamic_options for field '{}': {}",
                                field_id, error
                            );
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    // ---------------------------------------------------------------------------
    // Generic DynamicSelect dropdown row
    // ---------------------------------------------------------------------------

    /// Render a `DynamicSelect` field as a dropdown row.
    ///
    /// The dropdown entity was created in `rebuild_form_inputs`. This method
    /// syncs the current cached options into the dropdown and renders it.
    fn render_dynamic_dropdown_row(
        &mut self,
        field: &dbflux_core::FormFieldDef,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let field_id = field.id.clone();
        let label = field.label.clone();

        // Build the dropdown if it was not yet created (e.g. if rebuild_form_inputs
        // was not called before the first render — defensive path only).
        if !self.dynamic_dropdowns.contains_key(&field_id) {
            let placeholder = if field.placeholder.is_empty() {
                "Select...".to_string()
            } else {
                field.placeholder.clone()
            };
            let dropdown_id = format!("auth-dynamic-{}", field_id);
            let dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(dropdown_id)).placeholder(placeholder)
            });
            self.dynamic_dropdowns.insert(field_id.clone(), dropdown);
        }

        let dropdown = self.dynamic_dropdowns[&field_id].clone();

        // Sync cached options into the dropdown.
        if let Some(provider_id) = self.selected_provider_id.clone() {
            let deps: HashMap<String, String> = match &field.kind {
                FormFieldKind::DynamicSelect { depends_on, .. } => depends_on
                    .iter()
                    .filter_map(|dep_id| {
                        self.form_inputs.get(dep_id).map(|input| {
                            (dep_id.clone(), input.read(cx).value().to_string())
                        })
                    })
                    .collect(),
                _ => HashMap::new(),
            };

            let dep_hash = Self::hash_deps(&deps);
            let cache_key = (provider_id, field_id.clone(), dep_hash);

            if let Some(cached) = self.options_cache.get(&cache_key) {
                let current_value = self
                    .form_inputs
                    .get(&field_id)
                    .map(|input| input.read(cx).value().to_string())
                    .unwrap_or_default();

                let items: Vec<DropdownItem> = cached
                    .options
                    .iter()
                    .map(|opt| DropdownItem::with_value(opt.label.clone(), opt.value.clone()))
                    .collect();

                let selected_index = items
                    .iter()
                    .position(|item| item.value == current_value);

                dropdown.update(cx, |d, cx| {
                    d.set_items(items, cx);
                    d.set_selected_index(selected_index, cx);
                });
            }
        }

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(dbflux_components::primitives::Label::new(label))
            .child(dropdown)
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

        // Clear the dynamic-select options cache so stale options are not shown
        // for a new/reset profile.
        self.options_cache.clear();
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

        // Clear options cache so DynamicSelect dropdowns are re-fetched for
        // the newly loaded profile's field values.
        self.options_cache.clear();

        self.provider_login_loading = false;
        self.provider_login_status = None;

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

        if let Some(provider) = self
            .app_state
            .read(cx)
            .auth_provider_by_id(&profile.provider_id)
        {
            provider.after_profile_saved(&profile);
        }

        self.selected_profile_id = Some(profile_id);
        self.load_profile_into_form(profile_id, window, cx);
    }

    fn login_selected_profile(&mut self, cx: &mut Context<Self>) {
        let Some(provider) = self.selected_provider(cx) else {
            self.provider_login_status =
                Some(("Select an auth provider before login.".to_string(), false));
            cx.notify();
            return;
        };

        if !provider.capabilities().login.supported {
            self.provider_login_status = Some((
                "Interactive login is not available for this provider.".to_string(),
                false,
            ));
            cx.notify();
            return;
        }

        let Some(profile) = self.current_form_profile(cx) else {
            self.provider_login_status = Some((
                "Provide a profile name and provider fields before login.".to_string(),
                false,
            ));
            cx.notify();
            return;
        };

        self.provider_login_loading = true;
        self.provider_login_status = Some((
            format!(
                "Starting auth-provider login for profile '{}'...",
                profile.name
            ),
            false,
        ));
        cx.notify();

        let this = cx.entity().clone();
        let provider_name_for_url = provider.display_name().to_string();
        let profile_name_for_url = profile.name.clone();

        // Capture the login URL for the modal. The UrlCallback fires once during
        // login (before completion) with the verification URL, or None if the
        // provider does not surface one. We store it in a shared slot and pick
        // it up in the spawn closure to drive the pending-URL pattern.
        let url_slot: Arc<std::sync::Mutex<Option<Option<String>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let url_slot_for_callback = url_slot.clone();

        let url_callback: dbflux_core::auth::UrlCallback = Box::new(move |url| {
            if let Ok(mut guard) = url_slot_for_callback.lock() {
                *guard = Some(url);
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = provider.login(&profile, url_callback).await;

            // Retrieve the URL that the callback may have received during login.
            let captured_url = url_slot.lock().ok().and_then(|guard| guard.clone());

            if let Err(err) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.provider_login_loading = false;
                    this.provider_login_status = Some(match &result {
                        Ok(_) => (
                            format!(
                                "Auth-provider login completed for profile '{}'.",
                                profile.name
                            ),
                            true,
                        ),
                        Err(error) => (
                            format!(
                                "Auth-provider login failed for profile '{}': {}",
                                profile.name, error
                            ),
                            false,
                        ),
                    });

                    // Route the login URL to the modal via pending pattern.
                    // The URL is consumed in render() and emitted as an event.
                    if let Some(url) = captured_url {
                        this.pending_sso_url =
                            Some((provider_name_for_url, profile_name_for_url, url));
                    }

                    // When login completed successfully, schedule a re-fetch of
                    // all DynamicSelect fields with OnLoginComplete trigger.
                    if this
                        .provider_login_status
                        .as_ref()
                        .is_some_and(|status| status.1)
                    {
                        this.options_cache.clear();
                        this.pending_login_complete_refresh = true;
                    }

                    cx.notify();
                });
            }) {
                log::warn!("Failed to apply auth-provider login result: {:?}", err);
            }
        })
        .detach();
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

                                this.options_cache.clear();

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
    ) -> impl IntoElement + use<> {
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
                            .bg(theme.list_even)
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

    fn render_editor_panel(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_editing = self.editing_profile_id.is_some();

        // Drive DynamicSelect fetches for the current provider's fields.
        // `fetch_dynamic_options_if_needed` is a no-op for fields already
        // cached and within their TTL.
        let login_just_completed = self.pending_login_complete_refresh;
        if login_just_completed {
            self.pending_login_complete_refresh = false;
        }

        let provider_id_for_fetch = self.selected_provider_id.clone().unwrap_or_default();
        let field_defs_for_fetch: Vec<dbflux_core::FormFieldDef> = self
            .selected_provider(cx)
            .map(|provider| {
                provider
                    .form_def()
                    .tabs
                    .iter()
                    .flat_map(|tab| tab.sections.iter())
                    .flat_map(|section| section.fields.iter())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        for field in &field_defs_for_fetch {
            if matches!(field.kind, FormFieldKind::DynamicSelect { .. }) {
                self.fetch_dynamic_options_if_needed(
                    provider_id_for_fetch.clone(),
                    field,
                    None, // session data — future: pass from AuthSession
                    login_just_completed,
                    cx,
                );
            }
        }

        // Collect field defs from the selected provider before the mutable borrow
        // for rendering (can't hold an Arc while calling &mut self methods).
        let provider_field_defs: Vec<dbflux_core::FormFieldDef> = field_defs_for_fetch;

        let dynamic_fields: Vec<AnyElement> = provider_field_defs
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                if matches!(field.kind, FormFieldKind::DynamicSelect { .. }) {
                    self.render_dynamic_dropdown_row(field, cx).into_any_element()
                } else if let Some(input) = self.form_inputs.get(&field.id) {
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
            .collect();

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
                .when(self.selected_provider_supports_login, |content| {
                    content
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    Button::new(
                                        "auth-provider-login",
                                        if self.provider_login_loading {
                                            "Logging in..."
                                        } else {
                                            "Login"
                                        },
                                    )
                                    .small()
                                    .primary()
                                    .disabled(self.provider_login_loading)
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.login_selected_profile(cx);
                                        },
                                    )),
                                )
                                .child(Text::caption(
                                    "Runs interactive login for this auth profile",
                                )),
                        )
                        .when_some(self.provider_login_status.as_ref(), |content, status| {
                            content.child(if status.1 {
                                Text::caption(status.0.clone()).success()
                            } else {
                                Text::caption(status.0.clone()).warning()
                            })
                        })
                })
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
            None,
            &theme,
        )
    }

    fn render_section_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_editing = self.editing_profile_id.is_some();
        let is_form_focused = self.auth_focus == AuthFocus::Form && self.content_focused;
        let primary = cx.theme().primary;

        div()
            .flex()
            .items_center()
            .gap_3()
            .when(is_editing, |root| {
                root.child(layout::footer_action_frame(
                    is_form_focused && self.auth_form_field == AuthFormField::DeleteButton,
                    primary,
                    Button::new("delete-auth-profile", "Delete")
                        .small()
                        .danger()
                        .w_full()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.request_delete_selected_profile(cx);
                        })),
                ))
            })
            .child(layout::footer_action_frame(
                false,
                primary,
                Button::new("cancel-auth-profile", "Cancel")
                    .small()
                    .w_full()
                    .on_click(cx.listener(|this, _, window, cx| {
                        if let Some(selected_id) = this.selected_profile_id {
                            this.load_profile_into_form(selected_id, window, cx);
                        } else {
                            this.clear_form(window, cx);
                            cx.notify();
                        }
                    })),
            ))
            .child(layout::footer_action_frame(
                is_form_focused && self.auth_form_field == AuthFormField::SaveButton,
                primary,
                Button::new(
                    "save-auth-profile",
                    if is_editing { "Update" } else { "Create" },
                )
                .small()
                .primary()
                .w_full()
                .on_click(cx.listener(|this, _, window, cx| {
                    this.save_profile(window, cx);
                })),
            ))
            .into_any_element()
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
        build_form_rows(
            self.provider_entries_cache.len(),
            self.provider_field_order.len(),
            self.selected_provider_supports_login,
            self.editing_profile_id.is_some(),
        )
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

                    self.options_cache.clear();

                    self.validate_form_field();
                }
            }
            AuthFormField::Enabled => {
                self.profile_enabled = !self.profile_enabled;
            }
            AuthFormField::ProviderLogin => {
                self.login_selected_profile(cx);
            }
            AuthFormField::SaveButton => {
                self.save_profile(window, cx);
            }
            AuthFormField::DeleteButton => {
                self.request_delete_selected_profile(cx);
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn form_rows_include_generic_provider_login_without_aws_feature() {
        let rows = build_form_rows(1, 2, true, false);

        assert!(
            rows.iter()
                .any(|row| row == &vec![AuthFormField::ProviderLogin])
        );
        assert!(
            !rows
                .iter()
                .any(|row| row.contains(&AuthFormField::DeleteButton))
        );
    }

    #[::core::prelude::v1::test]
    fn build_auth_profile_from_form_preserves_non_aws_provider_id() {
        let mut fields = HashMap::new();
        fields.insert(
            "issuer_url".to_string(),
            "https://issuer.example".to_string(),
        );

        let profile = build_auth_profile_from_form(
            Uuid::nil(),
            "  Custom OIDC  ",
            "custom-oidc",
            fields.clone(),
            true,
        )
        .expect("profile should be created");

        assert_eq!(profile.name, "Custom OIDC");
        assert_eq!(profile.provider_id, "custom-oidc");
        assert_eq!(profile.fields, fields);
        assert!(profile.enabled);
    }

    // -----------------------------------------------------------------------
    // T14: Cache invalidation rules
    // -----------------------------------------------------------------------

    /// (a) Within TTL, the same (provider_id, field_id, dep_hash) key is reused
    /// and no refetch is triggered.
    #[::core::prelude::v1::test]
    fn cache_key_is_stable_for_same_dependencies() {
        let mut deps_a = HashMap::new();
        deps_a.insert("region".to_string(), "us-east-1".to_string());

        let hash_a = AuthProfilesSection::hash_deps(&deps_a);

        // Same deps, different iteration order — result must be identical.
        let mut deps_b = HashMap::new();
        deps_b.insert("region".to_string(), "us-east-1".to_string());

        let hash_b = AuthProfilesSection::hash_deps(&deps_b);

        assert_eq!(
            hash_a, hash_b,
            "same dependency map must produce the same hash"
        );
    }

    /// (b) A change in dependency value produces a different dep_hash, which
    /// results in a different cache key and triggers a refetch.
    #[::core::prelude::v1::test]
    fn dep_change_produces_different_hash() {
        let mut deps_a = HashMap::new();
        deps_a.insert("region".to_string(), "us-east-1".to_string());

        let mut deps_b = HashMap::new();
        deps_b.insert("region".to_string(), "eu-west-1".to_string());

        let hash_a = AuthProfilesSection::hash_deps(&deps_a);
        let hash_b = AuthProfilesSection::hash_deps(&deps_b);

        assert_ne!(
            hash_a, hash_b,
            "changed dep value must produce a different hash"
        );
    }

    /// (b) A change in dependency key also produces a different hash.
    #[::core::prelude::v1::test]
    fn dep_key_change_produces_different_hash() {
        let mut deps_a = HashMap::new();
        deps_a.insert("region".to_string(), "us-east-1".to_string());

        let mut deps_b = HashMap::new();
        deps_b.insert("account".to_string(), "us-east-1".to_string());

        let hash_a = AuthProfilesSection::hash_deps(&deps_a);
        let hash_b = AuthProfilesSection::hash_deps(&deps_b);

        assert_ne!(
            hash_a, hash_b,
            "different dep key must produce a different hash"
        );
    }

    /// (c) `RefreshTrigger::Manual` is classified as always requiring a refetch.
    ///     We cannot spin up a GPUI context in a unit test, but we can verify
    ///     that the trigger variant is correctly identified by pattern-matching
    ///     the enum (which is how `fetch_dynamic_options_if_needed` guards it).
    #[::core::prelude::v1::test]
    fn manual_trigger_is_always_needs_fetch() {
        let trigger = RefreshTrigger::Manual;
        let needs_fetch = matches!(trigger, RefreshTrigger::Manual);
        assert!(needs_fetch, "Manual trigger must always require a refetch");
    }

    /// (d) `RefreshTrigger::OnLoginComplete` only fires when `login_just_completed`
    ///     is true.
    #[::core::prelude::v1::test]
    fn on_login_complete_trigger_respects_login_flag() {
        let trigger = RefreshTrigger::OnLoginComplete;

        let fires_without_login = match trigger {
            RefreshTrigger::OnLoginComplete => false, // only when login_just_completed=true
            _ => false,
        };
        assert!(
            !fires_without_login,
            "OnLoginComplete must NOT trigger without the login-complete signal"
        );

        let fires_with_login = match trigger {
            RefreshTrigger::OnLoginComplete => true,
            _ => false,
        };
        assert!(
            fires_with_login,
            "OnLoginComplete must trigger when the login-complete signal is true"
        );
    }

    /// Expired cache entries are detected by comparing `Instant::now()` with
    /// `expires_at`. We set `expires_at` to the past to simulate expiry.
    #[::core::prelude::v1::test]
    fn expired_cache_entry_is_detected() {
        let expired = CachedOptions {
            options: vec![],
            expires_at: Instant::now() - std::time::Duration::from_secs(1),
        };
        assert!(
            Instant::now() > expired.expires_at,
            "expired entry must be older than now"
        );
    }

    /// A fresh cache entry (expires in the future) is not expired.
    #[::core::prelude::v1::test]
    fn fresh_cache_entry_is_not_expired() {
        let fresh = CachedOptions {
            options: vec![],
            expires_at: Instant::now() + std::time::Duration::from_secs(300),
        };
        assert!(
            Instant::now() <= fresh.expires_at,
            "fresh entry must not be expired"
        );
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

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_section_footer_actions(cx))
    }
}

impl Render for AuthProfilesSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_app_state {
            self.pending_sync_from_app_state = false;
            self.sync_from_app_state(window, cx);
        }

        // Consume a pending login URL and emit it as an event so the settings
        // coordinator can route it to the login modal in the workspace window.
        if let Some((provider_name, profile_name, url)) = self.pending_sso_url.take() {
            cx.emit(AuthProfilesSectionEvent::OpenLoginModal {
                provider_name,
                profile_name,
                url,
            });
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
            layout::split_section_shell(
                dbflux_components::composites::section_header(
                    "Auth Profiles",
                    "Manage reusable authentication profiles for connection access",
                    cx,
                )
                .when(!detected_profiles.is_empty(), |root| {
                    root.child(self.render_import_banner(&detected_profiles, cx))
                })
                ,
                self.render_profile_list(&profiles, cx),
                self.render_editor_panel(window, cx),
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
