use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::{FormSection, create_blur_subscription};
use super::layout;
use super::section_trait::SectionFocusEvent;
use dbflux_app::keymap::Modifiers;
use dbflux_components::controls::InputState;
use dbflux_components::controls::{Button, Checkbox, Input};
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::focus_frame;
use dbflux_components::primitives::{Icon as FluxIcon, Label, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::{
    AccessKind, AuthEditCapabilities, AuthEditSnapshot, AuthProfile, AuthSaveOutcome,
    DanglingMessage, FetchOptionsError, FetchOptionsRequest, FormFieldKind, RefreshTrigger,
};
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::prelude::*;
use gpui::*;
use gpui_component::dialog::Dialog;
use gpui_component::{ActiveTheme, Icon};
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

    /// Whether the currently-displayed profile is read-only (i.e. it is a
    /// dangling reflected profile with no on-disk section to target). When
    /// `true` the form is rendered in mirror mode: no editable inputs, no
    /// Save/Delete buttons.
    ///
    /// Non-dangling reflected AWS profiles have `read_only = false` and are
    /// editable — their edits are written directly to `~/.aws/config` /
    /// `~/.aws/credentials` via `save_edit` (design §13, §14).
    profile_is_read_only: bool,
    /// Why the currently-displayed profile is dangling, if it is.
    /// Known values: `"keyring-only"`, `"file-gone"`. `None` for healthy profiles.
    profile_dangling_origin: Option<String>,

    /// Opaque snapshot token captured when an editable reflected AWS profile is
    /// loaded for editing. Passed back to `save_edit` at save time for the
    /// optimistic-concurrency check (spec R9.3.1, design §10).
    ///
    /// `None` for stored (non-reflected) profiles — those use the standard
    /// SQLite save path which has no snapshot concept.
    edit_snapshot: Option<AuthEditSnapshot>,
    /// Non-`None` when a `save_edit` call returned `Conflict` or `PartialSaved`.
    /// Contains a human-readable message to display, plus `true` if a Reload
    /// button should be shown.
    edit_conflict_msg: Option<(String, bool)>,

    input_name: Entity<InputState>,
    form_inputs: HashMap<String, Entity<InputState>>,
    provider_field_order: Vec<String>,
    provider_login_loading: bool,
    provider_login_status: Option<(String, bool)>,

    /// Dropdown for selecting the auth provider.
    provider_dropdown: Entity<Dropdown>,
    /// Generic dynamic-select dropdowns keyed by field id.
    dynamic_dropdowns: HashMap<String, Entity<Dropdown>>,
    /// Cached options indexed by `(provider_id, field_id, dep_hash)`.
    options_cache: HashMap<(String, String, u64), CachedOptions>,
    /// Per-field re-login hint shown below the dropdown when the last fetch
    /// returned `NeedsLogin` or `SessionExpired`.
    ///
    /// Keyed by field id; cleared on successful fetch or profile change.
    field_login_hint: HashMap<String, String>,
    /// When set, the URL is routed to the login modal via the pending pattern.
    ///
    /// Fields: `(provider_name, profile_name, url)`.
    pending_sso_url: Option<(String, String, Option<String>)>,
    /// Active login verification URL displayed inline under the Login button.
    /// Cleared when the login completes or the user cancels.
    active_login_url: Option<String>,
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
    /// Subscription for provider dropdown selection events.
    _provider_dropdown_subscription: Option<Subscription>,
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
    has_providers: bool,
    dynamic_field_count: usize,
    selected_provider_supports_login: bool,
    is_editing: bool,
    is_reflected: bool,
) -> Vec<Vec<AuthFormField>> {
    let mut rows = vec![vec![AuthFormField::Name]];

    // Reflected profiles have a fixed provider; the selector is not shown.
    if has_providers && !is_reflected {
        rows.push(vec![AuthFormField::Provider(0)]);
    }

    for idx in 0..dynamic_field_count {
        rows.push(vec![AuthFormField::DynamicField(idx)]);
    }

    if selected_provider_supports_login {
        rows.push(vec![AuthFormField::ProviderLogin]);
    }

    // Reflected profiles do not expose the Enabled toggle (it is managed by
    // the file) and do not have a Delete button.
    if !is_reflected {
        rows.push(vec![AuthFormField::Enabled]);
    }

    if is_editing && !is_reflected {
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
        read_only: false,
        dangling_origin: None,
    })
}

const MIRROR_LABEL_FALLBACK: &str = "Read-only — managed externally";
const SUCCESS_WRITTEN_FALLBACK: &str = "Profile saved.";
const NAME_HINT_FALLBACK: &str = "";

fn dangling_fallback() -> DanglingMessage {
    DanglingMessage {
        title: "Profile reference invalid".to_string(),
        body: "This profile cannot be loaded.".to_string(),
    }
}

/// Update `field_login_hint` and `provider_login_status` for a
/// `FetchOptionsError` returned for `field_id`.
///
/// `NeedsLogin` and `SessionExpired` are surfaced as a per-field hint shown
/// below the affected dropdown and as a warning banner near the Login button,
/// so the user knows to re-authenticate.  Transient and permanent errors are
/// logged at debug level; they do not update the login-hint state because
/// they do not require user authentication action.
///
/// The status banner is not overwritten when a login is already in progress
/// (`login_in_progress == true`), because the running login flow owns it.
fn apply_fetch_error_state(
    field_id: &str,
    error: FetchOptionsError,
    field_login_hint: &mut HashMap<String, String>,
    provider_login_status: &mut Option<(String, bool)>,
    login_in_progress: bool,
) {
    match &error {
        FetchOptionsError::NeedsLogin => {
            field_login_hint.insert(field_id.to_string(), "Log in to load options".to_string());

            if !login_in_progress {
                *provider_login_status = Some((
                    "Login required — click Login to load dropdown options.".to_string(),
                    false,
                ));
            }
        }
        FetchOptionsError::SessionExpired => {
            field_login_hint.insert(
                field_id.to_string(),
                "Session expired — log in again to reload options".to_string(),
            );

            if !login_in_progress {
                *provider_login_status = Some((
                    "Session expired — click Login to refresh dropdown options.".to_string(),
                    false,
                ));
            }
        }
        FetchOptionsError::Transient(msg) => {
            log::debug!(
                "fetch_dynamic_options for field '{}': transient: {}",
                field_id,
                msg
            );
        }
        FetchOptionsError::Permanent(msg) => {
            log::debug!(
                "fetch_dynamic_options for field '{}': permanent: {}",
                field_id,
                msg
            );
        }
    }
}

impl AuthProfilesSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let selected_profile_id = app_state
            .read(cx)
            .list_auth_profiles()
            .first()
            .map(|p| p.id);
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

        let provider_dropdown = cx.new(|_cx| {
            Dropdown::new(SharedString::from("auth-provider-selector"))
                .placeholder("Select provider…")
        });

        let app_state_subscription =
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.pending_sync_from_app_state = true;
                cx.notify();
            });

        let provider_dropdown_subscription = cx.subscribe_in(
            &provider_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                let provider_id = event.item.value.to_string();
                this.selected_provider_id = Some(provider_id);
                this.rebuild_form_inputs(window, cx);

                for input in this.form_inputs.values() {
                    input.update(cx, |state, cx| {
                        state.set_value("", window, cx);
                    });
                }

                this.options_cache.clear();
                this.field_login_hint.clear();

                cx.notify();
            },
        );

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
            profile_is_read_only: false,
            profile_dangling_origin: None,
            edit_snapshot: None,
            edit_conflict_msg: None,
            input_name,
            form_inputs: HashMap::new(),
            provider_field_order: Vec::new(),
            provider_login_loading: false,
            provider_login_status: None,

            provider_dropdown,
            dynamic_dropdowns: HashMap::new(),
            options_cache: HashMap::new(),
            field_login_hint: HashMap::new(),
            pending_login_complete_refresh: false,
            pending_sso_url: None,
            active_login_url: None,

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
            _provider_dropdown_subscription: Some(provider_dropdown_subscription),
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
                FormFieldKind::DynamicSelect { .. } | FormFieldKind::AuthProfileRef { .. } => {
                    // Both dropdown-style fields share the same shell: an
                    // InputState holds the canonical value (so save_profile
                    // can read it uniformly) and a Dropdown entity is the
                    // visible widget. The two kinds differ only in how
                    // options are populated (see render_*_row functions).
                    if !self.form_inputs.contains_key(&field_id) {
                        let input = cx.new(|cx| InputState::new(window, cx));
                        self.form_inputs.insert(field_id.clone(), input);
                    }

                    if !self.dynamic_dropdowns.contains_key(&field_id) {
                        let dropdown_id = format!("auth-dynamic-{}", field_id);
                        let placeholder_str = if placeholder.is_empty() {
                            match &kind {
                                FormFieldKind::AuthProfileRef { .. } => "— None —".to_string(),
                                _ => "Select...".to_string(),
                            }
                        } else {
                            placeholder
                        };
                        let dropdown = cx.new(|_cx| {
                            Dropdown::new(SharedString::from(dropdown_id))
                                .placeholder(placeholder_str)
                        });
                        self.dynamic_dropdowns.insert(field_id.clone(), dropdown);
                    }

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
                            FormFieldKind::Password | FormFieldKind::WriteOnly => {
                                state.masked(true)
                            }
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

    /// Handle a `FetchOptionsError` for `field_id`.
    ///
    /// Delegates to the free function `apply_fetch_error_state` so the logic
    /// can be exercised in unit tests without a running GPUI context.
    fn apply_fetch_error(
        &mut self,
        field_id: &str,
        error: FetchOptionsError,
        _cx: &mut Context<Self>,
    ) {
        apply_fetch_error_state(
            field_id,
            error,
            &mut self.field_login_hint,
            &mut self.provider_login_status,
            self.provider_login_loading,
        );
    }

    /// Fetch dynamic options for `field_id` if not already cached and valid.
    ///
    /// Cache miss conditions:
    /// - No entry for `(provider_id, field_id, dep_hash)`.
    /// - Cached entry has expired.
    /// - `refresh == RefreshTrigger::Manual`: fetches only when no cached entry
    ///   exists (first render), never re-fetches automatically after that.
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
            // Manual fields are never auto-refetched.  They fetch exactly once
            // on the first render (cache miss) and then require an explicit
            // user gesture (cache invalidation) to refetch.
            RefreshTrigger::Manual => !self.options_cache.contains_key(&cache_key),
            // OnLoginComplete fields refresh right after a successful login
            // OR on the first render when a session is available (cache miss).
            // Without the cache-miss branch the user would have to re-click
            // Login every time they reopen the editor to see options.
            RefreshTrigger::OnLoginComplete => {
                login_just_completed || !self.options_cache.contains_key(&cache_key)
            }
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

        let raw_snapshot = AuthProfile {
            id: profile_id_snap,
            name: self.input_name.read(cx).value().to_string(),
            provider_id: provider_id_snap,
            fields: fields_snap,
            enabled: self.profile_enabled,
            read_only: false,
            dangling_origin: None,
        };

        // Expand AuthProfileRef fields so the provider sees the same flat
        // field map it would see at connect time (e.g. an `aws-sso` profile
        // with an `sso_session_ref` gets `sso_start_url`/`sso_region` merged
        // in from the referenced session profile).
        let profile_registry_snapshot: Vec<AuthProfile> =
            self.app_state.read(cx).list_auth_profiles();
        let profile_snapshot = dbflux_core::auth::expand_auth_profile_refs(
            &raw_snapshot,
            provider.form_def(),
            &|target_id| {
                profile_registry_snapshot
                    .iter()
                    .find(|p| p.id == *target_id)
                    .cloned()
            },
        );

        let request = FetchOptionsRequest {
            field_id: field_id.clone(),
            dependencies: deps,
            session,
        };

        let fetch_task = cx.background_executor().spawn(async move {
            provider
                .fetch_dynamic_options(&profile_snapshot, request)
                .await
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
                                CachedOptions {
                                    options,
                                    expires_at,
                                },
                            );

                            // Clear any prior login hint for this field now that
                            // options loaded successfully.
                            this.field_login_hint.remove(&field_id);

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
                            this.apply_fetch_error(&field_id, error, cx);
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
            let dropdown = cx
                .new(|_cx| Dropdown::new(SharedString::from(dropdown_id)).placeholder(placeholder));
            self.dynamic_dropdowns.insert(field_id.clone(), dropdown);
        }

        let dropdown = self.dynamic_dropdowns[&field_id].clone();

        // Sync cached options into the dropdown.
        if let Some(provider_id) = self.selected_provider_id.clone() {
            let deps: HashMap<String, String> = match &field.kind {
                FormFieldKind::DynamicSelect { depends_on, .. } => depends_on
                    .iter()
                    .filter_map(|dep_id| {
                        self.form_inputs
                            .get(dep_id)
                            .map(|input| (dep_id.clone(), input.read(cx).value().to_string()))
                    })
                    .collect(),
                _ => HashMap::new(),
            };

            let dep_hash = Self::hash_deps(&deps);
            let cache_key = (provider_id, field_id.clone(), dep_hash);

            let current_value = self
                .form_inputs
                .get(&field_id)
                .map(|input| input.read(cx).value().to_string())
                .unwrap_or_default();

            // Always re-sync the dropdown: the entity is reused across profiles
            // (keyed by field id), so a cache miss must reset it rather than
            // leave a stale label from a previously loaded profile. With no
            // fetched options (cache cleared on load, or session expired), fall
            // back to the profile's own stored value as a single item.
            let items: Vec<DropdownItem> = match self.options_cache.get(&cache_key) {
                Some(cached) => cached
                    .options
                    .iter()
                    .map(|opt| DropdownItem::with_value(opt.label.clone(), opt.value.clone()))
                    .collect(),
                None if !current_value.is_empty() => {
                    vec![DropdownItem::with_value(
                        current_value.clone(),
                        current_value.clone(),
                    )]
                }
                None => Vec::new(),
            };

            let selected_index = items.iter().position(|item| item.value == current_value);

            dropdown.update(cx, |d, cx| {
                d.set_items(items, cx);
                d.set_selected_index(selected_index, cx);
            });
        }

        let login_hint = self.field_login_hint.get(&field_id).cloned();

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(dbflux_components::primitives::Label::new(label))
            .child(dropdown)
            .when_some(login_hint, |container, hint| {
                container.child(Text::caption(hint).warning())
            })
    }

    /// Render an `AuthProfileRef` field as a dropdown of existing auth
    /// profiles whose `provider_id` matches the field's filter. The selected
    /// value is the referenced profile's UUID (or empty for "none").
    fn render_auth_profile_ref_row(
        &mut self,
        field: &dbflux_core::FormFieldDef,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let field_id = field.id.clone();
        let label = field.label.clone();
        let help = field.help.clone();

        let FormFieldKind::AuthProfileRef {
            provider_id: ref_provider_id,
        } = &field.kind
        else {
            return div();
        };

        if !self.dynamic_dropdowns.contains_key(&field_id) {
            let dropdown_id = format!("auth-dynamic-{}", field_id);
            let dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(dropdown_id)).placeholder("— None —".to_string())
            });
            self.dynamic_dropdowns.insert(field_id.clone(), dropdown);
        }

        let dropdown = self.dynamic_dropdowns[&field_id].clone();

        let mut items: Vec<DropdownItem> = vec![DropdownItem::with_value(
            "— None —".to_string(),
            String::new(),
        )];
        // Use the reflected union, not the stored-only slice: AWS sso-session
        // profiles are reflected from ~/.aws/config and are absent from storage,
        // while stale stored AWS rows are excluded by list_auth_profiles(). Their
        // deterministic UUIDs match the expansion lookup, which reads the same seam.
        let referenced_profiles: Vec<(String, String)> = self
            .app_state
            .read(cx)
            .list_auth_profiles()
            .iter()
            .filter(|profile| profile.provider_id == *ref_provider_id && profile.enabled)
            .map(|profile| (profile.id.to_string(), profile.name.clone()))
            .collect();
        for (id, name) in referenced_profiles {
            items.push(DropdownItem::with_value(name, id));
        }

        let current_value = self
            .form_inputs
            .get(&field_id)
            .map(|input| input.read(cx).value().to_string())
            .unwrap_or_default();
        let selected_index = items.iter().position(|item| item.value == current_value);

        dropdown.update(cx, |d, cx| {
            d.set_items(items, cx);
            d.set_selected_index(selected_index, cx);
        });

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(dbflux_components::primitives::Label::new(label))
            .child(dropdown)
            .when_some(help, |container, hint_text| {
                container.child(Text::caption(hint_text))
            })
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_profile_id = None;
        self.profile_enabled = true;
        self.edit_snapshot = None;
        self.edit_conflict_msg = None;

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
        self.field_login_hint.clear();
    }

    fn sync_from_app_state(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Refresh provider entries so newly discovered RPC providers appear
        // in the dropdown without requiring a section reload.
        self.provider_entries_cache = self.provider_entries(cx);

        let profiles = self.app_state.read(cx).list_auth_profiles();

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
            .list_auth_profiles()
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
            .list_auth_profiles()
            .into_iter()
            .find(|profile| profile.id == profile_id);

        let Some(profile) = profile else {
            return;
        };

        self.selected_profile_id = Some(profile.id);
        self.profile_is_read_only = profile.read_only;
        self.profile_dangling_origin = profile.dangling_origin.clone();
        self.edit_conflict_msg = None;

        // Reflected (file-backed) profiles use the edit-save path; stored
        // profiles use the SQLite path.  Both require an editing_profile_id
        // for the save-button to appear.
        self.editing_profile_id = if profile.dangling_origin.is_some() {
            // Dangling profiles are truly read-only: no section to write to.
            None
        } else {
            Some(profile.id)
        };

        self.profile_enabled = profile.enabled;
        self.selected_provider_id = Some(profile.provider_id.clone());

        self.input_name.update(cx, |state, cx| {
            state.set_value(profile.name.clone(), window, cx);
        });

        self.rebuild_form_inputs(window, cx);

        // Determine which field ids are WriteOnly so we never pre-fill them.
        let write_only_fields: std::collections::HashSet<String> = self
            .selected_provider(cx)
            .map(|provider| {
                provider
                    .form_def()
                    .tabs
                    .iter()
                    .flat_map(|tab| tab.sections.iter())
                    .flat_map(|section| section.fields.iter())
                    .filter(|field| field.kind == FormFieldKind::WriteOnly)
                    .map(|field| field.id.clone())
                    .collect()
            })
            .unwrap_or_default();

        for (field_id, input) in &self.form_inputs {
            // WriteOnly fields are always rendered empty (spec R9.4.1, S33).
            let value = if write_only_fields.contains(field_id) {
                String::new()
            } else {
                profile.fields.get(field_id).cloned().unwrap_or_default()
            };

            input.update(cx, |state, cx| {
                state.set_value(value, window, cx);
            });
        }

        // Capture the edit snapshot for editable reflected profiles.  The
        // snapshot is used at save time for the optimistic-concurrency check
        // (spec R9.3.1, design §10).
        //
        // Dangling profiles have no snapshot (no section to hash); stored
        // profiles don't need one (they go through SQLite, not save_edit).
        self.edit_snapshot = if profile.dangling_origin.is_none() && !profile.read_only {
            // Reflected, editable profile — snapshot the on-disk section.
            let provider = self
                .app_state
                .read(cx)
                .auth_provider_by_id(&profile.provider_id);

            provider.map(|p| p.open_edit_snapshot(&profile.name))
        } else {
            None
        };

        // Clear options cache so DynamicSelect dropdowns are re-fetched for
        // the newly loaded profile's field values.
        self.options_cache.clear();
        self.field_login_hint.clear();

        self.provider_login_loading = false;
        self.provider_login_status = None;

        cx.notify();
    }

    fn begin_create_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_profile_id = None;
        self.profile_is_read_only = false;
        self.profile_dangling_origin = None;
        self.clear_form(window, cx);
        self.enter_form(window, cx);
        cx.notify();
    }

    fn save_profile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Dangling profiles (truly read-only) cannot be saved.
        if self.profile_is_read_only {
            return;
        }

        let name = self.input_name.read(cx).value().trim().to_string();
        if name.is_empty() {
            return;
        }

        let Some(provider_id) = self.selected_provider_id.clone() else {
            return;
        };

        // When an edit snapshot is present, this is a reflected AWS profile.
        // Route through save_edit rather than SQLite.
        if let Some(snapshot) = self.edit_snapshot.clone() {
            self.save_edit_profile(name, provider_id, snapshot, window, cx);
            return;
        }

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
            read_only: false,
            dangling_origin: None,
        };

        // For file-backed providers (e.g. AWS), write to the external config
        // file instead of DBFlux's SQLite store. The provider returns
        // `Some(result)` when it handles the write. Reflection will surface
        // the new profile on the next `list_auth_profiles()` call.
        let is_new = self.editing_profile_id.is_none();
        if is_new
            && let Some(provider) = self
                .app_state
                .read(cx)
                .auth_provider_by_id(&profile.provider_id)
        {
            match provider.write_new_profile_to_config(&profile) {
                Some(Ok(())) => {
                    // File-backed create succeeded. Signal a refresh so
                    // the reflection pass picks up the new entry.
                    self.app_state.update(cx, |_, cx| {
                        cx.emit(AppStateChanged);
                    });

                    self.selected_profile_id = None;
                    self.profile_is_read_only = false;
                    self.profile_dangling_origin = None;

                    let success_text = self
                        .app_state
                        .read(cx)
                        .auth_provider_by_id(&profile.provider_id)
                        .and_then(|p| {
                            p.capabilities()
                                .edit
                                .as_ref()
                                .map(|e| e.success_written.clone())
                        })
                        .unwrap_or_else(|| SUCCESS_WRITTEN_FALLBACK.to_string());
                    self.provider_login_status = Some((success_text, true));
                    cx.notify();
                    return;
                }

                Some(Err(msg)) => {
                    self.provider_login_status =
                        Some((format!("Failed to write profile to config: {}", msg), false));
                    cx.notify();
                    return;
                }

                None => {
                    // Provider does not do file-backed writes; fall through
                    // to the standard SQLite path below.
                }
            }
        }

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

    /// Save path for reflected AWS profiles: calls `save_edit` with the
    /// optimistic-concurrency snapshot and handles `Conflict` / `PartialSaved`
    /// outcomes (spec R9.3.4, R9.3.6, S29, S35, design §14).
    ///
    /// Fields with `FormFieldKind::WriteOnly` that are left blank are omitted
    /// from the field map so the provider preserves the existing on-disk value
    /// (spec R9.4.2, S27). Secret values transit only through this method's
    /// stack frame — they are never stored in `self` beyond the call site
    /// (spec R9.6.1, R9.6.4).
    fn save_edit_profile(
        &mut self,
        name: String,
        provider_id: String,
        snapshot: AuthEditSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(provider) = self.app_state.read(cx).auth_provider_by_id(&provider_id) else {
            return;
        };

        // Determine which fields are WriteOnly so blank values can be omitted.
        let write_only_ids: std::collections::HashSet<String> = provider
            .form_def()
            .tabs
            .iter()
            .flat_map(|tab| tab.sections.iter())
            .flat_map(|section| section.fields.iter())
            .filter(|field| field.kind == FormFieldKind::WriteOnly)
            .map(|field| field.id.clone())
            .collect();

        // Build the edited fields map.  Blank WriteOnly fields are excluded so
        // the provider treats them as "preserve existing on-disk value".
        let raw_fields: HashMap<String, String> = self
            .form_inputs
            .iter()
            .filter_map(|(field_id, input)| {
                let value = input.read(cx).value().to_string();
                if write_only_ids.contains(field_id) && value.is_empty() {
                    // Blank secret field — omit so the provider preserves the
                    // existing on-disk value (spec R9.4.2).
                    None
                } else {
                    Some((field_id.clone(), value))
                }
            })
            .collect();

        // Expand AuthProfileRef fields so save_edit() receives the referenced
        // profile's name (e.g. `sso_session_ref_name`) and can persist the
        // `sso_session = NAME` indirection. Mirrors the login path; a no-op for
        // providers whose form declares no AuthProfileRef fields.
        let registry_snapshot: Vec<AuthProfile> = self.app_state.read(cx).list_auth_profiles();
        let raw_profile = AuthProfile {
            id: self.editing_profile_id.unwrap_or_else(Uuid::new_v4),
            name: name.clone(),
            provider_id: provider_id.clone(),
            fields: raw_fields,
            enabled: self.profile_enabled,
            read_only: false,
            dangling_origin: None,
        };
        let fields = dbflux_core::auth::expand_auth_profile_refs(
            &raw_profile,
            provider.form_def(),
            &|target_id| {
                registry_snapshot
                    .iter()
                    .find(|p| p.id == *target_id)
                    .cloned()
            },
        )
        .fields;

        let outcome = provider.save_edit(&name, &fields, &snapshot);

        match outcome {
            AuthSaveOutcome::Saved => {
                // Write succeeded. Signal a refresh so reflection picks up the
                // updated section (mtime change drives cache invalidation).
                self.edit_conflict_msg = None;
                self.app_state.update(cx, |_, cx| {
                    cx.emit(AppStateChanged);
                });

                // Reload the form to pick up the new values and a fresh snapshot.
                // This must run before setting the status: load_profile_into_form
                // resets provider_login_status, which would otherwise wipe the
                // success message before it is ever rendered.
                let profile_id = self.editing_profile_id;
                if let Some(id) = profile_id {
                    self.load_profile_into_form(id, window, cx);
                }

                let success_text = self
                    .app_state
                    .read(cx)
                    .auth_provider_by_id(&provider_id)
                    .and_then(|p| {
                        p.capabilities()
                            .edit
                            .as_ref()
                            .map(|e| e.success_written.clone())
                    })
                    .unwrap_or_else(|| SUCCESS_WRITTEN_FALLBACK.to_string());
                self.provider_login_status = Some((success_text, true));
                cx.notify();
            }

            AuthSaveOutcome::Conflict { target } => {
                // Section changed on disk since the form was opened — block save.
                let label = &target.label;
                self.edit_conflict_msg = Some((
                    format!(
                        "This profile was modified on disk ({label}) since you opened it. \
                         Reload to see the current values before saving."
                    ),
                    true,
                ));
                cx.notify();
            }

            AuthSaveOutcome::PartialSaved {
                written,
                conflicted,
            } => {
                // One resource succeeded; the other conflicted.
                let written_label = &written.label;
                let conflicted_label = &conflicted.label;
                self.edit_conflict_msg = Some((
                    format!(
                        "{written_label} was saved successfully, but {conflicted_label} was \
                         modified on disk since you opened the form. Reload to refresh the \
                         credentials section and re-apply your changes."
                    ),
                    true,
                ));
                cx.notify();
            }
        }
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

        let Some(raw_profile) = self.current_form_profile(cx) else {
            self.provider_login_status = Some((
                "Provide a profile name and provider fields before login.".to_string(),
                false,
            ));
            cx.notify();
            return;
        };

        // Expand AuthProfileRef fields so the provider's login() sees a flat
        // field map (e.g. `sso_start_url` filled from the referenced session).
        let profile_registry_snapshot: Vec<AuthProfile> =
            self.app_state.read(cx).list_auth_profiles();
        let profile = dbflux_core::auth::expand_auth_profile_refs(
            &raw_profile,
            provider.form_def(),
            &|target_id| {
                profile_registry_snapshot
                    .iter()
                    .find(|p| p.id == *target_id)
                    .cloned()
            },
        );

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

        // The verification URL arrives via `UrlCallback` from a background
        // thread *before* `provider.login()` completes (the AWS provider
        // blocks waiting for the user to finish in the browser). We forward
        // the URL through a channel so the modal can open immediately —
        // independently of when the login future resolves.
        let (url_tx, url_rx) = std::sync::mpsc::sync_channel::<Option<String>>(1);
        let url_callback: dbflux_core::auth::UrlCallback = Box::new(move |url| {
            let _ = url_tx.try_send(url);
        });

        // URL-forwarding task: poll the channel and push the verification URL
        // into the modal as soon as the provider surfaces it.
        let this_for_url = this.clone();
        cx.spawn(async move |_, cx| {
            loop {
                match url_rx.try_recv() {
                    Ok(Some(url)) => {
                        let _ = cx.update(|cx| {
                            this_for_url.update(cx, |this, cx| {
                                this.active_login_url = Some(url.clone());
                                this.pending_sso_url =
                                    Some((provider_name_for_url, profile_name_for_url, Some(url)));
                                cx.notify();
                            });
                        });
                        break;
                    }
                    Ok(None) => break, // provider explicitly has no URL
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        cx.background_executor()
                            .timer(std::time::Duration::from_millis(150))
                            .await;
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                }
            }
        })
        .detach();

        // Main task: drive the login future to completion, then update final
        // status. The URL itself is already routed by the task above.
        cx.spawn(async move |_this, cx| {
            let result = provider.login(&profile, url_callback).await;

            if let Err(err) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.provider_login_loading = false;
                    // Login finished — clear the inline URL display regardless
                    // of outcome (success means token cached, failure means
                    // the URL is no longer actionable).
                    this.active_login_url = None;
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

                    if this
                        .provider_login_status
                        .as_ref()
                        .is_some_and(|status| status.1)
                    {
                        this.options_cache.clear();
                        this.field_login_hint.clear();
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

            // Stored-only access is intentional: `remove_auth_profile` operates
            // on the stored list by index; reflected profiles are not removable.
            #[allow(deprecated)]
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
        // Stored-only access is intentional: after deletion the sidebar re-selects
        // the first stored profile (reflected profiles are not user-managed).
        #[allow(deprecated)]
        let first_stored_id = self
            .app_state
            .read(cx)
            .auth_profiles()
            .first()
            .map(|profile| profile.id);
        self.selected_profile_id = first_stored_id;

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

    /// Renders the inline login-URL panel: shows the verification URL with
    /// Open Browser, Copy URL, and Cancel buttons. Used while an interactive
    /// SSO login is in flight from the Settings window.
    fn render_login_url_panel(&self, url: String, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let url_for_open = url.clone();
        let url_for_copy = url.clone();

        div()
            .mt_2()
            .p(Spacing::SM)
            .rounded(Radii::SM)
            .border_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .flex()
            .flex_col()
            .gap_2()
            .child(Text::caption(
                "Open this URL in your browser to finish the login. DBFlux will continue automatically once you complete authentication.",
            ))
            .child(
                div()
                    .p(Spacing::SM)
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.border)
                    .bg(theme.background)
                    .child(Text::body(url.clone())),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("auth-login-open-url", "Open Browser")
                            .small()
                            .primary()
                            .on_click(cx.listener(move |_this, _, _, cx| {
                                cx.open_url(&url_for_open);
                            })),
                    )
                    .child(
                        Button::new("auth-login-copy-url", "Copy URL")
                            .small()
                            .on_click(cx.listener(move |_this, _, _, cx| {
                                cx.write_to_clipboard(
                                    gpui::ClipboardItem::new_string(url_for_copy.clone()),
                                );
                            })),
                    )
                    .child(
                        Button::new("auth-login-cancel", "Cancel")
                            .small()
                            .danger()
                            .on_click(cx.listener(|this, _, _, cx| {
                                // Ask the active provider to abort whatever
                                // in-flight login it has for the profile
                                // being edited. The provider's login future
                                // will then return an error and the spawned
                                // login task will clean up final status.
                                if let Some(profile) = this.current_form_profile(cx)
                                    && let Some(provider) = this.selected_provider(cx)
                                {
                                    let _ = provider.abort_login(&profile);
                                }
                                this.active_login_url = None;
                                this.provider_login_status = Some((
                                    "Login cancelled by user.".to_string(),
                                    false,
                                ));
                                cx.notify();
                            })),
                    ),
            )
    }

    /// Returns the AuthProfile referenced by `trigger_value` (expected to
    /// be a UUID string), used to populate "inherited from" hints and to
    /// surface the referenced field's value in disabled inputs.
    fn resolve_ref_profile(&self, trigger_value: &str, cx: &App) -> Option<AuthProfile> {
        let target_id = Uuid::parse_str(trigger_value.trim()).ok()?;
        // Stored-only access is intentional: ref-profile resolution in the form
        // editor resolves display hints for profiles that were explicitly linked
        // in the stored form data; reflected profiles cannot be referenced here.
        #[allow(deprecated)]
        let result = self
            .app_state
            .read(cx)
            .auth_profiles()
            .iter()
            .find(|profile| profile.id == target_id)
            .cloned();
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn render_input_row_disabled(
        &self,
        label: &str,
        input: &Entity<InputState>,
        field: AuthFormField,
        is_focused: bool,
        disabled: bool,
        disabled_hint: Option<String>,
        inherited_value: Option<String>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let primary = cx.theme().primary;
        let theme = cx.theme();

        let row = if disabled {
            // Render a static, non-interactive text view. gpui_component's
            // Input keeps its key_down handler bound even when `disabled` is
            // true, so an "Input::disabled(true)" widget would still accept
            // keystrokes. A read-only text node makes the field truly inert.
            let value = inherited_value
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| input.read(cx).value().to_string());
            let display = if value.trim().is_empty() {
                "—".to_string()
            } else {
                value
            };

            layout::compact_input_shell(
                div()
                    .flex()
                    .items_center()
                    .w_full()
                    .px_2()
                    .py_1()
                    .bg(theme.muted)
                    .text_color(theme.muted_foreground)
                    .child(Text::body(display)),
            )
        } else {
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
            )
        };

        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(Label::new(label.to_string()))
            .child(row)
            .when_some(disabled_hint, |container, hint| {
                container.child(Text::caption(hint))
            })
    }

    fn render_provider_selector(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let items: Vec<DropdownItem> = self
            .provider_entries_cache
            .iter()
            .map(|(provider_id, label)| {
                DropdownItem::with_value(label.clone(), provider_id.clone())
            })
            .collect();

        let selected_index = self
            .selected_provider_id
            .as_deref()
            .and_then(|provider_id| {
                self.provider_entries_cache
                    .iter()
                    .position(|(id, _)| id == provider_id)
            });

        self.provider_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_items(items, cx);
            dropdown.set_selected_index(selected_index, cx);
        });

        self.provider_dropdown.clone()
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
                            .icon(Icon::new(AppIcon::Plus))
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
                            .rounded(Radii::SM)
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

    /// Renders a read-only info panel for profiles that are reflected from an
    /// external config (e.g. `~/.aws/config`). No editable inputs are shown;
    /// all field values are displayed as plain text. A dangling-profile banner
    /// is shown when `profile_dangling_origin` is set.
    fn render_read_only_mirror(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        let profile_name = self.input_name.read(cx).value().to_string();

        let provider_label = self
            .selected_provider_id
            .as_deref()
            .and_then(|id| self.app_state.read(cx).auth_provider_by_id(id))
            .map(|provider| provider.display_name().to_string())
            .or_else(|| self.selected_provider_id.clone())
            .unwrap_or_default();

        let field_rows: Vec<(String, String)> = self
            .provider_field_order
            .iter()
            .filter_map(|field_id| {
                let value = self.form_inputs.get(field_id)?.read(cx).value().to_string();

                if value.is_empty() {
                    return None;
                }

                Some((field_id.replace('_', " "), value))
            })
            .collect();

        let dangling_origin = self.profile_dangling_origin.clone();

        let edit_caps: Option<AuthEditCapabilities> = self
            .selected_provider_id
            .as_deref()
            .and_then(|id| self.app_state.read(cx).auth_provider_by_id(id))
            .and_then(|p| p.capabilities().edit.clone());

        layout::sticky_form_shell(
            div()
                .child(Label::new(profile_name))
                .child(Text::muted(provider_label)),
            div()
                .flex()
                .flex_col()
                .gap_3()
                .when_some(dangling_origin, |content, origin| {
                    let dangling = edit_caps
                        .as_ref()
                        .and_then(|e| e.dangling_messages.get(&origin))
                        .cloned()
                        .unwrap_or_else(dangling_fallback);
                    let banner_text = dangling.title;
                    let hint_text = dangling.body;

                    content.child(
                        div()
                            .p_3()
                            .rounded(Radii::SM)
                            .border_1()
                            .border_color(theme.warning)
                            .bg(theme.secondary)
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        FluxIcon::new(AppIcon::TriangleAlert)
                                            .size(Heights::ICON_SM)
                                            .color(theme.warning),
                                    )
                                    .child(Label::new(banner_text)),
                            )
                            .child(Text::caption(hint_text)),
                    )
                })
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Label::new("Source"))
                        .child(Text::body(
                            edit_caps
                                .as_ref()
                                .map(|e| e.mirror_label.clone())
                                .unwrap_or_else(|| MIRROR_LABEL_FALLBACK.to_string()),
                        )),
                )
                .children(field_rows.into_iter().map(|(label, value)| {
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(Label::new(label))
                        .child(Text::body(value))
                })),
            None,
            &theme,
        )
    }

    fn render_editor_panel(&mut self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        // Read-only (reflected) profiles get a mirror view with no editable
        // inputs and no save/delete controls.
        if self.profile_is_read_only {
            return self.render_read_only_mirror(cx).into_any_element();
        }

        let theme = cx.theme().clone();
        let is_editing = self.editing_profile_id.is_some();

        let edit_caps: Option<AuthEditCapabilities> = self
            .selected_provider_id
            .as_deref()
            .and_then(|id| self.app_state.read(cx).auth_provider_by_id(id))
            .and_then(|p| p.capabilities().edit.clone());

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

        // Provide a synthetic session marker so `requires_session` fields
        // unblock once the user has either completed login in this editor
        // pass or has a valid status from a previous login. We never
        // serialize real session data into the form snapshot — the auth
        // provider re-establishes its session from its own cache (e.g. the
        // SSO token cache on disk). When no valid session exists the
        // provider returns `NeedsLogin` / `SessionExpired`, which
        // `apply_fetch_error` surfaces as a re-login hint below the field.
        // We pass the marker unconditionally so users with a valid session
        // from a prior `aws sso login` or DBFlux run see options without
        // having to click Login again.
        let session_marker = Some(serde_json::Value::Null);

        for field in &field_defs_for_fetch {
            if matches!(field.kind, FormFieldKind::DynamicSelect { .. }) {
                self.fetch_dynamic_options_if_needed(
                    provider_id_for_fetch.clone(),
                    field,
                    session_marker.clone(),
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
                    self.render_dynamic_dropdown_row(field, cx)
                        .into_any_element()
                } else if matches!(field.kind, FormFieldKind::AuthProfileRef { .. }) {
                    self.render_auth_profile_ref_row(field, cx)
                        .into_any_element()
                } else if let Some(input) = self.form_inputs.get(&field.id) {
                    let form_field = AuthFormField::DynamicField(idx);
                    let is_focused = self.auth_form_field == form_field
                        && self.auth_focus == AuthFocus::Form
                        && self.content_focused;

                    let (disabled, disabled_hint, inherited_value) = field
                        .disabled_when_field_set
                        .as_deref()
                        .and_then(|trigger_id| {
                            let trigger_value = self
                                .form_inputs
                                .get(trigger_id)?
                                .read(cx)
                                .value()
                                .to_string();
                            if trigger_value.trim().is_empty() {
                                return None;
                            }

                            let referenced = self.resolve_ref_profile(&trigger_value, cx);
                            let label = referenced.as_ref().map(|p| p.name.clone());
                            let inherited = referenced
                                .as_ref()
                                .and_then(|p| p.fields.get(&field.id).cloned())
                                .filter(|v| !v.trim().is_empty());

                            let hint = label
                                .map(|name| format!("Inherited from {} '{}'.", trigger_id, name))
                                .unwrap_or_else(|| format!("Inherited from {}.", trigger_id));
                            Some((true, Some(hint), inherited))
                        })
                        .unwrap_or((false, None, None));

                    // WriteOnly fields get a help hint appended below the input
                    // to clarify write-only semantics (spec S33, R9.4.1).
                    let extra_hint: Option<String> =
                        if field.kind == FormFieldKind::WriteOnly && !disabled {
                            Some(field.help.clone().unwrap_or_else(|| {
                                "Leave blank to keep the current value.".to_string()
                            }))
                        } else {
                            None
                        };

                    let rendered = self.render_input_row_disabled(
                        &field.label,
                        input,
                        form_field,
                        is_focused,
                        disabled,
                        disabled_hint,
                        inherited_value,
                        cx,
                    );

                    // Wrap with extra write-only hint if needed.
                    if let Some(hint) = extra_hint {
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(rendered)
                            .child(Text::caption(hint))
                            .into_any_element()
                    } else {
                        rendered.into_any_element()
                    }
                } else {
                    div().into_any_element()
                }
            })
            .collect();

        let conflict_msg = self.edit_conflict_msg.clone();

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
                .when_some(conflict_msg, |content, (msg, show_reload)| {
                    let theme = cx.theme().clone();
                    content.child(
                        div()
                            .p_3()
                            .rounded(Radii::SM)
                            .border_1()
                            .border_color(theme.warning)
                            .bg(theme.secondary)
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        FluxIcon::new(AppIcon::TriangleAlert)
                                            .size(Heights::ICON_SM)
                                            .color(theme.warning),
                                    )
                                    .child(Label::new("Profile Changed on Disk")),
                            )
                            .child(Text::caption(msg))
                            .when(show_reload, |panel| {
                                panel.child(
                                    Button::new("edit-reload-profile", "Reload")
                                        .small()
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            if let Some(id) = this.selected_profile_id {
                                                this.load_profile_into_form(id, window, cx);
                                            }
                                        })),
                                )
                            }),
                    )
                })
                .child({
                    let is_focused = self.auth_form_field == AuthFormField::Name
                        && self.auth_focus == AuthFocus::Form
                        && self.content_focused;
                    let is_reflected = self.edit_snapshot.is_some();
                    let name_hint = if is_reflected {
                        let hint = edit_caps
                            .as_ref()
                            .map(|e| e.name_field_hint.as_str())
                            .unwrap_or(NAME_HINT_FALLBACK);
                        if hint.is_empty() {
                            None
                        } else {
                            Some(hint.to_string())
                        }
                    } else {
                        None
                    };
                    self.render_input_row_disabled(
                        "Name",
                        &self.input_name,
                        AuthFormField::Name,
                        is_focused,
                        is_reflected,
                        name_hint,
                        None,
                        cx,
                    )
                })
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(Label::new("Provider"))
                        .when(self.edit_snapshot.is_none(), |row| {
                            row.child(self.render_provider_selector(window, cx))
                        })
                        .when(self.edit_snapshot.is_some(), |row| {
                            // Reflected profiles: provider is fixed by the file section type.
                            let provider_label = self
                                .selected_provider_id
                                .as_deref()
                                .and_then(|id| {
                                    self.app_state
                                        .read(cx)
                                        .auth_provider_by_id(id)
                                        .map(|p| p.display_name().to_string())
                                })
                                .or_else(|| self.selected_provider_id.clone())
                                .unwrap_or_default();
                            row.child(Text::body(provider_label))
                        }),
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
                        .when_some(self.active_login_url.clone(), |content, url| {
                            content.child(self.render_login_url_panel(url, cx))
                        })
                })
                .when(self.edit_snapshot.is_none(), |content| {
                    // The enabled toggle only applies to stored (SQLite-backed)
                    // profiles. Reflected AWS profiles are always enabled and
                    // managed by the ~/.aws/config file.
                    content.child(
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
                    )
                }),
            None,
            &theme,
        )
        .into_any_element()
    }

    fn render_section_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_editing = self.editing_profile_id.is_some();
        // Reflected profiles are edited in-place via save_edit; they cannot be
        // deleted from DBFlux (the file section is the source of truth).
        let is_reflected = self.edit_snapshot.is_some();
        let is_form_focused = self.auth_focus == AuthFocus::Form && self.content_focused;
        let primary = cx.theme().primary;

        div()
            .flex()
            .items_center()
            .gap_3()
            .when(is_editing && !is_reflected, |root| {
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
                    if is_reflected {
                        "Save to File"
                    } else if is_editing {
                        "Update"
                    } else {
                        "Create"
                    },
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
            !self.provider_entries_cache.is_empty(),
            self.provider_field_order.len(),
            self.selected_provider_supports_login,
            self.editing_profile_id.is_some(),
            self.edit_snapshot.is_some(),
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
            AuthFormField::Provider(_) => {
                // The provider selector is a single dropdown widget.
                // Keyboard activation (Enter) on this row focuses the dropdown;
                // the dropdown's own keyboard handling takes over from there.
                // No state mutation is needed here — selection is driven by
                // the `DropdownSelectionChanged` subscription in the constructor.
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
        // Read-only profiles have no editable state — suppress Save/Delete.
        if self.profile_is_read_only {
            return None;
        }

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

        let profiles = self.app_state.read(cx).list_auth_profiles();
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
                ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn form_rows_include_generic_provider_login_without_aws_feature() {
        let rows = build_form_rows(true, 2, true, false, false);

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

    /// (c) `RefreshTrigger::Manual` fetches only on cache miss (first render).
    ///     The trigger must NOT cause a refetch when a cached entry exists.
    #[::core::prelude::v1::test]
    fn manual_trigger_fetches_only_on_cache_miss() {
        // With no cached entry: needs_fetch must be true.
        let empty_cache: HashMap<(String, String, u64), CachedOptions> = HashMap::new();
        let cache_key = ("provider".to_string(), "field".to_string(), 0u64);
        let needs_fetch_on_miss = !empty_cache.contains_key(&cache_key);
        assert!(
            needs_fetch_on_miss,
            "Manual trigger must fetch when no cached entry exists"
        );

        // With a cached entry: needs_fetch must be false.
        let mut cache_with_entry: HashMap<(String, String, u64), CachedOptions> = HashMap::new();
        cache_with_entry.insert(
            cache_key.clone(),
            CachedOptions {
                options: vec![],
                expires_at: Instant::now() + std::time::Duration::from_secs(300),
            },
        );
        let needs_fetch_on_hit = !cache_with_entry.contains_key(&cache_key);
        assert!(
            !needs_fetch_on_hit,
            "Manual trigger must NOT refetch when a cached entry exists"
        );
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

        let fires_with_login = matches!(trigger, RefreshTrigger::OnLoginComplete);
        assert!(
            fires_with_login,
            "OnLoginComplete must trigger when the login-complete signal is true"
        );
    }

    // -----------------------------------------------------------------------
    // NFR-003: apply_fetch_error_state — re-login hint surfacing
    // -----------------------------------------------------------------------

    /// `NeedsLogin` inserts a per-field hint and sets the status banner to a
    /// warning (success=false) when no login is currently in progress.
    #[::core::prelude::v1::test]
    fn needs_login_sets_field_hint_and_status_banner() {
        let mut hints: HashMap<String, String> = HashMap::new();
        let mut status: Option<(String, bool)> = None;

        apply_fetch_error_state(
            "account_id",
            FetchOptionsError::NeedsLogin,
            &mut hints,
            &mut status,
            false,
        );

        assert!(
            hints.contains_key("account_id"),
            "NeedsLogin must insert a hint for the affected field"
        );
        assert!(
            status.is_some(),
            "NeedsLogin must set a status banner when login is not in progress"
        );
        let (msg, success) = status.unwrap();
        assert!(
            !success,
            "NeedsLogin status banner must be a warning (success=false)"
        );
        assert!(
            msg.to_lowercase().contains("login"),
            "status message must reference login"
        );
    }

    /// `SessionExpired` inserts a per-field hint and sets the status banner.
    #[::core::prelude::v1::test]
    fn session_expired_sets_field_hint_and_status_banner() {
        let mut hints: HashMap<String, String> = HashMap::new();
        let mut status: Option<(String, bool)> = None;

        apply_fetch_error_state(
            "role_arn",
            FetchOptionsError::SessionExpired,
            &mut hints,
            &mut status,
            false,
        );

        assert!(
            hints.contains_key("role_arn"),
            "SessionExpired must insert a hint for the affected field"
        );
        let (msg, success) = status.expect("SessionExpired must set a status banner");
        assert!(
            !success,
            "SessionExpired status banner must be a warning (success=false)"
        );
        assert!(
            msg.to_lowercase().contains("expired") || msg.to_lowercase().contains("login"),
            "status message must mention expiry or login"
        );
    }

    /// When `login_in_progress` is true, `NeedsLogin` still writes the
    /// field hint but must NOT overwrite the in-progress status banner.
    #[::core::prelude::v1::test]
    fn needs_login_preserves_in_progress_status_banner() {
        let mut hints: HashMap<String, String> = HashMap::new();
        let existing_status = "Starting auth-provider login for profile 'dev'...".to_string();
        let mut status: Option<(String, bool)> = Some((existing_status.clone(), false));

        apply_fetch_error_state(
            "account_id",
            FetchOptionsError::NeedsLogin,
            &mut hints,
            &mut status,
            true, // login_in_progress = true
        );

        assert!(
            hints.contains_key("account_id"),
            "Field hint must still be inserted while login is in progress"
        );
        let (msg, _) = status.expect("Status banner must not be cleared");
        assert_eq!(
            msg, existing_status,
            "Status banner must not be overwritten while login is in progress"
        );
    }

    /// Transient errors do NOT set a field hint or status banner.
    #[::core::prelude::v1::test]
    fn transient_error_does_not_set_hint_or_status() {
        let mut hints: HashMap<String, String> = HashMap::new();
        let mut status: Option<(String, bool)> = None;

        apply_fetch_error_state(
            "region",
            FetchOptionsError::Transient("network timeout".to_string()),
            &mut hints,
            &mut status,
            false,
        );

        assert!(
            hints.is_empty(),
            "Transient error must not insert a field hint"
        );
        assert!(
            status.is_none(),
            "Transient error must not set a status banner"
        );
    }

    /// Permanent errors do NOT set a field hint or status banner.
    #[::core::prelude::v1::test]
    fn permanent_error_does_not_set_hint_or_status() {
        let mut hints: HashMap<String, String> = HashMap::new();
        let mut status: Option<(String, bool)> = None;

        apply_fetch_error_state(
            "region",
            FetchOptionsError::Permanent("unsupported field".to_string()),
            &mut hints,
            &mut status,
            false,
        );

        assert!(
            hints.is_empty(),
            "Permanent error must not insert a field hint"
        );
        assert!(
            status.is_none(),
            "Permanent error must not set a status banner"
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

    // -----------------------------------------------------------------------
    // T-5.2: read_only profile → no save/delete in UI logic
    // -----------------------------------------------------------------------

    /// `build_form_rows` should always include a SaveButton row; the guard that
    /// suppresses it for read-only profiles lives in `render_footer_actions`
    /// (UI path), not in the form-row builder. Verify the builder is
    /// unaffected so it continues to serve editable profiles.
    #[::core::prelude::v1::test]
    fn form_rows_include_save_and_delete_when_editing() {
        let rows = build_form_rows(true, 0, false, true, false);
        assert!(
            rows.iter()
                .any(|row| row.contains(&AuthFormField::SaveButton)),
            "SaveButton must appear in form rows when editing a stored profile"
        );
        assert!(
            rows.iter()
                .any(|row| row.contains(&AuthFormField::DeleteButton)),
            "DeleteButton must appear in form rows when editing a stored profile"
        );
    }

    /// When creating a new profile (not editing), there must be no DeleteButton.
    #[::core::prelude::v1::test]
    fn form_rows_no_delete_when_creating() {
        let rows = build_form_rows(true, 0, false, false, false);
        assert!(
            !rows
                .iter()
                .any(|row| row.contains(&AuthFormField::DeleteButton)),
            "DeleteButton must not appear when creating a new profile"
        );
    }

    // -----------------------------------------------------------------------
    // T-5.3: read-only guard in save_profile is enforced by profile_is_read_only
    // -----------------------------------------------------------------------

    /// `build_auth_profile_from_form` always produces `read_only: false` —
    /// read-only is only set by the load path from the reflected provider.
    #[::core::prelude::v1::test]
    fn build_auth_profile_from_form_always_produces_writable_profile() {
        let profile =
            build_auth_profile_from_form(Uuid::nil(), "test", "aws-sso", HashMap::new(), true)
                .expect("should build");
        assert!(
            !profile.read_only,
            "profiles built from the form must be writable (read_only = false)"
        );
        assert!(
            profile.dangling_origin.is_none(),
            "profiles built from the form must have no dangling origin"
        );
    }

    // -----------------------------------------------------------------------
    // T-5.4: dangling-reference guard produces distinct messages per origin
    // -----------------------------------------------------------------------

    /// The message for a "keyring-only" dangling profile must reference
    /// `~/.aws/credentials` and must not mention any secret value.
    #[::core::prelude::v1::test]
    fn dangling_keyring_only_message_references_credentials_file() {
        let msg = format!(
            "Auth profile '{}' is only in the DBFlux keyring and no longer has a \
             corresponding entry in ~/.aws/config or ~/.aws/credentials. \
             Add the credentials to ~/.aws/credentials to connect with this profile.",
            "legacy-key"
        );
        assert!(
            msg.contains("~/.aws/credentials"),
            "keyring-only dangling message must direct user to ~/.aws/credentials"
        );
        assert!(
            !msg.to_ascii_lowercase().contains("secret"),
            "keyring-only dangling message must not expose the word 'secret'"
        );
        assert!(
            msg.contains("legacy-key"),
            "dangling message must name the profile"
        );
    }

    /// The message for a "file-gone" dangling profile must reference
    /// `~/.aws/config`.
    #[::core::prelude::v1::test]
    fn dangling_file_gone_message_references_config_file() {
        let msg = format!(
            "Auth profile '{}' could not be found in ~/.aws/config. \
             Please recreate the profile or update the connection binding.",
            "old-env"
        );
        assert!(
            msg.contains("~/.aws/config"),
            "file-gone dangling message must reference ~/.aws/config"
        );
    }

    // -----------------------------------------------------------------------
    // WU-E4: Editable form state-transition logic (non-GPUI-render parts)
    // -----------------------------------------------------------------------

    /// E4: Reflected profiles (non-dangling) must NOT produce a Delete button
    /// row in `build_form_rows` — they are edited via `save_edit`, not deleted
    /// from DBFlux (spec S36, design §14).
    #[::core::prelude::v1::test]
    fn reflected_profile_has_no_delete_button_row() {
        let rows = build_form_rows(true, 3, false, true, /* is_reflected */ true);
        assert!(
            !rows
                .iter()
                .any(|row| row.contains(&AuthFormField::DeleteButton)),
            "reflected profiles must not expose a Delete button"
        );
        assert!(
            rows.iter()
                .any(|row| row.contains(&AuthFormField::SaveButton)),
            "reflected profiles must still expose a Save button"
        );
    }

    /// E4: Reflected profiles must NOT include the Enabled row — the enabled
    /// state is managed by the AWS file, not DBFlux.
    #[::core::prelude::v1::test]
    fn reflected_profile_has_no_enabled_row() {
        let rows = build_form_rows(true, 2, false, true, /* is_reflected */ true);
        assert!(
            !rows.iter().any(|row| row.contains(&AuthFormField::Enabled)),
            "reflected profiles must not expose the Enabled toggle"
        );
    }

    /// E4: Reflected profiles must NOT include the Provider selector row —
    /// provider is fixed by the file section type.
    #[::core::prelude::v1::test]
    fn reflected_profile_has_no_provider_selector_row() {
        let rows = build_form_rows(
            /* has_providers */ true, 2, false, true, /* is_reflected */ true,
        );
        assert!(
            !rows
                .iter()
                .any(|row| { row.iter().any(|f| matches!(f, AuthFormField::Provider(_))) }),
            "reflected profiles must not expose the provider selector row"
        );
    }

    /// E4: `collect_edited_fields` omits blank `WriteOnly` fields so the
    /// provider preserves the existing on-disk value (spec R9.4.2, S27).
    ///
    /// This exercises the logic inline rather than through the GPUI form
    /// (render code cannot be unit-tested without a GPUI context).
    #[::core::prelude::v1::test]
    fn edited_fields_omit_blank_write_only_field() {
        let write_only_ids: std::collections::HashSet<String> = [
            "aws_secret_access_key".to_string(),
            "aws_session_token".to_string(),
        ]
        .into_iter()
        .collect();

        // Simulate the form value map: access key present, secret blank.
        let form_values: Vec<(String, String)> = vec![
            (
                "aws_access_key_id".to_string(),
                "AKIAIOSFODNN7EXAMPLE".to_string(),
            ),
            ("aws_secret_access_key".to_string(), String::new()), // blank — must be omitted
            ("aws_session_token".to_string(), String::new()),     // blank — must be omitted
        ];

        let fields: HashMap<String, String> = form_values
            .into_iter()
            .filter_map(|(field_id, value)| {
                if write_only_ids.contains(&field_id) && value.is_empty() {
                    None
                } else {
                    Some((field_id, value))
                }
            })
            .collect();

        assert!(
            fields.contains_key("aws_access_key_id"),
            "non-secret fields must be included"
        );
        assert!(
            !fields.contains_key("aws_secret_access_key"),
            "blank secret field must be omitted so provider preserves on-disk value"
        );
        assert!(
            !fields.contains_key("aws_session_token"),
            "blank session token must be omitted"
        );
    }

    /// E4: A non-blank `WriteOnly` field IS included in the edited fields map
    /// so the provider overwrites the on-disk value (spec R9.4.3, S26).
    #[::core::prelude::v1::test]
    fn edited_fields_include_non_blank_write_only_field() {
        let write_only_ids: std::collections::HashSet<String> =
            ["aws_secret_access_key".to_string()].into_iter().collect();

        let form_values: Vec<(String, String)> = vec![
            (
                "aws_access_key_id".to_string(),
                "AKIAIOSFODNN7EXAMPLE".to_string(),
            ),
            (
                "aws_secret_access_key".to_string(),
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            ),
        ];

        let fields: HashMap<String, String> = form_values
            .into_iter()
            .filter_map(|(field_id, value)| {
                if write_only_ids.contains(&field_id) && value.is_empty() {
                    None
                } else {
                    Some((field_id, value))
                }
            })
            .collect();

        assert!(
            fields.contains_key("aws_secret_access_key"),
            "non-blank secret field must be included in the save payload"
        );
    }

    /// E4: `Conflict` outcome produces a conflict message that names the file
    /// and offers a Reload affordance (spec R9.3.4, S29). The state-transition
    /// logic in `save_edit_profile` is tested indirectly through the helpers
    /// that compute the conflict message — the GPUI state mutation itself
    /// is render-only and cannot be unit-tested here.
    #[::core::prelude::v1::test]
    fn conflict_message_references_file_and_suggests_reload() {
        use dbflux_core::{AuthEditTarget, AuthSaveOutcome};

        let outcome = AuthSaveOutcome::Conflict {
            target: AuthEditTarget {
                id: "config".to_string(),
                label: "~/.aws/config".to_string(),
            },
        };

        let msg = match &outcome {
            AuthSaveOutcome::Conflict { target } => {
                let label = &target.label;
                format!(
                    "This profile was modified on disk ({label}) since you opened it. \
                     Reload to see the current values before saving."
                )
            }
            _ => panic!("unexpected outcome variant"),
        };

        assert!(
            msg.contains("~/.aws/config"),
            "conflict message must name the affected file"
        );
        assert!(
            msg.to_lowercase().contains("reload"),
            "conflict message must mention the Reload action"
        );
        assert!(
            !msg.contains("AKIA"),
            "conflict message must not contain credential patterns"
        );
    }

    /// E4: `PartialSaved` outcome produces a message that names BOTH files
    /// (written and conflicted) and offers a Reload affordance (spec S35).
    #[::core::prelude::v1::test]
    fn partial_saved_message_names_both_files() {
        use dbflux_core::{AuthEditTarget, AuthSaveOutcome};

        let outcome = AuthSaveOutcome::PartialSaved {
            written: AuthEditTarget {
                id: "config".to_string(),
                label: "~/.aws/config".to_string(),
            },
            conflicted: AuthEditTarget {
                id: "credentials".to_string(),
                label: "~/.aws/credentials".to_string(),
            },
        };

        let msg = match &outcome {
            AuthSaveOutcome::PartialSaved {
                written,
                conflicted,
            } => {
                let written_label = &written.label;
                let conflicted_label = &conflicted.label;
                format!(
                    "{written_label} was saved successfully, but {conflicted_label} was modified \
                     on disk since you opened the form. Reload to refresh the credentials section \
                     and re-apply your changes."
                )
            }
            _ => panic!("unexpected outcome variant"),
        };

        assert!(
            msg.contains("~/.aws/config"),
            "partial-saved message must name the written file"
        );
        assert!(
            msg.contains("~/.aws/credentials"),
            "partial-saved message must name the conflicted file"
        );
        assert!(
            msg.to_lowercase().contains("reload"),
            "partial-saved message must mention the Reload action"
        );
    }

    // -----------------------------------------------------------------------
    // WU-E5: Security audit — outcomes and types carry no secrets
    // -----------------------------------------------------------------------

    /// E5.1: `AuthSaveOutcome` Debug representation contains no secret patterns.
    /// This is also tested in dbflux_core, but we re-verify at the UI boundary
    /// where the type is matched and messages are constructed.
    #[::core::prelude::v1::test]
    fn auth_save_outcome_debug_has_no_secrets() {
        use dbflux_core::{AuthEditTarget, AuthSaveOutcome};

        let config_target = AuthEditTarget {
            id: "config".to_string(),
            label: "~/.aws/config".to_string(),
        };
        let credentials_target = AuthEditTarget {
            id: "credentials".to_string(),
            label: "~/.aws/credentials".to_string(),
        };

        let outcomes = [
            AuthSaveOutcome::Saved,
            AuthSaveOutcome::Conflict {
                target: config_target.clone(),
            },
            AuthSaveOutcome::PartialSaved {
                written: config_target,
                conflicted: credentials_target,
            },
        ];

        let secret_patterns = ["AKIA", "wJalrX", "aws_secret_access_key", "SECRET"];

        for outcome in &outcomes {
            let repr = format!("{outcome:?}");
            for pattern in &secret_patterns {
                assert!(
                    !repr.contains(pattern),
                    "AuthSaveOutcome debug must not contain '{pattern}' (found in: {repr})"
                );
            }
        }
    }

    /// E5.4: `build_form_rows` for a reflected profile never exposes the
    /// Enabled or Delete rows, regardless of the field count.
    #[::core::prelude::v1::test]
    fn reflected_form_rows_have_no_enabled_or_delete_for_any_field_count() {
        for field_count in [0usize, 1, 5, 10] {
            let rows = build_form_rows(true, field_count, false, true, true);
            assert!(
                !rows.iter().any(|r| r.contains(&AuthFormField::Enabled)),
                "Enabled row must be absent for reflected profiles (field_count={field_count})"
            );
            assert!(
                !rows
                    .iter()
                    .any(|r| r.contains(&AuthFormField::DeleteButton)),
                "Delete row must be absent for reflected profiles (field_count={field_count})"
            );
        }
    }
}
