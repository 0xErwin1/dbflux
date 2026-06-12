use std::collections::HashMap;

use dbflux_app::portability::{
    ConfirmSummary, ImportOutcome, ImportPersistence, OwnedDestSnapshot, confirm_summary,
    mapto_candidates,
};
use dbflux_components::controls::{Button, Checkbox, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    BannerBlock, BannerVariant, IconButton, SegmentedControl, SegmentedItem, Text, surface_raised,
};
use dbflux_components::tokens::{FontSizes, Heights, Spacing};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{AuthProfile, ConnectionProfile, ProxyProfile, SshTunnelProfile};
use dbflux_portability::{
    ConflictChoice, ConflictKind, ImportPlan, ParsedBundle, RequiredResolutionKind,
    ResolutionChoices,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Event emitted by [`ImportConnectionsPanel`] so the connection manager can
/// switch its view back to the driver picker and refresh the sidebar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportConnectionsPanelEvent {
    /// The user cancelled or pressed Back from the first step.
    Cancelled,
    /// An import run finished (fully or partially) and the user dismissed it.
    Completed,
}

/// Segment ids for the per-conflict choice control.
const CHOICE_REUSE: &str = "reuse";
const CHOICE_CREATE: &str = "create";
/// Prefix for "Map to <id>" segment ids; the destination UUID is appended.
const CHOICE_MAP_PREFIX: &str = "map:";

/// Sentinel segment id for "skip" on auth-profile required references.
const AUTH_SKIP: &str = "skip";
/// Prefix for "use destination auth profile <id>" segment ids.
const AUTH_USE_PREFIX: &str = "use:";

/// Steps of the import flow. Mirrors the previous wizard's state machine, minus
/// the window chrome.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Step {
    SelectFile,
    Preview,
    Conflicts,
    RequiredReferences,
    Outcome,
}

/// Bridges `AppState` to the portability `ImportPersistence` seam.
///
/// Identical wiring to the previous wizard's persistence adapter; the import
/// backend itself is unchanged.
struct AppStatePersistence<'a> {
    state: &'a mut dbflux_app::AppState,
    registered_drivers: std::collections::HashSet<String>,
}

impl<'a> AppStatePersistence<'a> {
    fn new(state: &'a mut dbflux_app::AppState) -> Self {
        let registered_drivers = state.drivers().keys().cloned().collect();
        Self {
            state,
            registered_drivers,
        }
    }
}

impl ImportPersistence for AppStatePersistence<'_> {
    fn add_auth_profile(&mut self, profile: AuthProfile) {
        self.state.add_auth_profile(profile);
    }

    fn add_ssh_tunnel(&mut self, tunnel: SshTunnelProfile) {
        self.state.add_ssh_tunnel(tunnel);
    }

    fn add_proxy(&mut self, proxy: ProxyProfile) {
        self.state.add_proxy(proxy);
    }

    fn add_connection(
        &mut self,
        profile: ConnectionProfile,
    ) -> dbflux_app::portability::ConnectionInsertResult {
        use dbflux_app::portability::ConnectionInsertResult;

        let driver_id = profile.driver_id().to_string();
        if !self.registered_drivers.contains(&driver_id) {
            return ConnectionInsertResult::NeedsDriver;
        }

        let Some(driver) = self.state.drivers().get(&driver_id).cloned() else {
            return ConnectionInsertResult::NeedsDriver;
        };

        if let dbflux_core::DbConfig::External { values, .. } = &profile.config {
            match driver.build_config(values) {
                Ok(config) => {
                    let mut rebuilt = profile;
                    rebuilt.config = config;
                    self.state.add_profile_in_folder(rebuilt, None);
                    ConnectionInsertResult::Ok
                }
                Err(e) => ConnectionInsertResult::ConfigFailed(e.to_string()),
            }
        } else {
            self.state.add_profile_in_folder(profile, None);
            ConnectionInsertResult::Ok
        }
    }

    fn write_secret(&self, secret_ref: &str, secret: &SecretString) -> bool {
        self.state.facade.secrets.set_by_ref(secret_ref, secret)
    }

    fn hydrate_auth_secret_fields(
        &mut self,
        auth_id: uuid::Uuid,
        fields: HashMap<String, SecretString>,
    ) {
        if let Some(profile) = self
            .state
            .facade
            .auth_profiles
            .items
            .iter_mut()
            .find(|p| p.id == auth_id)
        {
            profile.secret_fields = fields;
        }
    }
}

/// Result of a completed import run, rendered by reference (never `.take()`n)
/// so failure details survive every frame.
enum ImportRunResult {
    Outcome(ImportOutcome),
    Failed(String),
}

/// In-window connection import panel.
///
/// Lives INSIDE the connection manager window (rendered when the manager's view
/// is `View::Import`). It owns the parse -> plan -> resolve -> apply pipeline and
/// reuses the same leaf components as the export modal. All disk and crypto work
/// runs on the background executor.
pub struct ImportConnectionsPanel {
    app_state: Entity<AppStateEntity>,

    step: Step,

    // Step 1: file + passphrase.
    file_input: Entity<InputState>,
    pending_file_path: Option<String>,
    /// Set when a Browse attempt found no native picker; drives a one-line muted
    /// hint so the user types the path instead of seeing a blocking error.
    native_picker_unavailable: bool,
    bundle_encrypted: bool,
    show_passphrase: bool,
    passphrase_input: Entity<InputState>,
    parse_error: Option<String>,
    is_parsing: bool,

    // Plan products. Consumed by `do_apply`; counts survive via `confirm_summary`.
    parsed_bundle: Option<ParsedBundle>,
    import_plan: Option<ImportPlan>,
    confirm_summary: Option<ConfirmSummary>,

    // Conflict resolution choices, keyed by `bundle_local_id`.
    conflict_choices: HashMap<String, ConflictChoice>,

    // Required-secret inputs, keyed by `(owner_local_id, field)`. Created in
    // render via the pending flag (needs `&mut Window`).
    secret_inputs: HashMap<(String, String), Entity<InputState>>,
    secret_values: HashMap<(String, String), String>,
    pending_provision_secrets: bool,
    // Auth-profile picks for AwsReference / AuthProfileRef resolutions.
    auth_profile_choices: HashMap<(String, String), Uuid>,

    // Apply.
    is_applying: bool,
    run_result: Option<ImportRunResult>,

    dest_auth_profiles: Vec<AuthProfile>,

    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<ImportConnectionsPanelEvent> for ImportConnectionsPanel {}

impl ImportConnectionsPanel {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let passphrase_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Bundle passphrase")
                .masked(true)
        });

        let passphrase_sub = cx.subscribe(&passphrase_input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change | InputEvent::Blur) {
                this.parse_error = None;
                cx.notify();
            }
        });

        let file_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Path to TOML bundle\u{2026}"));

        let file_sub = cx.subscribe(&file_input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change | InputEvent::Blur) {
                this.parse_error = None;
                cx.notify();
            }
        });

        let dest_auth_profiles = app_state.read(cx).list_auth_profiles();
        let focus_handle = cx.focus_handle();

        Self {
            app_state,
            step: Step::SelectFile,
            file_input,
            pending_file_path: None,
            native_picker_unavailable: false,
            bundle_encrypted: false,
            show_passphrase: false,
            passphrase_input,
            parse_error: None,
            is_parsing: false,
            parsed_bundle: None,
            import_plan: None,
            confirm_summary: None,
            conflict_choices: HashMap::new(),
            secret_inputs: HashMap::new(),
            secret_values: HashMap::new(),
            pending_provision_secrets: false,
            auth_profile_choices: HashMap::new(),
            is_applying: false,
            run_result: None,
            dest_auth_profiles,
            focus_handle,
            _subscriptions: vec![passphrase_sub, file_sub],
        }
    }

    /// Reset the panel to its first step and focus it. Called by the connection
    /// manager when switching into the import view.
    pub fn reset(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.step = Step::SelectFile;
        self.pending_file_path = None;
        self.native_picker_unavailable = false;
        self.bundle_encrypted = false;
        self.show_passphrase = false;
        self.parse_error = None;
        self.is_parsing = false;
        self.parsed_bundle = None;
        self.import_plan = None;
        self.confirm_summary = None;
        self.conflict_choices.clear();
        self.secret_inputs.clear();
        self.secret_values.clear();
        self.pending_provision_secrets = false;
        self.auth_profile_choices.clear();
        self.is_applying = false;
        self.run_result = None;
        self.dest_auth_profiles = self.app_state.read(cx).list_auth_profiles();

        self.passphrase_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.file_input
            .update(cx, |state, cx| state.set_value("", window, cx));

        window.focus(&self.focus_handle);
        cx.notify();
    }

    fn browse_input_path(&mut self, cx: &mut Context<Self>) {
        if dbflux_ui_base::file_dialog::is_native_file_dialog_available() {
            let this = cx.entity().clone();
            let task = cx.background_executor().spawn(async move {
                rfd::FileDialog::new()
                    .set_title("Open Connection Bundle")
                    .add_filter("TOML bundle", &["toml"])
                    .add_filter("All files", &["*"])
                    .pick_file()
            });

            cx.spawn(async move |_this, cx| {
                if let Some(path) = task.await
                    && let Err(error) = cx.update(|cx| {
                        this.update(cx, |this, cx| {
                            this.pending_file_path = Some(path.to_string_lossy().to_string());
                            cx.notify();
                        });
                    })
                {
                    log::warn!("Failed to apply import path to panel state: {:?}", error);
                }
            })
            .detach();
        } else {
            // No native picker: do not block with a red error — the user can
            // type the path directly. Surface only a one-line muted hint.
            self.native_picker_unavailable = true;
            cx.notify();
        }
    }

    fn dest_snapshot(&self, cx: &Context<Self>) -> OwnedDestSnapshot {
        let state = self.app_state.read(cx);
        OwnedDestSnapshot {
            auth_profiles: state.list_auth_profiles(),
            ssh_tunnels: state.ssh_tunnels().to_vec(),
            proxies: state.proxies().to_vec(),
            connections: state
                .connections()
                .values()
                .map(|c| c.profile.clone())
                .collect(),
        }
    }

    /// Parse + decrypt + plan the selected bundle on the background executor,
    /// then advance to the preview step.
    fn do_parse_and_plan(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let path = self.file_input.read(cx).value().trim().to_string();
        if path.is_empty() {
            self.parse_error = Some("Choose a bundle file to import.".to_string());
            cx.notify();
            return;
        }

        let passphrase = SecretString::from(self.passphrase_input.read(cx).value().to_string());
        let dest = self.dest_snapshot(cx);

        let this = cx.entity().clone();
        self.is_parsing = true;
        self.parse_error = None;
        self.run_result = None;
        window.focus(&self.focus_handle);
        cx.notify();

        cx.spawn(async move |_this, cx| {
            let result: (bool, Result<(ParsedBundle, ImportPlan), String>) = cx
                .background_executor()
                .spawn(async move {
                    let bytes = match std::fs::read(&path) {
                        Ok(b) => b,
                        Err(e) => return (false, Err(format!("Cannot read file: {e}"))),
                    };

                    let mut parsed = match dbflux_portability::import::parse(&bytes) {
                        Ok(p) => p,
                        Err(e) => return (false, Err(format!("Parse error: {e}"))),
                    };

                    use dbflux_portability::bundle::EncryptionMode;
                    let is_encrypted =
                        parsed.bundle.bundle.encryption == EncryptionMode::AgePassphrase;

                    use dbflux_core::secrecy::ExposeSecret;
                    if is_encrypted && passphrase.expose_secret().is_empty() {
                        return (
                            true,
                            Err("This bundle is encrypted. Enter the passphrase and try again."
                                .to_string()),
                        );
                    }

                    if let Err(e) = dbflux_portability::import::decrypt(&mut parsed, &passphrase) {
                        use dbflux_portability::PortabilityError;
                        let msg = if e.is_encryption_unavailable() {
                            "This bundle is passphrase-encrypted but the encryption \
                             feature is not available in this build of DBFlux."
                                .to_string()
                        } else if matches!(&e, PortabilityError::Decryption(_)) {
                            "Passphrase incorrect or bundle corrupted.".to_string()
                        } else {
                            format!("Decryption error: {e}")
                        };
                        return (is_encrypted, Err(msg));
                    }

                    let plan = dbflux_portability::import::plan(&parsed, &dest.as_ref_snapshot());
                    (is_encrypted, Ok((parsed, plan)))
                })
                .await;

            let (is_encrypted, outcome) = result;

            if let Err(e) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.is_parsing = false;
                    this.bundle_encrypted = is_encrypted;

                    match outcome {
                        Ok((parsed, plan)) => {
                            let registered = this
                                .app_state
                                .read(cx)
                                .drivers()
                                .keys()
                                .cloned()
                                .collect::<std::collections::HashSet<_>>();
                            this.confirm_summary =
                                Some(confirm_summary(&parsed, &plan, &registered));

                            this.parsed_bundle = Some(parsed);
                            this.import_plan = Some(plan);
                            this.pending_provision_secrets = true;
                            this.step = Step::Preview;
                        }
                        Err(e) => {
                            this.parse_error = Some(e);
                        }
                    }
                    cx.notify();
                });
            }) {
                log::warn!("Failed to update import panel after parse: {:?}", e);
            }
        })
        .detach();
    }

    /// Create one masked input per required-secret resolution. Called from render
    /// via `pending_provision_secrets`, the only path with `&mut Window` after the
    /// async parse completes.
    fn provision_secret_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(plan) = &self.import_plan else {
            return;
        };

        let secret_keys: Vec<(String, String)> = plan
            .required_resolutions
            .iter()
            .filter(|r| matches!(r.kind, RequiredResolutionKind::Secret))
            .map(|r| (r.owner_local_id.clone(), r.field.clone()))
            .collect();

        self.secret_inputs.clear();

        for key in secret_keys {
            let key_for_sub = key.clone();

            let input = cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder("Enter secret value")
                    .masked(true)
            });

            let sub = cx.subscribe(&input, move |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change | InputEvent::Blur) {
                    let value = this
                        .secret_inputs
                        .get(&key_for_sub)
                        .map(|inp| inp.read(cx).value().to_string())
                        .unwrap_or_default();
                    this.secret_values.insert(key_for_sub.clone(), value);
                    cx.notify();
                }
            });

            self._subscriptions.push(sub);
            self.secret_inputs.insert(key, input);
        }
    }

    fn has_conflicts(&self) -> bool {
        self.import_plan
            .as_ref()
            .map(|p| !p.conflicts.is_empty())
            .unwrap_or(false)
    }

    fn has_required(&self) -> bool {
        self.import_plan
            .as_ref()
            .map(|p| !p.required_resolutions.is_empty())
            .unwrap_or(false)
    }

    fn all_conflicts_resolved(&self) -> bool {
        let Some(plan) = &self.import_plan else {
            return true;
        };
        plan.conflicts
            .iter()
            .all(|c| self.conflict_choices.contains_key(&c.bundle_local_id))
    }

    /// Advance from preview to the first applicable resolution step, or straight
    /// to apply when nothing needs resolving.
    fn advance_from_preview(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_conflicts() {
            self.step = Step::Conflicts;
            cx.notify();
        } else if self.has_required() {
            self.step = Step::RequiredReferences;
            cx.notify();
        } else {
            self.do_apply(window, cx);
        }
    }

    fn advance_from_conflicts(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_required() {
            self.step = Step::RequiredReferences;
            cx.notify();
        } else {
            self.do_apply(window, cx);
        }
    }

    fn build_resolution_choices(&self) -> ResolutionChoices {
        let secret_values = self
            .secret_values
            .iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|((owner, field), value)| {
                (
                    (owner.clone(), field.clone()),
                    SecretString::from(value.clone()),
                )
            })
            .collect();

        ResolutionChoices {
            conflict_choices: self.conflict_choices.clone(),
            secret_values,
            auth_profile_choices: self.auth_profile_choices.clone(),
        }
    }

    /// Run `apply()` on the background executor, persist through `AppState`, and
    /// move to the outcome step.
    fn do_apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(parsed) = self.parsed_bundle.take() else {
            return;
        };
        let Some(plan) = self.import_plan.take() else {
            return;
        };
        let choices = self.build_resolution_choices();
        let app_state_entity = self.app_state.clone();
        let this = cx.entity().clone();

        self.is_applying = true;
        window.focus(&self.focus_handle);
        cx.notify();

        cx.spawn(async move |_this, cx| {
            let apply_result = cx
                .background_executor()
                .spawn(async move { dbflux_portability::import::apply(&parsed, &plan, &choices) })
                .await;

            if let Err(e) = cx.update(|cx| match apply_result {
                Err(e) => {
                    this.update(cx, |this, cx| {
                        this.is_applying = false;
                        this.run_result = Some(ImportRunResult::Failed(format!("Import failed: {e}")));
                        this.step = Step::Outcome;
                        cx.notify();
                    });
                }
                Ok(actions) => {
                    let outcome = app_state_entity.update(cx, |state, cx| {
                        let result = {
                            let mut deps = AppStatePersistence::new(state);
                            dbflux_app::portability::persist_import_actions(actions, &mut deps)
                        };
                        cx.emit(dbflux_ui_base::AppStateChanged);
                        cx.notify();
                        result
                    });

                    // Only the first catch site reports through the user-error seam.
                    if !outcome.secret_failures.is_empty() {
                        let count = outcome.secret_failures.len();
                        let msg = format!(
                            "{count} secret(s) could not be written to the keyring \
                             during import. The keyring may be locked or unavailable."
                        );
                        report_error(UserFacingError::new(ErrorKind::Storage, msg), cx);
                    }

                    this.update(cx, |this, cx| {
                        this.is_applying = false;
                        let succeeded = outcome.succeeded.len();
                        let has_failures = !outcome.secret_failures.is_empty()
                            || !outcome.needs_driver.is_empty()
                            || !outcome.config_failures.is_empty()
                            || !outcome.unresolved_refs.is_empty();

                        if !has_failures {
                            dbflux_ui_base::toast::Toast::success(format!(
                                "Imported {succeeded} entity/entities."
                            ))
                            .push(cx);
                        }

                        this.run_result = Some(ImportRunResult::Outcome(outcome));
                        this.step = Step::Outcome;
                        cx.notify();
                    });
                }
            }) {
                log::warn!("Failed to update import panel after apply: {:?}", e);
            }
        })
        .detach();
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for ImportConnectionsPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_file_path.take() {
            self.file_input
                .update(cx, |state, cx| state.set_value(path, window, cx));
        }

        if self.pending_provision_secrets {
            self.pending_provision_secrets = false;
            self.provision_secret_inputs(window, cx);
        }

        // Masking is render-driven so the eye toggle can reveal the passphrase.
        let show_passphrase = self.show_passphrase;
        self.passphrase_input.update(cx, |state, cx| {
            state.set_masked(!show_passphrase, window, cx);
        });

        let theme = cx.theme().clone();

        let body: AnyElement = match self.step {
            Step::SelectFile => self.render_select_file(cx),
            Step::Preview => self.render_preview(cx),
            Step::Conflicts => self.render_conflicts(cx),
            Step::RequiredReferences => self.render_required_references(cx),
            Step::Outcome => self.render_outcome(cx),
        };

        div()
            .track_focus(&self.focus_handle)
            .flex()
            .flex_col()
            .size_full()
            .bg(theme.background)
            .child(self.render_header(cx))
            .child(
                div()
                    .id("import-panel-body")
                    .flex_1()
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .px(Spacing::LG)
                    .py(Spacing::MD)
                    .overflow_scroll()
                    .child(body),
            )
            .child(self.render_footer(cx))
    }
}

impl ImportConnectionsPanel {
    fn render_header(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        div()
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::LG)
            .py(Spacing::MD)
            .border_b_1()
            .border_color(theme.border)
            .child(Text::heading("Import Connections").font_size(FontSizes::LG))
            .child(
                Text::muted("Load a TOML bundle exported from DBFlux.")
                    .font_size(FontSizes::SM),
            )
            .into_any_element()
    }

    fn render_select_file(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().clone();

        let browse = IconButton::new("import-input-browse", AppIcon::Folder.into())
            .icon_size(Heights::ICON_SM)
            .on_click(move |_event, _window, cx| {
                entity.update(cx, |this, cx| this.browse_input_path(cx));
            });

        let file_row = div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(div().flex_1().child(Input::new(&self.file_input)))
            .child(browse);

        let mut file_block = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body("Bundle file").color(theme.muted_foreground))
            .child(file_row);

        if self.native_picker_unavailable {
            file_block = file_block.child(
                Text::muted(
                    "No native file picker on this system — type or paste the bundle path above.",
                )
                .font_size(FontSizes::XS),
            );
        }

        let mut col = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(file_block)
            .child(
                Checkbox::new("import-encrypted-toggle")
                    .checked(self.bundle_encrypted)
                    .label("Bundle is encrypted")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.bundle_encrypted = *checked;
                        cx.notify();
                    })),
            );

        if self.bundle_encrypted {
            let eye_icon = if self.show_passphrase {
                AppIcon::EyeOff
            } else {
                AppIcon::Eye
            };

            let toggle = IconButton::new("import-passphrase-eye", eye_icon.into()).on_click({
                let entity = cx.entity().clone();
                move |_event, _window, cx| {
                    entity.update(cx, |this, cx| {
                        this.show_passphrase = !this.show_passphrase;
                        cx.notify();
                    });
                }
            });

            col = col.child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::body("Passphrase").color(theme.muted_foreground))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(div().flex_1().child(Input::new(&self.passphrase_input)))
                            .child(toggle),
                    ),
            );
        }

        if let Some(err) = self.parse_error.clone() {
            col = col.child(BannerBlock::new(BannerVariant::Danger, err));
        }

        col.into_any_element()
    }

    fn render_preview(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let Some(summary) = self.confirm_summary.as_ref() else {
            return div().into_any_element();
        };

        let mut counts = surface_raised(cx)
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .flex()
            .flex_col()
            .gap(Spacing::XS);

        for line in [
            format!("{} connection(s)", summary.connection_count),
            format!("{} auth profile(s)", summary.auth_profile_count),
            format!("{} SSH tunnel(s)", summary.ssh_tunnel_count),
            format!("{} proxy profile(s)", summary.proxy_count),
        ] {
            counts = counts.child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .child(line),
            );
        }

        let mut col = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(
                Text::body("This bundle will import the following:")
                    .color(theme.muted_foreground),
            )
            .child(counts);

        if summary.conflict_count > 0 {
            col = col.child(BannerBlock::new(
                BannerVariant::Warning,
                format!(
                    "{} profile(s) already exist at the destination — you will choose how to \
                     resolve each.",
                    summary.conflict_count
                ),
            ));
        }

        if summary.required_resolution_count > 0 {
            col = col.child(BannerBlock::new(
                BannerVariant::Info,
                format!(
                    "{} value(s) may be required after import — secrets omitted from the bundle \
                     can be entered or skipped.",
                    summary.required_resolution_count
                ),
            ));
        }

        if summary.has_driver_not_installed {
            col = col.child(BannerBlock::new(
                BannerVariant::Warning,
                "One or more connections reference a driver not installed on this machine and \
                 will be skipped.",
            ));
        }

        col = col.child(
            BannerBlock::new(BannerVariant::Info, "External value references travel as-is")
                .with_body(
                    "SSM, Secrets Manager, and environment references are imported unchanged and \
                     resolved against this machine at connect time.",
                ),
        );

        col.into_any_element()
    }

    fn render_conflicts(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let Some(plan) = &self.import_plan else {
            return div().into_any_element();
        };

        let dest = self.dest_snapshot(cx);

        let mut col = div().flex().flex_col().gap(Spacing::SM).child(
            Text::body(
                "Some profiles in this bundle already exist. Choose how to handle each conflict.",
            )
            .color(theme.muted_foreground),
        );

        for conflict in &plan.conflicts {
            col = col.child(self.render_conflict_row(conflict, &dest, cx));
        }

        col.into_any_element()
    }

    fn render_conflict_row(
        &self,
        conflict: &dbflux_portability::ProfileConflict,
        dest: &OwnedDestSnapshot,
        cx: &Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();

        let kind_label = match conflict.kind {
            ConflictKind::AuthProfile => "Auth profile",
            ConflictKind::SshTunnel => "SSH tunnel",
            ConflictKind::Proxy => "Proxy",
            ConflictKind::Connection => "Connection",
        };

        let candidates = mapto_candidates(conflict.kind, dest);
        let current = self.conflict_choices.get(&conflict.bundle_local_id).cloned();

        let mut items = vec![
            SegmentedItem::new(CHOICE_REUSE, "Reuse existing"),
            SegmentedItem::new(CHOICE_CREATE, "Create new"),
        ];
        for (candidate_id, candidate_name) in &candidates {
            items.push(SegmentedItem::new(
                format!("{CHOICE_MAP_PREFIX}{candidate_id}"),
                format!("Map to: {candidate_name}"),
            ));
        }

        let active = match &current {
            Some(ConflictChoice::Reuse) => CHOICE_REUSE.to_string(),
            Some(ConflictChoice::CreateNew) => CHOICE_CREATE.to_string(),
            Some(ConflictChoice::MapTo(id)) => format!("{CHOICE_MAP_PREFIX}{id}"),
            None => String::new(),
        };

        let local_id = conflict.bundle_local_id.clone();
        let entity = cx.entity().clone();
        let control = SegmentedControl::new(items, active, move |selected, _window, cx| {
            let choice = parse_conflict_choice(selected.as_ref());
            let local_id = local_id.clone();
            entity.update(cx, |this, cx| {
                if let Some(choice) = choice {
                    this.conflict_choices.insert(local_id, choice);
                    cx.notify();
                }
            });
        });

        surface_raised(cx)
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .id(SharedString::from(format!(
                "import-conflict-{}",
                conflict.bundle_local_id
            )))
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.foreground)
                    .child(format!(
                        "{kind_label}: \"{}\" conflicts with \"{}\"",
                        conflict.bundle_name, conflict.existing_name
                    )),
            )
            .child(control)
            .into_any_element()
    }

    fn render_required_references(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let Some(plan) = &self.import_plan else {
            return div().into_any_element();
        };

        let mut col = div().flex().flex_col().gap(Spacing::SM).child(
            Text::body(
                "The following values may be required. Leave a secret empty to skip it — the \
                 connection still imports without it.",
            )
            .color(theme.muted_foreground),
        );

        for resolution in &plan.required_resolutions {
            col = col.child(self.render_required_row(resolution, cx));
        }

        col.into_any_element()
    }

    fn render_required_row(
        &self,
        resolution: &dbflux_portability::RequiredResolution,
        cx: &Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let key = (resolution.owner_local_id.clone(), resolution.field.clone());

        let mut row = surface_raised(cx)
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .id(SharedString::from(format!(
                "import-required-{}-{}",
                resolution.owner_local_id, resolution.field
            )));

        match &resolution.kind {
            RequiredResolutionKind::Secret => {
                row = row
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .child(format!(
                                "Secret for \"{}\": {}",
                                resolution.owner_name, resolution.field
                            )),
                    )
                    .child(
                        Text::muted("Leave empty to skip.").font_size(FontSizes::XS),
                    );

                if let Some(input) = self.secret_inputs.get(&key) {
                    row = row.child(Input::new(input));
                }
            }

            RequiredResolutionKind::AwsReference { provider_id, name } => {
                let matching: Vec<(Uuid, String)> = self
                    .dest_auth_profiles
                    .iter()
                    .filter(|p| &p.provider_id == provider_id)
                    .map(|p| (p.id, p.name.clone()))
                    .collect();

                row = row.child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.foreground)
                        .child(format!(
                            "AWS auth profile \"{name}\" ({provider_id}) for \"{}\"",
                            resolution.owner_name
                        )),
                );

                row = row.child(self.auth_choice_block(&key, &matching, cx));
            }

            RequiredResolutionKind::AuthProfileRef => {
                let all_dest: Vec<(Uuid, String)> = self
                    .dest_auth_profiles
                    .iter()
                    .map(|p| (p.id, p.name.clone()))
                    .collect();

                row = row.child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.foreground)
                        .child(format!(
                            "Auth profile for \"{}\": {}",
                            resolution.owner_name, resolution.field
                        )),
                );

                row = row.child(self.auth_choice_block(&key, &all_dest, cx));
            }
        }

        row.into_any_element()
    }

    /// Build a "Skip / Use <profile>" picker block for an auth-profile resolution.
    fn auth_choice_block(
        &self,
        key: &(String, String),
        candidates: &[(Uuid, String)],
        cx: &Context<Self>,
    ) -> AnyElement {
        if candidates.is_empty() {
            return Text::muted(
                "No matching auth profile on this machine. The connection imports without one; \
                 assign it later in Settings > Auth Profiles.",
            )
            .font_size(FontSizes::XS)
            .into_any_element();
        }

        let current = self.auth_profile_choices.get(key).copied();

        let mut items = vec![SegmentedItem::new(AUTH_SKIP, "Skip")];
        for (id, name) in candidates {
            items.push(SegmentedItem::new(
                format!("{AUTH_USE_PREFIX}{id}"),
                format!("Use: {name}"),
            ));
        }

        let active = match current {
            Some(id) => format!("{AUTH_USE_PREFIX}{id}"),
            None => AUTH_SKIP.to_string(),
        };

        let key_for_cb = key.clone();
        let entity = cx.entity().clone();
        let control = SegmentedControl::new(items, active, move |selected, _window, cx| {
            let key = key_for_cb.clone();
            let pick = parse_auth_choice(selected.as_ref());
            entity.update(cx, |this, cx| {
                match pick {
                    Some(id) => {
                        this.auth_profile_choices.insert(key, id);
                    }
                    None => {
                        this.auth_profile_choices.remove(&key);
                    }
                }
                cx.notify();
            });
        });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::muted("Select a profile or skip:").font_size(FontSizes::XS))
            .child(control)
            .into_any_element()
    }

    fn render_outcome(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        let mut col = div().flex().flex_col().gap(Spacing::SM);

        match self.run_result.as_ref() {
            None => {
                col = col.child(Text::body("Import complete.").color(theme.foreground));
            }
            Some(ImportRunResult::Failed(msg)) => {
                col = col.child(
                    BannerBlock::new(BannerVariant::Danger, "Import failed")
                        .with_body(msg.clone()),
                );
            }
            Some(ImportRunResult::Outcome(outcome)) => {
                if !outcome.succeeded.is_empty() {
                    col = col.child(BannerBlock::new(
                        BannerVariant::Success,
                        format!("{} entity/entities imported.", outcome.succeeded.len()),
                    ));
                }

                if !outcome.needs_driver.is_empty() {
                    let body = outcome
                        .needs_driver
                        .iter()
                        .map(|(name, driver)| format!("\"{name}\" (driver: {driver})"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    col = col.child(
                        BannerBlock::new(
                            BannerVariant::Warning,
                            format!(
                                "{} connection(s) skipped — driver not installed.",
                                outcome.needs_driver.len()
                            ),
                        )
                        .with_body(body),
                    );
                }

                if !outcome.config_failures.is_empty() {
                    let body = outcome
                        .config_failures
                        .iter()
                        .map(|(name, reason)| format!("\"{name}\": {reason}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    col = col.child(
                        BannerBlock::new(
                            BannerVariant::Warning,
                            format!(
                                "{} connection(s) could not be configured.",
                                outcome.config_failures.len()
                            ),
                        )
                        .with_body(body),
                    );
                }

                if !outcome.unresolved_refs.is_empty() {
                    let body = outcome
                        .unresolved_refs
                        .iter()
                        .map(|name| format!("\"{name}\""))
                        .collect::<Vec<_>>()
                        .join("\n");
                    col = col.child(
                        BannerBlock::new(
                            BannerVariant::Warning,
                            format!(
                                "{} connection(s) had unresolvable references and were not imported.",
                                outcome.unresolved_refs.len()
                            ),
                        )
                        .with_body(body),
                    );
                }

                if !outcome.secret_failures.is_empty() {
                    col = col.child(BannerBlock::new(
                        BannerVariant::Warning,
                        format!(
                            "{} secret(s) could not be written to the keyring. \
                             Enter them manually for each affected connection.",
                            outcome.secret_failures.len()
                        ),
                    ));
                }
            }
        }

        col.into_any_element()
    }

    fn render_footer(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        let left = match self.step {
            Step::SelectFile => Button::new("import-cancel", "Cancel").ghost().on_click(
                cx.listener(|_this, _: &gpui::ClickEvent, _, cx| {
                    cx.emit(ImportConnectionsPanelEvent::Cancelled);
                }),
            ),
            Step::Preview => Button::new("import-back", "Back").ghost().on_click(cx.listener(
                |this, _: &gpui::ClickEvent, _, cx| {
                    this.step = Step::SelectFile;
                    cx.notify();
                },
            )),
            Step::Conflicts => Button::new("import-back", "Back").ghost().on_click(cx.listener(
                |this, _: &gpui::ClickEvent, _, cx| {
                    this.step = Step::Preview;
                    cx.notify();
                },
            )),
            Step::RequiredReferences => {
                Button::new("import-back", "Back").ghost().on_click(cx.listener(
                    |this, _: &gpui::ClickEvent, _, cx| {
                        this.step = if this.has_conflicts() {
                            Step::Conflicts
                        } else {
                            Step::Preview
                        };
                        cx.notify();
                    },
                ))
            }
            Step::Outcome => Button::new("import-done", "Done").ghost().on_click(cx.listener(
                |_this, _: &gpui::ClickEvent, _, cx| {
                    cx.emit(ImportConnectionsPanelEvent::Completed);
                },
            )),
        };

        let primary = self.render_primary_button(cx);

        div()
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::LG)
            .py(Spacing::MD)
            .border_t_1()
            .border_color(theme.border)
            .child(left)
            .when_some(primary, |el, btn| el.child(btn))
            .into_any_element()
    }

    fn render_primary_button(&self, cx: &Context<Self>) -> Option<AnyElement> {
        match self.step {
            Step::SelectFile => {
                let can_load =
                    !self.file_input.read(cx).value().trim().is_empty() && !self.is_parsing;
                let label = if self.is_parsing { "Loading\u{2026}" } else { "Load" };
                Some(
                    Button::new("import-load", label)
                        .primary()
                        .disabled(!can_load)
                        .on_click(cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
                            this.do_parse_and_plan(window, cx);
                        }))
                        .into_any_element(),
                )
            }
            Step::Preview => Some(
                Button::new("import-preview-next", "Continue")
                    .primary()
                    .on_click(cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
                        this.advance_from_preview(window, cx);
                    }))
                    .into_any_element(),
            ),
            Step::Conflicts => {
                let resolved = self.all_conflicts_resolved();
                Some(
                    Button::new("import-conflicts-next", "Continue")
                        .primary()
                        .disabled(!resolved)
                        .on_click(cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
                            if this.all_conflicts_resolved() {
                                this.advance_from_conflicts(window, cx);
                            }
                        }))
                        .into_any_element(),
                )
            }
            Step::RequiredReferences => {
                let label = if self.is_applying {
                    "Importing\u{2026}"
                } else {
                    "Import"
                };
                Some(
                    Button::new("import-required-apply", label)
                        .primary()
                        .disabled(self.is_applying)
                        .on_click(cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
                            this.do_apply(window, cx);
                        }))
                        .into_any_element(),
                )
            }
            Step::Outcome => None,
        }
    }
}

/// Parse a conflict-choice segment id back into a `ConflictChoice`.
fn parse_conflict_choice(id: &str) -> Option<ConflictChoice> {
    if id == CHOICE_REUSE {
        return Some(ConflictChoice::Reuse);
    }
    if id == CHOICE_CREATE {
        return Some(ConflictChoice::CreateNew);
    }
    if let Some(uuid_str) = id.strip_prefix(CHOICE_MAP_PREFIX)
        && let Ok(uuid) = Uuid::parse_str(uuid_str)
    {
        return Some(ConflictChoice::MapTo(uuid));
    }
    None
}

/// Parse an auth-choice segment id; `None` means "skip".
fn parse_auth_choice(id: &str) -> Option<Uuid> {
    id.strip_prefix(AUTH_USE_PREFIX)
        .and_then(|uuid_str| Uuid::parse_str(uuid_str).ok())
}
