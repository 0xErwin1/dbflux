use std::collections::HashMap;

use dbflux_app::portability::{ImportOutcome, ImportPersistence, OwnedDestSnapshot};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::primitives::{BannerBlock, BannerVariant};
use dbflux_components::tokens::{Radii, Spacing};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{AuthProfile, ConnectionProfile, ProxyProfile, SshTunnelProfile};
use dbflux_portability::{
    ConflictChoice, ConflictKind, ImportPlan, ParsedBundle, ResolutionChoices,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Step machine
// ---------------------------------------------------------------------------

/// Steps of the import wizard.
#[derive(Clone, PartialEq, Debug)]
enum WizardStep {
    FileAndPassphrase,
    ConflictResolution,
    RequiredReferences,
    Confirm,
    PartialSummary,
}

// ---------------------------------------------------------------------------
// Wire: AppState -> ImportPersistence
// ---------------------------------------------------------------------------

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

    /// Insert a connection profile into the app state.
    ///
    /// # Driver-id invariant
    ///
    /// The portability crate emits `ImportActions::connections` with
    /// `DbConfig::External { values, .. }` so the full form values from the
    /// bundle are carried without loss.  This method looks up the driver by
    /// `profile.driver_id()` and calls `driver.build_config(values)` to produce
    /// the correct concrete `DbConfig`.  If `build_config` fails the profile is
    /// still persisted with the `External` config — it will produce a
    /// connect-time error that the user can fix by editing the connection.
    ///
    /// Returns `None` only when the driver is absent from the registry; the
    /// caller records the connection in `needs_driver` rather than persisting a
    /// mis-typed placeholder.
    fn add_connection(&mut self, profile: ConnectionProfile) -> Option<()> {
        let driver_id = profile.driver_id().to_string();
        if !self.registered_drivers.contains(&driver_id) {
            return None;
        }

        let rebuilt = if let Some(driver) = self.state.drivers().get(&driver_id).cloned() {
            if let dbflux_core::DbConfig::External { values, .. } = &profile.config {
                match driver.build_config(values) {
                    Ok(config) => {
                        let mut rebuilt = profile.clone();
                        rebuilt.config = config;
                        rebuilt
                    }
                    Err(_) => profile,
                }
            } else {
                profile
            }
        } else {
            profile
        };

        self.state.add_profile_in_folder(rebuilt, None);
        Some(())
    }

    fn write_secret(&self, secret_ref: &str, secret: &SecretString) -> bool {
        self.state.facade.secrets.set_by_ref(secret_ref, secret)
    }
}

// ---------------------------------------------------------------------------
// ImportWizard entity
// ---------------------------------------------------------------------------

/// Multi-step import wizard for connection bundles.
///
/// Orchestrates the parse -> plan -> resolve -> apply pipeline.
/// All I/O (parse, decrypt, apply) runs on the background executor so the UI
/// thread is never blocked.  State transitions happen via `pending_*` fields
/// drained in `render`.
pub struct ImportWizard {
    app_state: Entity<AppStateEntity>,

    step: WizardStep,

    // Step 1: FileAndPassphrase
    file_path: String,
    pending_file_path: Option<String>,
    passphrase_input: Entity<InputState>,
    bundle_encrypted: bool,
    parse_error: Option<String>,
    is_parsing: bool,

    // Step 2: ConflictResolution
    parsed_bundle: Option<ParsedBundle>,
    import_plan: Option<ImportPlan>,
    conflict_choices: HashMap<String, ConflictChoice>,

    // Step 3: RequiredReferences
    // One masked InputState per (owner_local_id, field) secret resolution.
    // Created in render() via the pending flag pattern (requires Window access).
    secret_inputs: HashMap<(String, String), Entity<InputState>>,
    // Mirrors the current value() of each secret input for quick lookup.
    secret_values: HashMap<(String, String), String>,
    // Set to true when the plan is resolved; drained in render() to create
    // the per-secret InputState entities (which require &mut Window).
    pending_provision_secrets: bool,
    auth_profile_choices: HashMap<(String, String), Uuid>,

    // Step 4/5: Confirm / PartialSummary
    is_applying: bool,
    pending_outcome: Option<Result<ImportOutcome, String>>,

    #[allow(dead_code)]
    dest_auth_profiles: Vec<AuthProfile>,

    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl ImportWizard {
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

        let dest_auth_profiles = app_state.read(cx).list_auth_profiles();
        let focus_handle = cx.focus_handle();
        window.focus(&focus_handle);

        Self {
            app_state,
            step: WizardStep::FileAndPassphrase,
            file_path: String::new(),
            pending_file_path: None,
            passphrase_input,
            bundle_encrypted: false,
            parse_error: None,
            is_parsing: false,
            parsed_bundle: None,
            import_plan: None,
            conflict_choices: HashMap::new(),
            secret_inputs: HashMap::new(),
            secret_values: HashMap::new(),
            pending_provision_secrets: false,
            auth_profile_choices: HashMap::new(),
            is_applying: false,
            pending_outcome: None,
            dest_auth_profiles,
            focus_handle,
            _subscriptions: vec![passphrase_sub],
        }
    }

    fn browse_input_path(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();
        let use_native = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        if use_native {
            let task = cx.background_executor().spawn(async move {
                rfd::FileDialog::new()
                    .set_title("Open Connection Bundle")
                    .add_filter("DBFlux bundle", &["dbflux"])
                    .add_filter("All files", &["*"])
                    .pick_file()
            });

            cx.spawn(async move |_this, cx| {
                let path = task.await;
                if let Some(path) = path
                    && let Err(e) = cx.update(|cx| {
                        this.update(cx, |this, cx| {
                            this.pending_file_path = Some(path.to_string_lossy().to_string());
                            cx.notify();
                        });
                    })
                {
                    log::warn!("Failed to apply import path: {:?}", e);
                }
            })
            .detach();
        }
    }

    fn do_parse_and_plan(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let path = self.file_path.trim().to_string();
        if path.is_empty() {
            self.parse_error = Some("Choose a bundle file to import.".to_string());
            cx.notify();
            return;
        }

        let passphrase = SecretString::from(self.passphrase_input.read(cx).value().to_string());

        let dest = {
            let state = self.app_state.read(cx);
            OwnedDestSnapshot {
                auth_profiles: state.list_auth_profiles(),
                ssh_tunnels: state.ssh_tunnels().to_vec(),
                proxies: state.proxies().to_vec(),
            }
        };

        let this = cx.entity().clone();
        self.is_parsing = true;
        self.parse_error = None;
        window.focus(&self.focus_handle);
        cx.notify();

        cx.spawn(async move |_this, cx| {
            let result: Result<(ParsedBundle, ImportPlan), String> = cx
                .background_executor()
                .spawn(async move {
                    let bytes =
                        std::fs::read(&path).map_err(|e| format!("Cannot read file: {e}"))?;

                    let mut parsed = dbflux_portability::import::parse(&bytes)
                        .map_err(|e| format!("Parse error: {e}"))?;

                    use dbflux_portability::bundle::EncryptionMode;
                    // decrypt() is a no-op for plaintext bundles; always call it
                    // to populate decrypted_secrets from the plaintext section.
                    let _ = dbflux_portability::import::decrypt(&mut parsed, &passphrase);

                    if parsed.bundle.bundle.encryption == EncryptionMode::AgePassphrase
                        && parsed.decrypted_secrets.is_none()
                    {
                        return Err("Wrong passphrase or corrupt encrypted bundle.".to_string());
                    }

                    let plan = dbflux_portability::import::plan(&parsed, &dest.as_ref_snapshot());
                    Ok((parsed, plan))
                })
                .await;

            if let Err(e) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.is_parsing = false;
                    match result {
                        Ok((parsed, plan)) => {
                            let has_conflicts = !plan.conflicts.is_empty();
                            let has_required = !plan.required_resolutions.is_empty();

                            this.parsed_bundle = Some(parsed);
                            this.import_plan = Some(plan);
                            this.pending_provision_secrets = true;

                            if has_conflicts {
                                this.step = WizardStep::ConflictResolution;
                            } else if has_required {
                                this.step = WizardStep::RequiredReferences;
                            } else {
                                this.step = WizardStep::Confirm;
                            }
                        }
                        Err(e) => {
                            this.parse_error = Some(e);
                        }
                    }
                    cx.notify();
                });
            }) {
                log::warn!("Failed to update import wizard after parse: {:?}", e);
            }
        })
        .detach();
    }

    /// Create one masked `InputState` per `RequiredResolutionKind::Secret` entry in
    /// the resolved plan and subscribe to mirror input values into `self.secret_values`.
    ///
    /// # Invariants
    ///
    /// - Called from `render()` via the `pending_provision_secrets` flag, which
    ///   is the only path that provides `&mut Window` after an async background task.
    /// - Each subscription updates `self.secret_values[(owner, field)]` on every
    ///   `InputEvent::Change` / `Blur`, so `build_resolution_choices` assembles
    ///   real user-typed `SecretString` values for `apply()`.
    /// - No sentinel strings are written: if the user leaves an input empty it is
    ///   omitted from `ResolutionChoices.secret_values` and `apply()` will not
    ///   write anything to the keyring for that field.
    fn provision_secret_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use dbflux_portability::RequiredResolutionKind;

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

    fn advance_from_conflicts(&mut self, cx: &mut Context<Self>) {
        let has_required = self
            .import_plan
            .as_ref()
            .map(|p| !p.required_resolutions.is_empty())
            .unwrap_or(false);

        if has_required {
            self.step = WizardStep::RequiredReferences;
        } else {
            self.step = WizardStep::Confirm;
        }
        cx.notify();
    }

    fn all_conflicts_resolved(&self) -> bool {
        let Some(plan) = &self.import_plan else {
            return true;
        };
        plan.conflicts
            .iter()
            .all(|c| self.conflict_choices.contains_key(&c.bundle_local_id))
    }

    fn all_required_resolved(&self) -> bool {
        let Some(plan) = &self.import_plan else {
            return true;
        };
        plan.required_resolutions.iter().all(|r| {
            use dbflux_portability::RequiredResolutionKind;
            let key = (r.owner_local_id.clone(), r.field.clone());
            match &r.kind {
                RequiredResolutionKind::Secret => {
                    // A secret is resolved only when the user has typed a non-empty value.
                    // We never write a sentinel; if the value is empty the seam skips the
                    // keyring write and the outcome reports it as still-required.
                    self.secret_values
                        .get(&key)
                        .map(|v| !v.is_empty())
                        .unwrap_or(false)
                }
                RequiredResolutionKind::AwsReference { .. } => {
                    // AWS references that aren't in dest are informational; allow advancing.
                    true
                }
                RequiredResolutionKind::AuthProfileRef => {
                    self.auth_profile_choices.contains_key(&key)
                }
            }
        })
    }

    fn build_resolution_choices(&self) -> ResolutionChoices {
        // Only include non-empty secret values so apply() never receives a
        // sentinel string as a real secret.  Empty entries are omitted and
        // will surface as still-required in ImportOutcome.
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

    fn do_apply(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
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
        cx.notify();

        cx.spawn(async move |_this, cx| {
            // Run portability::apply() on the background (pure computation).
            let apply_result = cx
                .background_executor()
                .spawn(async move { dbflux_portability::import::apply(&parsed, &plan, &choices) })
                .await;

            // Bring back to foreground for persistence + state update.
            if let Err(e) = cx.update(|cx| {
                match apply_result {
                    Err(e) => {
                        this.update(cx, |this, cx| {
                            this.is_applying = false;
                            this.pending_outcome = Some(Err(format!("Import failed: {e}")));
                            this.step = WizardStep::PartialSummary;
                            cx.notify();
                        });
                    }
                    Ok(actions) => {
                        // Persist through AppState on the foreground thread.
                        let outcome = app_state_entity.update(cx, |state, _cx| {
                            let mut deps = AppStatePersistence::new(state);
                            dbflux_app::portability::persist_import_actions(actions, &mut deps)
                        });

                        // Surface keyring failures through the report_error seam
                        // (toast + audit row + status-bar badge) in addition to
                        // the PartialSummary banner.  Only this catch site reports;
                        // the PartialSummary render does NOT call report_error again.
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
                            let has_failures = !outcome.secret_failures.is_empty()
                                || !outcome.needs_driver.is_empty();
                            this.pending_outcome = Some(Ok(outcome));
                            if has_failures {
                                this.step = WizardStep::PartialSummary;
                            } else {
                                cx.emit(ImportWizardEvent::ImportSucceeded);
                            }
                            cx.notify();
                        });
                    }
                }
            }) {
                log::warn!("Failed to update import wizard after apply: {:?}", e);
            }
        })
        .detach();
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for ImportWizard {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(path) = self.pending_file_path.take() {
            self.file_path = path;
        }

        if self.pending_provision_secrets {
            self.pending_provision_secrets = false;
            self.provision_secret_inputs(window, cx);
        }

        let theme = cx.theme().clone();
        let step = self.step.clone();

        let body: AnyElement = match step {
            WizardStep::FileAndPassphrase => self
                .render_file_and_passphrase(window, cx)
                .into_any_element(),
            WizardStep::ConflictResolution => {
                self.render_conflict_resolution(cx).into_any_element()
            }
            WizardStep::RequiredReferences => {
                self.render_required_references(cx).into_any_element()
            }
            WizardStep::Confirm => self.render_confirm(window, cx).into_any_element(),
            WizardStep::PartialSummary => self.render_partial_summary(cx).into_any_element(),
        };

        div()
            .flex()
            .flex_col()
            .w(px(540.0))
            .min_h(px(400.0))
            .max_h(px(620.0))
            .bg(theme.background)
            .p(Spacing::LG)
            .gap(Spacing::MD)
            .child(
                div().flex().items_center().justify_between().child(
                    div()
                        .text_xl()
                        .font_weight(gpui::FontWeight::SEMIBOLD)
                        .text_color(theme.foreground)
                        .child("Import Connections"),
                ),
            )
            .child(body)
    }
}

impl ImportWizard {
    fn render_file_and_passphrase(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let file_path = self.file_path.clone();
        let bundle_encrypted = self.bundle_encrypted;
        let is_parsing = self.is_parsing;
        let parse_error = self.parse_error.clone();
        let this_browse = cx.entity().clone();
        let this_toggle = cx.entity().clone();
        let this_next = cx.entity().clone();

        let mut col = div().flex().flex_col().gap(Spacing::SM);

        col = col
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("Select a .dbflux bundle file to import."),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .flex()
                            .flex_1()
                            .px(Spacing::SM)
                            .py(px(4.0))
                            .border_1()
                            .border_color(theme.border)
                            .rounded(Radii::SM)
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(if file_path.is_empty() {
                                        theme.muted_foreground
                                    } else {
                                        theme.foreground
                                    })
                                    .child(if file_path.is_empty() {
                                        "No file selected".to_string()
                                    } else {
                                        file_path.clone()
                                    }),
                            ),
                    )
                    .child(
                        div()
                            .id("import-browse-btn")
                            .px(Spacing::SM)
                            .py(px(4.0))
                            .border_1()
                            .border_color(theme.border)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(move |_, window, cx| {
                                this_browse.update(cx, |this, cx| {
                                    this.browse_input_path(window, cx);
                                });
                            })
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(theme.foreground)
                                    .child("Browse..."),
                            ),
                    ),
            )
            .child(
                div().flex().items_center().gap(Spacing::SM).child(
                    div()
                        .id("import-encrypted-toggle")
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .cursor_pointer()
                        .on_click(move |_, _, cx| {
                            this_toggle.update(cx, |this, cx| {
                                this.bundle_encrypted = !this.bundle_encrypted;
                                cx.notify();
                            });
                        })
                        .child(
                            div()
                                .w(px(14.0))
                                .h(px(14.0))
                                .border_1()
                                .border_color(theme.border)
                                .rounded(Radii::SM)
                                .bg(if bundle_encrypted {
                                    theme.accent
                                } else {
                                    theme.background
                                }),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(theme.foreground)
                                .child("Bundle is passphrase-encrypted"),
                        ),
                ),
            );

        if bundle_encrypted {
            col = col.child(self.passphrase_input.clone());
        }

        if let Some(err) = parse_error {
            col = col.child(BannerBlock::new(BannerVariant::Danger, err));
        }

        // value_refs informational note (non-blocking, spec R-IMP-*)
        col = col.child(
            div()
                .px(Spacing::SM)
                .py(px(4.0))
                .rounded(Radii::SM)
                .bg(theme.secondary)
                .child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(
                            "Note: value_refs (SSM/Secrets Manager/env references) are imported as-is \
                             and will be resolved against the destination infrastructure at connect time.",
                        ),
                ),
        );

        let can_next = !file_path.is_empty() && !is_parsing;
        col.child(
            div().flex().justify_end().child(
                div()
                    .id("import-step1-next")
                    .px(Spacing::MD)
                    .py(px(6.0))
                    .border_1()
                    .border_color(if can_next { theme.accent } else { theme.border })
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .when(can_next, |d| d.hover(|d| d.bg(theme.accent.opacity(0.15))))
                    .on_click(move |_, window, cx| {
                        if !is_parsing {
                            this_next.update(cx, |this, cx| {
                                this.do_parse_and_plan(window, cx);
                            });
                        }
                    })
                    .child(
                        div()
                            .text_sm()
                            .text_color(if can_next {
                                theme.accent
                            } else {
                                theme.muted_foreground
                            })
                            .child(if is_parsing { "Parsing..." } else { "Next" }),
                    ),
            ),
        )
    }

    fn render_conflict_resolution(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let Some(plan) = &self.import_plan else {
            return div()
                .child(div().text_sm().child("No plan available."))
                .into_any_element();
        };

        let conflicts: Vec<_> = plan
            .conflicts
            .iter()
            .map(|c| {
                (
                    c.bundle_local_id.clone(),
                    c.bundle_name.clone(),
                    c.existing_name.clone(),
                    c.existing_id,
                    c.kind,
                )
            })
            .collect();

        let all_resolved = self.all_conflicts_resolved();
        let this_next = cx.entity().clone();

        let rows: Vec<AnyElement> = conflicts
            .iter()
            .map(
                |(local_id, bundle_name, existing_name, existing_id, kind)| {
                    let kind_label = match kind {
                        ConflictKind::AuthProfile => "Auth profile",
                        ConflictKind::SshTunnel => "SSH tunnel",
                        ConflictKind::Proxy => "Proxy",
                    };
                    let current_choice = self.conflict_choices.get(local_id).cloned();
                    let lid_reuse = local_id.clone();
                    let lid_new = local_id.clone();
                    let lid_map = local_id.clone();
                    let eid = *existing_id;
                    let this_r = cx.entity().clone();
                    let this_n = cx.entity().clone();
                    let this_m = cx.entity().clone();

                    div()
                        .flex()
                        .flex_col()
                        .gap(px(2.0))
                        .p(Spacing::SM)
                        .border_1()
                        .border_color(theme.border)
                        .rounded(Radii::SM)
                        .child(div().text_sm().text_color(theme.foreground).child(format!(
                            "{kind_label}: \"{bundle_name}\" conflicts with \"{existing_name}\""
                        )))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .child(small_choice_button(
                                    "Reuse existing",
                                    current_choice == Some(ConflictChoice::Reuse),
                                    move |_, _, cx| {
                                        this_r.update(cx, |this, cx| {
                                            this.conflict_choices
                                                .insert(lid_reuse.clone(), ConflictChoice::Reuse);
                                            cx.notify();
                                        });
                                    },
                                    &theme,
                                ))
                                .child(small_choice_button(
                                    "Create new",
                                    current_choice == Some(ConflictChoice::CreateNew),
                                    move |_, _, cx| {
                                        this_n.update(cx, |this, cx| {
                                            this.conflict_choices
                                                .insert(lid_new.clone(), ConflictChoice::CreateNew);
                                            cx.notify();
                                        });
                                    },
                                    &theme,
                                ))
                                .child(small_choice_button(
                                    &format!("Map to \"{existing_name}\""),
                                    current_choice == Some(ConflictChoice::MapTo(eid)),
                                    move |_, _, cx| {
                                        this_m.update(cx, |this, cx| {
                                            this.conflict_choices.insert(
                                                lid_map.clone(),
                                                ConflictChoice::MapTo(eid),
                                            );
                                            cx.notify();
                                        });
                                    },
                                    &theme,
                                )),
                        )
                        .into_any_element()
                },
            )
            .collect();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(div().text_sm().text_color(theme.muted_foreground).child(
                "Some profiles in this bundle already exist at the destination. \
                         Choose how to handle each conflict.",
            ))
            .children(rows)
            .child(
                div().flex().justify_end().child(
                    div()
                        .id("import-conflict-next")
                        .px(Spacing::MD)
                        .py(px(6.0))
                        .border_1()
                        .border_color(if all_resolved {
                            theme.accent
                        } else {
                            theme.border
                        })
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .when(all_resolved, |d| {
                            d.hover(|d| d.bg(theme.accent.opacity(0.15)))
                        })
                        .on_click(move |_, _, cx| {
                            this_next.update(cx, |this, cx| {
                                if this.all_conflicts_resolved() {
                                    this.advance_from_conflicts(cx);
                                }
                            });
                        })
                        .child(
                            div()
                                .text_sm()
                                .text_color(if all_resolved {
                                    theme.accent
                                } else {
                                    theme.muted_foreground
                                })
                                .child("Next"),
                        ),
                ),
            )
            .into_any_element()
    }

    fn render_required_references(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let Some(plan) = &self.import_plan else {
            return div()
                .child(div().text_sm().child("No plan available."))
                .into_any_element();
        };

        let resolutions: Vec<_> = plan
            .required_resolutions
            .iter()
            .map(|r| (r.owner_local_id.clone(), r.field.clone(), r.kind.clone()))
            .collect();

        let all_resolved = self.all_required_resolved();
        let this_next = cx.entity().clone();

        let rows: Vec<AnyElement> = resolutions.iter().map(|(owner, field, kind)| {
            use dbflux_portability::RequiredResolutionKind;
            let key = (owner.clone(), field.clone());

            match kind {
                RequiredResolutionKind::Secret => {
                    let label = format!("Secret required: \"{field}\"");
                    let input = self.secret_inputs.get(&key).cloned();

                    let mut row = div()
                        .flex()
                        .flex_col()
                        .gap(px(2.0))
                        .p(Spacing::SM)
                        .border_1()
                        .border_color(theme.border)
                        .rounded(Radii::SM)
                        .child(div().text_sm().text_color(theme.foreground).child(label));

                    if let Some(input_entity) = input {
                        row = row.child(input_entity);
                    }

                    row.into_any_element()
                }

                RequiredResolutionKind::AwsReference { provider_id, name } => {
                    let label = format!(
                        "AWS auth reference not found on this machine: \"{name}\" ({provider_id})"
                    );
                    div()
                        .flex()
                        .flex_col()
                        .gap(px(2.0))
                        .p(Spacing::SM)
                        .border_1()
                        .border_color(theme.border)
                        .rounded(Radii::SM)
                        .child(div().text_sm().text_color(theme.foreground).child(label))
                        .child(
                            div()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child(
                                    "This AWS profile is not available on this machine. \
                                     The connection will be imported without an auth profile. \
                                     You can assign one after importing.",
                                ),
                        )
                        .into_any_element()
                }

                RequiredResolutionKind::AuthProfileRef => {
                    div()
                        .text_sm()
                        .child(format!("Auth profile required for: \"{field}\""))
                        .into_any_element()
                }
            }
        }).collect();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("The following values are required before the bundle can be imported."),
            )
            .children(rows)
            .child(
                div().flex().justify_end().child(
                    div()
                        .id("import-required-next")
                        .px(Spacing::MD)
                        .py(px(6.0))
                        .border_1()
                        .border_color(if all_resolved {
                            theme.accent
                        } else {
                            theme.border
                        })
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .when(all_resolved, |d| {
                            d.hover(|d| d.bg(theme.accent.opacity(0.15)))
                        })
                        .on_click(move |_, _, cx| {
                            this_next.update(cx, |this, cx| {
                                if this.all_required_resolved() {
                                    this.step = WizardStep::Confirm;
                                    cx.notify();
                                }
                            });
                        })
                        .child(
                            div()
                                .text_sm()
                                .text_color(if all_resolved {
                                    theme.accent
                                } else {
                                    theme.muted_foreground
                                })
                                .child("Next"),
                        ),
                ),
            )
            .into_any_element()
    }

    fn render_confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_applying = self.is_applying;
        let this_apply = cx.entity().clone();

        let conn_count = self
            .parsed_bundle
            .as_ref()
            .map(|p| p.bundle.connections.len())
            .unwrap_or(0);
        let auth_count = self
            .parsed_bundle
            .as_ref()
            .map(|p| p.bundle.auth_profiles.len())
            .unwrap_or(0);
        let ssh_count = self
            .parsed_bundle
            .as_ref()
            .map(|p| p.bundle.ssh_tunnels.len())
            .unwrap_or(0);
        let proxy_count = self
            .parsed_bundle
            .as_ref()
            .map(|p| p.bundle.proxies.len())
            .unwrap_or(0);

        // T5.7: connections with unregistered driver_id — informational, non-blocking.
        let drivers = self
            .app_state
            .read(cx)
            .drivers()
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let needs_driver_note: Vec<String> = self
            .parsed_bundle
            .as_ref()
            .map(|p| {
                p.bundle
                    .connections
                    .iter()
                    .filter(|c| !drivers.contains(&c.driver_id))
                    .map(|c| format!("\"{}\" (driver: {})", c.name, c.driver_id))
                    .collect()
            })
            .unwrap_or_default();

        let mut col = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child("Ready to import the following entities:"),
            )
            .child(
                div()
                    .p(Spacing::SM)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::SM)
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(format!("{conn_count} connection(s)")),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(format!("{auth_count} auth profile(s)")),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(format!("{ssh_count} SSH tunnel(s)")),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(theme.foreground)
                            .child(format!("{proxy_count} proxy profile(s)")),
                    ),
            );

        if !needs_driver_note.is_empty() {
            col = col.child(
                div()
                    .px(Spacing::SM)
                    .py(px(4.0))
                    .rounded(Radii::SM)
                    .bg(theme.secondary)
                    .flex()
                    .flex_col()
                    .gap(px(2.0))
                    .child(div().text_xs().text_color(theme.muted_foreground).child(
                        "The following connections reference a driver that is not installed \
                                 on this machine and will be skipped:",
                    ))
                    .children(needs_driver_note.iter().map(|n| {
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("- {n}"))
                    })),
            );
        }

        col.child(
            div().flex().justify_end().child(
                div()
                    .id("import-confirm-btn")
                    .px(Spacing::MD)
                    .py(px(6.0))
                    .border_1()
                    .border_color(if !is_applying {
                        theme.accent
                    } else {
                        theme.border
                    })
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .when(!is_applying, |d| {
                        d.hover(|d| d.bg(theme.accent.opacity(0.15)))
                    })
                    .on_click(move |_, window, cx| {
                        if !is_applying {
                            this_apply.update(cx, |this, cx| {
                                this.do_apply(window, cx);
                            });
                        }
                    })
                    .child(
                        div()
                            .text_sm()
                            .text_color(if !is_applying {
                                theme.accent
                            } else {
                                theme.muted_foreground
                            })
                            .child(if is_applying {
                                "Importing..."
                            } else {
                                "Import"
                            }),
                    ),
            ),
        )
    }

    fn render_partial_summary(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let this_close = cx.entity().clone();

        let mut col = div().flex().flex_col().gap(Spacing::SM);

        match self.pending_outcome.take() {
            None => {
                col = col.child(
                    div()
                        .text_sm()
                        .text_color(theme.foreground)
                        .child("Import complete."),
                );
            }
            Some(Err(e)) => {
                col = col.child(BannerBlock::new(
                    BannerVariant::Danger,
                    format!("Import failed: {e}"),
                ));
            }
            Some(Ok(outcome)) => {
                // T5.7: needs_driver items — informational, not an error.
                if !outcome.needs_driver.is_empty() {
                    col = col
                        .child(div().text_sm().text_color(theme.muted_foreground).child(
                            "The following connections were skipped (driver not installed):",
                        ))
                        .children(outcome.needs_driver.iter().map(|(name, driver)| {
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child(format!("- \"{name}\" (driver: {driver})"))
                        }));
                }

                if !outcome.secret_failures.is_empty() {
                    col = col.child(BannerBlock::new(
                        BannerVariant::Warning,
                        format!(
                            "{} secret(s) could not be written to the keyring. \
                                 The keyring may be locked or unavailable. \
                                 You can enter the secrets manually for each affected connection.",
                            outcome.secret_failures.len()
                        ),
                    ));
                }

                if !outcome.succeeded.is_empty() {
                    col = col.child(div().text_sm().text_color(theme.foreground).child(format!(
                        "{} entity/entities imported successfully.",
                        outcome.succeeded.len()
                    )));
                }
            }
        }

        col.child(
            div().flex().justify_end().child(
                div()
                    .id("import-close-btn")
                    .px(Spacing::MD)
                    .py(px(6.0))
                    .border_1()
                    .border_color(theme.accent)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.accent.opacity(0.15)))
                    .on_click(move |_, _, cx| {
                        this_close.update(cx, |_this, cx| {
                            cx.emit(ImportWizardEvent::Close);
                        });
                    })
                    .child(div().text_sm().text_color(theme.accent).child("Close")),
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Helper — small choice button
// ---------------------------------------------------------------------------

fn small_choice_button(
    label: &str,
    selected: bool,
    handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    theme: &gpui_component::Theme,
) -> impl IntoElement {
    let id_str: gpui::SharedString = format!("choice-btn-{label}").into();
    div()
        .id(ElementId::Name(id_str))
        .px(Spacing::SM)
        .py(px(3.0))
        .border_1()
        .border_color(if selected { theme.accent } else { theme.border })
        .rounded(Radii::SM)
        .cursor_pointer()
        .bg(if selected {
            theme.accent.opacity(0.1)
        } else {
            theme.background
        })
        .hover(|d| d.bg(theme.secondary))
        .on_click(handler)
        .child(
            div()
                .text_sm()
                .text_color(if selected {
                    theme.accent
                } else {
                    theme.foreground
                })
                .child(label.to_string()),
        )
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub enum ImportWizardEvent {
    ImportSucceeded,
    Close,
}

impl EventEmitter<ImportWizardEvent> for ImportWizard {}
