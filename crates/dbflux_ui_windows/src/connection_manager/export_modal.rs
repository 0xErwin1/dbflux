use std::collections::HashMap;
use std::path::PathBuf;

use dbflux_app::portability::{
    AppExportTransformResolver, AppFieldHintResolver, AppSecretReader, ExportInputs,
    build_export_graph,
};
use dbflux_components::controls::{
    Button, Checkbox, Dropdown, DropdownItem, DropdownSelectionChanged, Input, InputEvent,
    InputState,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::shell::ModalShell;
use dbflux_components::primitives::{
    BannerBlock, BannerVariant, IconButton, Text, surface_raised,
};
use dbflux_components::tokens::{FontSizes, Heights, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_core::access::AccessKind;
use dbflux_core::secrecy::SecretString;
use dbflux_portability::{AuthExportMode, AwsRef, EncryptionChoice, ExportOptions, IncludeExclude};
use dbflux_ui_base::{
    AppStateEntity,
    user_error::{ErrorKind, UserFacingError, report_error_async},
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Event emitted by [`ExportConnectionModal`] so the workspace host can react
/// to dismissal (closing the overlay clears the rendered child).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExportConnectionModalEvent {
    Close,
}

/// Auth-profile export-mode dropdown values (kept in sync with `auth_mode_from_id`).
const AUTH_MODE_INCLUDE: &str = "include";
const AUTH_MODE_REFERENCE: &str = "reference";
const AUTH_MODE_REQUIRED: &str = "required";
const AUTH_MODE_EXCLUDE: &str = "exclude";

/// A read-only description of one auth profile referenced by the exported
/// connection. `locked` marks AWS reflected profiles, which travel only as a
/// mappable reference and therefore expose a disabled control.
struct AuthProfileRow {
    id: Uuid,
    name: String,
    locked: bool,
}

/// A short, read-only summary of everything that will travel in the bundle.
///
/// Computed once in [`ExportConnectionModal::open`] so the body renders from
/// stable values instead of re-reading `AppState` on every frame.
struct ExportSummary {
    connection_name: String,
    auth_profiles: Vec<AuthProfileRow>,
    proxy_name: Option<String>,
    ssh_name: Option<String>,
}

/// Result of a completed export run, shown as a banner in the modal body.
#[derive(Clone)]
enum ExportResult {
    Success {
        path: PathBuf,
        warnings: Vec<String>,
        required_ref_count: usize,
    },
    Failed(String),
}

/// In-app, single-connection export modal.
///
/// Scoped to exactly one connection (plus its referenced auth / proxy / SSH
/// profiles). Opened from a connection's three-dots menu via
/// [`ExportConnectionModal::open`] and hosted as a workspace overlay — it never
/// opens an OS window.
pub struct ExportConnectionModal {
    app_state: Entity<AppStateEntity>,

    visible: bool,
    profile_id: Option<Uuid>,
    summary: Option<ExportSummary>,

    // Per-category include/exclude controls.
    include_connection_password: bool,
    include_proxy_credentials: bool,
    include_ssh_password: bool,
    embed_ssh_keys: bool,
    /// Per auth-profile export mode. Absent = default (`IncludeValues`).
    auth_modes: HashMap<Uuid, AuthExportMode>,
    /// The single auth profile (if any) the exported connection references,
    /// together with whether it is locked (AWS reflected). The export is scoped
    /// to one connection, which references at most one auth profile.
    auth_profile: Option<AuthProfileRow>,
    /// Dropdown for the single referenced auth profile's export mode. Absent when
    /// the connection has no auth profile, or when it is AWS-locked (a muted
    /// label is shown instead).
    auth_dropdown: Option<Entity<Dropdown>>,
    auth_dropdown_sub: Option<Subscription>,

    // Encryption.
    force_plaintext: bool,
    show_passphrase: bool,
    passphrase_input: Entity<InputState>,
    confirm_input: Entity<InputState>,

    // Output path.
    output_input: Entity<InputState>,
    pending_output_path: Option<String>,

    // Run state.
    is_exporting: bool,
    pending_result: Option<ExportResult>,
    validation_error: Option<String>,

    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<ExportConnectionModalEvent> for ExportConnectionModal {}

impl ExportConnectionModal {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let passphrase_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Passphrase")
                .masked(true)
        });

        let confirm_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Confirm passphrase")
                .masked(true)
        });

        let passphrase_sub = cx.subscribe(&passphrase_input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change | InputEvent::Blur) {
                this.validation_error = None;
                cx.notify();
            }
        });

        let confirm_sub = cx.subscribe(&confirm_input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change | InputEvent::Blur) {
                this.validation_error = None;
                cx.notify();
            }
        });

        let output_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Path to output file\u{2026}"));

        let output_sub = cx.subscribe(&output_input, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change | InputEvent::Blur) {
                this.validation_error = None;
                cx.notify();
            }
        });

        let focus_handle = cx.focus_handle();

        Self {
            app_state,
            visible: false,
            profile_id: None,
            summary: None,
            include_connection_password: true,
            include_proxy_credentials: false,
            include_ssh_password: false,
            embed_ssh_keys: false,
            auth_modes: HashMap::new(),
            auth_profile: None,
            auth_dropdown: None,
            auth_dropdown_sub: None,
            force_plaintext: false,
            show_passphrase: false,
            passphrase_input,
            confirm_input,
            output_input,
            pending_output_path: None,
            is_exporting: false,
            pending_result: None,
            validation_error: None,
            focus_handle,
            _subscriptions: vec![passphrase_sub, confirm_sub, output_sub],
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal for a single connection profile.
    ///
    /// Resets all run state to defaults, computes the read-only summary of the
    /// connection and its referenced profiles, and seeds the per-auth-profile
    /// export modes (AWS reflected profiles are locked to a reference).
    pub fn open(&mut self, profile_id: Uuid, window: &mut Window, cx: &mut Context<Self>) {
        self.profile_id = Some(profile_id);
        self.summary = self.build_summary(profile_id, cx);

        // Reset to ready-to-use defaults on every open.
        self.include_connection_password = true;
        self.include_proxy_credentials = false;
        self.include_ssh_password = false;
        self.embed_ssh_keys = false;
        self.force_plaintext = false;
        self.show_passphrase = false;
        self.pending_output_path = None;
        self.is_exporting = false;
        self.pending_result = None;
        self.validation_error = None;

        // The export is scoped to one connection, which references at most one
        // auth profile. Seed the default mode (AWS reflected = locked reference).
        self.auth_profile = self
            .summary
            .as_ref()
            .and_then(|summary| summary.auth_profiles.first())
            .map(|row| AuthProfileRow {
                id: row.id,
                name: row.name.clone(),
                locked: row.locked,
            });

        self.auth_modes.clear();
        if let Some(auth) = self.auth_profile.as_ref() {
            let mode = if auth.locked {
                AuthExportMode::MappableReference
            } else {
                AuthExportMode::IncludeValues
            };
            self.auth_modes.insert(auth.id, mode);
        }

        self.build_auth_dropdown(window, cx);

        self.passphrase_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.confirm_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.output_input
            .update(cx, |state, cx| state.set_value("", window, cx));

        self.visible = true;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    /// Build (or clear) the auth-profile export-mode dropdown for the single
    /// referenced auth profile. AWS-locked profiles get no dropdown — the render
    /// shows a muted "Reference (AWS profile)" label and the mode stays forced to
    /// `MappableReference`.
    fn build_auth_dropdown(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.auth_dropdown = None;
        self.auth_dropdown_sub = None;

        let Some(auth) = self.auth_profile.as_ref() else {
            return;
        };
        if auth.locked {
            return;
        }

        let auth_id = auth.id;
        let current = self
            .auth_modes
            .get(&auth_id)
            .copied()
            .unwrap_or(AuthExportMode::IncludeValues);

        let items = auth_mode_items();
        let selected = items
            .iter()
            .position(|item| item.value.as_ref() == auth_mode_id(current));

        let dropdown = cx.new(|_cx| {
            Dropdown::new("export-auth-mode")
                .items(items)
                .selected_index(selected)
        });

        let sub = cx.subscribe(
            &dropdown,
            move |this, dropdown, _event: &DropdownSelectionChanged, cx| {
                if let Some(value) = dropdown.read(cx).selected_value() {
                    let mode = auth_mode_from_id(value.as_ref());
                    this.auth_modes.insert(auth_id, mode);
                    cx.notify();
                }
            },
        );

        self.auth_dropdown = Some(dropdown);
        self.auth_dropdown_sub = Some(sub);
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.profile_id = None;
        self.summary = None;
        self.auth_profile = None;
        self.auth_dropdown = None;
        self.auth_dropdown_sub = None;
        cx.notify();
    }

    /// Collect the connection and its referenced auth / proxy / SSH profile
    /// names for the read-only summary block.
    fn build_summary(&self, profile_id: Uuid, cx: &Context<Self>) -> Option<ExportSummary> {
        let state = self.app_state.read(cx);

        let profile = state
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)?
            .clone();

        let mut auth_profiles: Vec<AuthProfileRow> = Vec::new();
        if let Some(auth_id) = profile.auth_profile_id {
            let all_auth = state.list_auth_profiles();
            if let Some(auth) = all_auth.iter().find(|a| a.id == auth_id) {
                auth_profiles.push(AuthProfileRow {
                    id: auth.id,
                    name: auth.name.clone(),
                    locked: auth.read_only,
                });
            }
        }

        let (proxy_name, ssh_name) = match profile.access_kind.as_ref() {
            Some(AccessKind::Proxy { proxy_profile_id }) => {
                let name = state
                    .proxies()
                    .iter()
                    .find(|p| &p.id == proxy_profile_id)
                    .map(|p| p.name.clone());
                (name, None)
            }
            Some(AccessKind::Ssh {
                ssh_tunnel_profile_id,
            }) => {
                let name = state
                    .ssh_tunnels()
                    .iter()
                    .find(|s| &s.id == ssh_tunnel_profile_id)
                    .map(|s| s.name.clone());
                (None, name)
            }
            _ => (None, None),
        };

        Some(ExportSummary {
            connection_name: profile.name.clone(),
            auth_profiles,
            proxy_name,
            ssh_name,
        })
    }

    /// Whether the Export button may run: a passphrase is set when encrypting,
    /// and an output path has been chosen.
    fn can_export(&self, cx: &Context<Self>) -> bool {
        if self.output_input.read(cx).value().trim().is_empty() {
            return false;
        }
        if self.force_plaintext {
            return true;
        }
        !self.passphrase_input.read(cx).value().trim().is_empty()
    }

    fn browse_output_path(&mut self, cx: &mut Context<Self>) {
        if dbflux_ui_base::file_dialog::is_native_file_dialog_available() {
            let this = cx.entity().clone();
            let task = cx.background_executor().spawn(async move {
                rfd::FileDialog::new()
                    .set_title("Export Connection")
                    .add_filter("TOML bundle", &["toml"])
                    .add_filter("All files", &["*"])
                    .set_file_name("connections.toml")
                    .save_file()
            });

            cx.spawn(async move |_this, cx| {
                if let Some(path) = task.await
                    && let Err(error) = cx.update(|cx| {
                        this.update(cx, |this, cx| {
                            this.pending_output_path = Some(path.to_string_lossy().to_string());
                            cx.notify();
                        });
                    })
                {
                    log::warn!("Failed to apply export path to modal state: {:?}", error);
                }
            })
            .detach();
        } else {
            match dbflux_ui_base::file_dialog::fallback_export_dir() {
                Ok(dir) => {
                    let path =
                        dbflux_ui_base::file_dialog::unique_path_in(&dir, "connections.toml");
                    self.pending_output_path = Some(path.to_string_lossy().to_string());
                    cx.notify();
                }
                Err(e) => {
                    self.validation_error = Some(format!("Cannot determine output path: {e}"));
                    cx.notify();
                }
            }
        }
    }

    /// Validate inputs, assemble the export graph for the single connection, and
    /// run the export on a background thread.
    fn do_export(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(profile_id) = self.profile_id else {
            return;
        };

        let output_value = self.output_input.read(cx).value().trim().to_string();
        if output_value.is_empty() {
            self.validation_error = Some("Choose an output file path.".to_string());
            cx.notify();
            return;
        }

        let encryption = if self.force_plaintext {
            EncryptionChoice::Plaintext { forced: true }
        } else {
            let passphrase = self.passphrase_input.read(cx).value().to_string();
            let confirm = self.confirm_input.read(cx).value().to_string();

            if passphrase.is_empty() {
                self.validation_error =
                    Some("Enter a passphrase or enable force-plaintext mode.".to_string());
                cx.notify();
                return;
            }

            if passphrase != confirm {
                self.validation_error =
                    Some("Passphrase and confirmation do not match.".to_string());
                cx.notify();
                return;
            }

            EncryptionChoice::Passphrase(SecretString::from(passphrase))
        };

        let output_path = PathBuf::from(&output_value);

        let Some((inputs, drivers, secret_store)) = self.assemble_inputs(profile_id, cx) else {
            self.validation_error =
                Some("Connection driver is not registered; cannot export.".to_string());
            cx.notify();
            return;
        };

        let opts = ExportOptions {
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: self.embed_ssh_keys,
            encryption,
            connection_password: include_exclude(self.include_connection_password),
            proxy_credentials: include_exclude(self.include_proxy_credentials),
            ssh_password: include_exclude(self.include_ssh_password),
            auth_modes: self.auth_modes.clone(),
            per_secret_overrides: HashMap::new(),
        };

        let this = cx.entity().clone();
        self.is_exporting = true;
        self.validation_error = None;
        self.pending_result = None;
        cx.notify();

        window.focus(&self.focus_handle);

        cx.spawn(async move |_this, cx| {
            // Run the export and write the file entirely on the background
            // executor so the UI thread is never blocked by disk I/O.
            let outcome = cx
                .background_executor()
                .spawn(async move {
                    let transforms = AppExportTransformResolver::new(drivers.clone());
                    let hints = AppFieldHintResolver::new(drivers);
                    let reader = AppSecretReader::new(secret_store);
                    let graph = build_export_graph(&inputs);

                    let (bytes, report) = match dbflux_portability::export::export(
                        &graph,
                        &opts,
                        &hints,
                        &transforms,
                        &reader,
                    ) {
                        Ok(value) => value,
                        Err(e) => return ExportResult::Failed(format!("Export failed: {e}")),
                    };

                    match std::fs::write(&output_path, &bytes) {
                        Ok(()) => ExportResult::Success {
                            path: output_path,
                            warnings: report.warnings,
                            required_ref_count: report.required_ref_count,
                        },
                        Err(e) => ExportResult::Failed(format!("Failed to write export file: {e}")),
                    }
                })
                .await;

            if let Err(update_err) = cx.update(|cx| {
                this.update(cx, |this, cx| {
                    this.is_exporting = false;
                    match &outcome {
                        ExportResult::Success { path, .. } => {
                            dbflux_ui_base::toast::Toast::success(format!(
                                "Exported connection to {}",
                                path.display()
                            ))
                            .push(cx);
                            this.close(cx);
                            cx.emit(ExportConnectionModalEvent::Close);
                        }
                        ExportResult::Failed(_) => {
                            this.pending_result = Some(outcome.clone());
                            cx.notify();
                        }
                    }
                });
            }) {
                log::warn!(
                    "Failed to update export modal after export: {:?}",
                    update_err
                );

                if let ExportResult::Failed(msg) = outcome {
                    report_error_async(UserFacingError::new(ErrorKind::Storage, msg), cx);
                }
            }
        })
        .detach();
    }

    /// Assemble the `ExportInputs` for one connection plus its references.
    ///
    /// Returns `None` when the connection's driver is not registered (export of
    /// a connection with an unknown driver is rejected rather than producing an
    /// empty-fields entry).
    #[allow(clippy::type_complexity)]
    fn assemble_inputs(
        &self,
        profile_id: Uuid,
        cx: &Context<Self>,
    ) -> Option<(
        ExportInputs,
        std::collections::HashMap<String, std::sync::Arc<dyn dbflux_core::DbDriver>>,
        std::sync::Arc<std::sync::RwLock<Box<dyn dbflux_core::SecretStore>>>,
    )> {
        let state = self.app_state.read(cx);

        let profile = state
            .profiles()
            .iter()
            .find(|p| p.id == profile_id)?
            .clone();
        let driver = state.driver_for_profile(&profile)?;
        let values = driver.extract_values(&profile.config);

        let mut auth_profiles: Vec<dbflux_core::AuthProfile> = Vec::new();
        let mut aws_references: Vec<AwsRef> = Vec::new();

        if let Some(auth_id) = profile.auth_profile_id {
            let all_auth = state.list_auth_profiles();
            if let Some(auth) = all_auth.iter().find(|a| a.id == auth_id) {
                if auth.read_only {
                    aws_references.push(AwsRef {
                        provider_id: auth.provider_id.clone(),
                        name: auth.name.clone(),
                    });
                } else {
                    auth_profiles.push(auth.clone());
                }
            }
        }

        let mut ssh_tunnels: Vec<dbflux_core::SshTunnelProfile> = Vec::new();
        let mut proxies: Vec<dbflux_core::ProxyProfile> = Vec::new();

        match profile.access_kind.as_ref() {
            Some(AccessKind::Ssh {
                ssh_tunnel_profile_id,
            }) => {
                if let Some(ssh) = state
                    .ssh_tunnels()
                    .iter()
                    .find(|s| &s.id == ssh_tunnel_profile_id)
                {
                    ssh_tunnels.push(ssh.clone());
                }
            }
            Some(AccessKind::Proxy { proxy_profile_id }) => {
                if let Some(proxy) = state.proxies().iter().find(|p| &p.id == proxy_profile_id) {
                    proxies.push(proxy.clone());
                }
            }
            _ => {}
        }

        let inputs = ExportInputs {
            connections_with_values: vec![(profile, values)],
            auth_profiles,
            aws_references,
            ssh_tunnels,
            proxies,
        };

        let drivers = state.drivers().clone();
        let secret_store = state.facade.secrets.secret_store_arc();

        Some((inputs, drivers, secret_store))
    }
}

fn include_exclude(include: bool) -> IncludeExclude {
    if include {
        IncludeExclude::Include
    } else {
        IncludeExclude::Exclude
    }
}

fn auth_mode_id(mode: AuthExportMode) -> &'static str {
    match mode {
        AuthExportMode::IncludeValues => AUTH_MODE_INCLUDE,
        AuthExportMode::MappableReference => AUTH_MODE_REFERENCE,
        AuthExportMode::RequiredOnImport => AUTH_MODE_REQUIRED,
        AuthExportMode::Exclude => AUTH_MODE_EXCLUDE,
    }
}

fn auth_mode_from_id(id: &str) -> AuthExportMode {
    match id {
        AUTH_MODE_INCLUDE => AuthExportMode::IncludeValues,
        AUTH_MODE_REQUIRED => AuthExportMode::RequiredOnImport,
        AUTH_MODE_EXCLUDE => AuthExportMode::Exclude,
        _ => AuthExportMode::MappableReference,
    }
}

/// The four selectable auth-profile export modes, in display order.
fn auth_mode_items() -> Vec<DropdownItem> {
    vec![
        DropdownItem::with_value("Include values", AUTH_MODE_INCLUDE),
        DropdownItem::with_value("Reference", AUTH_MODE_REFERENCE),
        DropdownItem::with_value("Required on import", AUTH_MODE_REQUIRED),
        DropdownItem::with_value("Exclude", AUTH_MODE_EXCLUDE),
    ]
}

impl Render for ExportConnectionModal {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        // Drain pending output path from the file-dialog callback (or fallback)
        // into the editable input.
        if let Some(path) = self.pending_output_path.take() {
            self.output_input
                .update(cx, |state, cx| state.set_value(path, window, cx));
        }

        // Masking is render-driven so the eye toggle can reveal the value.
        let show_passphrase = self.show_passphrase;
        self.passphrase_input.update(cx, |state, cx| {
            state.set_masked(!show_passphrase, window, cx);
        });
        self.confirm_input.update(cx, |state, cx| {
            state.set_masked(!show_passphrase, window, cx);
        });

        let can_export = self.can_export(cx);
        let is_exporting = self.is_exporting;

        let body = div()
            .track_focus(&self.focus_handle)
            .key_context(dbflux_core::keymap_types::ContextId::ConfirmModal.as_gpui_context())
            .on_key_down(cx.listener(|this, ev: &KeyDownEvent, _w, cx| {
                if ev.keystroke.key == "escape" {
                    this.close(cx);
                    cx.emit(ExportConnectionModalEvent::Close);
                }
            }))
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(self.render_summary(cx))
            .child(self.render_credentials_section(cx))
            .when_some(self.render_auth_mode_section(cx), |el, section| {
                el.child(section)
            })
            .child(self.render_encryption_section(cx))
            .child(self.render_output_section(cx))
            .when_some(self.validation_error.clone(), |el, msg| {
                el.child(BannerBlock::new(BannerVariant::Danger, msg))
            })
            .when_some(self.render_result(), |el, banner| el.child(banner));

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.close(cx);
            cx.emit(ExportConnectionModalEvent::Close);
        });

        let export_label = if is_exporting {
            "Exporting\u{2026}"
        } else {
            "Export"
        };
        let on_export = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.do_export(window, cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("export-conn-cancel", "Cancel")
                    .ghost()
                    .on_click(on_cancel),
            )
            .child(
                Button::new("export-conn-confirm", export_label)
                    .primary()
                    .disabled(!can_export || is_exporting)
                    .on_click(on_export),
            );

        let close_for_x = cx.entity().clone();

        ModalShell::new(
            "Export connection",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .width(px(640.0))
        .on_close(move |_window, cx| {
            close_for_x.update(cx, |this, cx| {
                this.close(cx);
                cx.emit(ExportConnectionModalEvent::Close);
            });
        })
        .into_any_element()
    }
}

impl ExportConnectionModal {
    fn render_summary(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        let Some(summary) = self.summary.as_ref() else {
            return div().into_any_element();
        };

        let mut lines: Vec<String> = Vec::new();
        for auth in &summary.auth_profiles {
            let suffix = if auth.locked { " (reference)" } else { "" };
            lines.push(format!("Auth profile: {}{}", auth.name, suffix));
        }
        if let Some(proxy) = &summary.proxy_name {
            lines.push(format!("Proxy: {proxy}"));
        }
        if let Some(ssh) = &summary.ssh_name {
            lines.push(format!("SSH tunnel: {ssh}"));
        }

        let mut block = surface_raised(cx)
            .w_full()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .font_family(AppFonts::MONO)
                    .text_color(theme.foreground)
                    .child(summary.connection_name.clone()),
            );

        for line in lines {
            block = block.child(
                div()
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(line),
            );
        }

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                Text::body("This connection and its profiles will be exported.")
                    .color(theme.muted_foreground),
            )
            .child(block)
            .into_any_element()
    }

    fn render_credentials_section(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let summary = self.summary.as_ref();
        let has_proxy = summary.map(|s| s.proxy_name.is_some()).unwrap_or(false);
        let has_ssh = summary.map(|s| s.ssh_name.is_some()).unwrap_or(false);

        let conn_pw = self.include_connection_password;
        let proxy_creds = self.include_proxy_credentials;
        let ssh_pw = self.include_ssh_password;
        let embed_keys = self.embed_ssh_keys;
        let force_plaintext = self.force_plaintext;

        let mut col = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::body("Credentials").color(theme.muted_foreground))
            .child(
                Checkbox::new("export-conn-pw")
                    .checked(conn_pw)
                    .label("Include connection password")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.include_connection_password = *checked;
                        cx.notify();
                    })),
            );

        if has_proxy {
            col = col.child(
                Checkbox::new("export-proxy-creds")
                    .checked(proxy_creds)
                    .label("Include proxy credentials")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.include_proxy_credentials = *checked;
                        cx.notify();
                    })),
            );
        }

        if has_ssh {
            col = col.child(
                Checkbox::new("export-ssh-pw")
                    .checked(ssh_pw)
                    .label("Include SSH password")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.include_ssh_password = *checked;
                        cx.notify();
                    })),
            );

            col = col.child(
                Checkbox::new("export-embed-ssh-keys")
                    .checked(embed_keys && !force_plaintext)
                    .label("Embed SSH private keys (requires encryption)")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        if this.force_plaintext {
                            return;
                        }
                        this.embed_ssh_keys = *checked;
                        cx.notify();
                    })),
            );
        }

        col.into_any_element()
    }

    /// The single referenced auth profile's export-mode control: a dropdown for
    /// normal profiles, or a muted "Reference (AWS profile)" label for AWS-locked
    /// ones.
    fn render_auth_mode_section(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let theme = cx.theme().clone();
        let auth = self.auth_profile.as_ref()?;

        let control: AnyElement = if auth.locked {
            Text::body("Reference (AWS profile)")
                .color(theme.muted_foreground)
                .into_any_element()
        } else if let Some(dropdown) = self.auth_dropdown.as_ref() {
            dropdown.clone().into_any_element()
        } else {
            return None;
        };

        let row = div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .id(SharedString::from(format!("auth-mode-row-{}", auth.id)))
            .child(Text::body(auth.name.clone()).color(theme.foreground))
            .child(control);

        Some(
            div()
                .flex()
                .flex_col()
                .gap(Spacing::SM)
                .child(Text::body("Auth profile export mode").color(theme.muted_foreground))
                .child(row)
                .into_any_element(),
        )
    }

    fn render_encryption_section(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let force_plaintext = self.force_plaintext;

        let toggle = Checkbox::new("export-force-plaintext")
            .checked(force_plaintext)
            .label("Disable encryption (force plaintext)")
            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                this.force_plaintext = *checked;
                if *checked {
                    this.embed_ssh_keys = false;
                }
                cx.notify();
            }));

        let inner = if force_plaintext {
            BannerBlock::new(
                BannerVariant::Warning,
                "Secrets will be written in cleartext. \
                 Only use this if the output file is stored securely.",
            )
            .into_any_element()
        } else {
            let eye_icon = if self.show_passphrase {
                AppIcon::EyeOff
            } else {
                AppIcon::Eye
            };

            let toggle = IconButton::new("export-passphrase-eye", eye_icon.into()).on_click({
                let entity = cx.entity().clone();
                move |_event, _window, cx| {
                    entity.update(cx, |this, cx| {
                        this.show_passphrase = !this.show_passphrase;
                        cx.notify();
                    });
                }
            });

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
                )
                .child(Text::body("Confirm passphrase").color(theme.muted_foreground))
                .child(Input::new(&self.confirm_input))
                .into_any_element()
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::body("Encryption").color(theme.muted_foreground))
            .child(toggle)
            .child(inner)
            .into_any_element()
    }

    fn render_output_section(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().clone();

        let browse = IconButton::new("export-output-browse", AppIcon::Folder.into())
            .icon_size(Heights::ICON_SM)
            .on_click(move |_event, _window, cx| {
                entity.update(cx, |this, cx| this.browse_output_path(cx));
            });

        let row = div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(div().flex_1().child(Input::new(&self.output_input)))
            .child(browse);

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body("Output file").color(theme.muted_foreground))
            .child(row)
            .into_any_element()
    }

    fn render_result(&self) -> Option<AnyElement> {
        let result = self.pending_result.as_ref()?;
        match result {
            ExportResult::Success {
                path,
                warnings,
                required_ref_count,
            } => {
                let mut body_lines: Vec<String> = Vec::new();
                if *required_ref_count > 0 {
                    body_lines.push(format!(
                        "{required_ref_count} field(s) omitted — recipient must supply them on import."
                    ));
                }
                for w in warnings {
                    body_lines.push(format!("Warning: {w}"));
                }
                let mut banner = BannerBlock::new(
                    BannerVariant::Success,
                    format!("Exported to {}", path.display()),
                );
                if !body_lines.is_empty() {
                    banner = banner.with_body(body_lines.join("\n"));
                }
                Some(banner.into_any_element())
            }
            ExportResult::Failed(msg) => Some(
                BannerBlock::new(BannerVariant::Danger, "Export failed")
                    .with_body(msg.clone())
                    .into_any_element(),
            ),
        }
    }
}
