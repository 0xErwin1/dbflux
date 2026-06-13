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
use dbflux_components::primitives::{BannerBlock, BannerVariant, IconButton, Text, surface_raised};
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

/// Event emitted by [`ExportBundleModal`] so the workspace host can react
/// to dismissal (closing the overlay clears the rendered child).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExportBundleModalEvent {
    Close,
}

/// What the modal exports: a full connection (plus its referenced profiles) or a
/// single standalone profile from a Settings section.
///
/// The portability pipeline already supports a bundle with no connections, so a
/// standalone profile travels as a one-entry bundle of the matching kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportTarget {
    Connection(Uuid),
    AuthProfile(Uuid),
    SshTunnel(Uuid),
    Proxy(Uuid),
}

impl ExportTarget {
    /// Human label for the exported entity kind, used in the modal title and the
    /// summary block.
    fn kind_label(self) -> &'static str {
        match self {
            ExportTarget::Connection(_) => "connection",
            ExportTarget::AuthProfile(_) => "auth profile",
            ExportTarget::SshTunnel(_) => "SSH tunnel",
            ExportTarget::Proxy(_) => "proxy",
        }
    }
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
/// Computed once in [`ExportBundleModal::open`] so the body renders from
/// stable values instead of re-reading `AppState` on every frame.
struct ExportSummary {
    /// Name of the primary entity being exported (connection or standalone profile).
    primary_name: String,
    /// Auth profiles referenced by the connection, or the single auth profile when
    /// the target is a standalone auth profile.
    auth_profiles: Vec<AuthProfileRow>,
    proxy_name: Option<String>,
    ssh_name: Option<String>,
    /// For an SSH target: whether the tunnel authenticates with a password (vs a
    /// private key). Drives which secret control the credentials section shows.
    ssh_uses_password: bool,
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

/// In-app export modal for a single portability target.
///
/// Scoped to exactly one entity: a connection (plus its referenced auth / proxy /
/// SSH profiles) when opened from a connection's three-dots menu, or a single
/// standalone profile when opened from a Settings section. Hosted as an overlay —
/// it never opens an OS window.
pub struct ExportBundleModal {
    app_state: Entity<AppStateEntity>,

    visible: bool,
    target: Option<ExportTarget>,
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
    /// Suggested bundle file name derived from the connection name (sanitized),
    /// used as the save-dialog default and the no-picker fallback file name.
    default_file_name: String,

    // Run state.
    is_exporting: bool,
    pending_result: Option<ExportResult>,
    validation_error: Option<String>,

    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<ExportBundleModalEvent> for ExportBundleModal {}

impl ExportBundleModal {
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
            target: None,
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
            default_file_name: String::new(),
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
    /// Thin wrapper over [`Self::open_target`] kept for the connection entry
    /// points (three-dots menu, command palette).
    pub fn open(&mut self, profile_id: Uuid, window: &mut Window, cx: &mut Context<Self>) {
        self.open_target(ExportTarget::Connection(profile_id), window, cx);
    }

    /// Open the modal for any portability target.
    ///
    /// Resets all run state to defaults, computes the read-only summary of the
    /// target (and, for a connection, its referenced profiles), and seeds the
    /// per-secret defaults appropriate for the target kind. AWS reflected auth
    /// profiles are locked to a reference.
    pub fn open_target(
        &mut self,
        target: ExportTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.target = Some(target);
        self.summary = self.build_summary(target, cx);

        let ssh_uses_password = self
            .summary
            .as_ref()
            .map(|summary| summary.ssh_uses_password)
            .unwrap_or(false);

        // Reset to ready-to-use defaults on every open. For a standalone profile
        // the profile's own secret is the subject of the export, so its primary
        // secret defaults on (mirroring the connection-password default);
        // embedding SSH keys stays consent-gated and off.
        self.include_connection_password = matches!(target, ExportTarget::Connection(_));
        self.include_proxy_credentials = matches!(target, ExportTarget::Proxy(_));
        self.include_ssh_password =
            matches!(target, ExportTarget::SshTunnel(_)) && ssh_uses_password;
        self.embed_ssh_keys = false;
        self.force_plaintext = false;
        self.show_passphrase = false;
        self.pending_output_path = None;
        self.is_exporting = false;
        self.pending_result = None;
        self.validation_error = None;

        // The connection references at most one auth profile; a standalone auth
        // target is itself the single profile. Seed the default export mode (AWS
        // reflected = locked reference; everything else = include values).
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

        // Default file name = sanitized entity name; pre-fill the output path with
        // it under the exports directory so Export works out of the box and the
        // user can still edit or browse to another location.
        let stem = self
            .summary
            .as_ref()
            .map(|summary| sanitize_filename(&summary.primary_name))
            .unwrap_or_else(|| target.kind_label().replace(' ', "-"));
        self.default_file_name = format!("{stem}.toml");

        let default_path = dbflux_ui_base::file_dialog::fallback_export_dir()
            .ok()
            .map(|dir| {
                dir.join(&self.default_file_name)
                    .to_string_lossy()
                    .to_string()
            })
            .unwrap_or_default();
        self.output_input
            .update(cx, |state, cx| state.set_value(default_path, window, cx));

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
        self.target = None;
        self.summary = None;
        self.auth_profile = None;
        self.auth_dropdown = None;
        self.auth_dropdown_sub = None;
        cx.notify();
    }

    /// Build the read-only summary block for the target.
    fn build_summary(&self, target: ExportTarget, cx: &Context<Self>) -> Option<ExportSummary> {
        match target {
            ExportTarget::Connection(profile_id) => self.build_connection_summary(profile_id, cx),
            ExportTarget::AuthProfile(auth_id) => self.build_auth_summary(auth_id, cx),
            ExportTarget::SshTunnel(ssh_id) => self.build_ssh_summary(ssh_id, cx),
            ExportTarget::Proxy(proxy_id) => self.build_proxy_summary(proxy_id, cx),
        }
    }

    /// Collect the connection and its referenced auth / proxy / SSH profile names
    /// for the read-only summary block.
    fn build_connection_summary(
        &self,
        profile_id: Uuid,
        cx: &Context<Self>,
    ) -> Option<ExportSummary> {
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
            primary_name: profile.name.clone(),
            auth_profiles,
            proxy_name,
            ssh_name,
            ssh_uses_password: false,
        })
    }

    /// Summary for a standalone auth profile. The profile itself is the single
    /// entry in `auth_profiles` so the auth-mode control renders for it.
    fn build_auth_summary(&self, auth_id: Uuid, cx: &Context<Self>) -> Option<ExportSummary> {
        let state = self.app_state.read(cx);
        let all_auth = state.list_auth_profiles();
        let auth = all_auth.iter().find(|a| a.id == auth_id)?;

        Some(ExportSummary {
            primary_name: auth.name.clone(),
            auth_profiles: vec![AuthProfileRow {
                id: auth.id,
                name: auth.name.clone(),
                locked: auth.read_only,
            }],
            proxy_name: None,
            ssh_name: None,
            ssh_uses_password: false,
        })
    }

    /// Summary for a standalone SSH tunnel profile.
    fn build_ssh_summary(&self, ssh_id: Uuid, cx: &Context<Self>) -> Option<ExportSummary> {
        let state = self.app_state.read(cx);
        let ssh = state.ssh_tunnels().iter().find(|s| s.id == ssh_id)?.clone();

        let ssh_uses_password =
            matches!(ssh.config.auth_method, dbflux_core::SshAuthMethod::Password);

        Some(ExportSummary {
            primary_name: ssh.name.clone(),
            auth_profiles: Vec::new(),
            proxy_name: None,
            ssh_name: Some(ssh.name.clone()),
            ssh_uses_password,
        })
    }

    /// Summary for a standalone proxy profile.
    fn build_proxy_summary(&self, proxy_id: Uuid, cx: &Context<Self>) -> Option<ExportSummary> {
        let state = self.app_state.read(cx);
        let proxy = state.proxies().iter().find(|p| p.id == proxy_id)?.clone();

        Some(ExportSummary {
            primary_name: proxy.name.clone(),
            auth_profiles: Vec::new(),
            proxy_name: Some(proxy.name.clone()),
            ssh_name: None,
            ssh_uses_password: false,
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
        let kind_label = self
            .target
            .map(ExportTarget::kind_label)
            .unwrap_or("bundle");

        let file_name = if self.default_file_name.is_empty() {
            format!("{}.toml", kind_label.replace(' ', "-"))
        } else {
            self.default_file_name.clone()
        };

        let title = format!("Export {kind_label}");

        if dbflux_ui_base::file_dialog::is_native_file_dialog_available() {
            let this = cx.entity().clone();
            let task = cx.background_executor().spawn(async move {
                rfd::FileDialog::new()
                    .set_title(title)
                    .add_filter("TOML bundle", &["toml"])
                    .add_filter("All files", &["*"])
                    .set_file_name(file_name)
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
                    let path = dbflux_ui_base::file_dialog::unique_path_in(&dir, &file_name);
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

    /// Validate inputs, assemble the export graph for the target, and run the
    /// export on a background thread.
    fn do_export(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(target) = self.target else {
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

        let Some((inputs, drivers, secret_store)) = self.assemble_inputs(target, cx) else {
            self.validation_error = Some(
                "This profile can no longer be exported; it may have been removed.".to_string(),
            );
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
                            let kind = this
                                .target
                                .map(ExportTarget::kind_label)
                                .unwrap_or("bundle");
                            dbflux_ui_base::toast::Toast::success(format!(
                                "Exported {kind} to {}",
                                path.display()
                            ))
                            .push(cx);
                            this.close(cx);
                            cx.emit(ExportBundleModalEvent::Close);
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

    /// Assemble the `ExportInputs`, drivers, and secret store for the target.
    ///
    /// For a connection, returns `None` when its driver is not registered (export
    /// of a connection with an unknown driver is rejected rather than producing an
    /// empty-fields entry). For a standalone profile, returns `None` only when the
    /// profile no longer exists.
    #[allow(clippy::type_complexity)]
    fn assemble_inputs(
        &self,
        target: ExportTarget,
        cx: &Context<Self>,
    ) -> Option<(
        ExportInputs,
        std::collections::HashMap<String, std::sync::Arc<dyn dbflux_core::DbDriver>>,
        std::sync::Arc<std::sync::RwLock<Box<dyn dbflux_core::SecretStore>>>,
    )> {
        let state = self.app_state.read(cx);

        let inputs = match target {
            ExportTarget::Connection(profile_id) => {
                self.assemble_connection_inputs(state, profile_id)?
            }
            ExportTarget::AuthProfile(auth_id) => {
                let all_auth = state.list_auth_profiles();
                let auth = all_auth.iter().find(|a| a.id == auth_id)?;

                // AWS reflected profiles are read-only mirrors of ~/.aws and have
                // no standalone payload; the Settings UI disables Export for them.
                if auth.read_only {
                    return None;
                }

                ExportInputs {
                    auth_profiles: vec![auth.clone()],
                    ..Default::default()
                }
            }
            ExportTarget::SshTunnel(ssh_id) => {
                let ssh = state.ssh_tunnels().iter().find(|s| s.id == ssh_id)?.clone();
                ExportInputs {
                    ssh_tunnels: vec![ssh],
                    ..Default::default()
                }
            }
            ExportTarget::Proxy(proxy_id) => {
                let proxy = state.proxies().iter().find(|p| p.id == proxy_id)?.clone();
                ExportInputs {
                    proxies: vec![proxy],
                    ..Default::default()
                }
            }
        };

        let drivers = state.drivers().clone();
        let secret_store = state.facade.secrets.secret_store_arc();

        Some((inputs, drivers, secret_store))
    }

    /// Assemble inputs for a connection plus its referenced auth / proxy / SSH
    /// profiles. Returns `None` when the connection or its driver is missing.
    fn assemble_connection_inputs(
        &self,
        state: &AppStateEntity,
        profile_id: Uuid,
    ) -> Option<ExportInputs> {
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

        Some(ExportInputs {
            connections_with_values: vec![(profile, values)],
            auth_profiles,
            aws_references,
            ssh_tunnels,
            proxies,
        })
    }
}

fn include_exclude(include: bool) -> IncludeExclude {
    if include {
        IncludeExclude::Include
    } else {
        IncludeExclude::Exclude
    }
}

/// Turn a connection name into a safe file stem: keep ASCII alphanumerics, `-`,
/// `_` and `.`; replace any other character (spaces, `/`, etc.) with a single
/// `-`; collapse runs and trim leading/trailing separators. Falls back to
/// "connection" when nothing usable remains.
fn sanitize_filename(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_dash = false;

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.' {
            out.push(ch);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }

    let trimmed = out.trim_matches(|c| c == '-' || c == '.');
    if trimmed.is_empty() {
        "connection".to_string()
    } else {
        trimmed.to_string()
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

impl Render for ExportBundleModal {
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
                    cx.emit(ExportBundleModalEvent::Close);
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
            cx.emit(ExportBundleModalEvent::Close);
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

        let title = match self.target {
            Some(target) => format!("Export {}", target.kind_label()),
            None => "Export".to_string(),
        };

        ModalShell::new(title, body.into_any_element(), footer.into_any_element())
            .width(px(640.0))
            .on_close(move |_window, cx| {
                close_for_x.update(cx, |this, cx| {
                    this.close(cx);
                    cx.emit(ExportBundleModalEvent::Close);
                });
            })
            .into_any_element()
    }
}

impl ExportBundleModal {
    fn render_summary(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        let Some(summary) = self.summary.as_ref() else {
            return div().into_any_element();
        };

        // Referenced-profile lines only make sense for a connection; a standalone
        // profile is fully named by the primary block above.
        let is_connection = matches!(self.target, Some(ExportTarget::Connection(_)));

        let mut lines: Vec<String> = Vec::new();
        if is_connection {
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
                    .child(summary.primary_name.clone()),
            );

        for line in lines {
            block = block.child(
                div()
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(line),
            );
        }

        let intro = if is_connection {
            "This connection and its profiles will be exported."
        } else {
            "This profile will be exported."
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body(intro).color(theme.muted_foreground))
            .child(block)
            .into_any_element()
    }

    fn render_credentials_section(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        // An auth profile carries no credential checkboxes; its secret material is
        // governed by the export-mode control rendered below.
        if matches!(self.target, Some(ExportTarget::AuthProfile(_))) {
            return div().into_any_element();
        }

        let summary = self.summary.as_ref();
        let conn_pw = self.include_connection_password;
        let proxy_creds = self.include_proxy_credentials;
        let ssh_pw = self.include_ssh_password;
        let embed_keys = self.embed_ssh_keys;
        let force_plaintext = self.force_plaintext;

        // Which controls apply depends on the target. A connection shows its
        // password plus controls for any referenced proxy / SSH profile; a
        // standalone profile shows only its own secret control.
        let show_conn_pw = matches!(self.target, Some(ExportTarget::Connection(_)));
        let show_proxy = match self.target {
            Some(ExportTarget::Connection(_)) => {
                summary.map(|s| s.proxy_name.is_some()).unwrap_or(false)
            }
            Some(ExportTarget::Proxy(_)) => true,
            _ => false,
        };
        let (show_ssh_pw, show_embed) = match self.target {
            Some(ExportTarget::Connection(_)) => {
                let has_ssh = summary.map(|s| s.ssh_name.is_some()).unwrap_or(false);
                (has_ssh, has_ssh)
            }
            Some(ExportTarget::SshTunnel(_)) => {
                let uses_password = summary.map(|s| s.ssh_uses_password).unwrap_or(false);
                (uses_password, !uses_password)
            }
            _ => (false, false),
        };

        let mut col = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::body("Credentials").color(theme.muted_foreground));

        if show_conn_pw {
            col = col.child(
                Checkbox::new("export-conn-pw")
                    .checked(conn_pw)
                    .label("Include connection password")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.include_connection_password = *checked;
                        cx.notify();
                    })),
            );
        }

        if show_proxy {
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

        if show_ssh_pw {
            col = col.child(
                Checkbox::new("export-ssh-pw")
                    .checked(ssh_pw)
                    .label("Include SSH password")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.include_ssh_password = *checked;
                        cx.notify();
                    })),
            );
        }

        if show_embed {
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
