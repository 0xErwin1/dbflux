use std::collections::HashMap;
use std::path::PathBuf;

use dbflux_app::portability::{
    AppExportTransformResolver, AppFieldHintResolver, AppSecretReader, ExportInputs,
    build_export_graph,
};
use dbflux_components::controls::{Button, Checkbox, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::shell::ModalShell;
use dbflux_components::primitives::{
    BannerBlock, BannerVariant, FilePicker, SegmentedControl, SegmentedItem, Text, surface_raised,
};
use dbflux_components::tokens::{FontSizes, Spacing};
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

/// Per-auth-profile export mode segment ids (kept in sync with `auth_mode_from_id`).
const AUTH_MODE_INCLUDE: &str = "include";
const AUTH_MODE_REFERENCE: &str = "reference";
const AUTH_MODE_REQUIRED: &str = "required";
const AUTH_MODE_EXCLUDE: &str = "exclude";

/// Include/exclude segment ids (shared by every two-way credential control).
const INCLUDE_ID: &str = "include";
const EXCLUDE_ID: &str = "exclude";

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

    // Encryption.
    force_plaintext: bool,
    passphrase_input: Entity<InputState>,
    confirm_input: Entity<InputState>,

    // Output path.
    output_path: String,
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
            force_plaintext: false,
            passphrase_input,
            confirm_input,
            output_path: String::new(),
            pending_output_path: None,
            is_exporting: false,
            pending_result: None,
            validation_error: None,
            focus_handle,
            _subscriptions: vec![passphrase_sub, confirm_sub],
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
        self.output_path.clear();
        self.pending_output_path = None;
        self.is_exporting = false;
        self.pending_result = None;
        self.validation_error = None;

        self.auth_modes = self
            .summary
            .as_ref()
            .map(|summary| {
                summary
                    .auth_profiles
                    .iter()
                    .map(|row| {
                        let mode = if row.locked {
                            AuthExportMode::MappableReference
                        } else {
                            AuthExportMode::IncludeValues
                        };
                        (row.id, mode)
                    })
                    .collect()
            })
            .unwrap_or_default();

        self.passphrase_input
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.confirm_input
            .update(cx, |state, cx| state.set_value("", window, cx));

        self.visible = true;
        window.focus(&self.focus_handle);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.profile_id = None;
        self.summary = None;
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
        if self.output_path.trim().is_empty() {
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
                    self.output_path = path.to_string_lossy().to_string();
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

        if self.output_path.trim().is_empty() {
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

        let output_path = PathBuf::from(self.output_path.trim());

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

impl Render for ExportConnectionModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        // Drain pending output path from the native file-dialog callback.
        if let Some(path) = self.pending_output_path.take() {
            self.output_path = path;
        }

        let can_export = self.can_export(cx);
        let is_exporting = self.is_exporting;

        let body = div()
            .track_focus(&self.focus_handle)
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(self.render_summary(cx))
            .child(self.render_credentials_section(cx))
            .when_some(self.render_auth_modes_section(cx), |el, section| {
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
        .width(px(520.0))
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
            .child(self.include_exclude_row(
                "export-conn-pw",
                "Connection password",
                conn_pw,
                |this, include, cx| {
                    this.include_connection_password = include;
                    cx.notify();
                },
                cx,
            ));

        if has_proxy {
            col = col.child(self.include_exclude_row(
                "export-proxy-creds",
                "Proxy credentials",
                proxy_creds,
                |this, include, cx| {
                    this.include_proxy_credentials = include;
                    cx.notify();
                },
                cx,
            ));
        }

        if has_ssh {
            col = col.child(self.include_exclude_row(
                "export-ssh-pw",
                "SSH password",
                ssh_pw,
                |this, include, cx| {
                    this.include_ssh_password = include;
                    cx.notify();
                },
                cx,
            ));

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

    /// One labelled include/exclude segmented control bound to a `bool` flag.
    fn include_exclude_row(
        &self,
        id: &str,
        label: &str,
        include: bool,
        on_change: impl Fn(&mut ExportConnectionModal, bool, &mut Context<ExportConnectionModal>)
        + 'static,
        cx: &Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().clone();
        let active = if include { INCLUDE_ID } else { EXCLUDE_ID };

        let items = vec![
            SegmentedItem::new(INCLUDE_ID, "Include"),
            SegmentedItem::new(EXCLUDE_ID, "Exclude"),
        ];

        let on_change = std::sync::Arc::new(on_change);

        let control = SegmentedControl::new(items, active, move |selected, _window, cx| {
            let include = selected.as_ref() == INCLUDE_ID;
            let on_change = on_change.clone();
            entity.update(cx, |this, cx| on_change(this, include, cx));
        });

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .id(SharedString::from(format!("{id}-row")))
            .child(Text::body(label.to_string()).color(theme.foreground))
            .child(control)
            .into_any_element()
    }

    fn render_auth_modes_section(&self, cx: &Context<Self>) -> Option<AnyElement> {
        let theme = cx.theme().clone();
        let summary = self.summary.as_ref()?;
        if summary.auth_profiles.is_empty() {
            return None;
        }

        let mut col = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::body("Auth profile export mode").color(theme.muted_foreground));

        for auth in &summary.auth_profiles {
            col = col.child(self.render_auth_mode_row(auth, cx));
        }

        Some(col.into_any_element())
    }

    fn render_auth_mode_row(&self, auth: &AuthProfileRow, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let auth_id = auth.id;
        let current = self
            .auth_modes
            .get(&auth_id)
            .copied()
            .unwrap_or(AuthExportMode::MappableReference);

        // AWS reflected profiles are locked to a mappable reference: show only
        // that segment and disable selection.
        let items: Vec<SegmentedItem> = if auth.locked {
            vec![SegmentedItem::new(AUTH_MODE_REFERENCE, "Reference")]
        } else {
            vec![
                SegmentedItem::new(AUTH_MODE_INCLUDE, "Include values"),
                SegmentedItem::new(AUTH_MODE_REFERENCE, "Reference"),
                SegmentedItem::new(AUTH_MODE_REQUIRED, "Required on import"),
                SegmentedItem::new(AUTH_MODE_EXCLUDE, "Exclude"),
            ]
        };

        let entity = cx.entity().clone();
        let locked = auth.locked;
        let control = SegmentedControl::new(
            items,
            auth_mode_id(current),
            move |selected, _window, cx| {
                if locked {
                    return;
                }
                let mode = auth_mode_from_id(selected.as_ref());
                entity.update(cx, |this, cx| {
                    this.auth_modes.insert(auth_id, mode);
                    cx.notify();
                });
            },
        );

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .id(SharedString::from(format!("auth-mode-row-{auth_id}")))
            .child(Text::body(auth.name.clone()).color(theme.foreground))
            .child(div().opacity(if locked { 0.6 } else { 1.0 }).child(control))
            .into_any_element()
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
            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .child(Text::body("Passphrase").color(theme.muted_foreground))
                .child(Input::new(&self.passphrase_input))
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

        let picker = FilePicker::new(
            "export-output-picker",
            self.output_path.clone(),
            AppIcon::Folder,
            AppIcon::X,
        )
        .placeholder("Choose output file\u{2026}")
        .on_browse(move |_event, _window, cx| {
            entity.update(cx, |this, cx| this.browse_output_path(cx));
        });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body("Output file").color(theme.muted_foreground))
            .child(picker)
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
