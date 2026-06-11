use std::path::PathBuf;

use dbflux_app::portability::{
    AppFieldHintResolver, AppSecretReader, ExportInputs, build_export_graph,
};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::tokens::{Radii, Spacing};
use dbflux_core::secrecy::SecretString;
use dbflux_portability::{AwsRef, EncryptionChoice, ExportOptions};
use dbflux_ui_base::{
    AppStateEntity,
    user_error::{ErrorKind, UserFacingError, report_error_async},
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// GPUI actions
// ---------------------------------------------------------------------------

gpui::actions!(connection_manager, [ExportConnections]);

// ---------------------------------------------------------------------------
// Export state
// ---------------------------------------------------------------------------

/// Result of a completed export run, stored in `ExportModal` for display.
#[derive(Clone)]
enum ExportResult {
    Success {
        path: PathBuf,
        warnings: Vec<String>,
        required_ref_count: usize,
    },
    Failed(String),
}

// ---------------------------------------------------------------------------
// ExportModal entity
// ---------------------------------------------------------------------------

/// Multi-step export modal for connection profiles.
///
/// The modal assembles an `ExportGraph` from the selected connections (plus their
/// transitive auth/ssh/proxy references), builds `ExportOptions` from the toggles,
/// then runs `dbflux_portability::export()` on a background thread.
pub struct ExportModal {
    app_state: Entity<AppStateEntity>,

    // Selection
    connection_ids: Vec<Uuid>,
    selected_ids: Vec<Uuid>,

    // Options toggles
    include_hooks: bool,
    include_settings_overrides: bool,
    embed_ssh_keys: bool,
    force_plaintext: bool,

    // Passphrase inputs
    passphrase_input: Entity<InputState>,
    confirm_input: Entity<InputState>,

    // Output path
    output_path: String,
    pending_output_path: Option<String>,

    // State
    is_exporting: bool,
    pending_result: Option<ExportResult>,
    validation_error: Option<String>,

    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl ExportModal {
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

        let connection_ids = app_state.read(cx).profiles().iter().map(|p| p.id).collect();

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
        window.focus(&focus_handle);

        Self {
            app_state,
            connection_ids,
            selected_ids: Vec::new(),
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: false,
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

    /// Refresh the connection list from AppState (called when the modal becomes visible).
    pub fn refresh_connections(&mut self, cx: &mut Context<Self>) {
        self.connection_ids = self
            .app_state
            .read(cx)
            .profiles()
            .iter()
            .map(|p| p.id)
            .collect();
        cx.notify();
    }

    fn toggle_selection(&mut self, id: Uuid, cx: &mut Context<Self>) {
        if let Some(pos) = self.selected_ids.iter().position(|&s| s == id) {
            self.selected_ids.remove(pos);
        } else {
            self.selected_ids.push(id);
        }
        cx.notify();
    }

    fn select_all(&mut self, cx: &mut Context<Self>) {
        self.selected_ids = self.connection_ids.clone();
        cx.notify();
    }

    fn select_none(&mut self, cx: &mut Context<Self>) {
        self.selected_ids.clear();
        cx.notify();
    }

    fn browse_output_path(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();
        let use_native = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        if use_native {
            let task = cx.background_executor().spawn(async move {
                let dialog = rfd::FileDialog::new()
                    .set_title("Export Connections")
                    .add_filter("DBFlux bundle", &["dbflux"])
                    .add_filter("All files", &["*"])
                    .set_file_name("connections.dbflux");

                dialog.save_file()
            });

            cx.spawn(async move |_this, cx| {
                let path = task.await;

                if let Some(path) = path
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
                        dbflux_ui_base::file_dialog::unique_path_in(&dir, "connections.dbflux");
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

    /// Validate inputs, assemble the export graph, and run the export.
    fn do_export(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.selected_ids.is_empty() {
            self.validation_error = Some("Select at least one connection to export.".to_string());
            cx.notify();
            return;
        }

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

        // Read everything from AppState inside a scoped block so the borrow ends
        // before we start the background task.
        let (inputs, drivers, secret_store) = {
            let state = self.app_state.read(cx);

            let selected_profiles: Vec<dbflux_core::ConnectionProfile> = state
                .profiles()
                .iter()
                .filter(|p| self.selected_ids.contains(&p.id))
                .cloned()
                .collect();

            let mut connections_with_values: Vec<(
                dbflux_core::ConnectionProfile,
                dbflux_core::FormValues,
            )> = Vec::new();
            for profile in &selected_profiles {
                let values = if let Some(driver) = state.driver_for_profile(profile) {
                    driver.extract_values(&profile.config)
                } else {
                    dbflux_core::FormValues::default()
                };
                connections_with_values.push((profile.clone(), values));
            }

            let mut auth_profile_ids: Vec<Uuid> = Vec::new();
            let mut aws_references: Vec<AwsRef> = Vec::new();
            let mut ssh_tunnel_ids: Vec<Uuid> = Vec::new();
            let mut proxy_ids: Vec<Uuid> = Vec::new();

            for profile in &selected_profiles {
                if let Some(auth_id) = profile.auth_profile_id {
                    let all_auth = state.list_auth_profiles();
                    let auth = all_auth.iter().find(|a| a.id == auth_id);

                    if let Some(auth) = auth {
                        if auth.read_only {
                            aws_references.push(AwsRef {
                                provider_id: auth.provider_id.clone(),
                                name: auth.name.clone(),
                            });
                        } else if !auth_profile_ids.contains(&auth_id) {
                            auth_profile_ids.push(auth_id);
                        }
                    }
                }

                use dbflux_core::access::AccessKind;
                match profile.access_kind.as_ref() {
                    Some(AccessKind::Ssh {
                        ssh_tunnel_profile_id,
                    }) if !ssh_tunnel_ids.contains(ssh_tunnel_profile_id) => {
                        ssh_tunnel_ids.push(*ssh_tunnel_profile_id);
                    }
                    Some(AccessKind::Proxy { proxy_profile_id })
                        if !proxy_ids.contains(proxy_profile_id) =>
                    {
                        proxy_ids.push(*proxy_profile_id);
                    }
                    _ => {}
                }
            }

            let all_auth = state.list_auth_profiles();
            let auth_profiles: Vec<dbflux_core::AuthProfile> = auth_profile_ids
                .iter()
                .filter_map(|id| all_auth.iter().find(|a| &a.id == id).cloned())
                .collect();

            let all_ssh = state.ssh_tunnels().to_vec();
            let ssh_tunnels: Vec<dbflux_core::SshTunnelProfile> = ssh_tunnel_ids
                .iter()
                .filter_map(|id| all_ssh.iter().find(|s| &s.id == id).cloned())
                .collect();

            let all_proxy = state.proxies().to_vec();
            let proxies: Vec<dbflux_core::ProxyProfile> = proxy_ids
                .iter()
                .filter_map(|id| all_proxy.iter().find(|p| &p.id == id).cloned())
                .collect();

            let inputs = ExportInputs {
                connections_with_values,
                auth_profiles,
                aws_references,
                ssh_tunnels,
                proxies,
            };

            let drivers = state.drivers().clone();
            let secret_store = state.facade.secrets.secret_store_arc();

            (inputs, drivers, secret_store)
        };

        let opts = ExportOptions {
            include_hooks: self.include_hooks,
            include_settings_overrides: self.include_settings_overrides,
            embed_ssh_keys: self.embed_ssh_keys,
            encryption,
        };

        let this = cx.entity().clone();
        self.is_exporting = true;
        self.validation_error = None;
        cx.notify();

        // Dismiss focus from any input before spawning.
        window.focus(&self.focus_handle);

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    let hints = AppFieldHintResolver::new(drivers);
                    let reader = AppSecretReader::new(secret_store);
                    let graph = build_export_graph(&inputs);
                    dbflux_portability::export::export(&graph, &opts, &hints, &reader)
                })
                .await;

            match result {
                Ok((bytes, report)) => {
                    let write_result = std::fs::write(&output_path, &bytes);

                    if let Err(e) = cx.update(|cx| {
                        this.update(cx, |this, cx| {
                            this.is_exporting = false;
                            match write_result {
                                Ok(()) => {
                                    this.pending_result = Some(ExportResult::Success {
                                        path: output_path,
                                        warnings: report.warnings,
                                        required_ref_count: report.required_ref_count,
                                    });
                                }
                                Err(e) => {
                                    this.pending_result = Some(ExportResult::Failed(format!(
                                        "Failed to write export file: {e}"
                                    )));
                                }
                            }
                            cx.notify();
                        });
                    }) {
                        log::warn!("Failed to update export modal after export: {:?}", e);
                    }
                }

                Err(e) => {
                    let error_msg = format!("Export failed: {e}");
                    if let Err(update_err) = cx.update(|cx| {
                        this.update(cx, |this, cx| {
                            this.is_exporting = false;
                            this.pending_result = Some(ExportResult::Failed(error_msg.clone()));
                            cx.notify();
                        });
                    }) {
                        log::warn!(
                            "Failed to update export modal after export error: {:?}",
                            update_err
                        );

                        report_error_async(UserFacingError::new(ErrorKind::Storage, error_msg), cx);
                    }
                }
            }
        })
        .detach();
    }
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

impl Render for ExportModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use dbflux_components::primitives::{BannerBlock, BannerVariant, Text};

        // Drain pending output path from the file-dialog callback.
        if let Some(path) = self.pending_output_path.take() {
            self.output_path = path;
        }

        let theme = cx.theme().clone();

        let profiles = self.app_state.read(cx).profiles().to_vec();

        let connection_list = div()
            .id("export-connection-list")
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .max_h(px(200.0))
            .overflow_scroll()
            .children(profiles.iter().map(|p| {
                let id = p.id;
                let name = p.name.clone();
                let selected = self.selected_ids.contains(&id);
                let entity = cx.entity().clone();

                div()
                    .id(ElementId::Name(format!("conn-{}", id).into()))
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .px(Spacing::SM)
                    .py(px(2.0))
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .when(selected, |d| d.bg(theme.accent.opacity(0.15)))
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(move |_, _, cx| {
                        entity.update(cx, |this, cx| this.toggle_selection(id, cx));
                    })
                    .child(
                        div()
                            .w(px(14.0))
                            .h(px(14.0))
                            .rounded(Radii::SM)
                            .border_1()
                            .border_color(if selected { theme.accent } else { theme.border })
                            .when(selected, |d| d.bg(theme.accent)),
                    )
                    .child(Text::body(name).color(theme.foreground))
            }));

        let entity_for_all = cx.entity().clone();
        let entity_for_none = cx.entity().clone();
        let entity_for_hooks = cx.entity().clone();
        let entity_for_settings = cx.entity().clone();
        let entity_for_ssh = cx.entity().clone();
        let entity_for_plaintext = cx.entity().clone();
        let entity_for_browse = cx.entity().clone();
        let entity_for_export = cx.entity().clone();

        let is_exporting = self.is_exporting;
        let force_plaintext = self.force_plaintext;
        let include_hooks = self.include_hooks;
        let include_settings = self.include_settings_overrides;
        let embed_ssh = self.embed_ssh_keys;
        let output_path = self.output_path.clone();

        let encryption_section = if force_plaintext {
            BannerBlock::new(
                BannerVariant::Warning,
                "Secrets will be written in cleartext. \
                 Only use this option if the output file is stored securely.",
            )
            .into_any_element()
        } else {
            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .child(Text::body("Passphrase").color(theme.muted_foreground))
                .child(self.passphrase_input.clone())
                .child(Text::body("Confirm passphrase").color(theme.muted_foreground))
                .child(self.confirm_input.clone())
                .into_any_element()
        };

        let result_section = self.pending_result.as_ref().map(|result| {
            match result {
                ExportResult::Success {
                    path,
                    warnings,
                    required_ref_count,
                } => {
                    let summary = format!("Exported to {}", path.display());
                    let mut body_lines: Vec<String> = Vec::new();
                    if *required_ref_count > 0 {
                        body_lines.push(format!(
                            "{required_ref_count} field(s) omitted — recipient must supply them on import."
                        ));
                    }
                    for w in warnings {
                        body_lines.push(format!("Warning: {w}"));
                    }
                    let body = if body_lines.is_empty() {
                        None
                    } else {
                        Some(body_lines.join("\n"))
                    };
                    let mut banner = BannerBlock::new(BannerVariant::Success, summary);
                    if let Some(b) = body {
                        banner = banner.with_body(b);
                    }
                    banner.into_any_element()
                }
                ExportResult::Failed(msg) => {
                    BannerBlock::new(BannerVariant::Danger, "Export failed")
                        .with_body(msg.clone())
                        .into_any_element()
                }
            }
        });

        div()
            .track_focus(&self.focus_handle)
            .w(px(520.0))
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::LG)
            .child(div().child(Text::body("Export Connections").font_weight(FontWeight::SEMIBOLD)))
            // Connection list
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_between()
                            .child(Text::body("Connections").color(theme.muted_foreground))
                            .child(
                                div()
                                    .flex()
                                    .gap(Spacing::XS)
                                    .child(
                                        div()
                                            .id("select-all")
                                            .cursor_pointer()
                                            .hover(|d| d.opacity(0.7))
                                            .on_click(move |_, _, cx| {
                                                entity_for_all.update(cx, |this, cx| {
                                                    this.select_all(cx);
                                                });
                                            })
                                            .child(Text::body("All").color(theme.accent)),
                                    )
                                    .child(Text::body("·").color(theme.muted_foreground))
                                    .child(
                                        div()
                                            .id("select-none")
                                            .cursor_pointer()
                                            .hover(|d| d.opacity(0.7))
                                            .on_click(move |_, _, cx| {
                                                entity_for_none.update(cx, |this, cx| {
                                                    this.select_none(cx);
                                                });
                                            })
                                            .child(Text::body("None").color(theme.accent)),
                                    ),
                            ),
                    )
                    .child(connection_list),
            )
            // Options
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::body("Options").color(theme.muted_foreground))
                    .child(self.render_toggle(
                        "toggle-hooks",
                        include_hooks,
                        "Include hooks",
                        entity_for_hooks,
                        |this, cx| {
                            this.include_hooks = !this.include_hooks;
                            cx.notify();
                        },
                        cx,
                    ))
                    .child(self.render_toggle(
                        "toggle-settings",
                        include_settings,
                        "Include settings overrides",
                        entity_for_settings,
                        |this, cx| {
                            this.include_settings_overrides = !this.include_settings_overrides;
                            cx.notify();
                        },
                        cx,
                    ))
                    .child(self.render_toggle(
                        "toggle-ssh",
                        embed_ssh,
                        "Embed SSH private keys (requires passphrase)",
                        entity_for_ssh,
                        |this, cx| {
                            this.embed_ssh_keys = !this.embed_ssh_keys;
                            cx.notify();
                        },
                        cx,
                    ))
                    .child(self.render_toggle(
                        "toggle-plaintext",
                        force_plaintext,
                        "Disable encryption (force plaintext)",
                        entity_for_plaintext,
                        |this, cx| {
                            this.force_plaintext = !this.force_plaintext;
                            cx.notify();
                        },
                        cx,
                    )),
            )
            // Encryption section (passphrase or warning)
            .child(encryption_section)
            // Output path
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::body("Output file").color(theme.muted_foreground))
                    .child(
                        div()
                            .flex()
                            .gap(Spacing::XS)
                            .child(div().flex_1().child(if output_path.is_empty() {
                                Text::body("No file chosen").color(theme.muted_foreground)
                            } else {
                                Text::body(output_path.clone()).color(theme.foreground)
                            }))
                            .child(
                                div()
                                    .id("browse-btn")
                                    .px(Spacing::SM)
                                    .py(px(4.0))
                                    .rounded(Radii::SM)
                                    .bg(theme.secondary)
                                    .cursor_pointer()
                                    .hover(|d| d.opacity(0.8))
                                    .on_click(move |_, window, cx| {
                                        entity_for_browse.update(cx, |this, cx| {
                                            this.browse_output_path(window, cx);
                                        });
                                    })
                                    .child(Text::body("Browse…").color(theme.foreground)),
                            ),
                    ),
            )
            // Validation error
            .when_some(self.validation_error.as_ref(), |el, msg| {
                el.child(BannerBlock::new(BannerVariant::Danger, msg.clone()))
            })
            // Result section
            .when_some(result_section, |el, section| el.child(section))
            // Export button
            .child(
                div().flex().justify_end().child(
                    div()
                        .id("export-btn")
                        .px(Spacing::MD)
                        .py(px(6.0))
                        .rounded(Radii::SM)
                        .bg(if is_exporting {
                            theme.secondary
                        } else {
                            theme.accent
                        })
                        .cursor_pointer()
                        .hover(|d| d.opacity(0.8))
                        .when(!is_exporting, |d| {
                            d.on_click(move |_, window, cx| {
                                entity_for_export.update(cx, |this, cx| {
                                    this.do_export(window, cx);
                                });
                            })
                        })
                        .child(
                            Text::body(if is_exporting {
                                "Exporting…"
                            } else {
                                "Export"
                            })
                            .color(if is_exporting {
                                theme.muted_foreground
                            } else {
                                theme.accent_foreground
                            }),
                        ),
                ),
            )
    }
}

impl ExportModal {
    /// Render a labelled toggle row (checkbox-style).
    fn render_toggle<F>(
        &self,
        id: impl Into<ElementId>,
        checked: bool,
        label: &str,
        entity: Entity<ExportModal>,
        on_toggle: F,
        cx: &Context<Self>,
    ) -> impl IntoElement
    where
        F: Fn(&mut ExportModal, &mut Context<ExportModal>) + 'static,
    {
        use dbflux_components::primitives::Text;

        let theme = cx.theme().clone();
        let label = label.to_string();

        div()
            .id(id)
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .cursor_pointer()
            .on_click(move |_, _, cx| {
                entity.update(cx, |this, cx| on_toggle(this, cx));
            })
            .child(
                div()
                    .w(px(14.0))
                    .h(px(14.0))
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(if checked { theme.accent } else { theme.border })
                    .when(checked, |d| d.bg(theme.accent)),
            )
            .child(Text::body(label).color(theme.foreground))
    }
}
