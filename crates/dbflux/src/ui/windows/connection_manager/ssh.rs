use crate::ui::dropdown::DropdownSelectionChanged;
use crate::ui::windows::ssh_shared::SshAuthSelection;
use dbflux_core::{SshAuthMethod, SshTunnelProfile};
use gpui::*;
use log::info;
use uuid::Uuid;

use super::{ConnectionManagerWindow, TestStatus};

impl ConnectionManagerWindow {
    pub(super) fn handle_ssh_tunnel_dropdown_selection(
        &mut self,
        event: &DropdownSelectionChanged,
        cx: &mut Context<Self>,
    ) {
        if let Some(uuid) = self.ssh_tunnel_uuids.get(event.index).copied() {
            self.pending_ssh_tunnel_selection = Some(uuid);
            cx.notify();
        }
    }

    pub(super) fn apply_ssh_tunnel(
        &mut self,
        tunnel: &SshTunnelProfile,
        secret: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.selected_ssh_tunnel_id = Some(tunnel.id);
        self.ssh_enabled = true;

        self.input_ssh_host.update(cx, |state, cx| {
            state.set_value(&tunnel.config.host, window, cx);
        });
        self.input_ssh_port.update(cx, |state, cx| {
            state.set_value(tunnel.config.port.to_string(), window, cx);
        });
        self.input_ssh_user.update(cx, |state, cx| {
            state.set_value(&tunnel.config.user, window, cx);
        });

        match &tunnel.config.auth_method {
            SshAuthMethod::PrivateKey { key_path } => {
                self.ssh_auth_method = SshAuthSelection::PrivateKey;
                if let Some(path) = key_path {
                    self.input_ssh_key_path.update(cx, |state, cx| {
                        state.set_value(path.to_string_lossy().to_string(), window, cx);
                    });
                }
                if let Some(ref passphrase) = secret {
                    self.input_ssh_key_passphrase.update(cx, |state, cx| {
                        state.set_value(passphrase, window, cx);
                    });
                }
            }
            SshAuthMethod::Password => {
                self.ssh_auth_method = SshAuthSelection::Password;
                if let Some(ref password) = secret {
                    self.input_ssh_password.update(cx, |state, cx| {
                        state.set_value(password, window, cx);
                    });
                }
            }
        }

        self.form_save_ssh_secret = tunnel.save_secret && secret.is_some();
        self.ssh_test_status = TestStatus::None;
        self.ssh_test_error = None;
        cx.notify();
    }

    pub(super) fn clear_ssh_tunnel_selection(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.selected_ssh_tunnel_id = None;

        self.input_ssh_host.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_port.update(cx, |state, cx| {
            state.set_value("22", window, cx);
        });
        self.input_ssh_user.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_key_path.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_key_passphrase.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.input_ssh_password.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        self.ssh_auth_method = SshAuthSelection::PrivateKey;
        self.form_save_ssh_secret = true;
        self.ssh_test_status = TestStatus::None;
        self.ssh_test_error = None;
        cx.notify();
    }

    pub(super) fn save_current_ssh_as_tunnel(&mut self, cx: &mut Context<Self>) {
        let Some(config) = self.build_ssh_config(cx) else {
            return;
        };

        let name = format!("{}@{}", config.user, config.host);
        let secret = self.get_ssh_secret(cx);

        let tunnel = SshTunnelProfile {
            id: Uuid::new_v4(),
            name,
            config,
            save_secret: self.form_save_ssh_secret,
        };

        self.app_state.update(cx, |state, cx| {
            if tunnel.save_secret
                && let Some(ref secret) = secret
            {
                state.save_ssh_tunnel_secret(&tunnel, secret);
            }
            state.add_ssh_tunnel(tunnel.clone());
            cx.emit(crate::app::AppStateChanged);
        });

        self.selected_ssh_tunnel_id = Some(tunnel.id);
        cx.notify();
    }

    pub(super) fn test_ssh_connection(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.ssh_enabled {
            return;
        }

        self.ssh_test_status = TestStatus::Testing;
        self.ssh_test_error = None;
        cx.notify();

        let Some(ssh_config) = self.build_ssh_config(cx) else {
            self.ssh_test_status = TestStatus::Failed;
            self.ssh_test_error = Some("SSH configuration incomplete".to_string());
            cx.notify();
            return;
        };

        let ssh_secret = self.get_ssh_secret(cx);

        let this = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            match dbflux_ssh::establish_session(&ssh_config, ssh_secret.as_deref()) {
                Ok(_session) => Ok(()),
                Err(e) => Err(format!("{:?}", e)),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                this.update(cx, |this, cx| {
                    match result {
                        Ok(()) => {
                            info!("SSH test connection successful");
                            this.ssh_test_status = TestStatus::Success;
                            this.ssh_test_error = None;
                        }
                        Err(e) => {
                            info!("SSH test connection failed: {}", e);
                            this.ssh_test_status = TestStatus::Failed;
                            this.ssh_test_error = Some(e);
                        }
                    }
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn browse_ssh_key(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let this = cx.entity().clone();

        let start_dir = dirs::home_dir().map(|h| h.join(".ssh")).unwrap_or_default();

        let task = cx.background_executor().spawn(async move {
            let dialog = rfd::FileDialog::new()
                .set_title("Select SSH Private Key")
                .set_directory(&start_dir);

            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let path = task.await;

            if let Some(path) = path {
                cx.update(|cx| {
                    this.update(cx, |this, cx| {
                        this.pending_ssh_key_path = Some(path.to_string_lossy().to_string());
                        cx.notify();
                    });
                })
                .ok();
            }
        })
        .detach();
    }
}
