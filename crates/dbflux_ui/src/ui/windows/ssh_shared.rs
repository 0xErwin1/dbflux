use std::path::PathBuf;

use dbflux_core::{SshAuthMethod, SshTunnelConfig};
use gpui::prelude::*;
use gpui::{Hsla, px};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum SshAuthSelection {
    PrivateKey,
    Password,
}

pub fn expand_path(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest);
    }
    PathBuf::from(path)
}

pub fn build_ssh_config(
    host: &str,
    port: &str,
    user: &str,
    auth_method: SshAuthSelection,
    key_path_str: &str,
) -> SshTunnelConfig {
    let parsed_port = port.parse().unwrap_or(22);

    let auth = match auth_method {
        SshAuthSelection::PrivateKey => {
            let key_path = if key_path_str.trim().is_empty() {
                None
            } else {
                Some(expand_path(key_path_str))
            };
            SshAuthMethod::PrivateKey { key_path }
        }
        SshAuthSelection::Password => SshAuthMethod::Password,
    };

    SshTunnelConfig {
        host: host.to_string(),
        port: parsed_port,
        user: user.to_string(),
        auth_method: auth,
    }
}

pub fn get_ssh_secret(
    auth_method: SshAuthSelection,
    passphrase: &str,
    password: &str,
) -> Option<String> {
    let secret = match auth_method {
        SshAuthSelection::PrivateKey => passphrase.to_string(),
        SshAuthSelection::Password => password.to_string(),
    };

    if secret.is_empty() {
        None
    } else {
        Some(secret)
    }
}

pub fn render_radio_button(selected: bool, primary: Hsla, border: Hsla) -> impl IntoElement {
    gpui::div()
        .w(px(16.0))
        .h(px(16.0))
        .rounded_full()
        .border_2()
        .border_color(if selected { primary } else { border })
        .when(selected, |d| {
            d.child(
                gpui::div()
                    .absolute()
                    .top(px(3.0))
                    .left(px(3.0))
                    .w(px(6.0))
                    .h(px(6.0))
                    .rounded_full()
                    .bg(primary),
            )
        })
}
