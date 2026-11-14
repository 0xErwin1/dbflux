use std::path::PathBuf;

use dbflux_core::{SshAuthMethod, SshTunnelConfig};

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
    build_ssh_config_with_alias(host, port, user, auth_method, key_path_str, None)
}

/// [`build_ssh_config`] for a form that carries an SSH config host
/// selection. While an alias is active the alias is the only source of the
/// target (A7): the manual `host` and `user` are saved empty and `port`
/// keeps 22, so no stored snapshot of the resolved values can be dialed.
pub fn build_ssh_config_with_alias(
    host: &str,
    port: &str,
    user: &str,
    auth_method: SshAuthSelection,
    key_path_str: &str,
    ssh_config_host: Option<&str>,
) -> SshTunnelConfig {
    // While an alias is active it is the only source of the target (A7):
    // the manual values are discarded, so neither the save path nor the
    // test path can dial a stale snapshot of the resolved host.
    let (host, parsed_port, user) = match ssh_config_host {
        Some(_) => (String::new(), 22, String::new()),
        None => (
            host.to_string(),
            port.parse().unwrap_or(22),
            user.to_string(),
        ),
    };

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
        host,
        port: parsed_port,
        user,
        auth_method: auth,
        ssh_config_host: ssh_config_host.map(str::to_string),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selecting_an_alias_stores_it_with_empty_manual_fields_and_default_port() {
        let config = build_ssh_config_with_alias(
            "manual.example.com",
            "2200",
            "manualuser",
            SshAuthSelection::PrivateKey,
            "",
            Some("alpha"),
        );

        assert_eq!(config.ssh_config_host.as_deref(), Some("alpha"));
        assert_eq!(
            config.host, "",
            "the alias is the only source of the target (A7)"
        );
        assert_eq!(config.user, "");
        assert_eq!(config.port, 22);
    }

    #[test]
    fn clearing_the_alias_stores_none_and_the_manual_fields_are_the_target_again() {
        let config = build_ssh_config_with_alias(
            "manual.example.com",
            "2200",
            "manualuser",
            SshAuthSelection::PrivateKey,
            "",
            None,
        );

        assert_eq!(config.ssh_config_host, None);
        assert_eq!(config.host, "manual.example.com");
        assert_eq!(config.user, "manualuser");
        assert_eq!(config.port, 2200);
    }
}
