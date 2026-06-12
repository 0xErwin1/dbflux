/// Conflict-identity predicates for import.
///
/// Each predicate accepts a bundle entry's identity fields and a `DestSnapshot`,
/// and returns the UUID of the first matching destination entity, or `None` when
/// no match is found.
///
/// Identity predicates use content tuples, not display names:
/// - Auth profiles: `(provider_id, name)` — mirrors the deterministic `aws_profile_uuid` derivation.
/// - SSH tunnels: `(host, port, user)` — the actual endpoint matters; a tunnel renamed on the
///   destination still refers to the same bastion.
/// - Proxies: `(kind, host, port)` — the proxy endpoint identity; username excluded so a
///   credential change is not treated as a distinct proxy.
///
/// Predicates SUGGEST conflict candidates; they never auto-apply. The user chooses
/// Reuse / CreateNew / MapTo for each detected conflict.
use uuid::Uuid;

use crate::DestSnapshot;

/// Return the UUID of the first destination auth profile matching `(provider_id, name)`.
pub fn auth_conflict(provider_id: &str, name: &str, dest: &DestSnapshot<'_>) -> Option<Uuid> {
    dest.auth_profiles
        .iter()
        .find(|a| a.provider_id == provider_id && a.name == name)
        .map(|a| a.id)
}

/// Return the UUID of the first destination SSH tunnel matching `(host, port, user)`.
pub fn ssh_conflict(host: &str, port: u16, user: &str, dest: &DestSnapshot<'_>) -> Option<Uuid> {
    dest.ssh_tunnels
        .iter()
        .find(|s| s.config.host == host && s.config.port == port && s.config.user == user)
        .map(|s| s.id)
}

/// Return the UUID of the first destination proxy matching `(kind, host, port)`.
pub fn proxy_conflict(kind: &str, host: &str, port: u16, dest: &DestSnapshot<'_>) -> Option<Uuid> {
    dest.proxies
        .iter()
        .find(|p| p.kind.scheme() == kind && p.host == host && p.port == port)
        .map(|p| p.id)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use dbflux_core::{
        AuthProfile, ProxyAuth, ProxyKind, ProxyProfile, SshAuthMethod, SshTunnelConfig,
        SshTunnelProfile,
    };
    use uuid::Uuid;

    use crate::DestSnapshot;

    use super::*;

    fn make_auth(provider_id: &str, name: &str) -> AuthProfile {
        AuthProfile {
            id: Uuid::new_v4(),
            name: name.to_string(),
            provider_id: provider_id.to_string(),
            fields: Default::default(),
            secret_fields: Default::default(),
            enabled: true,
            read_only: false,
            dangling_origin: None,
        }
    }

    fn make_ssh(host: &str, port: u16, user: &str) -> SshTunnelProfile {
        SshTunnelProfile::new(
            "Tunnel",
            SshTunnelConfig {
                host: host.to_string(),
                port,
                user: user.to_string(),
                auth_method: SshAuthMethod::Password,
            },
        )
    }

    fn make_proxy(kind: ProxyKind, host: &str, port: u16) -> ProxyProfile {
        ProxyProfile {
            id: Uuid::new_v4(),
            name: "proxy".to_string(),
            kind,
            host: host.to_string(),
            port,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    // --- auth_conflict ---

    #[test]
    fn auth_conflict_exact_match_returns_id() {
        let auth = make_auth("aws-sso", "My SSO");
        let expected_id = auth.id;

        let dest = DestSnapshot {
            auth_profiles: vec![&auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let result = auth_conflict("aws-sso", "My SSO", &dest);
        assert_eq!(result, Some(expected_id));
    }

    #[test]
    fn auth_conflict_same_name_different_provider_no_match() {
        let auth = make_auth("aws-sso", "My SSO");

        let dest = DestSnapshot {
            auth_profiles: vec![&auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let result = auth_conflict("other-provider", "My SSO", &dest);
        assert!(result.is_none());
    }

    #[test]
    fn auth_conflict_same_provider_different_name_no_match() {
        let auth = make_auth("aws-sso", "My SSO");

        let dest = DestSnapshot {
            auth_profiles: vec![&auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let result = auth_conflict("aws-sso", "Different Name", &dest);
        assert!(result.is_none());
    }

    #[test]
    fn auth_conflict_empty_dest_returns_none() {
        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };
        assert!(auth_conflict("aws-sso", "My SSO", &dest).is_none());
    }

    #[test]
    fn auth_conflict_multiple_candidates_returns_first() {
        let auth1 = make_auth("aws-sso", "My SSO");
        let auth2 = make_auth("aws-sso", "My SSO");
        let first_id = auth1.id;

        let dest = DestSnapshot {
            auth_profiles: vec![&auth1, &auth2],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let result = auth_conflict("aws-sso", "My SSO", &dest);
        assert_eq!(result, Some(first_id));
    }

    // --- ssh_conflict ---

    #[test]
    fn ssh_conflict_exact_match_returns_id() {
        let ssh = make_ssh("bastion.example.com", 22, "ec2-user");
        let expected_id = ssh.id;

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&ssh],
            proxies: vec![],
            connections: vec![],
        };

        let result = ssh_conflict("bastion.example.com", 22, "ec2-user", &dest);
        assert_eq!(result, Some(expected_id));
    }

    #[test]
    fn ssh_conflict_name_differs_but_tuple_matches() {
        // Name is NOT part of the identity — a renamed tunnel still matches.
        let mut ssh = make_ssh("bastion.example.com", 22, "ec2-user");
        ssh.name = "Renamed Bastion".to_string();
        let expected_id = ssh.id;

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&ssh],
            proxies: vec![],
            connections: vec![],
        };

        let result = ssh_conflict("bastion.example.com", 22, "ec2-user", &dest);
        assert_eq!(result, Some(expected_id));
    }

    #[test]
    fn ssh_conflict_different_user_no_match() {
        let ssh = make_ssh("bastion.example.com", 22, "ubuntu");

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&ssh],
            proxies: vec![],
            connections: vec![],
        };

        let result = ssh_conflict("bastion.example.com", 22, "ec2-user", &dest);
        assert!(result.is_none());
    }

    #[test]
    fn ssh_conflict_different_port_no_match() {
        let ssh = make_ssh("bastion.example.com", 2222, "ec2-user");

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&ssh],
            proxies: vec![],
            connections: vec![],
        };

        let result = ssh_conflict("bastion.example.com", 22, "ec2-user", &dest);
        assert!(result.is_none());
    }

    #[test]
    fn ssh_conflict_empty_dest_returns_none() {
        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };
        assert!(ssh_conflict("bastion.example.com", 22, "ec2-user", &dest).is_none());
    }

    #[test]
    fn ssh_conflict_multiple_candidates_returns_first() {
        let ssh1 = make_ssh("bastion.example.com", 22, "ec2-user");
        let ssh2 = make_ssh("bastion.example.com", 22, "ec2-user");
        let first_id = ssh1.id;

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&ssh1, &ssh2],
            proxies: vec![],
            connections: vec![],
        };

        let result = ssh_conflict("bastion.example.com", 22, "ec2-user", &dest);
        assert_eq!(result, Some(first_id));
    }

    // --- proxy_conflict ---

    #[test]
    fn proxy_conflict_exact_match_returns_id() {
        let proxy = make_proxy(ProxyKind::Http, "proxy.corp.com", 8080);
        let expected_id = proxy.id;

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy],
            connections: vec![],
        };

        let result = proxy_conflict("http", "proxy.corp.com", 8080, &dest);
        assert_eq!(result, Some(expected_id));
    }

    #[test]
    fn proxy_conflict_name_differs_but_tuple_matches() {
        // Name is NOT part of the identity.
        let mut proxy = make_proxy(ProxyKind::Socks5, "proxy.corp.com", 1080);
        proxy.name = "Renamed Proxy".to_string();
        let expected_id = proxy.id;

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy],
            connections: vec![],
        };

        let result = proxy_conflict("socks5", "proxy.corp.com", 1080, &dest);
        assert_eq!(result, Some(expected_id));
    }

    #[test]
    fn proxy_conflict_different_kind_no_match() {
        let proxy = make_proxy(ProxyKind::Http, "proxy.corp.com", 8080);

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy],
            connections: vec![],
        };

        let result = proxy_conflict("socks5", "proxy.corp.com", 8080, &dest);
        assert!(result.is_none());
    }

    #[test]
    fn proxy_conflict_different_port_no_match() {
        let proxy = make_proxy(ProxyKind::Http, "proxy.corp.com", 8080);

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy],
            connections: vec![],
        };

        let result = proxy_conflict("http", "proxy.corp.com", 9090, &dest);
        assert!(result.is_none());
    }

    #[test]
    fn proxy_conflict_empty_dest_returns_none() {
        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };
        assert!(proxy_conflict("http", "proxy.corp.com", 8080, &dest).is_none());
    }

    #[test]
    fn proxy_conflict_multiple_candidates_returns_first() {
        let proxy1 = make_proxy(ProxyKind::Http, "proxy.corp.com", 8080);
        let proxy2 = make_proxy(ProxyKind::Http, "proxy.corp.com", 8080);
        let first_id = proxy1.id;

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy1, &proxy2],
            connections: vec![],
        };

        let result = proxy_conflict("http", "proxy.corp.com", 8080, &dest);
        assert_eq!(result, Some(first_id));
    }
}
