use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyKind {
    Http,
    Https,
    Socks5,
}

impl ProxyKind {
    pub fn scheme(&self) -> &'static str {
        match self {
            ProxyKind::Http => "http",
            ProxyKind::Https => "https",
            ProxyKind::Socks5 => "socks5",
        }
    }

    pub fn default_port(&self) -> u16 {
        match self {
            ProxyKind::Http | ProxyKind::Https => 8080,
            ProxyKind::Socks5 => 1080,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            ProxyKind::Http => "HTTP",
            ProxyKind::Https => "HTTPS",
            ProxyKind::Socks5 => "SOCKS5",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyAuth {
    #[default]
    None,
    Basic {
        username: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyProfile {
    pub id: Uuid,
    pub name: String,
    pub kind: ProxyKind,
    pub host: String,
    pub port: u16,

    #[serde(default)]
    pub auth: ProxyAuth,

    /// Comma-separated list of hosts/CIDRs to bypass the proxy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_proxy: Option<String>,

    /// Soft-disable the proxy without deleting the profile.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Whether to persist the password (for Basic auth) in the system keyring.
    #[serde(default)]
    pub save_secret: bool,
}

fn default_true() -> bool {
    true
}

impl ProxyProfile {
    pub fn new(name: impl Into<String>, kind: ProxyKind) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            kind,
            host: String::new(),
            port: kind.default_port(),
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    pub fn secret_ref(&self) -> String {
        crate::storage::secrets::proxy_secret_ref(&self.id)
    }

    /// Full proxy URL with credentials. Do not log — may contain a password.
    pub fn proxy_url(&self, password: Option<&str>) -> String {
        let scheme = self.kind.scheme();

        match &self.auth {
            ProxyAuth::None => {
                format!("{}://{}:{}", scheme, self.host, self.port)
            }
            ProxyAuth::Basic { username } => {
                let encoded_user = urlencoding::encode(username);
                match password {
                    Some(pass) => {
                        let encoded_pass = urlencoding::encode(pass);
                        format!(
                            "{}://{}:{}@{}:{}",
                            scheme, encoded_user, encoded_pass, self.host, self.port
                        )
                    }
                    None => {
                        format!("{}://{}@{}:{}", scheme, encoded_user, self.host, self.port)
                    }
                }
            }
        }
    }

    /// URL with credentials masked, safe for display/logging.
    pub fn display_url(&self) -> String {
        let scheme = self.kind.scheme();

        match &self.auth {
            ProxyAuth::None => {
                format!("{}://{}:{}", scheme, self.host, self.port)
            }
            ProxyAuth::Basic { username } => {
                format!("{}://{}:***@{}:{}", scheme, username, self.host, self.port)
            }
        }
    }
}

/// Checks whether `host` matches a comma-separated `no_proxy` pattern list
/// (curl/wget `NO_PROXY` semantics: `*`, exact, suffix with/without leading dot).
/// CIDR notation is not supported.
pub fn host_matches_no_proxy(host: &str, patterns: &str) -> bool {
    let host_lower = host.to_lowercase();

    for raw_pattern in patterns.split(',') {
        let pattern = raw_pattern.trim().to_lowercase();
        if pattern.is_empty() {
            continue;
        }

        if pattern == "*" {
            return true;
        }

        if host_lower == pattern {
            return true;
        }

        // ".example.com" matches "foo.example.com"
        if pattern.starts_with('.') && host_lower.ends_with(&pattern) {
            return true;
        }

        // "example.com" also matches "foo.example.com" (suffix with implied dot)
        if !pattern.starts_with('.') && host_lower.ends_with(&format!(".{}", pattern)) {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_proxy(host: &str, port: u16) -> ProxyProfile {
        ProxyProfile {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            kind: ProxyKind::Http,
            host: host.to_string(),
            port,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    fn basic_auth_proxy(host: &str, port: u16, username: &str) -> ProxyProfile {
        ProxyProfile {
            auth: ProxyAuth::Basic {
                username: username.to_string(),
            },
            ..http_proxy(host, port)
        }
    }

    #[test]
    fn proxy_url_no_auth() {
        let proxy = http_proxy("proxy.local", 8080);
        assert_eq!(proxy.proxy_url(None), "http://proxy.local:8080");
    }

    #[test]
    fn proxy_url_basic_no_password() {
        let proxy = basic_auth_proxy("proxy.local", 8080, "user");
        assert_eq!(proxy.proxy_url(None), "http://user@proxy.local:8080");
    }

    #[test]
    fn proxy_url_basic_with_password() {
        let proxy = basic_auth_proxy("proxy.local", 8080, "user");
        assert_eq!(
            proxy.proxy_url(Some("pass")),
            "http://user:pass@proxy.local:8080"
        );
    }

    #[test]
    fn proxy_url_special_chars_encoded() {
        let proxy = basic_auth_proxy("proxy.local", 8080, "a@b");
        assert_eq!(
            proxy.proxy_url(Some("c:d/e")),
            "http://a%40b:c%3Ad%2Fe@proxy.local:8080"
        );
    }

    #[test]
    fn proxy_url_socks5_scheme() {
        let proxy = ProxyProfile {
            kind: ProxyKind::Socks5,
            ..http_proxy("proxy.local", 1080)
        };
        assert_eq!(proxy.proxy_url(None), "socks5://proxy.local:1080");
    }

    #[test]
    fn proxy_url_https_scheme() {
        let proxy = ProxyProfile {
            kind: ProxyKind::Https,
            ..http_proxy("proxy.local", 8080)
        };
        assert_eq!(proxy.proxy_url(None), "https://proxy.local:8080");
    }

    #[test]
    fn display_url_masks_password() {
        let proxy = basic_auth_proxy("proxy.local", 8080, "user");
        assert_eq!(proxy.display_url(), "http://user:***@proxy.local:8080");
    }

    #[test]
    fn display_url_no_auth() {
        let proxy = http_proxy("proxy.local", 8080);
        assert_eq!(proxy.display_url(), "http://proxy.local:8080");
    }

    #[test]
    fn kind_default_port() {
        assert_eq!(ProxyKind::Http.default_port(), 8080);
        assert_eq!(ProxyKind::Https.default_port(), 8080);
        assert_eq!(ProxyKind::Socks5.default_port(), 1080);
    }

    #[test]
    fn kind_scheme() {
        assert_eq!(ProxyKind::Http.scheme(), "http");
        assert_eq!(ProxyKind::Https.scheme(), "https");
        assert_eq!(ProxyKind::Socks5.scheme(), "socks5");
    }

    #[test]
    fn kind_label() {
        assert_eq!(ProxyKind::Http.label(), "HTTP");
        assert_eq!(ProxyKind::Https.label(), "HTTPS");
        assert_eq!(ProxyKind::Socks5.label(), "SOCKS5");
    }

    #[test]
    fn serde_roundtrip() {
        let proxy = ProxyProfile {
            no_proxy: Some("localhost,127.0.0.1".to_string()),
            save_secret: true,
            ..basic_auth_proxy("proxy.local", 3128, "admin")
        };

        let json = serde_json::to_string(&proxy).unwrap();
        let restored: ProxyProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.id, proxy.id);
        assert_eq!(restored.name, proxy.name);
        assert_eq!(restored.kind, proxy.kind);
        assert_eq!(restored.host, proxy.host);
        assert_eq!(restored.port, proxy.port);
        assert_eq!(restored.auth, proxy.auth);
        assert_eq!(restored.no_proxy, proxy.no_proxy);
        assert_eq!(restored.enabled, proxy.enabled);
        assert_eq!(restored.save_secret, proxy.save_secret);
    }

    #[test]
    fn serde_backward_compat() {
        let json = r#"{
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "name": "Old Proxy",
            "kind": "Http",
            "host": "proxy.old",
            "port": 8080
        }"#;

        let proxy: ProxyProfile = serde_json::from_str(json).unwrap();
        assert_eq!(proxy.auth, ProxyAuth::None);
        assert_eq!(proxy.no_proxy, None);
        assert!(proxy.enabled);
        assert!(!proxy.save_secret);
    }

    #[test]
    fn secret_ref_format() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let proxy = ProxyProfile {
            id,
            ..http_proxy("proxy.local", 8080)
        };
        assert_eq!(
            proxy.secret_ref(),
            "dbflux:proxy:550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn new_creates_with_defaults() {
        let proxy = ProxyProfile::new("My Proxy", ProxyKind::Socks5);
        assert_eq!(proxy.name, "My Proxy");
        assert_eq!(proxy.kind, ProxyKind::Socks5);
        assert_eq!(proxy.port, 1080);
        assert!(proxy.enabled);
        assert_eq!(proxy.auth, ProxyAuth::None);
        assert!(proxy.host.is_empty());
        assert_eq!(proxy.no_proxy, None);
        assert!(!proxy.save_secret);
    }

    // --- host_matches_no_proxy tests ---

    #[test]
    fn no_proxy_exact_match() {
        assert!(host_matches_no_proxy("db.local", "db.local"));
    }

    #[test]
    fn no_proxy_case_insensitive() {
        assert!(host_matches_no_proxy("DB.Local", "db.local"));
        assert!(host_matches_no_proxy("db.local", "DB.LOCAL"));
    }

    #[test]
    fn no_proxy_wildcard() {
        assert!(host_matches_no_proxy("anything.com", "*"));
    }

    #[test]
    fn no_proxy_suffix_with_leading_dot() {
        assert!(host_matches_no_proxy("foo.example.com", ".example.com"));
        assert!(!host_matches_no_proxy("example.com", ".example.com"));
    }

    #[test]
    fn no_proxy_suffix_without_leading_dot() {
        assert!(host_matches_no_proxy("foo.example.com", "example.com"));
        assert!(host_matches_no_proxy("example.com", "example.com"));
        assert!(!host_matches_no_proxy("notexample.com", "example.com"));
    }

    #[test]
    fn no_proxy_comma_separated() {
        assert!(host_matches_no_proxy(
            "db.local",
            "foo.com, db.local, bar.org"
        ));
        assert!(!host_matches_no_proxy(
            "other.com",
            "foo.com, db.local, bar.org"
        ));
    }

    #[test]
    fn no_proxy_empty_patterns() {
        assert!(!host_matches_no_proxy("db.local", ""));
        assert!(!host_matches_no_proxy("db.local", " , , "));
    }

    #[test]
    fn no_proxy_localhost() {
        assert!(host_matches_no_proxy("localhost", "localhost,127.0.0.1"));
        assert!(host_matches_no_proxy("127.0.0.1", "localhost,127.0.0.1"));
        assert!(!host_matches_no_proxy("192.168.1.1", "localhost,127.0.0.1"));
    }

    #[test]
    fn no_proxy_ipv6_loopback() {
        assert!(host_matches_no_proxy("::1", "::1"));
        assert!(host_matches_no_proxy("::1", "localhost,::1,127.0.0.1"));
        assert!(!host_matches_no_proxy("::2", "::1"));
    }
}
