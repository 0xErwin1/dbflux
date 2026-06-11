/// Export pipeline: assembles a `Bundle` from the typed `ExportGraph` and
/// serializes it (with optional encryption) to TOML bytes.
///
/// # Invariants
///
/// - MCP governance data is NEVER written to any bundle, regardless of options.
///   This is a hard security invariant enforced here, not delegated to the caller.
/// - `value_refs` are always included (non-secret, informational SSM/env paths).
/// - Hooks and `settings_overrides` are excluded by default; opt-in via
///   `ExportOptions::{include_hooks, include_settings_overrides}`.
/// - `AuthProfileRef` form fields always route to `RequiredOnImport` via the
///   hint resolver; they never appear in the cleartext `[connections.fields]`.
/// - SSH private-key bytes are staged into `[secrets]` only when
///   `ExportOptions::embed_ssh_keys = true`; otherwise the key becomes a
///   `required_ref`.
use std::collections::HashMap;

use dbflux_core::{
    ExportFieldHint, ProxyAuth, SshAuthMethod, auth_field_secret_ref, connection_secret_ref,
    proxy_secret_ref, ssh_tunnel_secret_ref,
};
use secrecy::ExposeSecret;

use crate::{
    AwsRef, EncryptionChoice, ExportGraph, ExportOptions, ExportReport, FieldHintResolver,
    PortabilityError, SecretReader,
    bundle::{
        AccessEntry, AuthEntry, AuthRef, AuthRefKind, Bundle, BundleMeta, CURRENT_FORMAT_VERSION,
        ConnectionEntry, DriverRef, EncryptionMode, ProxyEntry, RequiredRef, RequiredRefKind,
        SecretsSection, SshAuthMethodKind, SshEntry,
    },
};

/// Export the connection graph to serialized (and optionally encrypted) TOML bytes.
///
/// Returns the bundle bytes and an `ExportReport` with any non-fatal warnings.
pub fn export(
    graph: &ExportGraph<'_>,
    opts: &ExportOptions,
    hints: &dyn FieldHintResolver,
    secrets: &dyn SecretReader,
) -> Result<(Vec<u8>, ExportReport), PortabilityError> {
    let mut report = ExportReport::default();
    let mut staged_secrets: HashMap<String, String> = HashMap::new();

    let mut connection_entries: Vec<ConnectionEntry> = Vec::new();
    let mut driver_refs: Vec<DriverRef> = Vec::new();

    for conn_with_values in &graph.connections {
        let profile = conn_with_values.profile;
        let values = &conn_with_values.values;

        let mut include_fields: HashMap<String, String> = HashMap::new();
        let mut local_path_fields: HashMap<String, String> = HashMap::new();
        let mut required_refs: Vec<RequiredRef> = Vec::new();

        for (field_id, field_value) in values.iter() {
            let hint = hints.hint(profile, field_id, values);

            match hint {
                ExportFieldHint::Include => {
                    include_fields.insert(field_id.clone(), field_value.clone());
                }

                ExportFieldHint::Secret => {
                    let secret_ref = connection_secret_ref(&profile.id);
                    let key = format!("conn:{}:{}", profile.id, field_id);
                    if let Some(secret) = secrets.read(&secret_ref) {
                        staged_secrets.insert(key, secret.expose_secret().to_string());
                    } else {
                        required_refs.push(RequiredRef {
                            field: field_id.clone(),
                            kind: RequiredRefKind::Secret,
                        });
                        report.required_ref_count += 1;
                    }
                }

                ExportFieldHint::LocalPath => {
                    local_path_fields.insert(field_id.clone(), field_value.clone());
                    report.warnings.push(format!(
                        "connection '{}' field '{}' is a local path and may not be portable on the target",
                        profile.name, field_id
                    ));
                }

                ExportFieldHint::RequiredOnImport => {
                    required_refs.push(RequiredRef {
                        field: field_id.clone(),
                        kind: RequiredRefKind::AuthProfile,
                    });
                    report.required_ref_count += 1;
                }
            }
        }

        let auth_ref = resolve_auth_ref(profile, &graph.aws_references);
        let auth_profile_local_id = if auth_ref.is_none() {
            profile.auth_profile_id.map(|id| id.to_string())
        } else {
            None
        };

        let access = build_access_entry(profile);

        let value_refs = build_value_refs(profile);

        let (hooks_payload, include_hooks) = if opts.include_hooks {
            let payload = profile.hooks.as_ref().and_then(|h| {
                serde_json::to_value(h)
                    .ok()
                    .and_then(|v| toml::Value::try_from(v).ok())
            });
            (payload, true)
        } else {
            (None, false)
        };

        let (settings_overrides_payload, include_settings_overrides) =
            if opts.include_settings_overrides {
                let payload = profile.settings_overrides.as_ref().and_then(|s| {
                    serde_json::to_value(s)
                        .ok()
                        .and_then(|v| toml::Value::try_from(v).ok())
                });
                (payload, true)
            } else {
                (None, false)
            };

        // MCP governance is NEVER written — enforced here unconditionally.
        // No opt-in path exists; the field is deliberately absent from ConnectionEntry.

        let driver_id = profile
            .driver_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        driver_refs.push(DriverRef {
            reference: format!("built-in:{}:unknown", driver_id),
        });

        connection_entries.push(ConnectionEntry {
            local_id: profile.id.to_string(),
            name: profile.name.clone(),
            driver_id,
            fields: include_fields,
            local_path_fields,
            required_refs,
            auth_ref,
            auth_profile_local_id,
            access,
            value_refs,
            include_hooks,
            include_settings_overrides,
            hooks_payload,
            settings_overrides_payload,
        });
    }

    let auth_entries = build_auth_entries(graph, secrets, &mut staged_secrets);
    let ssh_entries = build_ssh_entries(graph, opts, secrets, &mut staged_secrets, &mut report);
    let proxy_entries = build_proxy_entries(graph, secrets, &mut staged_secrets, &mut report);

    driver_refs.dedup_by(|a, b| a.reference == b.reference);

    let (encryption_mode, secrets_section) = build_secrets_section(staged_secrets, opts)?;

    let bundle = Bundle {
        bundle: BundleMeta {
            format_version: CURRENT_FORMAT_VERSION,
            created_at: chrono_now(),
            dbflux_version: env!("CARGO_PKG_VERSION").to_string(),
            encryption: encryption_mode,
        },
        drivers: driver_refs,
        connections: connection_entries,
        auth_profiles: auth_entries,
        ssh_tunnels: ssh_entries,
        proxies: proxy_entries,
        secrets: secrets_section,
    };

    let toml_bytes = toml::to_string_pretty(&bundle)
        .map_err(PortabilityError::Serialize)?
        .into_bytes();

    Ok((toml_bytes, report))
}

fn resolve_auth_ref(
    profile: &dbflux_core::ConnectionProfile,
    aws_refs: &[AwsRef],
) -> Option<AuthRef> {
    if aws_refs.is_empty() {
        return None;
    }

    profile.auth_profile_id.and_then(|auth_id| {
        use dbflux_core::auth::aws_profile_uuid;

        aws_refs
            .iter()
            .find(|r| aws_profile_uuid(&r.provider_id, &r.name) == auth_id)
            .map(|r| AuthRef {
                kind: AuthRefKind::AwsReference,
                provider_id: r.provider_id.clone(),
                name: r.name.clone(),
            })
    })
}

fn build_access_entry(profile: &dbflux_core::ConnectionProfile) -> Option<AccessEntry> {
    use dbflux_core::access::AccessKind;

    profile.access_kind.as_ref().and_then(|ak| match ak {
        AccessKind::Direct => None,
        AccessKind::Ssh {
            ssh_tunnel_profile_id,
        } => Some(AccessEntry::Ssh {
            ssh_local_id: ssh_tunnel_profile_id.to_string(),
        }),
        AccessKind::Proxy { proxy_profile_id } => Some(AccessEntry::Proxy {
            proxy_local_id: proxy_profile_id.to_string(),
        }),
        AccessKind::Managed { provider, params } => Some(AccessEntry::Managed {
            provider: provider.clone(),
            params: params.clone(),
        }),
    })
}

fn build_value_refs(profile: &dbflux_core::ConnectionProfile) -> HashMap<String, toml::Value> {
    profile
        .value_refs
        .iter()
        .filter_map(|(k, v)| {
            serde_json::to_value(v)
                .ok()
                .and_then(|jv| toml::Value::try_from(jv).ok())
                .map(|tv| (k.clone(), tv))
        })
        .collect()
}

fn build_auth_entries(
    graph: &ExportGraph<'_>,
    secrets: &dyn SecretReader,
    staged_secrets: &mut HashMap<String, String>,
) -> Vec<AuthEntry> {
    graph
        .auth_profiles
        .iter()
        .map(|auth| {
            let mut secret_field_names = Vec::new();

            for field_id in auth.secret_fields.keys() {
                let key_ref = auth_field_secret_ref(&auth.id, field_id);
                let bundle_key = format!("auth:{}:{}", auth.id, field_id);

                if let Some(secret) = secrets.read(&key_ref) {
                    staged_secrets.insert(bundle_key, secret.expose_secret().to_string());
                    secret_field_names.push(field_id.clone());
                }
            }

            AuthEntry {
                local_id: auth.id.to_string(),
                name: auth.name.clone(),
                provider_id: auth.provider_id.clone(),
                enabled: auth.enabled,
                fields: auth.fields.clone(),
                secret_field_names,
            }
        })
        .collect()
}

fn build_ssh_entries(
    graph: &ExportGraph<'_>,
    opts: &ExportOptions,
    secrets: &dyn SecretReader,
    staged_secrets: &mut HashMap<String, String>,
    report: &mut ExportReport,
) -> Vec<SshEntry> {
    graph
        .ssh_tunnels
        .iter()
        .map(|ssh| {
            let (auth_method, key_embedded) = match &ssh.config.auth_method {
                SshAuthMethod::Password => {
                    let secret_ref = ssh_tunnel_secret_ref(&ssh.id);
                    let bundle_key = format!("ssh_tunnel:{}:password", ssh.id);
                    if let Some(secret) = secrets.read(&secret_ref) {
                        staged_secrets.insert(bundle_key, secret.expose_secret().to_string());
                    }
                    (SshAuthMethodKind::Password, false)
                }

                SshAuthMethod::PrivateKey { .. } => {
                    if opts.embed_ssh_keys {
                        let secret_ref = ssh_tunnel_secret_ref(&ssh.id);
                        let bundle_key = format!("ssh_tunnel:{}:private_key", ssh.id);
                        if let Some(secret) = secrets.read(&secret_ref) {
                            staged_secrets.insert(bundle_key, secret.expose_secret().to_string());
                            (SshAuthMethodKind::PrivateKey, true)
                        } else {
                            report.warnings.push(format!(
                                "SSH tunnel '{}' key bytes not available; recorded as required_ref",
                                ssh.name
                            ));
                            (SshAuthMethodKind::PrivateKey, false)
                        }
                    } else {
                        (SshAuthMethodKind::PrivateKey, false)
                    }
                }
            };

            SshEntry {
                local_id: ssh.id.to_string(),
                name: ssh.name.clone(),
                host: ssh.config.host.clone(),
                port: ssh.config.port,
                user: ssh.config.user.clone(),
                auth_method,
                key_embedded,
            }
        })
        .collect()
}

fn build_proxy_entries(
    graph: &ExportGraph<'_>,
    secrets: &dyn SecretReader,
    staged_secrets: &mut HashMap<String, String>,
    report: &mut ExportReport,
) -> Vec<ProxyEntry> {
    graph
        .proxies
        .iter()
        .map(|proxy| {
            let (username, has_secret) = match &proxy.auth {
                ProxyAuth::None => (None, false),
                ProxyAuth::Basic { username } => {
                    let secret_ref = proxy_secret_ref(&proxy.id);
                    let bundle_key = format!("proxy:{}:password", proxy.id);
                    let has_secret = if let Some(secret) = secrets.read(&secret_ref) {
                        staged_secrets.insert(bundle_key, secret.expose_secret().to_string());
                        true
                    } else {
                        report.warnings.push(format!(
                            "proxy '{}' credential not available; recorded as warning",
                            proxy.name
                        ));
                        false
                    };
                    (Some(username.clone()), has_secret)
                }
            };

            ProxyEntry {
                local_id: proxy.id.to_string(),
                name: proxy.name.clone(),
                kind: format!("{:?}", proxy.kind).to_lowercase(),
                host: proxy.host.clone(),
                port: proxy.port,
                username,
                no_proxy: proxy.no_proxy.clone(),
                has_secret,
            }
        })
        .collect()
}

fn build_secrets_section(
    staged_secrets: HashMap<String, String>,
    opts: &ExportOptions,
) -> Result<(EncryptionMode, Option<SecretsSection>), PortabilityError> {
    if staged_secrets.is_empty() {
        return Ok((EncryptionMode::None, None));
    }

    match &opts.encryption {
        EncryptionChoice::Passphrase(passphrase) => {
            #[cfg(feature = "encryption")]
            {
                let ciphertext = crate::encryption::encrypt_secrets(&staged_secrets, passphrase)?;
                Ok((
                    EncryptionMode::AgePassphrase,
                    Some(SecretsSection::Encrypted { ciphertext }),
                ))
            }

            #[cfg(not(feature = "encryption"))]
            {
                let _ = passphrase;
                Err(PortabilityError::EncryptionUnavailable)
            }
        }

        EncryptionChoice::Plaintext => Ok((
            EncryptionMode::None,
            Some(SecretsSection::Plaintext {
                values: staged_secrets,
            }),
        )),
    }
}

fn chrono_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let (y, mo, d, h, mi, s) = unix_to_datetime(secs);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, mi, s)
}

fn unix_to_datetime(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let s = secs % 60;
    let total_min = secs / 60;
    let mi = total_min % 60;
    let total_hours = total_min / 60;
    let h = total_hours % 24;
    let total_days = total_hours / 24;

    let mut year = 1970u64;
    let mut remaining = total_days;

    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }

    let months = [
        31u64,
        if is_leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1u64;
    for days in months {
        if remaining < days {
            break;
        }
        remaining -= days;
        month += 1;
    }

    (year, month, remaining + 1, h, mi, s)
}

fn is_leap(y: u64) -> bool {
    y.is_multiple_of(400) || (y.is_multiple_of(4) && !y.is_multiple_of(100))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dbflux_core::{
        ConnectionHooks, ConnectionMcpGovernance, ConnectionMcpPolicyBinding, ConnectionProfile,
        DbConfig, ExportFieldHint, FormValues, SshTunnelConfig, SshTunnelProfile,
        ssh_tunnel_secret_ref,
    };
    use secrecy::SecretString;

    use crate::{
        AwsRef, ConnectionWithValues, EncryptionChoice, ExportGraph, ExportOptions,
        FieldHintResolver, SecretReader, export::export,
    };

    struct IncludeAllHints;

    impl FieldHintResolver for IncludeAllHints {
        fn hint(&self, _p: &ConnectionProfile, _f: &str, _v: &FormValues) -> ExportFieldHint {
            ExportFieldHint::Include
        }
    }

    struct SecretHintForPassword;

    impl FieldHintResolver for SecretHintForPassword {
        fn hint(&self, _p: &ConnectionProfile, field_id: &str, _v: &FormValues) -> ExportFieldHint {
            if field_id == "password" {
                ExportFieldHint::Secret
            } else {
                ExportFieldHint::Include
            }
        }
    }

    struct RequiredOnImportForProfile;

    impl FieldHintResolver for RequiredOnImportForProfile {
        fn hint(&self, _p: &ConnectionProfile, field_id: &str, _v: &FormValues) -> ExportFieldHint {
            if field_id == "profile" {
                ExportFieldHint::RequiredOnImport
            } else {
                ExportFieldHint::Include
            }
        }
    }

    struct LocalPathForCert;

    impl FieldHintResolver for LocalPathForCert {
        fn hint(&self, _p: &ConnectionProfile, field_id: &str, _v: &FormValues) -> ExportFieldHint {
            if field_id == "ssl_cert" {
                ExportFieldHint::LocalPath
            } else {
                ExportFieldHint::Include
            }
        }
    }

    struct NoSecrets;

    impl SecretReader for NoSecrets {
        fn read(&self, _: &str) -> Option<SecretString> {
            None
        }
    }

    struct FixedSecrets(HashMap<String, String>);

    impl SecretReader for FixedSecrets {
        fn read(&self, secret_ref: &str) -> Option<SecretString> {
            self.0
                .get(secret_ref)
                .map(|v| SecretString::from(v.clone()))
        }
    }

    fn postgres_profile() -> ConnectionProfile {
        ConnectionProfile::new(
            "Test PG",
            DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "db.internal".to_string(),
                port: 5432,
                user: "app".to_string(),
                database: "app".to_string(),
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        )
    }

    fn default_opts_plaintext() -> ExportOptions {
        ExportOptions {
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: false,
            encryption: EncryptionChoice::Plaintext,
        }
    }

    fn simple_graph<'a>(profile: &'a ConnectionProfile, values: FormValues) -> ExportGraph<'a> {
        ExportGraph {
            connections: vec![ConnectionWithValues { profile, values }],
            auth_profiles: vec![],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
        }
    }

    #[test]
    fn include_fields_appear_in_connection_fields() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("host".to_string(), "db.internal".to_string());

        let graph = simple_graph(&profile, values);

        let (bytes, report) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");
        assert!(
            text.contains("db.internal"),
            "include field must appear in bundle"
        );
        assert!(report.warnings.is_empty(), "no warnings expected");
    }

    #[test]
    fn secret_field_absent_from_cleartext_connections_fields_section() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("host".to_string(), "db.internal".to_string());
        values.insert("password".to_string(), "sekret".to_string());

        let graph = simple_graph(&profile, values);
        let secrets = FixedSecrets({
            let mut m = HashMap::new();
            m.insert(format!("dbflux:conn:{}", profile.id), "sekret".to_string());
            m
        });

        let (bytes, _) = export(
            &graph,
            &default_opts_plaintext(),
            &SecretHintForPassword,
            &secrets,
        )
        .expect("export");
        let text = String::from_utf8(bytes).expect("utf8");

        // The secret value must NOT appear in the [connections.fields] cleartext section.
        // In plaintext mode it is allowed in [secrets.values], which is the designated section.
        let connections_fields_section = text.split("[secrets").next().unwrap_or("");
        assert!(
            !connections_fields_section.contains("sekret"),
            "secret value must not appear in the cleartext [connections] section: {text}"
        );

        // It must appear in the secrets section.
        assert!(
            text.contains("sekret"),
            "secret value must be present in the secrets section (plaintext mode): {text}"
        );
        assert!(
            text.contains("[secrets"),
            "secrets section must be present: {text}"
        );
    }

    #[test]
    fn required_on_import_field_absent_and_recorded() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("host".to_string(), "db.internal".to_string());
        values.insert("profile".to_string(), "my-aws-profile".to_string());

        let graph = simple_graph(&profile, values);

        let (bytes, report) = export(
            &graph,
            &default_opts_plaintext(),
            &RequiredOnImportForProfile,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");

        assert!(
            !text.contains("my-aws-profile"),
            "RequiredOnImport value must not appear in bundle: {text}"
        );
        assert!(
            text.contains("required_refs"),
            "required_refs must be present: {text}"
        );
        assert_eq!(report.required_ref_count, 1);
    }

    #[test]
    fn local_path_field_included_with_warning() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("ssl_cert".to_string(), "/etc/ssl/certs/ca.pem".to_string());

        let graph = simple_graph(&profile, values);

        let (bytes, report) = export(
            &graph,
            &default_opts_plaintext(),
            &LocalPathForCert,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");
        assert!(
            text.contains("/etc/ssl/certs/ca.pem"),
            "local path must appear in bundle"
        );
        assert!(
            !report.warnings.is_empty(),
            "a portability warning must be recorded"
        );
    }

    #[test]
    fn mcp_governance_absent_from_bundle() {
        let mut profile = postgres_profile();
        profile.mcp_governance = Some(ConnectionMcpGovernance {
            enabled: true,
            policy_bindings: vec![],
        });

        let mut values = FormValues::default();
        values.insert("host".to_string(), "db.internal".to_string());

        let graph = simple_graph(&profile, values);

        let (bytes, _) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");
        assert!(
            !text.contains("mcp_governance"),
            "mcp_governance must NEVER appear in any bundle: {text}"
        );
    }

    #[test]
    fn hooks_excluded_by_default() {
        let mut profile = postgres_profile();
        profile.hooks = Some(ConnectionHooks::default());

        let mut values = FormValues::default();
        values.insert("host".to_string(), "db.internal".to_string());

        let graph = simple_graph(&profile, values);

        let (bytes, _) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");
        assert!(
            !text.contains("hooks_payload"),
            "hooks must be excluded by default: {text}"
        );
        assert!(
            text.contains("include_hooks = false"),
            "include_hooks must be false by default"
        );
    }

    #[test]
    fn value_refs_included_by_default() {
        use dbflux_core::values::ValueRef;

        let mut profile = postgres_profile();
        profile
            .value_refs
            .insert("password".to_string(), ValueRef::env("DB_PASS"));

        let mut values = FormValues::default();
        values.insert("host".to_string(), "db.internal".to_string());

        let graph = simple_graph(&profile, values);

        let (bytes, _) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");
        assert!(
            text.contains("DB_PASS"),
            "value_refs must appear in bundle: {text}"
        );
    }

    #[test]
    fn aws_reference_recorded_as_auth_ref_not_cleartext() {
        let mut profile = postgres_profile();
        let aws_ref = AwsRef {
            provider_id: "aws-sso".to_string(),
            name: "My AWS SSO".to_string(),
        };
        profile.auth_profile_id = Some(dbflux_core::auth::aws_profile_uuid(
            &aws_ref.provider_id,
            &aws_ref.name,
        ));

        let values = FormValues::default();

        let graph = ExportGraph {
            connections: vec![ConnectionWithValues {
                profile: &profile,
                values,
            }],
            auth_profiles: vec![],
            aws_references: vec![aws_ref],
            ssh_tunnels: vec![],
            proxies: vec![],
        };

        let (bytes, _) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");
        assert!(
            text.contains("aws_reference"),
            "auth_ref kind must be aws_reference: {text}"
        );
        assert!(
            text.contains("My AWS SSO"),
            "AWS profile name must appear: {text}"
        );
        assert!(text.contains("aws-sso"), "provider_id must appear: {text}");
    }

    #[test]
    fn ssh_key_embedded_in_secrets_when_opted_in() {
        let profile = postgres_profile();
        let ssh = SshTunnelProfile::new(
            "Bastion",
            SshTunnelConfig {
                host: "bastion.example.com".to_string(),
                port: 22,
                user: "ec2-user".to_string(),
                auth_method: dbflux_core::SshAuthMethod::PrivateKey { key_path: None },
            },
        );

        let secrets = FixedSecrets({
            let mut m = HashMap::new();
            let key_ref = ssh_tunnel_secret_ref(&ssh.id);
            use base64::Engine as _;
            m.insert(
                key_ref,
                base64::engine::general_purpose::STANDARD.encode("PRIVATE_KEY_DATA"),
            );
            m
        });

        let values = FormValues::default();
        let graph = ExportGraph {
            connections: vec![ConnectionWithValues {
                profile: &profile,
                values,
            }],
            auth_profiles: vec![],
            aws_references: vec![],
            ssh_tunnels: vec![&ssh],
            proxies: vec![],
        };

        let opts = ExportOptions {
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: true,
            encryption: EncryptionChoice::Plaintext,
        };

        let (bytes, _) = export(&graph, &opts, &IncludeAllHints, &secrets).expect("export");
        let text = String::from_utf8(bytes).expect("utf8");

        assert!(
            text.contains("key_embedded = true"),
            "key_embedded must be true when opted in: {text}"
        );
    }

    #[test]
    fn governance_never_in_bundle_with_full_opts() {
        let mut profile = postgres_profile();
        profile.mcp_governance = Some(ConnectionMcpGovernance {
            enabled: true,
            policy_bindings: vec![ConnectionMcpPolicyBinding {
                actor_id: "client-x".to_string(),
                role_ids: vec!["admin".to_string()],
                policy_ids: vec!["p1".to_string()],
            }],
        });

        let values = FormValues::default();
        let graph = simple_graph(&profile, values);

        let opts = ExportOptions {
            include_hooks: true,
            include_settings_overrides: true,
            embed_ssh_keys: true,
            encryption: EncryptionChoice::Plaintext,
        };

        let (bytes, _) = export(&graph, &opts, &IncludeAllHints, &NoSecrets).expect("export");
        let text = String::from_utf8(bytes).expect("utf8");

        assert!(
            !text.contains("mcp_governance"),
            "mcp_governance must NEVER appear even with all opts enabled: {text}"
        );
        assert!(
            !text.contains("client-x"),
            "governance actor must NEVER appear: {text}"
        );
    }
}
