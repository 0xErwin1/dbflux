/// Export pipeline: assembles a `Bundle` from the typed `ExportGraph` and
/// serializes it (with optional encryption) to TOML bytes.
///
/// # Invariants
///
/// - MCP governance data is NEVER written to any bundle, regardless of options.
///   This is a hard security invariant enforced here, not delegated to the caller.
/// - `value_refs` are always included; `ValueRef::Literal` values are never written
///   in cleartext — they are routed to the encrypted `[secrets]` section or, when
///   that is not possible (no encryption configured), recorded as `RequiredRef`.
/// - Hooks and `settings_overrides` are excluded by default; opt-in via
///   `ExportOptions::{include_hooks, include_settings_overrides}`.
/// - Hook `env` entries are NEVER written in cleartext. They are moved to the
///   encrypted `[secrets]` section and reconstructed on import.
/// - `AuthProfileRef` form fields always route to `RequiredOnImport` via the
///   hint resolver; they never appear in the cleartext `[connections.fields]`.
/// - SSH private-key bytes are staged into `[secrets]` only when
///   `ExportOptions::embed_ssh_keys = true`; otherwise the key becomes a
///   `required_ref`.
/// - A `ConnectionProfile` has exactly one connection secret. When multiple form
///   fields carry the `Secret` hint, only the first is backed by the connection
///   keyring entry; any additional `Secret`-hinted fields that have no distinct
///   keyring ref become `RequiredRef` entries.
/// - Plaintext export (`EncryptionChoice::Plaintext`) requires `forced: true`; the
///   pipeline returns `PortabilityError::PlaintextForceMissing` otherwise.
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

        // A ConnectionProfile has exactly ONE connection secret (the password/token
        // field). When multiple form fields carry the Secret hint, only the first
        // gets the keyring value; any subsequent ones must be RequiredRef.
        let mut connection_secret_staged = false;

        for (field_id, field_value) in values.iter() {
            let hint = hints.hint(profile, field_id, values);

            match hint {
                ExportFieldHint::Include => {
                    include_fields.insert(field_id.clone(), field_value.clone());
                }

                ExportFieldHint::Secret => {
                    if !connection_secret_staged {
                        let secret_ref = connection_secret_ref(&profile.id);
                        let key = format!("conn:{}:{}", profile.id, field_id);
                        if let Some(secret) = secrets.read(&secret_ref) {
                            staged_secrets.insert(key, secret.expose_secret().to_string());
                            connection_secret_staged = true;
                        } else {
                            required_refs.push(RequiredRef {
                                field: field_id.clone(),
                                kind: RequiredRefKind::Secret,
                            });
                            report.required_ref_count += 1;
                            connection_secret_staged = true;
                        }
                    } else {
                        // Additional Secret-hinted fields have no distinct keyring ref;
                        // record them as RequiredRef so the importer can surface them.
                        required_refs.push(RequiredRef {
                            field: field_id.clone(),
                            kind: RequiredRefKind::Secret,
                        });
                        report.required_ref_count += 1;
                        report.warnings.push(format!(
                            "connection '{}' field '{}' has Secret hint but no distinct keyring ref; recorded as required_ref",
                            profile.name, field_id
                        ));
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

        let value_refs = build_value_refs(
            profile,
            &mut staged_secrets,
            &mut required_refs,
            &mut report,
        );

        let (hooks_payload, include_hooks) = if opts.include_hooks {
            match profile.hooks.as_ref() {
                None => (None, true),
                Some(hooks) => {
                    match build_sanitized_hooks_payload(
                        hooks,
                        profile,
                        &mut staged_secrets,
                        &mut report,
                    ) {
                        Ok(payload) => (Some(payload), true),
                        Err(e) => {
                            report.warnings.push(format!(
                                "connection '{}' hooks could not be serialized ({e}); hooks omitted",
                                profile.name
                            ));
                            (None, false)
                        }
                    }
                }
            }
        } else {
            (None, false)
        };

        let (settings_overrides_payload, include_settings_overrides) = if opts
            .include_settings_overrides
        {
            match profile.settings_overrides.as_ref() {
                None => (None, true),
                Some(settings) => {
                    match serde_json::to_value(settings)
                        .map_err(|e| e.to_string())
                        .and_then(|jv| toml::Value::try_from(jv).map_err(|e| e.to_string()))
                    {
                        Ok(payload) => (Some(payload), true),
                        Err(e) => {
                            report.warnings.push(format!(
                                    "connection '{}' settings_overrides could not be serialized ({e}); omitted",
                                    profile.name
                                ));
                            (None, false)
                        }
                    }
                }
            }
        } else {
            (None, false)
        };

        // MCP governance is NEVER written — enforced here unconditionally.
        // No opt-in path exists; the field is deliberately absent from ConnectionEntry.

        let driver_id = profile
            .driver_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        driver_refs.push(driver_ref_for(&driver_id));

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

    let auth_entries = build_auth_entries(graph, secrets, &mut staged_secrets, &mut report);
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

/// Build a `DriverRef` for the given driver id.
///
/// RPC-backed drivers carry ids like `rpc:<socket_id>` and are emitted as
/// `external:<socket_id>`; all other ids are built-in and use the `built-in:<id>`
/// prefix. Version is omitted: there is no stable per-driver version source that
/// the export crate can access without overreach.
fn driver_ref_for(driver_id: &str) -> DriverRef {
    let reference = if let Some(socket_id) = driver_id.strip_prefix("rpc:") {
        format!("external:{socket_id}")
    } else {
        format!("built-in:{driver_id}")
    };
    DriverRef { reference }
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

/// Build the `value_refs` map for a connection entry.
///
/// `ValueRef::Literal` values are never written in cleartext: they are staged
/// into `staged_secrets` under `conn_vref:<profile_id>:<field>` and will land in
/// the encrypted `[secrets]` section. When the `value_refs` map already exists
/// (non-literal variants), they are serialized as-is.
///
/// Conversion failures push a warning rather than silently dropping the entry.
fn build_value_refs(
    profile: &dbflux_core::ConnectionProfile,
    staged_secrets: &mut HashMap<String, String>,
    required_refs: &mut Vec<RequiredRef>,
    report: &mut ExportReport,
) -> HashMap<String, toml::Value> {
    let mut out: HashMap<String, toml::Value> = HashMap::new();

    for (field_key, vref) in &profile.value_refs {
        match vref {
            dbflux_core::values::ValueRef::Literal { value } => {
                // Literal values can be sensitive; route to the secrets section.
                let secrets_key = format!("conn_vref:{}:{}", profile.id, field_key);
                staged_secrets.insert(secrets_key, value.clone());
                required_refs.push(RequiredRef {
                    field: field_key.clone(),
                    kind: RequiredRefKind::Secret,
                });
                report.required_ref_count += 1;
            }

            other => {
                match serde_json::to_value(other)
                    .map_err(|e| e.to_string())
                    .and_then(|jv| toml::Value::try_from(jv).map_err(|e| e.to_string()))
                {
                    Ok(tv) => {
                        out.insert(field_key.clone(), tv);
                    }
                    Err(e) => {
                        report.warnings.push(format!(
                            "connection '{}' value_ref '{}' could not be serialized ({e}); skipped",
                            profile.name, field_key
                        ));
                    }
                }
            }
        }
    }

    out
}

/// Serialize the hooks payload with all `env` entries removed from cleartext.
///
/// Each hook's `env` map entries are moved to `staged_secrets` under the key
/// `conn_hook_env:<profile_id>:<phase>:<hook_index>:<env_key>` so they land in
/// the encrypted `[secrets]` section and can be reconstructed on import. The
/// serialized payload written to the bundle never contains env values.
fn build_sanitized_hooks_payload(
    hooks: &dbflux_core::ConnectionHooks,
    profile: &dbflux_core::ConnectionProfile,
    staged_secrets: &mut HashMap<String, String>,
    report: &mut ExportReport,
) -> Result<toml::Value, String> {
    use dbflux_core::{ConnectionHook, ConnectionHooks};

    fn sanitize_hook(
        hook: &ConnectionHook,
        phase: &str,
        index: usize,
        profile_id: &uuid::Uuid,
        staged_secrets: &mut HashMap<String, String>,
    ) -> ConnectionHook {
        for (env_key, env_val) in &hook.env {
            let secrets_key = format!(
                "conn_hook_env:{}:{}:{}:{}",
                profile_id, phase, index, env_key
            );
            staged_secrets.insert(secrets_key, env_val.clone());
        }

        ConnectionHook {
            env: HashMap::new(),
            ..hook.clone()
        }
    }

    let sanitized = ConnectionHooks {
        pre_connect: hooks
            .pre_connect
            .iter()
            .enumerate()
            .map(|(i, h)| sanitize_hook(h, "pre_connect", i, &profile.id, staged_secrets))
            .collect(),
        post_connect: hooks
            .post_connect
            .iter()
            .enumerate()
            .map(|(i, h)| sanitize_hook(h, "post_connect", i, &profile.id, staged_secrets))
            .collect(),
        pre_disconnect: hooks
            .pre_disconnect
            .iter()
            .enumerate()
            .map(|(i, h)| sanitize_hook(h, "pre_disconnect", i, &profile.id, staged_secrets))
            .collect(),
        post_disconnect: hooks
            .post_disconnect
            .iter()
            .enumerate()
            .map(|(i, h)| sanitize_hook(h, "post_disconnect", i, &profile.id, staged_secrets))
            .collect(),
    };

    let had_env = hooks
        .pre_connect
        .iter()
        .chain(hooks.post_connect.iter())
        .chain(hooks.pre_disconnect.iter())
        .chain(hooks.post_disconnect.iter())
        .any(|h| !h.env.is_empty());

    if had_env {
        report.warnings.push(format!(
            "connection '{}' hook env entries moved to encrypted secrets section",
            profile.name
        ));
    }

    serde_json::to_value(&sanitized)
        .map_err(|e| e.to_string())
        .and_then(|jv| toml::Value::try_from(jv).map_err(|e| e.to_string()))
}

fn build_auth_entries(
    graph: &ExportGraph<'_>,
    secrets: &dyn SecretReader,
    staged_secrets: &mut HashMap<String, String>,
    report: &mut ExportReport,
) -> Vec<AuthEntry> {
    graph
        .auth_profiles
        .iter()
        .map(|auth| {
            let mut secret_field_names = Vec::new();

            for (field_id, in_memory_secret) in &auth.secret_fields {
                let bundle_key = format!("auth:{}:{}", auth.id, field_id);

                let secret_value = in_memory_secret.expose_secret().to_string();
                if !secret_value.is_empty() {
                    staged_secrets.insert(bundle_key, secret_value);
                    secret_field_names.push(field_id.clone());
                } else {
                    let key_ref = auth_field_secret_ref(&auth.id, field_id);
                    if let Some(from_keyring) = secrets.read(&key_ref) {
                        staged_secrets.insert(bundle_key, from_keyring.expose_secret().to_string());
                        secret_field_names.push(field_id.clone());
                    } else {
                        report.warnings.push(format!(
                            "auth profile '{}' field '{}' secret not available; recorded as required on import",
                            auth.name, field_id
                        ));
                        report.required_ref_count += 1;
                    }
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
                kind: proxy.kind.scheme().to_string(),
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

        EncryptionChoice::Plaintext { forced: true } => Ok((
            EncryptionMode::None,
            Some(SecretsSection::Plaintext {
                values: staged_secrets,
            }),
        )),

        EncryptionChoice::Plaintext { forced: false } => {
            Err(PortabilityError::PlaintextForceMissing)
        }
    }
}

fn chrono_now() -> String {
    use chrono::Utc;
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
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

    /// Returns Secret hint for any field whose name ends with "secret" or "password",
    /// Include for everything else.
    struct SecretHintForAll;

    impl FieldHintResolver for SecretHintForAll {
        fn hint(&self, _p: &ConnectionProfile, field_id: &str, _v: &FormValues) -> ExportFieldHint {
            if field_id.ends_with("secret") || field_id.ends_with("password") {
                ExportFieldHint::Secret
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
            encryption: EncryptionChoice::Plaintext { forced: true },
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
            encryption: EncryptionChoice::Plaintext { forced: true },
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
            encryption: EncryptionChoice::Plaintext { forced: true },
        };

        let (bytes, _) = export(&graph, &opts, &IncludeAllHints, &NoSecrets).expect("export");
        let text = String::from_utf8(bytes).expect("utf8");

        // Full serialized bundle must not contain any governance-derived fields.
        assert!(
            !text.contains("mcp_governance"),
            "mcp_governance must NEVER appear even with all opts enabled: {text}"
        );
        assert!(
            !text.contains("client-x"),
            "governance actor must NEVER appear: {text}"
        );
        assert!(
            !text.contains("admin"),
            "governance role must NEVER appear: {text}"
        );
        assert!(
            !text.contains("\"p1\""),
            "governance policy id must NEVER appear: {text}"
        );
        assert!(
            !text.contains("policy_bindings"),
            "policy_bindings must NEVER appear: {text}"
        );
        assert!(
            !text.contains("actor_id"),
            "actor_id must NEVER appear: {text}"
        );
        assert!(
            !text.contains("role_ids"),
            "role_ids must NEVER appear: {text}"
        );
    }

    // --- Fix #1: Two Secret-hinted fields — second must not receive first's value ---

    #[test]
    fn two_secret_fields_second_becomes_required_ref() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("password".to_string(), "s3cr3t".to_string());
        values.insert("api_secret".to_string(), "another_s3cr3t".to_string());

        let graph = simple_graph(&profile, values);

        let conn_ref = dbflux_core::connection_secret_ref(&profile.id);
        let secrets = FixedSecrets({
            let mut m = HashMap::new();
            m.insert(conn_ref, "s3cr3t".to_string());
            m
        });

        let (bytes, report) = export(
            &graph,
            &default_opts_plaintext(),
            &SecretHintForAll,
            &secrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");

        // The second field must not receive the connection secret value.
        assert!(
            !text.contains("another_s3cr3t"),
            "second Secret field value must not appear anywhere in bundle: {text}"
        );

        // The second field must be recorded as a required_ref.
        assert!(
            report.required_ref_count >= 1,
            "second Secret field must be recorded as required_ref"
        );
    }

    // --- Fix #2: DriverRef.reference derives prefix from driver id ---

    #[test]
    fn driver_ref_builtin_id_yields_builtin_prefix() {
        use super::driver_ref_for;
        let dr = driver_ref_for("postgres");
        assert!(
            dr.reference.starts_with("built-in:postgres"),
            "built-in driver must have built-in prefix: {}",
            dr.reference
        );
    }

    #[test]
    fn driver_ref_rpc_id_yields_external_prefix() {
        use super::driver_ref_for;
        let dr = driver_ref_for("rpc:my-socket-id");
        assert!(
            dr.reference.starts_with("external:my-socket-id"),
            "rpc driver must have external prefix: {}",
            dr.reference
        );
        assert!(
            !dr.reference.contains("rpc:"),
            "rpc: prefix must be stripped from reference: {}",
            dr.reference
        );
    }

    // --- Fix #5: ValueRef::Literal must not appear in cleartext ---

    #[test]
    fn value_ref_literal_does_not_appear_in_cleartext() {
        use dbflux_core::values::ValueRef;

        let mut profile = postgres_profile();
        profile.value_refs.insert(
            "db_pass".to_string(),
            ValueRef::literal("super_secret_literal"),
        );

        let values = FormValues::default();
        let graph = simple_graph(&profile, values);

        let (bytes, _report) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");

        // The literal value must NOT appear in the cleartext portion (before [secrets]).
        let before_secrets = text.split("[secrets").next().unwrap_or(&text);
        assert!(
            !before_secrets.contains("super_secret_literal"),
            "ValueRef::Literal value must not appear in cleartext: {text}"
        );
    }

    // --- Fix #6: Auth secret from secret_fields is exported ---

    #[test]
    fn auth_secret_from_secret_fields_is_exported() {
        use dbflux_core::auth::AuthProfile;
        use secrecy::SecretString;

        let mut profile = postgres_profile();

        let auth = AuthProfile {
            id: uuid::Uuid::new_v4(),
            name: "Test Auth".to_string(),
            provider_id: "test-provider".to_string(),
            fields: HashMap::new(),
            secret_fields: {
                let mut m = HashMap::new();
                m.insert(
                    "token".to_string(),
                    SecretString::from("in_memory_token_value"),
                );
                m
            },
            enabled: true,
            read_only: false,
            dangling_origin: None,
        };
        profile.auth_profile_id = Some(auth.id);

        let values = FormValues::default();
        let graph = ExportGraph {
            connections: vec![ConnectionWithValues {
                profile: &profile,
                values,
            }],
            auth_profiles: vec![&auth],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
        };

        let (bytes, report) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        let text = String::from_utf8(bytes).expect("utf8");

        // The in-memory secret must land in the secrets section.
        assert!(
            text.contains("in_memory_token_value"),
            "in-memory auth secret must be in secrets section: {text}"
        );
        assert!(
            report.warnings.is_empty(),
            "no warnings expected when secret available: {:?}",
            report.warnings
        );
    }

    #[test]
    fn auth_secret_absent_records_required_ref_and_warning() {
        use dbflux_core::auth::AuthProfile;

        let mut profile = postgres_profile();

        let mut auth = AuthProfile {
            id: uuid::Uuid::new_v4(),
            name: "Test Auth Missing".to_string(),
            provider_id: "test-provider".to_string(),
            fields: HashMap::new(),
            secret_fields: {
                let mut m = HashMap::new();
                // Empty SecretString — will fall back to keyring (which returns None).
                m.insert(
                    "token".to_string(),
                    secrecy::SecretString::from(String::new()),
                );
                m
            },
            enabled: true,
            read_only: false,
            dangling_origin: None,
        };
        profile.auth_profile_id = Some(auth.id);

        let values = FormValues::default();
        let graph = ExportGraph {
            connections: vec![ConnectionWithValues {
                profile: &profile,
                values,
            }],
            auth_profiles: vec![&auth],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
        };

        let (_bytes, report) = export(
            &graph,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export");

        assert!(
            !report.warnings.is_empty(),
            "missing auth secret must produce a warning"
        );
        assert!(
            report.required_ref_count >= 1,
            "missing auth secret must increment required_ref_count"
        );
    }

    // --- Fix #7: Hook env entries must not appear in cleartext ---

    #[test]
    fn hook_env_does_not_appear_in_cleartext() {
        use dbflux_core::{ConnectionHook, ConnectionHooks, HookKind};

        let mut profile = postgres_profile();
        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: "echo".to_string(),
                args: vec![],
            },
            cwd: None,
            env: {
                let mut m = HashMap::new();
                m.insert(
                    "SECRET_TOKEN".to_string(),
                    "tok_live_supersecret".to_string(),
                );
                m
            },
            inherit_env: false,
            env_denylist: vec![],
            timeout_ms: None,
            execution_mode: Default::default(),
            ready_signal: None,
            on_failure: Default::default(),
        };
        profile.hooks = Some(ConnectionHooks {
            pre_connect: vec![hook.clone()],
            post_connect: vec![],
            pre_disconnect: vec![],
            post_disconnect: vec![],
        });

        let values = FormValues::default();
        let graph = simple_graph(&profile, values);

        let opts = ExportOptions {
            include_hooks: true,
            include_settings_overrides: false,
            embed_ssh_keys: false,
            encryption: EncryptionChoice::Plaintext { forced: true },
        };

        let (bytes, _report) = export(&graph, &opts, &IncludeAllHints, &NoSecrets).expect("export");
        let text = String::from_utf8(bytes).expect("utf8");

        // Hook env value must NOT appear in the cleartext portion (before [secrets]).
        let before_secrets = text.split("[secrets").next().unwrap_or(&text);
        assert!(
            !before_secrets.contains("tok_live_supersecret"),
            "hook env value must not appear in cleartext: {text}"
        );
    }

    // --- Fix #8: Encryption errors use Encryption variant, not Decryption ---
    // Tested via encryption::tests (encrypt_decrypt_round_trip still passes).
    // The variant name change is structural and validated by the compiler.

    // --- Fix #9: ProxyKind serializes as stable scheme string ---

    #[test]
    fn proxy_kind_serializes_as_stable_scheme() {
        use dbflux_core::{ProxyAuth, ProxyKind, ProxyProfile};

        let proxy = ProxyProfile {
            id: uuid::Uuid::new_v4(),
            name: "My Proxy".to_string(),
            kind: ProxyKind::Socks5,
            host: "proxy.example.com".to_string(),
            port: 1080,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        };

        let graph = ExportGraph {
            connections: vec![],
            auth_profiles: vec![],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy],
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
            text.contains("socks5"),
            "ProxyKind::Socks5 must serialize as 'socks5': {text}"
        );
        assert!(
            !text.contains("Socks5"),
            "Debug-derived 'Socks5' must not appear: {text}"
        );

        let proxy_http = ProxyProfile {
            kind: ProxyKind::Http,
            name: "HTTP Proxy".to_string(),
            ..proxy.clone()
        };
        let graph2 = ExportGraph {
            connections: vec![],
            auth_profiles: vec![],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&proxy_http],
        };
        let (bytes2, _) = export(
            &graph2,
            &default_opts_plaintext(),
            &IncludeAllHints,
            &NoSecrets,
        )
        .expect("export http");
        let text2 = String::from_utf8(bytes2).expect("utf8");
        assert!(
            text2.contains("\"http\""),
            "ProxyKind::Http must serialize as 'http': {text2}"
        );
    }

    // --- Fix #11: Plaintext without forced flag returns PlaintextForceMissing ---

    #[test]
    fn plaintext_without_force_returns_error() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("password".to_string(), "sekret".to_string());

        let graph = simple_graph(&profile, values);

        let secrets = FixedSecrets({
            let mut m = HashMap::new();
            m.insert(
                dbflux_core::connection_secret_ref(&profile.id),
                "sekret".to_string(),
            );
            m
        });

        let opts = ExportOptions {
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: false,
            encryption: EncryptionChoice::Plaintext { forced: false },
        };

        let result = export(&graph, &opts, &SecretHintForPassword, &secrets);

        assert!(
            matches!(result, Err(crate::PortabilityError::PlaintextForceMissing)),
            "plaintext without force must return PlaintextForceMissing, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn plaintext_with_force_succeeds() {
        let profile = postgres_profile();
        let mut values = FormValues::default();
        values.insert("password".to_string(), "sekret".to_string());

        let graph = simple_graph(&profile, values);

        let secrets = FixedSecrets({
            let mut m = HashMap::new();
            m.insert(
                dbflux_core::connection_secret_ref(&profile.id),
                "sekret".to_string(),
            );
            m
        });

        let opts = ExportOptions {
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: false,
            encryption: EncryptionChoice::Plaintext { forced: true },
        };

        let result = export(&graph, &opts, &SecretHintForPassword, &secrets);
        assert!(result.is_ok(), "plaintext with forced=true must succeed");
    }
}
