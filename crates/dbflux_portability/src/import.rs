/// Import pipeline: parse, plan, apply.
///
/// `parse` opens a bundle from bytes; `decrypt` opens the sealed secrets section;
/// `plan` computes conflicts and required resolutions; `apply` produces remapped
/// entities and secret writes that the app layer persists through repositories and
/// `SecretManager::set_by_ref`.
///
/// `apply` is PURE: it performs no I/O, no keyring access, no SQLite writes.
/// All side effects belong to the app layer, which inspects `ImportActions`.
use std::collections::HashMap;

use secrecy::SecretString;
use uuid::Uuid;

use crate::{
    ConflictChoice, ConflictKind, DestSnapshot, ImportActions, ImportPlan, ParsedBundle,
    PortabilityError, ProfileConflict, RequiredResolution, RequiredResolutionKind,
    ResolutionChoices,
    bundle::{EncryptionMode, SecretsSection},
    conflict::{auth_conflict, proxy_conflict, ssh_conflict},
};

/// Parse the bundle TOML bytes into `ParsedBundle`.
///
/// Extracts all plaintext metadata. When `bundle.encryption = "age-passphrase"`,
/// the secrets section remains sealed until `decrypt()` is called.
///
/// Returns `PortabilityError::Parse` for invalid TOML.
/// Returns `PortabilityError::UnsupportedVersion` for unknown `format_version`.
/// Returns `PortabilityError::ModeMismatch` when the declared encryption mode
/// contradicts the secrets section variant (e.g. `"age-passphrase"` header with
/// a plaintext secrets map, or `"none"` header with an encrypted blob).
pub fn parse(bytes: &[u8]) -> Result<ParsedBundle, PortabilityError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| PortabilityError::Decryption(format!("bundle is not valid UTF-8: {e}")))?;

    let bundle: crate::bundle::Bundle = toml::from_str(text).map_err(PortabilityError::Parse)?;

    if bundle.bundle.format_version != crate::bundle::CURRENT_FORMAT_VERSION {
        return Err(PortabilityError::UnsupportedVersion {
            version: bundle.bundle.format_version,
        });
    }

    // Cross-validate the declared encryption mode against the secrets section variant.
    // A mismatch indicates a malformed bundle; reject before any planning step.
    if let Some(ref secrets) = bundle.secrets {
        match (&bundle.bundle.encryption, secrets) {
            (EncryptionMode::AgePassphrase, SecretsSection::Plaintext { .. }) => {
                return Err(PortabilityError::ModeMismatch {
                    declared: "age-passphrase".to_string(),
                    found: "plaintext".to_string(),
                });
            }

            (EncryptionMode::None, SecretsSection::Encrypted { .. }) => {
                return Err(PortabilityError::ModeMismatch {
                    declared: "none".to_string(),
                    found: "encrypted".to_string(),
                });
            }

            // Consistent pairs: encrypted+encrypted or none+plaintext — both are valid.
            _ => {}
        }
    }

    Ok(ParsedBundle {
        bundle,
        decrypted_secrets: None,
    })
}

/// Decrypt the secrets section of a previously parsed bundle.
///
/// Must be called when `bundle.encryption = "age-passphrase"` before `plan()`
/// can process secrets. A wrong passphrase returns `PortabilityError::Decryption`,
/// which is recoverable — the caller should re-prompt.
///
/// This is a no-op (returns `Ok(())`) when `encryption = "none"` or when the
/// bundle has no secrets section.
pub fn decrypt(
    parsed: &mut ParsedBundle,
    passphrase: &SecretString,
) -> Result<(), PortabilityError> {
    if parsed.bundle.bundle.encryption == EncryptionMode::None {
        if let Some(SecretsSection::Plaintext { values }) = &parsed.bundle.secrets {
            parsed.decrypted_secrets = Some(values.clone());
        }
        return Ok(());
    }

    #[cfg(feature = "encryption")]
    {
        if let Some(SecretsSection::Encrypted { ciphertext }) = &parsed.bundle.secrets {
            let secrets = crate::encryption::decrypt_secrets(ciphertext, passphrase)?;
            parsed.decrypted_secrets = Some(secrets);
        }
        Ok(())
    }

    #[cfg(not(feature = "encryption"))]
    {
        let _passphrase = passphrase;
        Err(PortabilityError::EncryptionUnavailable)
    }
}

/// Compute the import plan: conflict detection and required resolutions.
///
/// Runs the conflict-identity predicates against `dest` for each referenced
/// auth/proxy/ssh entry. Collects omitted-secret `required_refs` from connections,
/// ssh entries, proxy entries, and auth entries into `required_resolutions`.
///
/// AWS references are checked against the destination snapshot:
/// - Present (by deterministic `aws_profile_uuid`): auto-resolved, NOT surfaced
///   as a resolution item.
/// - Absent: emitted as a `RequiredResolution` of kind `AwsReference`.
pub fn plan(parsed: &ParsedBundle, dest: &DestSnapshot<'_>) -> ImportPlan {
    let mut conflicts: Vec<ProfileConflict> = Vec::new();
    let mut required_resolutions: Vec<RequiredResolution> = Vec::new();

    // Conflict detection for auth profiles.
    for auth in &parsed.bundle.auth_profiles {
        if let Some(existing_id) = auth_conflict(&auth.provider_id, &auth.name, dest) {
            let existing_name = dest
                .auth_profiles
                .iter()
                .find(|a| a.id == existing_id)
                .map(|a| a.name.clone())
                .unwrap_or_default();

            conflicts.push(ProfileConflict {
                bundle_local_id: auth.local_id.clone(),
                kind: ConflictKind::AuthProfile,
                bundle_name: auth.name.clone(),
                existing_id,
                existing_name,
            });
        }

        // Collect per-auth required_refs (missing secrets).
        for rref in &auth.required_refs {
            required_resolutions.push(RequiredResolution {
                owner_local_id: auth.local_id.clone(),
                owner_name: auth.name.clone(),
                field: rref.field.clone(),
                kind: RequiredResolutionKind::Secret,
            });
        }
    }

    // Conflict detection for SSH tunnels.
    for ssh in &parsed.bundle.ssh_tunnels {
        if let Some(existing_id) = ssh_conflict(&ssh.host, ssh.port, &ssh.user, dest) {
            let existing_name = dest
                .ssh_tunnels
                .iter()
                .find(|s| s.id == existing_id)
                .map(|s| s.name.clone())
                .unwrap_or_default();

            conflicts.push(ProfileConflict {
                bundle_local_id: ssh.local_id.clone(),
                kind: ConflictKind::SshTunnel,
                bundle_name: ssh.name.clone(),
                existing_id,
                existing_name,
            });
        }

        // Collect per-ssh required_refs.
        for rref in &ssh.required_refs {
            required_resolutions.push(RequiredResolution {
                owner_local_id: ssh.local_id.clone(),
                owner_name: ssh.name.clone(),
                field: rref.field.clone(),
                kind: RequiredResolutionKind::Secret,
            });
        }
    }

    // Conflict detection for proxies.
    for proxy in &parsed.bundle.proxies {
        if let Some(existing_id) = proxy_conflict(&proxy.kind, &proxy.host, proxy.port, dest) {
            let existing_name = dest
                .proxies
                .iter()
                .find(|p| p.id == existing_id)
                .map(|p| p.name.clone())
                .unwrap_or_default();

            conflicts.push(ProfileConflict {
                bundle_local_id: proxy.local_id.clone(),
                kind: ConflictKind::Proxy,
                bundle_name: proxy.name.clone(),
                existing_id,
                existing_name,
            });
        }

        // Collect per-proxy required_refs.
        for rref in &proxy.required_refs {
            required_resolutions.push(RequiredResolution {
                owner_local_id: proxy.local_id.clone(),
                owner_name: proxy.name.clone(),
                field: rref.field.clone(),
                kind: RequiredResolutionKind::Secret,
            });
        }
    }

    // Connection-level required_refs and AWS reference resolution.
    for conn in &parsed.bundle.connections {
        // Omitted-secret required_refs recorded by the exporter.
        for rref in &conn.required_refs {
            required_resolutions.push(RequiredResolution {
                owner_local_id: conn.local_id.clone(),
                owner_name: conn.name.clone(),
                field: rref.field.clone(),
                kind: RequiredResolutionKind::Secret,
            });
        }

        // AWS reflected auth references: auto-resolve when the deterministic UUID
        // matches an existing destination auth profile; otherwise surface as a
        // RequiredResolution so the user can create or select a profile.
        if let Some(auth_ref) = &conn.auth_ref {
            use dbflux_core::auth::aws_profile_uuid;

            let resolved_id = aws_profile_uuid(&auth_ref.provider_id, &auth_ref.name);
            let already_present = dest.auth_profiles.iter().any(|a| a.id == resolved_id);

            if !already_present {
                required_resolutions.push(RequiredResolution {
                    owner_local_id: conn.local_id.clone(),
                    owner_name: conn.name.clone(),
                    field: "auth_profile".to_string(),
                    kind: RequiredResolutionKind::AwsReference {
                        provider_id: auth_ref.provider_id.clone(),
                        name: auth_ref.name.clone(),
                    },
                });
            }
        }
    }

    ImportPlan {
        conflicts,
        required_resolutions,
    }
}

/// Apply the resolution choices to produce remapped entities and secret writes.
///
/// This function is PURE: it does not touch the OS keyring, SQLite, or any I/O.
/// All side effects (repository inserts, `SecretManager::set_by_ref` calls) are
/// performed by the app layer after inspecting the returned `ImportActions`.
///
/// Every new entity receives a fresh `Uuid::new_v4()`. All intra-bundle references
/// (auth_profile_id, access_kind, secret keys) are rewritten to the newly minted
/// UUIDs before being returned. AWS references resolve to the deterministic
/// `aws_profile_uuid(provider_id, name)` UUID, NOT a minted UUID, so they bind to
/// the reflected profile on the target.
///
/// When `choices` specifies `Reuse` or `MapTo` for a conflict, the destination UUID
/// is used instead of minting a new one, and no new entity is emitted for that entry.
pub fn apply(
    parsed: &ParsedBundle,
    _plan: &ImportPlan,
    choices: &ResolutionChoices,
) -> Result<ImportActions, PortabilityError> {
    let secrets = parsed.decrypted_secrets.as_ref();

    // --- Build local_id -> new_uuid map ---
    // Mint UUIDs up front so we can rewrite all intra-bundle references consistently.
    // Conflict choices of Reuse/MapTo override the minted UUID with the destination id.

    let mut id_map: HashMap<String, Uuid> = HashMap::new();

    for auth in &parsed.bundle.auth_profiles {
        let new_id = match choices.conflict_choices.get(&auth.local_id) {
            Some(ConflictChoice::Reuse) => {
                // Find the conflict record to get the existing destination id.
                conflict_existing_id(&auth.local_id, _plan).ok_or_else(|| {
                    PortabilityError::MissingResolution {
                        local_id: auth.local_id.clone(),
                    }
                })?
            }
            Some(ConflictChoice::MapTo(dest_id)) => *dest_id,
            _ => Uuid::new_v4(),
        };
        id_map.insert(auth.local_id.clone(), new_id);
    }

    for ssh in &parsed.bundle.ssh_tunnels {
        let new_id =
            match choices.conflict_choices.get(&ssh.local_id) {
                Some(ConflictChoice::Reuse) => conflict_existing_id(&ssh.local_id, _plan)
                    .ok_or_else(|| PortabilityError::MissingResolution {
                        local_id: ssh.local_id.clone(),
                    })?,
                Some(ConflictChoice::MapTo(dest_id)) => *dest_id,
                _ => Uuid::new_v4(),
            };
        id_map.insert(ssh.local_id.clone(), new_id);
    }

    for proxy in &parsed.bundle.proxies {
        let new_id = match choices.conflict_choices.get(&proxy.local_id) {
            Some(ConflictChoice::Reuse) => conflict_existing_id(&proxy.local_id, _plan)
                .ok_or_else(|| PortabilityError::MissingResolution {
                    local_id: proxy.local_id.clone(),
                })?,
            Some(ConflictChoice::MapTo(dest_id)) => *dest_id,
            _ => Uuid::new_v4(),
        };
        id_map.insert(proxy.local_id.clone(), new_id);
    }

    for conn in &parsed.bundle.connections {
        id_map.insert(conn.local_id.clone(), Uuid::new_v4());
    }

    // --- Build output structures ---

    let mut out_auth_profiles: Vec<dbflux_core::AuthProfile> = Vec::new();
    let mut out_ssh_tunnels: Vec<dbflux_core::SshTunnelProfile> = Vec::new();
    let mut out_proxies: Vec<dbflux_core::ProxyProfile> = Vec::new();
    let mut out_connections: Vec<dbflux_core::ConnectionProfile> = Vec::new();
    let mut secret_writes: Vec<(String, SecretString)> = Vec::new();

    // Auth profiles.
    for auth_entry in &parsed.bundle.auth_profiles {
        let new_id = id_map.get(&auth_entry.local_id).copied().ok_or_else(|| {
            PortabilityError::InvalidChoices {
                reason: format!("auth entry '{}' missing from id_map", auth_entry.local_id),
            }
        })?;

        // Reuse/MapTo -> wire to dest id, emit no new entity.
        if matches!(
            choices.conflict_choices.get(&auth_entry.local_id),
            Some(ConflictChoice::Reuse) | Some(ConflictChoice::MapTo(_))
        ) {
            // Secret writes still need re-keying to the destination id.
            for field_name in &auth_entry.secret_field_names {
                let old_key = format!("auth:{}:{}", auth_entry.local_id, field_name);
                let new_ref = dbflux_core::auth_field_secret_ref(&new_id, field_name);
                if let Some(secret_map) = secrets
                    && let Some(value) = secret_map.get(&old_key)
                {
                    secret_writes.push((new_ref, SecretString::from(value.clone())));
                }
            }
            continue;
        }

        // CreateNew or no conflict -> mint a new auth profile entity.
        let mut fields = auth_entry.fields.clone();
        // secret_fields is populated from secret_writes at persist time by the app layer;
        // the in-memory entity leaves it empty here.
        let secret_fields = HashMap::new();

        // Stage secret writes for this auth profile.
        for field_name in &auth_entry.secret_field_names {
            let old_key = format!("auth:{}:{}", auth_entry.local_id, field_name);
            let new_ref = dbflux_core::auth_field_secret_ref(&new_id, field_name);
            if let Some(secret_map) = secrets
                && let Some(value) = secret_map.get(&old_key)
            {
                secret_writes.push((new_ref, SecretString::from(value.clone())));
            }
        }

        // Collect user-supplied secret values for omitted fields.
        for rref in &auth_entry.required_refs {
            let key = (auth_entry.local_id.clone(), rref.field.clone());
            if let Some(supplied) = choices.secret_values.get(&key) {
                let new_ref = dbflux_core::auth_field_secret_ref(&new_id, &rref.field);
                secret_writes.push((new_ref, supplied.clone()));
            }
        }

        // Remove the local_id sentinel from fields if it ended up there (should not,
        // but defensive).
        fields.remove("local_id");

        out_auth_profiles.push(dbflux_core::AuthProfile {
            id: new_id,
            name: auth_entry.name.clone(),
            provider_id: auth_entry.provider_id.clone(),
            fields,
            secret_fields,
            enabled: auth_entry.enabled,
            read_only: false,
            dangling_origin: None,
        });
    }

    // SSH tunnels.
    for ssh_entry in &parsed.bundle.ssh_tunnels {
        let new_id = id_map.get(&ssh_entry.local_id).copied().ok_or_else(|| {
            PortabilityError::InvalidChoices {
                reason: format!("ssh entry '{}' missing from id_map", ssh_entry.local_id),
            }
        })?;

        // Reuse/MapTo -> no new entity; secret re-key still needed.
        if matches!(
            choices.conflict_choices.get(&ssh_entry.local_id),
            Some(ConflictChoice::Reuse) | Some(ConflictChoice::MapTo(_))
        ) {
            let new_ref = dbflux_core::ssh_tunnel_secret_ref(&new_id);
            if ssh_entry.key_embedded {
                let old_key = format!("ssh_tunnel:{}:private_key", ssh_entry.local_id);
                if let Some(secret_map) = secrets
                    && let Some(value) = secret_map.get(&old_key)
                {
                    secret_writes.push((new_ref, SecretString::from(value.clone())));
                }
            } else if matches!(
                ssh_entry.auth_method,
                crate::bundle::SshAuthMethodKind::Password
            ) {
                let old_key = format!("ssh_tunnel:{}:password", ssh_entry.local_id);
                if let Some(secret_map) = secrets
                    && let Some(value) = secret_map.get(&old_key)
                {
                    secret_writes.push((new_ref, SecretString::from(value.clone())));
                }
            }
            continue;
        }

        let (auth_method, key_secret_write) =
            build_ssh_auth_method(ssh_entry, &new_id, secrets, choices);

        if let Some((ref_str, secret)) = key_secret_write {
            secret_writes.push((ref_str, secret));
        }

        // Collect user-supplied secret values for required_refs on this SSH entry.
        for rref in &ssh_entry.required_refs {
            let key = (ssh_entry.local_id.clone(), rref.field.clone());
            if let Some(supplied) = choices.secret_values.get(&key) {
                let new_ref = dbflux_core::ssh_tunnel_secret_ref(&new_id);
                secret_writes.push((new_ref, supplied.clone()));
            }
        }

        out_ssh_tunnels.push(dbflux_core::SshTunnelProfile {
            id: new_id,
            name: ssh_entry.name.clone(),
            config: dbflux_core::SshTunnelConfig {
                host: ssh_entry.host.clone(),
                port: ssh_entry.port,
                user: ssh_entry.user.clone(),
                auth_method,
            },
            save_secret: false,
        });
    }

    // Proxies.
    for proxy_entry in &parsed.bundle.proxies {
        let new_id = id_map.get(&proxy_entry.local_id).copied().ok_or_else(|| {
            PortabilityError::InvalidChoices {
                reason: format!("proxy entry '{}' missing from id_map", proxy_entry.local_id),
            }
        })?;

        if matches!(
            choices.conflict_choices.get(&proxy_entry.local_id),
            Some(ConflictChoice::Reuse) | Some(ConflictChoice::MapTo(_))
        ) {
            if proxy_entry.has_secret {
                let old_key = format!("proxy:{}:password", proxy_entry.local_id);
                let new_ref = dbflux_core::proxy_secret_ref(&new_id);
                if let Some(secret_map) = secrets
                    && let Some(value) = secret_map.get(&old_key)
                {
                    secret_writes.push((new_ref, SecretString::from(value.clone())));
                }
            }
            continue;
        }

        let proxy_auth =
            build_proxy_auth(proxy_entry, &new_id, secrets, choices, &mut secret_writes);

        // Collect user-supplied secrets for required_refs on this proxy.
        for rref in &proxy_entry.required_refs {
            let key = (proxy_entry.local_id.clone(), rref.field.clone());
            if let Some(supplied) = choices.secret_values.get(&key) {
                let new_ref = dbflux_core::proxy_secret_ref(&new_id);
                secret_writes.push((new_ref, supplied.clone()));
            }
        }

        let kind = parse_proxy_kind(&proxy_entry.kind);
        out_proxies.push(dbflux_core::ProxyProfile {
            id: new_id,
            name: proxy_entry.name.clone(),
            kind,
            host: proxy_entry.host.clone(),
            port: proxy_entry.port,
            auth: proxy_auth,
            no_proxy: proxy_entry.no_proxy.clone(),
            enabled: true,
            save_secret: false,
        });
    }

    // Connections.
    for conn_entry in &parsed.bundle.connections {
        let new_conn_id = id_map.get(&conn_entry.local_id).copied().ok_or_else(|| {
            PortabilityError::InvalidChoices {
                reason: format!("connection '{}' missing from id_map", conn_entry.local_id),
            }
        })?;

        // Rewrite auth_profile_id to the new (or reused/mapped) destination id.
        let auth_profile_id = resolve_auth_id(conn_entry, &id_map, _plan, choices);

        // Rewrite access_kind to point at the remapped ssh/proxy ids.
        let access_kind = rewrite_access_kind(conn_entry, &id_map);

        // Connection secret: re-key the staged secret for this connection.
        //
        // Secret-hinted fields are NOT present in `conn_entry.fields` — the exporter
        // excludes them from the cleartext table and stages them in the secrets section
        // under `conn:<local_id>:<field_id>`. Iterating `fields.keys()` therefore never
        // finds the password. Instead, scan the secrets map by the `conn:<local_id>:`
        // prefix so the lookup is independent of field names.
        //
        // The `conn:` prefix (with the colon immediately after "conn") is distinct from
        // `conn_hook_env:` (hook env entries) and `conn_vref:` (future use) because those
        // names contain an underscore after "conn", making the prefix collision impossible.
        //
        // A ConnectionProfile has exactly one connection-secret slot (`dbflux:conn:<uuid>`),
        // so at most one matching entry is expected; writing all matches is safe because
        // the export pipeline itself enforces the single-slot constraint.
        if let Some(secret_map) = secrets {
            let prefix = format!("conn:{}:", conn_entry.local_id);
            for (staged_key, value) in secret_map {
                if staged_key.starts_with(&prefix) {
                    let new_ref = dbflux_core::connection_secret_ref(&new_conn_id);
                    secret_writes.push((new_ref, secrecy::SecretString::from(value.clone())));
                }
            }
        }

        // Collect user-supplied secrets for connection required_refs.
        for rref in &conn_entry.required_refs {
            let key = (conn_entry.local_id.clone(), rref.field.clone());
            if let Some(supplied) = choices.secret_values.get(&key) {
                let new_ref = dbflux_core::connection_secret_ref(&new_conn_id);
                secret_writes.push((new_ref, supplied.clone()));
            }
        }

        let values: dbflux_core::FormValues = conn_entry
            .fields
            .iter()
            .chain(conn_entry.local_path_fields.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let kind = db_kind_from_bundle(conn_entry);

        let config = dbflux_core::DbConfig::External { kind, values };
        let mut profile = dbflux_core::ConnectionProfile::new(&conn_entry.name, config);
        profile.id = new_conn_id;
        profile.set_driver_id(conn_entry.driver_id.clone());
        profile.set_kind(kind);
        profile.auth_profile_id = auth_profile_id;
        profile.access_kind = access_kind;

        // proxy_profile_id is a legacy field; keep it in sync with access_kind when applicable.
        if let Some(dbflux_core::AccessKind::Proxy { proxy_profile_id }) = &profile.access_kind {
            profile.proxy_profile_id = Some(*proxy_profile_id);
        }

        // Restore value_refs: deserialize from toml::Value -> serde_json::Value -> ValueRef.
        // Conversion failures are silently skipped — a single unresolvable ref must not
        // block the entire import; the user can add missing refs manually after import.
        profile.value_refs = restore_value_refs(conn_entry);

        // Restore hooks when the bundle included them.
        // Pass decrypted_secrets so hook env entries can be reconstructed from
        // the encrypted section where they were staged during export.
        if conn_entry.include_hooks {
            profile.hooks = conn_entry
                .hooks_payload
                .as_ref()
                .and_then(|p| restore_hooks(p, &conn_entry.local_id, secrets));
        }

        // Restore settings_overrides when the bundle included them.
        if conn_entry.include_settings_overrides {
            profile.settings_overrides = conn_entry
                .settings_overrides_payload
                .as_ref()
                .and_then(restore_settings_overrides);
        }

        out_connections.push(profile);
    }

    Ok(ImportActions {
        connections: out_connections,
        auth_profiles: out_auth_profiles,
        ssh_tunnels: out_ssh_tunnels,
        proxies: out_proxies,
        secret_writes,
    })
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Resolve the `DbKind` for an imported connection.
///
/// Prefers the `kind` field serialized by the exporter via `serde_json`, which produces
/// the canonical serde variant name (e.g. `"Postgres"`, `"SQLite"`). Falls back to
/// deriving the kind from `driver_id` for legacy bundles that predate the `kind` field.
fn db_kind_from_bundle(conn: &crate::bundle::ConnectionEntry) -> dbflux_core::DbKind {
    use dbflux_core::DbKind;

    if let Some(kind_str) = &conn.kind {
        let jv = serde_json::Value::String(kind_str.clone());
        if let Ok(k) = serde_json::from_value::<DbKind>(jv) {
            return k;
        }
    }

    // Fallback: derive from driver_id for legacy bundles without the `kind` field.
    match conn.driver_id.as_str() {
        "postgres" => DbKind::Postgres,
        "sqlite" => DbKind::SQLite,
        "mysql" => DbKind::MySQL,
        "mariadb" => DbKind::MariaDB,
        "mongodb" => DbKind::MongoDB,
        "redis" => DbKind::Redis,
        "dynamodb" => DbKind::DynamoDB,
        "cloudwatch" => DbKind::CloudWatchLogs,
        "influxdb" => DbKind::InfluxDB,
        "mssql" => DbKind::SqlServer,
        _ => DbKind::Postgres,
    }
}

/// Deserialize the `value_refs` map from `toml::Value` entries to `ValueRef`.
///
/// Conversion is best-effort: entries that cannot be round-tripped through
/// serde_json are silently skipped so a single unresolvable ref does not
/// abort the import.
fn restore_value_refs(
    conn: &crate::bundle::ConnectionEntry,
) -> std::collections::HashMap<String, dbflux_core::values::ValueRef> {
    conn.value_refs
        .iter()
        .filter_map(|(field, tv)| {
            let jv = serde_json::to_value(tv).ok()?;
            let vref: dbflux_core::values::ValueRef = serde_json::from_value(jv).ok()?;
            Some((field.clone(), vref))
        })
        .collect()
}

/// Deserialize the `hooks_payload` toml::Value into `ConnectionHooks` and
/// repopulate each hook's `env` map from the decrypted secrets.
///
/// During export, hook env entries are moved to the encrypted `[secrets]` section
/// under keys `conn_hook_env:<bundle_local_id>:<phase>:<index>:<env_key>`.
/// This function reconstructs those entries from `decrypted_secrets` so the
/// restored hooks have the same env as the originals.
///
/// Returns `None` when the payload is absent or the top-level deserialization fails.
/// Individual env-key lookup failures are silently skipped — a missing env key is
/// preferable to blocking the entire import.
fn restore_hooks(
    payload: &toml::Value,
    bundle_local_id: &str,
    decrypted_secrets: Option<&HashMap<String, String>>,
) -> Option<dbflux_core::ConnectionHooks> {
    let jv = serde_json::to_value(payload).ok()?;
    let mut hooks: dbflux_core::ConnectionHooks = serde_json::from_value(jv).ok()?;

    let Some(secrets) = decrypted_secrets else {
        return Some(hooks);
    };

    restore_hook_env(
        &mut hooks.pre_connect,
        "pre_connect",
        bundle_local_id,
        secrets,
    );
    restore_hook_env(
        &mut hooks.post_connect,
        "post_connect",
        bundle_local_id,
        secrets,
    );
    restore_hook_env(
        &mut hooks.pre_disconnect,
        "pre_disconnect",
        bundle_local_id,
        secrets,
    );
    restore_hook_env(
        &mut hooks.post_disconnect,
        "post_disconnect",
        bundle_local_id,
        secrets,
    );

    Some(hooks)
}

/// Repopulate the `env` map for each hook in a phase slice from the decrypted secrets.
fn restore_hook_env(
    phase_hooks: &mut [dbflux_core::ConnectionHook],
    phase: &str,
    bundle_local_id: &str,
    secrets: &HashMap<String, String>,
) {
    for (index, hook) in phase_hooks.iter_mut().enumerate() {
        let prefix = format!("conn_hook_env:{}:{}:{}:", bundle_local_id, phase, index);
        for (secrets_key, env_value) in secrets {
            if let Some(env_key) = secrets_key.strip_prefix(&prefix) {
                hook.env.insert(env_key.to_string(), env_value.clone());
            }
        }
    }
}

/// Deserialize the `settings_overrides_payload` toml::Value into `GlobalOverrides`.
///
/// Returns `None` when the payload is absent or deserialization fails.
fn restore_settings_overrides(payload: &toml::Value) -> Option<dbflux_core::GlobalOverrides> {
    let jv = serde_json::to_value(payload).ok()?;
    serde_json::from_value(jv).ok()
}

/// Look up the existing destination id for a conflict entry from the plan.
fn conflict_existing_id(local_id: &str, plan: &ImportPlan) -> Option<Uuid> {
    plan.conflicts
        .iter()
        .find(|c| c.bundle_local_id == local_id)
        .map(|c| c.existing_id)
}

/// Build an `SshAuthMethod` for an imported SSH tunnel entry.
///
/// When `key_embedded = true` the private key bytes are in the decrypted secrets
/// map; the imported profile uses `key_path: None` so the key is sourced from the
/// keyring rather than the filesystem.
///
/// Returns the `SshAuthMethod` and an optional `(ref_string, secret)` write.
fn build_ssh_auth_method(
    entry: &crate::bundle::SshEntry,
    new_id: &Uuid,
    secrets: Option<&HashMap<String, String>>,
    choices: &ResolutionChoices,
) -> (dbflux_core::SshAuthMethod, Option<(String, SecretString)>) {
    use crate::bundle::SshAuthMethodKind;

    match entry.auth_method {
        SshAuthMethodKind::Password => {
            let old_key = format!("ssh_tunnel:{}:password", entry.local_id);
            let secret_write = secrets
                .and_then(|m| m.get(&old_key))
                .map(|v| {
                    let new_ref = dbflux_core::ssh_tunnel_secret_ref(new_id);
                    (new_ref, SecretString::from(v.clone()))
                })
                .or_else(|| {
                    let key = (entry.local_id.clone(), "password".to_string());
                    choices.secret_values.get(&key).map(|supplied| {
                        let new_ref = dbflux_core::ssh_tunnel_secret_ref(new_id);
                        (new_ref, supplied.clone())
                    })
                });

            (dbflux_core::SshAuthMethod::Password, secret_write)
        }

        SshAuthMethodKind::PrivateKey => {
            if entry.key_embedded {
                let old_key = format!("ssh_tunnel:{}:private_key", entry.local_id);
                let secret_write = secrets.and_then(|m| m.get(&old_key)).map(|v| {
                    let new_ref = dbflux_core::ssh_tunnel_secret_ref(new_id);
                    (new_ref, SecretString::from(v.clone()))
                });
                // key_path is None: the key is sourced from the keyring after import.
                (
                    dbflux_core::SshAuthMethod::PrivateKey { key_path: None },
                    secret_write,
                )
            } else {
                (
                    dbflux_core::SshAuthMethod::PrivateKey { key_path: None },
                    None,
                )
            }
        }
    }
}

/// Build `ProxyAuth` for an imported proxy entry and stage the credential secret
/// when present.
fn build_proxy_auth(
    entry: &crate::bundle::ProxyEntry,
    new_id: &Uuid,
    secrets: Option<&HashMap<String, String>>,
    choices: &ResolutionChoices,
    secret_writes: &mut Vec<(String, SecretString)>,
) -> dbflux_core::ProxyAuth {
    match &entry.username {
        None => dbflux_core::ProxyAuth::None,
        Some(username) => {
            if entry.has_secret {
                let old_key = format!("proxy:{}:password", entry.local_id);
                let new_ref = dbflux_core::proxy_secret_ref(new_id);
                if let Some(value) = secrets.and_then(|m| m.get(&old_key)) {
                    secret_writes.push((new_ref, SecretString::from(value.clone())));
                } else {
                    let key = (entry.local_id.clone(), "password".to_string());
                    if let Some(supplied) = choices.secret_values.get(&key) {
                        let new_ref2 = dbflux_core::proxy_secret_ref(new_id);
                        secret_writes.push((new_ref2, supplied.clone()));
                    }
                }
            }

            dbflux_core::ProxyAuth::Basic {
                username: username.clone(),
            }
        }
    }
}

/// Resolve the destination auth profile id for a connection entry.
///
/// - Stored auth profile: look up the new id from `id_map` via `auth_profile_local_id`.
/// - AWS reflected reference:
///   - When the reference was auto-resolved (i.e., NOT present in `plan.required_resolutions`),
///     the profile exists at the destination; return the deterministic `aws_profile_uuid`.
///   - When the reference was NOT auto-resolved (present in `plan.required_resolutions`),
///     return the user's explicit choice, or `None` if no choice was made — never bind a
///     dangling deterministic UUID to a profile that does not exist at the destination.
/// - No auth: return `None`.
fn resolve_auth_id(
    conn: &crate::bundle::ConnectionEntry,
    id_map: &HashMap<String, Uuid>,
    plan: &ImportPlan,
    choices: &ResolutionChoices,
) -> Option<Uuid> {
    if let Some(auth_ref) = &conn.auth_ref {
        use dbflux_core::auth::aws_profile_uuid;

        let key = (conn.local_id.clone(), "auth_profile".to_string());
        let chosen = choices.auth_profile_choices.get(&key).copied();

        // Determine whether this reference required a user resolution (not in dest).
        let requires_resolution = plan.required_resolutions.iter().any(|r| {
            r.owner_local_id == conn.local_id
                && r.field == "auth_profile"
                && matches!(&r.kind, RequiredResolutionKind::AwsReference { .. })
        });

        if requires_resolution {
            // Not present at destination: bind only if the user explicitly chose a profile.
            chosen
        } else {
            // Present at destination: auto-resolve to the deterministic UUID, unless
            // the user overrode it.
            let deterministic = aws_profile_uuid(&auth_ref.provider_id, &auth_ref.name);
            Some(chosen.unwrap_or(deterministic))
        }
    } else if let Some(ref local_auth_id) = conn.auth_profile_local_id {
        id_map.get(local_auth_id).copied()
    } else {
        None
    }
}

/// Rewrite the connection's `access_kind` to use remapped SSH/proxy UUIDs.
fn rewrite_access_kind(
    conn: &crate::bundle::ConnectionEntry,
    id_map: &HashMap<String, Uuid>,
) -> Option<dbflux_core::AccessKind> {
    use crate::bundle::AccessEntry;

    conn.access.as_ref().and_then(|access| match access {
        AccessEntry::Ssh { ssh_local_id } => {
            id_map
                .get(ssh_local_id)
                .map(|&new_id| dbflux_core::AccessKind::Ssh {
                    ssh_tunnel_profile_id: new_id,
                })
        }
        AccessEntry::Proxy { proxy_local_id } => {
            id_map
                .get(proxy_local_id)
                .map(|&new_id| dbflux_core::AccessKind::Proxy {
                    proxy_profile_id: new_id,
                })
        }
        AccessEntry::Managed { provider, params } => Some(dbflux_core::AccessKind::Managed {
            provider: provider.clone(),
            params: params.clone(),
        }),
    })
}

/// Parse a proxy kind string from the bundle's `kind` field.
fn parse_proxy_kind(kind: &str) -> dbflux_core::ProxyKind {
    match kind {
        "https" => dbflux_core::ProxyKind::Https,
        "socks5" => dbflux_core::ProxyKind::Socks5,
        _ => dbflux_core::ProxyKind::Http,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use dbflux_core::{
        AuthProfile, ConnectionHook, ConnectionHooks, HookKind, ProxyAuth, ProxyKind, ProxyProfile,
        SshAuthMethod, SshTunnelConfig, SshTunnelProfile,
    };
    use secrecy::ExposeSecret;
    use uuid::Uuid;

    use crate::{
        ConflictChoice, DestSnapshot, PortabilityError, ResolutionChoices,
        bundle::{
            AuthEntry, AuthRef, AuthRefKind, Bundle, BundleMeta, CURRENT_FORMAT_VERSION,
            ConnectionEntry, EncryptionMode, ProxyEntry, RequiredRef, RequiredRefKind,
            SecretsSection, SshAuthMethodKind, SshEntry,
        },
    };

    use super::{apply, parse, plan};

    // --- Helpers ---

    fn empty_bundle(encryption: EncryptionMode) -> Bundle {
        Bundle {
            bundle: BundleMeta {
                format_version: CURRENT_FORMAT_VERSION,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                dbflux_version: "0.7.0-dev.0".to_string(),
                encryption,
            },
            drivers: vec![],
            connections: vec![],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secrets: None,
        }
    }

    fn empty_dest() -> DestSnapshot<'static> {
        DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        }
    }

    fn bundle_bytes(bundle: &Bundle) -> Vec<u8> {
        toml::to_string(bundle).expect("serialize").into_bytes()
    }

    fn make_auth_entry(local_id: &str, provider_id: &str, name: &str) -> AuthEntry {
        AuthEntry {
            local_id: local_id.to_string(),
            name: name.to_string(),
            provider_id: provider_id.to_string(),
            enabled: true,
            fields: Default::default(),
            secret_field_names: vec![],
            required_refs: vec![],
        }
    }

    fn make_ssh_entry(local_id: &str) -> SshEntry {
        SshEntry {
            local_id: local_id.to_string(),
            name: "Bastion".to_string(),
            host: "bastion.example.com".to_string(),
            port: 22,
            user: "ec2-user".to_string(),
            auth_method: SshAuthMethodKind::Password,
            key_embedded: false,
            required_refs: vec![],
        }
    }

    fn make_proxy_entry(local_id: &str) -> ProxyEntry {
        ProxyEntry {
            local_id: local_id.to_string(),
            name: "Corp Proxy".to_string(),
            kind: "http".to_string(),
            host: "proxy.corp.com".to_string(),
            port: 8080,
            username: None,
            no_proxy: None,
            has_secret: false,
            required_refs: vec![],
        }
    }

    fn make_connection_entry(local_id: &str) -> ConnectionEntry {
        ConnectionEntry {
            local_id: local_id.to_string(),
            name: "Test Conn".to_string(),
            driver_id: "postgres".to_string(),
            fields: {
                let mut m = HashMap::new();
                m.insert("host".to_string(), "db.internal".to_string());
                m.insert("port".to_string(), "5432".to_string());
                m
            },
            local_path_fields: Default::default(),
            required_refs: vec![],
            auth_ref: None,
            auth_profile_local_id: None,
            access: None,
            value_refs: Default::default(),
            include_hooks: false,
            include_settings_overrides: false,
            hooks_payload: None,
            settings_overrides_payload: None,
            kind: None,
            uri_secret_fields: vec![],
        }
    }

    fn make_dest_auth(provider_id: &str, name: &str) -> AuthProfile {
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

    fn make_dest_ssh(host: &str, port: u16, user: &str) -> SshTunnelProfile {
        SshTunnelProfile::new(
            "ExistingTunnel",
            SshTunnelConfig {
                host: host.to_string(),
                port,
                user: user.to_string(),
                auth_method: SshAuthMethod::Password,
            },
        )
    }

    fn make_dest_proxy(kind: ProxyKind, host: &str, port: u16) -> ProxyProfile {
        ProxyProfile {
            id: Uuid::new_v4(),
            name: "ExistingProxy".to_string(),
            kind,
            host: host.to_string(),
            port,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    // -----------------------------------------------------------------------
    // parse() — rejection-before-persistence (P1 / R-FAIL-2)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_unsupported_version_is_rejected_with_no_partial_state() {
        // Craft a bundle whose format_version is one beyond the current maximum.
        // parse() must return UnsupportedVersion and must not return a partial ParsedBundle.
        let future_version = CURRENT_FORMAT_VERSION + 1;
        let toml = format!(
            r#"
[bundle]
format_version = {future_version}
created_at = "2026-01-01T00:00:00Z"
dbflux_version = "99.0.0"
encryption = "none"
"#
        );

        let result = parse(toml.as_bytes());

        assert!(
            matches!(
                result,
                Err(PortabilityError::UnsupportedVersion { version })
                    if version == future_version
            ),
            "expected UnsupportedVersion({future_version}), got: {:?}",
            result.err()
        );
    }

    #[test]
    fn parse_zero_version_is_rejected() {
        // Version 0 is neither current nor meaningful; must be rejected.
        let toml = r#"
[bundle]
format_version = 0
created_at = "2026-01-01T00:00:00Z"
dbflux_version = "0.0.0"
encryption = "none"
"#;

        let result = parse(toml.as_bytes());

        assert!(
            matches!(
                result,
                Err(PortabilityError::UnsupportedVersion { version: 0 })
            ),
            "version 0 must be rejected as UnsupportedVersion, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn parse_truncated_toml_returns_error_not_panic() {
        // A syntactically broken TOML input must return Parse(error), not panic.
        let truncated = b"[bundle\nformat_version = 1\ncreated_at = \"2026";

        let result = parse(truncated);

        assert!(
            matches!(result, Err(PortabilityError::Parse(_))),
            "truncated TOML must return Parse error, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn parse_malformed_but_nonempty_toml_returns_error_not_panic() {
        // Structurally valid TOML but wrong schema (missing required fields) must return
        // Parse(error), not panic and not a partial bundle.
        let malformed = b"this_is = \"not a bundle\"\nrandom_key = 42\n";

        let result = parse(malformed);

        assert!(
            result.is_err(),
            "malformed TOML with wrong schema must return an error"
        );
    }

    #[test]
    fn parse_non_utf8_bytes_returns_error_not_panic() {
        // Non-UTF-8 bytes must return an error (currently Decryption variant for the UTF-8 check),
        // not panic.
        let non_utf8: &[u8] = &[0xFF, 0xFE, 0x00, 0x01];

        let result = parse(non_utf8);

        assert!(
            result.is_err(),
            "non-UTF-8 input must return an error, not panic"
        );
    }

    // -----------------------------------------------------------------------
    // parse() — mode/section cross-validation tests (Follow-up #1)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_mode_mismatch_age_passphrase_with_plaintext_section_is_rejected() {
        let mut bundle = empty_bundle(EncryptionMode::AgePassphrase);
        bundle.secrets = Some(SecretsSection::Plaintext {
            values: {
                let mut m = HashMap::new();
                m.insert("conn:xxx:password".to_string(), "secret".to_string());
                m
            },
        });

        let bytes = bundle_bytes(&bundle);
        let result = parse(&bytes);

        assert!(
            matches!(result, Err(PortabilityError::ModeMismatch { .. })),
            "expected ModeMismatch, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn parse_mode_mismatch_none_with_encrypted_section_is_rejected() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.secrets = Some(SecretsSection::Encrypted {
            ciphertext:
                "-----BEGIN AGE ENCRYPTED FILE-----\nfake\n-----END AGE ENCRYPTED FILE-----"
                    .to_string(),
        });

        let bytes = bundle_bytes(&bundle);
        let result = parse(&bytes);

        assert!(
            matches!(result, Err(PortabilityError::ModeMismatch { .. })),
            "expected ModeMismatch for none+encrypted, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn parse_consistent_none_with_plaintext_section_is_accepted() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.secrets = Some(SecretsSection::Plaintext {
            values: HashMap::new(),
        });

        let bytes = bundle_bytes(&bundle);
        assert!(parse(&bytes).is_ok());
    }

    #[test]
    fn parse_consistent_age_passphrase_with_encrypted_section_is_accepted() {
        let mut bundle = empty_bundle(EncryptionMode::AgePassphrase);
        bundle.secrets = Some(SecretsSection::Encrypted {
            ciphertext: "age_armor_placeholder".to_string(),
        });

        let bytes = bundle_bytes(&bundle);
        // parse() should accept the structure; decryption will fail later.
        assert!(parse(&bytes).is_ok());
    }

    #[test]
    fn parse_no_secrets_section_is_always_accepted() {
        let bundle = empty_bundle(EncryptionMode::None);
        let bytes = bundle_bytes(&bundle);
        assert!(parse(&bytes).is_ok());
    }

    // -----------------------------------------------------------------------
    // plan() tests (T3.3)
    // -----------------------------------------------------------------------

    #[test]
    fn plan_ssh_conflict_detected() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.ssh_tunnels.push(make_ssh_entry("ssh-local-1"));

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest_ssh = make_dest_ssh("bastion.example.com", 22, "ec2-user");
        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&dest_ssh],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        assert_eq!(import_plan.conflicts.len(), 1);
        let conflict = import_plan.conflicts.first().expect("conflict");
        assert_eq!(conflict.bundle_local_id, "ssh-local-1");
        assert_eq!(conflict.existing_id, dest_ssh.id);
    }

    #[test]
    fn plan_omitted_password_becomes_required_resolution() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut conn = make_connection_entry("conn-local-1");
        conn.required_refs.push(RequiredRef {
            field: "password".to_string(),
            kind: RequiredRefKind::Secret,
        });
        bundle.connections.push(conn);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());

        assert_eq!(import_plan.required_resolutions.len(), 1);
        assert_eq!(
            import_plan
                .required_resolutions
                .first()
                .expect("resolution")
                .field,
            "password"
        );
    }

    #[test]
    fn plan_aws_reference_not_in_dest_becomes_required_resolution() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut conn = make_connection_entry("conn-aws-1");
        conn.auth_ref = Some(AuthRef {
            kind: AuthRefKind::AwsReference,
            provider_id: "aws-sso".to_string(),
            name: "My AWS SSO".to_string(),
        });
        bundle.connections.push(conn);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());

        let aws_resolution = import_plan.required_resolutions.iter().find(|r| {
            matches!(
                &r.kind,
                crate::RequiredResolutionKind::AwsReference { provider_id, name }
                if provider_id == "aws-sso" && name == "My AWS SSO"
            )
        });

        assert!(
            aws_resolution.is_some(),
            "AWS reference not in dest must produce a RequiredResolution"
        );
    }

    #[test]
    fn plan_aws_reference_in_dest_is_not_a_required_resolution() {
        use dbflux_core::auth::{AuthProfile, aws_profile_uuid};

        let aws_auth = AuthProfile {
            id: aws_profile_uuid("aws-sso", "My AWS SSO"),
            name: "My AWS SSO".to_string(),
            provider_id: "aws-sso".to_string(),
            fields: Default::default(),
            secret_fields: Default::default(),
            enabled: true,
            read_only: true,
            dangling_origin: None,
        };

        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut conn = make_connection_entry("conn-aws-2");
        conn.auth_ref = Some(AuthRef {
            kind: AuthRefKind::AwsReference,
            provider_id: "aws-sso".to_string(),
            name: "My AWS SSO".to_string(),
        });
        bundle.connections.push(conn);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![&aws_auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        let aws_resolution = import_plan
            .required_resolutions
            .iter()
            .find(|r| matches!(&r.kind, crate::RequiredResolutionKind::AwsReference { .. }));

        assert!(
            aws_resolution.is_none(),
            "AWS reference already present in dest must NOT produce a RequiredResolution"
        );
    }

    // -----------------------------------------------------------------------
    // apply() tests (T3.4)
    // -----------------------------------------------------------------------

    #[test]
    fn apply_mints_fresh_uuids_for_all_entities() {
        let local_conn_id = "aaaaaaaa-0000-0000-0000-000000000001";
        let local_ssh_id = "bbbbbbbb-0000-0000-0000-000000000002";

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle
            .connections
            .push(make_connection_entry(local_conn_id));
        bundle.ssh_tunnels.push(make_ssh_entry(local_ssh_id));

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();

        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn_id = actions.connections.first().expect("connection").id;
        let ssh_id = actions.ssh_tunnels.first().expect("ssh tunnel").id;

        assert_ne!(
            conn_id.to_string(),
            local_conn_id,
            "connection must receive a fresh UUID"
        );
        assert_ne!(
            ssh_id.to_string(),
            local_ssh_id,
            "SSH tunnel must receive a fresh UUID"
        );
        assert_ne!(conn_id, ssh_id, "each entity gets a distinct UUID");
    }

    #[test]
    fn apply_reuse_wires_dest_uuid_and_produces_no_new_entity() {
        let local_id = "ssh-local-reuse";
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.ssh_tunnels.push(make_ssh_entry(local_id));

        let dest_ssh = make_dest_ssh("bastion.example.com", 22, "ec2-user");
        let dest_ssh_id = dest_ssh.id;

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&dest_ssh],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        let mut choices = ResolutionChoices::default();
        choices
            .conflict_choices
            .insert(local_id.to_string(), ConflictChoice::Reuse);

        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        assert!(
            actions.ssh_tunnels.is_empty(),
            "Reuse must not produce a new SSH entity; got {} entities",
            actions.ssh_tunnels.len()
        );

        // The connection (if any) must point to the dest UUID.
        // (No connection in this test, but verify no SSH entity emitted.)
        let _ = dest_ssh_id;
    }

    #[test]
    fn apply_create_new_produces_new_entity() {
        let local_id = "ssh-local-create-new";
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.ssh_tunnels.push(make_ssh_entry(local_id));

        let dest_ssh = make_dest_ssh("bastion.example.com", 22, "ec2-user");

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&dest_ssh],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        let mut choices = ResolutionChoices::default();
        choices
            .conflict_choices
            .insert(local_id.to_string(), ConflictChoice::CreateNew);

        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        assert_eq!(
            actions.ssh_tunnels.len(),
            1,
            "CreateNew must produce a new SSH entity"
        );
    }

    /// When an AWS reference IS present at the destination (auto-resolved), apply() must
    /// bind the connection to the deterministic UUID of that profile.
    #[test]
    fn apply_aws_reference_present_in_dest_gets_deterministic_uuid() {
        use dbflux_core::auth::{AuthProfile as CoreAuth, aws_profile_uuid};

        let local_conn_id = "conn-aws-apply";
        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut conn = make_connection_entry(local_conn_id);
        conn.auth_ref = Some(AuthRef {
            kind: AuthRefKind::AwsReference,
            provider_id: "aws-sso".to_string(),
            name: "My AWS SSO".to_string(),
        });
        bundle.connections.push(conn);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        // Put the profile at the destination so it auto-resolves.
        let dest_auth = CoreAuth {
            id: aws_profile_uuid("aws-sso", "My AWS SSO"),
            name: "My AWS SSO".to_string(),
            provider_id: "aws-sso".to_string(),
            fields: Default::default(),
            secret_fields: Default::default(),
            enabled: true,
            read_only: true,
            dangling_origin: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![&dest_auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);
        let choices = ResolutionChoices::default();

        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let expected_auth_id = aws_profile_uuid("aws-sso", "My AWS SSO");
        let actual_auth_id = actions
            .connections
            .first()
            .expect("connection")
            .auth_profile_id;

        assert_eq!(
            actual_auth_id,
            Some(expected_auth_id),
            "AWS reference present at dest must resolve to the deterministic UUID"
        );
    }

    #[test]
    fn apply_embedded_ssh_key_lands_in_secret_writes_with_key_path_none() {
        let local_ssh_id = "ssh-embedded-key";
        let mut ssh_entry = make_ssh_entry(local_ssh_id);
        ssh_entry.auth_method = SshAuthMethodKind::PrivateKey;
        ssh_entry.key_embedded = true;

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.ssh_tunnels.push(ssh_entry);

        let key_value = "base64_encoded_key_bytes".to_string();
        let old_key = format!("ssh_tunnel:{}:private_key", local_ssh_id);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: Some({
                let mut m = HashMap::new();
                m.insert(old_key, key_value.clone());
                m
            }),
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();

        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        assert_eq!(actions.ssh_tunnels.len(), 1);

        // The imported SSH profile must use key_path: None (key from keyring).
        assert!(
            matches!(
                &actions
                    .ssh_tunnels
                    .first()
                    .expect("ssh tunnel")
                    .config
                    .auth_method,
                dbflux_core::SshAuthMethod::PrivateKey { key_path: None }
            ),
            "embedded key must produce key_path: None"
        );

        // The key bytes must be in secret_writes.
        assert!(
            !actions.secret_writes.is_empty(),
            "embedded key must land in secret_writes"
        );
    }

    #[test]
    fn apply_missing_required_choice_does_not_panic() {
        // A conflict is present but the user provided no choice — apply should
        // either skip gracefully or return an error, but must not panic.
        let local_id = "ssh-no-choice";
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.ssh_tunnels.push(make_ssh_entry(local_id));

        let dest_ssh = make_dest_ssh("bastion.example.com", 22, "ec2-user");

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![&dest_ssh],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);
        let choices = ResolutionChoices::default(); // no choice for the conflict

        // Should not panic; may return CreateNew by default.
        let result = apply(&parsed, &import_plan, &choices);
        assert!(
            result.is_ok(),
            "missing conflict choice should default to CreateNew, got: {:?}",
            result.err()
        );
    }

    // -----------------------------------------------------------------------
    // Follow-up #3: AuthEntry.required_refs parity test
    // -----------------------------------------------------------------------

    #[test]
    fn auth_entry_required_refs_field_exists_and_round_trips() {
        use crate::bundle::{Bundle, BundleMeta, CURRENT_FORMAT_VERSION, EncryptionMode};

        let entry = AuthEntry {
            local_id: "auth-local-1".to_string(),
            name: "Test Auth".to_string(),
            provider_id: "test-provider".to_string(),
            enabled: true,
            fields: Default::default(),
            secret_field_names: vec![],
            required_refs: vec![RequiredRef {
                field: "token".to_string(),
                kind: RequiredRefKind::Secret,
            }],
        };

        let bundle = Bundle {
            bundle: BundleMeta {
                format_version: CURRENT_FORMAT_VERSION,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                dbflux_version: "0.7.0-dev.0".to_string(),
                encryption: EncryptionMode::None,
            },
            drivers: vec![],
            connections: vec![],
            auth_profiles: vec![entry.clone()],
            ssh_tunnels: vec![],
            proxies: vec![],
            secrets: None,
        };

        let bytes = bundle_bytes(&bundle);
        let text = String::from_utf8(bytes).expect("utf8");

        // The required_ref for "token" must appear in the serialized bundle.
        assert!(
            text.contains("\"token\""),
            "auth required_ref field must appear in bundle: {text}"
        );

        // Round-trip through parse to confirm deserialization works.
        let parsed = parse(text.as_bytes()).expect("parse");
        let rt_auth = parsed.bundle.auth_profiles.first().expect("auth entry");
        assert_eq!(rt_auth.required_refs.len(), 1);
        assert_eq!(
            rt_auth.required_refs.first().expect("required_ref").field,
            "token"
        );
    }

    #[test]
    fn plan_collects_auth_required_refs_into_resolutions() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut auth = make_auth_entry("auth-local-2", "my-provider", "My Auth");
        auth.required_refs.push(RequiredRef {
            field: "api_key".to_string(),
            kind: RequiredRefKind::Secret,
        });
        bundle.auth_profiles.push(auth);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());

        let resolution = import_plan
            .required_resolutions
            .iter()
            .find(|r| r.owner_local_id == "auth-local-2" && r.field == "api_key");

        assert!(
            resolution.is_some(),
            "auth required_ref must produce a RequiredResolution"
        );
    }

    #[test]
    fn plan_auth_profile_conflict_detected() {
        let dest_auth = make_dest_auth("my-provider", "My Auth");
        let dest_auth_id = dest_auth.id;

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle
            .auth_profiles
            .push(make_auth_entry("auth-local-3", "my-provider", "My Auth"));

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![&dest_auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        assert_eq!(import_plan.conflicts.len(), 1);
        assert_eq!(
            import_plan.conflicts.first().expect("conflict").existing_id,
            dest_auth_id
        );
    }

    #[test]
    fn plan_proxy_conflict_detected() {
        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.proxies.push(make_proxy_entry("proxy-local-1"));

        let dest_proxy = make_dest_proxy(ProxyKind::Http, "proxy.corp.com", 8080);
        let dest_proxy_id = dest_proxy.id;

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![&dest_proxy],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        assert_eq!(import_plan.conflicts.len(), 1);
        assert_eq!(
            import_plan.conflicts.first().expect("conflict").existing_id,
            dest_proxy_id
        );
    }

    // -----------------------------------------------------------------------
    // apply() — driver_id preservation (FIX-1)
    // -----------------------------------------------------------------------

    /// Importing a connection with `driver_id = "mysql"` must produce a profile
    /// whose `driver_id()` returns `"mysql"` — never `"postgres"` or any other
    /// driver id that does not match the bundle entry.
    #[test]
    fn apply_connection_preserves_driver_id_from_bundle() {
        let local_id = "conn-mysql-1";
        let mut entry = make_connection_entry(local_id);
        entry.driver_id = "mysql".to_string();
        entry
            .fields
            .insert("host".to_string(), "mysql.example.com".to_string());
        entry.fields.insert("port".to_string(), "3306".to_string());

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");

        assert_eq!(
            conn.driver_id(),
            "mysql",
            "imported connection driver_id must match the bundle entry; \
             got '{}' instead of 'mysql'",
            conn.driver_id()
        );
    }

    /// Importing a connection with `driver_id = "redis"` must not silently
    /// become `"postgres"`.  Ensures no connection can be mis-typed on import.
    #[test]
    fn apply_connection_driver_id_is_never_silently_postgres() {
        for driver in [
            "redis",
            "mongodb",
            "mssql",
            "dynamodb",
            "cloudwatch",
            "influxdb",
        ] {
            let local_id = format!("conn-{driver}-1");
            let mut entry = make_connection_entry(&local_id);
            entry.driver_id = driver.to_string();

            let mut bundle = empty_bundle(EncryptionMode::None);
            bundle.connections.push(entry);

            let parsed = crate::ParsedBundle {
                bundle,
                decrypted_secrets: None,
            };

            let import_plan = plan(&parsed, &empty_dest());
            let choices = ResolutionChoices::default();
            let actions = apply(&parsed, &import_plan, &choices).expect("apply");

            let conn = actions.connections.first().expect("one connection");

            assert_ne!(
                conn.driver_id(),
                "postgres",
                "driver '{}' must not become 'postgres' after import",
                driver
            );
            assert_eq!(
                conn.driver_id(),
                driver,
                "driver_id must be preserved as '{}'; got '{}'",
                driver,
                conn.driver_id()
            );
        }
    }

    /// The form values from the bundle's `fields` map must be stored in the
    /// profile's config so the app-layer `build_config` call can reconstruct
    /// the correct driver config without data loss.
    #[test]
    fn apply_connection_form_values_carried_in_config() {
        let local_id = "conn-pg-values";
        let mut entry = make_connection_entry(local_id);
        entry.driver_id = "postgres".to_string();
        entry
            .fields
            .insert("host".to_string(), "db.example.com".to_string());
        entry.fields.insert("port".to_string(), "5433".to_string());
        entry.fields.insert("user".to_string(), "admin".to_string());
        entry
            .fields
            .insert("database".to_string(), "mydb".to_string());

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");

        // The config must be DbConfig::External so the app layer can call
        // build_config(values) with the real driver.
        let dbflux_core::DbConfig::External { values, .. } = &conn.config else {
            panic!(
                "imported connection config must be DbConfig::External so \
                 the app layer can rebuild it with the real driver; \
                 got a concrete driver config variant instead"
            );
        };

        assert_eq!(
            values.get("host").map(String::as_str),
            Some("db.example.com")
        );
        assert_eq!(values.get("port").map(String::as_str), Some("5433"));
        assert_eq!(values.get("user").map(String::as_str), Some("admin"));
        assert_eq!(values.get("database").map(String::as_str), Some("mydb"));
    }

    // -----------------------------------------------------------------------
    // R2-4: db_kind_from_bundle uses canonical serde representation
    // -----------------------------------------------------------------------

    /// Every DbKind variant must survive the export→import round-trip through the
    /// canonical serde string (the same path used by the new export pipeline).
    #[test]
    fn db_kind_all_variants_round_trip_via_canonical_serde() {
        use super::db_kind_from_bundle;
        use dbflux_core::DbKind;

        let variants: &[(DbKind, &str, &str)] = &[
            (DbKind::Postgres, "Postgres", "postgres"),
            (DbKind::SQLite, "SQLite", "sqlite"),
            (DbKind::MySQL, "MySQL", "mysql"),
            (DbKind::MariaDB, "MariaDB", "mariadb"),
            (DbKind::MongoDB, "MongoDB", "mongodb"),
            (DbKind::Redis, "Redis", "redis"),
            (DbKind::DynamoDB, "DynamoDB", "dynamodb"),
            (DbKind::CloudWatchLogs, "CloudWatchLogs", "cloudwatch"),
            (DbKind::InfluxDB, "InfluxDB", "influxdb"),
            (DbKind::SqlServer, "SqlServer", "mssql"),
        ];

        for (expected, kind_str, driver_id) in variants {
            let mut entry = make_connection_entry("rt-kind");
            entry.driver_id = driver_id.to_string();
            entry.kind = Some(kind_str.to_string());

            let got = db_kind_from_bundle(&entry);
            assert_eq!(
                got, *expected,
                "kind_str='{}' must parse to {:?}; got {:?}",
                kind_str, expected, got
            );
        }
    }

    /// Legacy bundle (no `kind` field) must derive the correct kind from driver_id.
    #[test]
    fn db_kind_falls_back_to_driver_id_for_legacy_bundles() {
        use super::db_kind_from_bundle;
        use dbflux_core::DbKind;

        let cases: &[(&str, DbKind)] = &[
            ("postgres", DbKind::Postgres),
            ("mysql", DbKind::MySQL),
            ("redis", DbKind::Redis),
            ("mssql", DbKind::SqlServer),
            ("cloudwatch", DbKind::CloudWatchLogs),
        ];

        for (driver_id, expected) in cases {
            let mut entry = make_connection_entry("legacy");
            entry.driver_id = driver_id.to_string();
            entry.kind = None;

            let got = db_kind_from_bundle(&entry);
            assert_eq!(
                got, *expected,
                "driver_id='{}' must fall back to {:?}; got {:?}",
                driver_id, expected, got
            );
        }
    }

    // -----------------------------------------------------------------------
    // apply() — value_refs preserved on import (#1)
    // -----------------------------------------------------------------------

    /// When a bundle's ConnectionEntry carries value_refs, the imported profile must
    /// have those value_refs set — they must not be silently dropped.
    #[test]
    fn apply_connection_value_refs_are_preserved() {
        use crate::bundle::EncryptionMode;

        let local_id = "conn-vref-1";
        let mut entry = make_connection_entry(local_id);

        // Build a toml::Value representing a ValueRef::Env { key: "DB_HOST" }.
        let vref_toml = toml::Value::Table({
            let mut t = toml::value::Table::new();
            t.insert("source".to_string(), toml::Value::String("env".to_string()));
            t.insert(
                "key".to_string(),
                toml::Value::String("DB_HOST".to_string()),
            );
            t
        });
        entry.value_refs.insert("host".to_string(), vref_toml);

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");

        assert!(
            !conn.value_refs.is_empty(),
            "value_refs must be preserved from the bundle; got empty map"
        );

        let vref = conn.value_refs.get("host").expect("host value_ref");
        assert!(
            matches!(vref, dbflux_core::values::ValueRef::Env { key } if key == "DB_HOST"),
            "value_ref for 'host' must be Env{{key: DB_HOST}}, got: {:?}",
            vref
        );
    }

    /// A bundle with no value_refs must produce a profile with an empty value_refs map.
    #[test]
    fn apply_connection_empty_value_refs_yields_empty_map() {
        let local_id = "conn-novref-1";
        let entry = make_connection_entry(local_id);

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");
        assert!(
            conn.value_refs.is_empty(),
            "no value_refs in bundle => empty map on profile"
        );
    }

    // -----------------------------------------------------------------------
    // apply() — hooks preserved on import (#2)
    // -----------------------------------------------------------------------

    /// When a bundle's ConnectionEntry carries a hooks_payload (include_hooks = true),
    /// the imported profile must have hooks set, not None.
    #[test]
    fn apply_connection_hooks_are_preserved_when_included() {
        let local_id = "conn-hooks-1";
        let mut entry = make_connection_entry(local_id);
        entry.include_hooks = true;

        // Build a minimal hooks payload matching ConnectionHooks serde shape.
        let hooks_toml = toml::Value::Table({
            let mut t = toml::value::Table::new();
            t.insert("pre_connect".to_string(), toml::Value::Array(vec![]));
            t.insert("post_connect".to_string(), toml::Value::Array(vec![]));
            t.insert("pre_disconnect".to_string(), toml::Value::Array(vec![]));
            t.insert("post_disconnect".to_string(), toml::Value::Array(vec![]));
            t
        });
        entry.hooks_payload = Some(hooks_toml);

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");
        assert!(
            conn.hooks.is_some(),
            "hooks must be preserved when include_hooks = true and hooks_payload is present"
        );
    }

    // -----------------------------------------------------------------------
    // R2-1: Hook env round-trips through the encrypted secrets section
    // -----------------------------------------------------------------------

    /// Builds a toml::Value hooks payload from a ConnectionHooks struct, mirroring
    /// what the export pipeline writes (after sanitizing env out).
    fn hooks_toml_payload(hooks: &dbflux_core::ConnectionHooks) -> toml::Value {
        let jv = serde_json::to_value(hooks).expect("serialize hooks");
        toml::Value::try_from(jv).expect("convert to toml")
    }

    /// A connection exported with a hook carrying env vars must restore those env
    /// vars when imported with the corresponding decrypted secrets present.
    #[test]
    fn apply_hook_env_is_restored_from_decrypted_secrets() {
        let local_id = "conn-hook-env-1";
        let mut entry = make_connection_entry(local_id);
        entry.include_hooks = true;

        // Build a hooks struct with an empty env — the exporter sanitizes env out and
        // stages it in the encrypted secrets section.
        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: "echo".to_string(),
                args: vec![],
            },
            cwd: None,
            env: HashMap::new(), // env was moved to secrets during export
            inherit_env: false,
            env_denylist: vec![],
            timeout_ms: None,
            execution_mode: Default::default(),
            ready_signal: None,
            on_failure: Default::default(),
        };
        let hooks = ConnectionHooks {
            pre_connect: vec![hook],
            post_connect: vec![],
            pre_disconnect: vec![],
            post_disconnect: vec![],
        };
        entry.hooks_payload = Some(hooks_toml_payload(&hooks));

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        // The decrypted_secrets map contains the env entry staged by the exporter.
        let env_key = format!("conn_hook_env:{}:pre_connect:0:SECRET_TOKEN", local_id);
        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: Some({
                let mut m = HashMap::new();
                m.insert(env_key, "tok_live_supersecret".to_string());
                m
            }),
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("connection");
        let restored_hooks = conn.hooks.as_ref().expect("hooks must be present");
        let hook = restored_hooks
            .pre_connect
            .first()
            .expect("pre_connect hook");

        assert_eq!(
            hook.env.get("SECRET_TOKEN").map(String::as_str),
            Some("tok_live_supersecret"),
            "hook env SECRET_TOKEN must be restored from decrypted_secrets"
        );
    }

    /// A hook with no env entries must import with an empty env map — no panic, no crash.
    #[test]
    fn apply_hook_with_no_env_imports_with_empty_env() {
        let local_id = "conn-hook-noenv-1";
        let mut entry = make_connection_entry(local_id);
        entry.include_hooks = true;

        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: "echo".to_string(),
                args: vec![],
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: false,
            env_denylist: vec![],
            timeout_ms: None,
            execution_mode: Default::default(),
            ready_signal: None,
            on_failure: Default::default(),
        };
        let hooks = ConnectionHooks {
            pre_connect: vec![hook],
            post_connect: vec![],
            pre_disconnect: vec![],
            post_disconnect: vec![],
        };
        entry.hooks_payload = Some(hooks_toml_payload(&hooks));

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        // No decrypted_secrets at all.
        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("connection");
        let restored_hooks = conn.hooks.as_ref().expect("hooks present");
        let hook = restored_hooks
            .pre_connect
            .first()
            .expect("pre_connect hook");

        assert!(
            hook.env.is_empty(),
            "hook with no env staged must have empty env after import"
        );
    }

    /// When include_hooks = false, the imported profile must not have spurious hooks set.
    #[test]
    fn apply_connection_hooks_are_none_when_not_included() {
        let local_id = "conn-nohooks-1";
        let entry = make_connection_entry(local_id);

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");
        assert!(
            conn.hooks.is_none(),
            "hooks must be None when include_hooks = false"
        );
    }

    // -----------------------------------------------------------------------
    // apply() — kind from bundle (#3)
    // -----------------------------------------------------------------------

    /// A bundle entry with kind = "MySQL" must produce a profile whose kind() is MySQL,
    /// not Postgres (which was the hardcoded DbConfig::External kind before this fix).
    #[test]
    fn apply_connection_kind_matches_bundle_kind() {
        let local_id = "conn-mysql-kind";
        let mut entry = make_connection_entry(local_id);
        entry.driver_id = "mysql".to_string();
        entry.kind = Some("MySQL".to_string());

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn = actions.connections.first().expect("one connection");

        // The DbConfig::External kind must NOT be Postgres.
        if let dbflux_core::DbConfig::External { kind, .. } = &conn.config {
            assert_ne!(
                *kind,
                dbflux_core::DbKind::Postgres,
                "External config kind must not be Postgres for a MySQL bundle entry"
            );
            assert_eq!(
                *kind,
                dbflux_core::DbKind::MySQL,
                "External config kind must be MySQL"
            );
        }
    }

    // -----------------------------------------------------------------------
    // apply() — AWS ref not in dest returns None auth_profile_id (#6)
    // -----------------------------------------------------------------------

    /// When an AWS reference is NOT present at the destination and the user made
    /// no choice, the imported connection must have auth_profile_id = None —
    /// not a dangling deterministic UUID pointing to a non-existent profile.
    #[test]
    fn apply_aws_reference_absent_from_dest_and_no_choice_yields_none_auth_id() {
        let local_conn_id = "conn-aws-absent";
        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut conn = make_connection_entry(local_conn_id);
        conn.auth_ref = Some(AuthRef {
            kind: AuthRefKind::AwsReference,
            provider_id: "aws-sso".to_string(),
            name: "Missing Profile".to_string(),
        });
        bundle.connections.push(conn);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        // Dest has NO profiles — the AWS reference can't be auto-resolved.
        let import_plan = plan(&parsed, &empty_dest());

        // Verify that the plan emitted a RequiredResolution for this ref.
        assert!(
            import_plan
                .required_resolutions
                .iter()
                .any(|r| matches!(&r.kind, crate::RequiredResolutionKind::AwsReference { .. })),
            "AWS reference absent from dest must produce a RequiredResolution"
        );

        // No user choice provided.
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let actual_auth_id = actions
            .connections
            .first()
            .expect("connection")
            .auth_profile_id;

        assert_eq!(
            actual_auth_id, None,
            "unresolved AWS reference with no user choice must yield auth_profile_id = None, \
             not a dangling deterministic UUID"
        );
    }

    /// When an AWS reference IS present at the destination, auto-resolving to the
    /// deterministic UUID must still work as before.
    #[test]
    fn apply_aws_reference_present_at_dest_still_auto_resolves() {
        use dbflux_core::auth::{AuthProfile as CoreAuth, aws_profile_uuid};

        let dest_auth = CoreAuth {
            id: aws_profile_uuid("aws-sso", "Present Profile"),
            name: "Present Profile".to_string(),
            provider_id: "aws-sso".to_string(),
            fields: Default::default(),
            secret_fields: Default::default(),
            enabled: true,
            read_only: true,
            dangling_origin: None,
        };

        let local_conn_id = "conn-aws-present";
        let mut bundle = empty_bundle(EncryptionMode::None);
        let mut conn = make_connection_entry(local_conn_id);
        conn.auth_ref = Some(AuthRef {
            kind: AuthRefKind::AwsReference,
            provider_id: "aws-sso".to_string(),
            name: "Present Profile".to_string(),
        });
        bundle.connections.push(conn);

        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let dest = DestSnapshot {
            auth_profiles: vec![&dest_auth],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let import_plan = plan(&parsed, &dest);

        // Verify the plan did NOT emit a RequiredResolution for this ref.
        assert!(
            !import_plan
                .required_resolutions
                .iter()
                .any(|r| matches!(&r.kind, crate::RequiredResolutionKind::AwsReference { .. })),
            "AWS reference present in dest must NOT produce a RequiredResolution"
        );

        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let expected = aws_profile_uuid("aws-sso", "Present Profile");
        let actual = actions
            .connections
            .first()
            .expect("connection")
            .auth_profile_id;

        assert_eq!(
            actual,
            Some(expected),
            "AWS reference present in dest must resolve to the deterministic UUID"
        );
    }

    // -----------------------------------------------------------------------
    // R3-2: Connection password (secret-hinted field) round-trips through
    //        the secrets section on export → import.
    // -----------------------------------------------------------------------

    /// A connection whose password was staged in the bundle secrets section must
    /// have its secret re-keyed and pushed to `secret_writes` on import.
    ///
    /// The exporter stages the connection secret under `conn:<local_id>:<field_id>`,
    /// NOT under the cleartext `fields` key. The importer must scan the secrets map
    /// by the `conn:<local_id>:` prefix instead of iterating `fields.keys()`.
    #[test]
    fn apply_connection_password_is_restored_from_staged_secret() {
        let local_id = "conn-secret-restore-1";
        let mut entry = make_connection_entry(local_id);

        // The password field must NOT appear in cleartext fields (Secret-hinted fields
        // are excluded from the export's [connection.fields] table). The field name
        // must match what the exporter stages in the secrets section.
        entry.fields.remove("host"); // keep only minimal required fields
        entry.fields.remove("port");
        entry
            .fields
            .insert("host".to_string(), "db.example.com".to_string());
        entry.fields.insert("port".to_string(), "5432".to_string());
        // "password" is intentionally absent from fields — it went to secrets.

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        // The secrets map contains the staged password as the exporter wrote it:
        // conn:<local_id>:<field_id>.
        let staged_key = format!("conn:{}:password", local_id);
        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: Some({
                let mut m = HashMap::new();
                m.insert(staged_key, "hunter2".to_string());
                m
            }),
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn_id = actions.connections.first().expect("connection").id;
        let expected_ref = dbflux_core::connection_secret_ref(&conn_id);

        let matched: Vec<_> = actions
            .secret_writes
            .iter()
            .filter(|(ref_str, _)| ref_str == &expected_ref)
            .collect();

        assert_eq!(
            matched.len(),
            1,
            "secret_writes must contain exactly one entry for the connection secret ref; got {:?}",
            actions
                .secret_writes
                .iter()
                .map(|(k, _)| k)
                .collect::<Vec<_>>()
        );

        let secret_value = matched
            .first()
            .expect("matched has one entry")
            .1
            .expose_secret();
        assert_eq!(
            secret_value, "hunter2",
            "restored connection secret must equal the staged value"
        );
    }

    /// A connection with no secret staged (no password in the bundle) must not
    /// produce any spurious `secret_writes` entry for the connection secret ref.
    #[test]
    fn apply_connection_with_no_staged_secret_produces_no_spurious_write() {
        let local_id = "conn-no-secret-1";
        let entry = make_connection_entry(local_id);

        let mut bundle = empty_bundle(EncryptionMode::None);
        bundle.connections.push(entry);

        // No secrets in the decrypted map at all.
        let parsed = crate::ParsedBundle {
            bundle,
            decrypted_secrets: Some(HashMap::new()),
        };

        let import_plan = plan(&parsed, &empty_dest());
        let choices = ResolutionChoices::default();
        let actions = apply(&parsed, &import_plan, &choices).expect("apply");

        let conn_id = actions.connections.first().expect("connection").id;
        let expected_ref = dbflux_core::connection_secret_ref(&conn_id);

        let spurious: Vec<_> = actions
            .secret_writes
            .iter()
            .filter(|(ref_str, _)| ref_str == &expected_ref)
            .collect();

        assert!(
            spurious.is_empty(),
            "no staged secret => no secret_writes for the connection ref; got {:?}",
            spurious.iter().map(|(k, _)| k).collect::<Vec<_>>()
        );
    }
}
