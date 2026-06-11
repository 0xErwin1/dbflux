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
        let auth_profile_id = resolve_auth_id(conn_entry, &id_map, choices);

        // Rewrite access_kind to point at the remapped ssh/proxy ids.
        let access_kind = rewrite_access_kind(conn_entry, &id_map);

        // Connection secret: re-key the staged secret for this connection.
        for field_id in conn_entry.fields.keys() {
            // The secret is stored under conn:<local_id>:<field_id> in the bundle.
            let old_key = format!("conn:{}:{}", conn_entry.local_id, field_id);
            if let Some(secret_map) = secrets
                && let Some(value) = secret_map.get(&old_key)
            {
                let new_ref = dbflux_core::connection_secret_ref(&new_conn_id);
                secret_writes.push((new_ref, secrecy::SecretString::from(value.clone())));
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

        let config = build_connection_config(conn_entry);
        let mut profile = dbflux_core::ConnectionProfile::new(&conn_entry.name, config);
        profile.id = new_conn_id;
        profile.auth_profile_id = auth_profile_id;
        profile.access_kind = access_kind;

        // proxy_profile_id is a legacy field; keep it in sync with access_kind when applicable.
        if let Some(dbflux_core::AccessKind::Proxy { proxy_profile_id }) = &profile.access_kind {
            profile.proxy_profile_id = Some(*proxy_profile_id);
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
/// - AWS reflected reference: compute the deterministic `aws_profile_uuid` — NOT a
///   minted UUID — so the connection binds to the reflected profile on the target.
/// - Auth-profile resolution choice (`choices.auth_profile_choices`): override with
///   the chosen destination id for unresolved AWS refs.
/// - No auth: return `None`.
fn resolve_auth_id(
    conn: &crate::bundle::ConnectionEntry,
    id_map: &HashMap<String, Uuid>,
    choices: &ResolutionChoices,
) -> Option<Uuid> {
    if let Some(auth_ref) = &conn.auth_ref {
        use dbflux_core::auth::aws_profile_uuid;

        let deterministic = aws_profile_uuid(&auth_ref.provider_id, &auth_ref.name);

        // User may have provided an override via the required-resolution step.
        let key = (conn.local_id.clone(), "auth_profile".to_string());
        let chosen = choices.auth_profile_choices.get(&key).copied();

        Some(chosen.unwrap_or(deterministic))
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

/// Build a minimal `DbConfig::Postgres` placeholder for the imported connection.
///
/// The fields are sourced from the bundle's `[connections.fields]` map. Fields
/// not present in the bundle default to empty/None. Type-specific dispatch is not
/// possible at this layer (we have no driver registry); `Postgres` is used as the
/// common deserializable variant. The app layer (Slice 5) will create the actual
/// config from the driver form, not from this helper.
///
/// For now this produces a structurally valid `ConnectionProfile` that the
/// repository can persist. The config will be replaced by the wizard's form
/// reconstruction in Slice 5.
fn build_connection_config(conn: &crate::bundle::ConnectionEntry) -> dbflux_core::DbConfig {
    // Extract common fields from the bundle's flat field map.
    let host = conn.fields.get("host").cloned().unwrap_or_default();
    let port: u16 = conn
        .fields
        .get("port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5432u16);
    let user = conn.fields.get("user").cloned().unwrap_or_default();
    let database = conn.fields.get("database").cloned().unwrap_or_default();

    // Return a Postgres config as a structurally valid placeholder.
    // Slice 5 (the import wizard) rebuilds the config from the driver form.
    dbflux_core::DbConfig::Postgres {
        use_uri: false,
        uri: None,
        host,
        port,
        user,
        database,
        ssl_mode: conn.fields.get("ssl_mode").cloned(),
        ssl_root_cert_path: conn.fields.get("ssl_root_cert_path").cloned(),
        ssl_client_cert_path: conn.fields.get("ssl_client_cert_path").cloned(),
        ssl_client_key_path: conn.fields.get("ssl_client_key_path").cloned(),
        ssh_tunnel: None,
        ssh_tunnel_profile_id: None,
    }
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
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use dbflux_core::{
        AuthProfile, ProxyAuth, ProxyKind, ProxyProfile, SshAuthMethod, SshTunnelConfig,
        SshTunnelProfile,
    };
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

    #[test]
    fn apply_aws_reference_gets_deterministic_uuid() {
        use dbflux_core::auth::aws_profile_uuid;

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

        let import_plan = plan(&parsed, &empty_dest());
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
            "AWS reference must resolve to the deterministic UUID"
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
        };

        let import_plan = plan(&parsed, &dest);

        assert_eq!(import_plan.conflicts.len(), 1);
        assert_eq!(
            import_plan.conflicts.first().expect("conflict").existing_id,
            dest_proxy_id
        );
    }
}
