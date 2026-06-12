//! Connection-export/import bridge between `dbflux_portability` and `AppState`.
//!
//! This module owns the two seam implementations that the portability crate
//! requires from the app layer — `FieldHintResolver` and `SecretReader` — the
//! function that assembles an `ExportGraph` from AppState data, and the
//! `apply_import` orchestration that persists `ImportActions` through the
//! existing repositories and keyring.
//!
//! # Testability contract
//!
//! All public items in this module depend only on plain data or on trait objects
//! that can be satisfied by fakes in unit tests.  No GPUI `Entity` or `Context`
//! types appear here, which is what allows the `dbflux_app` test binary to
//! compile and run these tests (unlike `dbflux_ui_windows`, whose GPUI proc-macro
//! expansion causes rustc to SIGSEGV during test-binary compilation).
//!
//! # Import persistence invariants
//!
//! `apply_import` persists `ImportActions` with ordered best-effort writes and
//! NO DB+keyring two-phase commit (2PC).  The insertion order is:
//!
//! 1. Auth profiles (so later connection foreign-key-like references exist)
//! 2. SSH tunnel profiles
//! 3. Proxy profiles
//! 4. Connections (referencing all of the above)
//! 5. Secret writes via `SecretManager::set_by_ref`
//!
//! A `false` return from `set_by_ref` (keyring locked or unavailable) is
//! captured in `ImportOutcome::secret_failures` — it is NEVER silently
//! discarded.  Connections whose `driver_id` is unregistered are recorded in
//! `ImportOutcome::needs_driver` and NOT persisted with a placeholder config.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    AuthProfile, ConnectionProfile, DbDriver, ExportFieldHint, FieldExportTransform, FormValues,
    ProxyProfile, SecretStore, SshTunnelProfile,
};
use dbflux_portability::{
    AwsRef, ConnectionWithValues, DestSnapshot, ExportGraph, ExportTransformResolver,
    FieldHintResolver, ImportActions, ImportPlan, ParsedBundle, ResolutionChoices, SecretReader,
};

// ---------------------------------------------------------------------------
// FieldHintResolver — backed by a driver snapshot
// ---------------------------------------------------------------------------

/// Resolves export field hints by delegating to the registered driver.
///
/// The driver registry is captured as a snapshot (`HashMap`) at the point the
/// user presses "Export", so this type can be sent across threads without
/// holding a reference to `AppState`.  No `driver_id` string branching occurs
/// here; the driver is looked up by its registered ID and the trait method is
/// called generically.
pub struct AppFieldHintResolver {
    drivers: HashMap<String, Arc<dyn DbDriver>>,
}

impl AppFieldHintResolver {
    pub fn new(drivers: HashMap<String, Arc<dyn DbDriver>>) -> Self {
        Self { drivers }
    }
}

impl FieldHintResolver for AppFieldHintResolver {
    fn hint(
        &self,
        profile: &ConnectionProfile,
        field_id: &str,
        values: &FormValues,
    ) -> ExportFieldHint {
        let Some(driver_id) = profile.driver_id.as_deref() else {
            return ExportFieldHint::Include;
        };

        let Some(driver) = self.drivers.get(driver_id) else {
            return ExportFieldHint::Include;
        };

        driver.export_field_hint(field_id, values)
    }
}

// ---------------------------------------------------------------------------
// ExportTransformResolver — backed by the same driver snapshot
// ---------------------------------------------------------------------------

/// Resolves per-field export transforms by delegating to the registered driver.
///
/// Shares the same `drivers` snapshot as `AppFieldHintResolver`. The transform
/// is consulted before the hint in the export loop; drivers that embed credentials
/// in URI fields return `SplitSecret` for those fields.
pub struct AppExportTransformResolver {
    drivers: HashMap<String, Arc<dyn DbDriver>>,
}

impl AppExportTransformResolver {
    pub fn new(drivers: HashMap<String, Arc<dyn DbDriver>>) -> Self {
        Self { drivers }
    }
}

impl ExportTransformResolver for AppExportTransformResolver {
    fn transform(
        &self,
        profile: &ConnectionProfile,
        field_id: &str,
        values: &FormValues,
    ) -> FieldExportTransform {
        let Some(driver_id) = profile.driver_id.as_deref() else {
            return FieldExportTransform::None;
        };

        let Some(driver) = self.drivers.get(driver_id) else {
            return FieldExportTransform::None;
        };

        driver.export_field_transform(field_id, values)
    }
}

// ---------------------------------------------------------------------------
// SecretReader — backed by a shared SecretStore arc
// ---------------------------------------------------------------------------

/// Reads secrets from the OS keyring through a shared `SecretStore`.
///
/// The `Arc<RwLock<...>>` is cloned from `AppState`'s secret store before
/// spawning the background export task, so no borrow of `AppState` is held
/// across await points.
///
/// Returns `None` when the keyring is locked or the entry is absent; never
/// panics.
pub struct AppSecretReader {
    store: Arc<RwLock<Box<dyn SecretStore>>>,
}

impl AppSecretReader {
    pub fn new(store: Arc<RwLock<Box<dyn SecretStore>>>) -> Self {
        Self { store }
    }
}

impl SecretReader for AppSecretReader {
    fn read(&self, secret_ref: &str) -> Option<SecretString> {
        let store = self.store.read().ok()?;
        store.get(secret_ref).ok().flatten()
    }
}

// ---------------------------------------------------------------------------
// ExportGraph assembly
// ---------------------------------------------------------------------------

/// Input data needed to assemble an `ExportGraph`.
///
/// The caller (UI layer) extracts this from `AppState` and passes it here so
/// the assembly function has no dependency on GPUI types.
pub struct ExportInputs {
    /// Selected connection profiles with their driver-extracted form values.
    pub connections_with_values: Vec<(ConnectionProfile, FormValues)>,
    /// Stored (non-reflected) auth profiles referenced by any selected connection.
    pub auth_profiles: Vec<AuthProfile>,
    /// Read-only AWS reflected auth references (cannot be stored, travel as metadata).
    pub aws_references: Vec<AwsRef>,
    /// SSH tunnel profiles referenced by any selected connection.
    pub ssh_tunnels: Vec<SshTunnelProfile>,
    /// Proxy profiles referenced by any selected connection.
    pub proxies: Vec<ProxyProfile>,
}

/// Assemble an `ExportGraph` from pre-extracted app-state inputs.
///
/// Borrows from `inputs` to produce a graph whose lifetimes are tied to the
/// `inputs` slice data.  The caller owns the data and is responsible for
/// keeping `inputs` alive for the duration of the export call.
pub fn build_export_graph(inputs: &ExportInputs) -> ExportGraph<'_> {
    let connections: Vec<ConnectionWithValues<'_>> = inputs
        .connections_with_values
        .iter()
        .map(|(profile, values)| ConnectionWithValues {
            profile,
            values: values.clone(),
        })
        .collect();

    ExportGraph {
        connections,
        auth_profiles: inputs.auth_profiles.iter().collect(),
        aws_references: inputs.aws_references.clone(),
        ssh_tunnels: inputs.ssh_tunnels.iter().collect(),
        proxies: inputs.proxies.iter().collect(),
    }
}

// ---------------------------------------------------------------------------
// Import orchestration — DestSnapshot builder + apply_import
// ---------------------------------------------------------------------------

/// Snapshot of the existing destination profiles used for conflict detection.
///
/// The four owned `Vec`s hold clones taken just before `plan()` so the data
/// is stable for the entire import flow.
pub struct OwnedDestSnapshot {
    pub auth_profiles: Vec<AuthProfile>,
    pub ssh_tunnels: Vec<SshTunnelProfile>,
    pub proxies: Vec<ProxyProfile>,
    pub connections: Vec<ConnectionProfile>,
}

impl OwnedDestSnapshot {
    /// Borrow the snapshot as a `DestSnapshot<'_>` suitable for passing to
    /// `dbflux_portability::plan()`.
    pub fn as_ref_snapshot(&self) -> DestSnapshot<'_> {
        DestSnapshot {
            auth_profiles: self.auth_profiles.iter().collect(),
            ssh_tunnels: self.ssh_tunnels.iter().collect(),
            proxies: self.proxies.iter().collect(),
            connections: self.connections.iter().collect(),
        }
    }
}

/// The result of a completed import apply.
///
/// Entities listed in `succeeded` were inserted into the repository and their
/// secrets written to the keyring (where available).  Items in
/// `secret_failures` were inserted but at least one secret write was rejected
/// by the OS keyring (returned `false` from `set_by_ref`).  Items in
/// `needs_driver` were skipped entirely because their `driver_id` is not
/// registered in the current driver registry.  Items in `config_failures` were
/// skipped because the registered driver's `build_config` call returned an error —
/// the UI surfaces both buckets as informational notes.
#[derive(Debug, Default)]
pub struct ImportOutcome {
    /// Names of entities that were fully persisted.
    pub succeeded: Vec<String>,
    /// `(entity_name, secret_ref)` pairs where the keyring write returned `false`.
    pub secret_failures: Vec<(String, String)>,
    /// `(connection_name, driver_id)` pairs whose driver is not registered.
    pub needs_driver: Vec<(String, String)>,
    /// `(connection_name, error_message)` pairs where `build_config` returned an error.
    pub config_failures: Vec<(String, String)>,
    /// Connection names whose intra-bundle references (ssh/proxy/auth local_ids) could
    /// not be resolved. These connections were NOT imported to prevent silent topology
    /// degradation (e.g. bastion-routed → direct-connect).
    pub unresolved_refs: Vec<String>,
}

/// Result returned by `ImportPersistence::add_connection`.
#[derive(Debug)]
pub enum ConnectionInsertResult {
    /// Connection was inserted successfully.
    Ok,
    /// The `driver_id` is not registered; connection skipped.
    NeedsDriver,
    /// The driver is registered but `build_config` failed; connection skipped.
    ConfigFailed(String),
}

/// Seam for inserting entities into the repository, used by `apply_import`.
///
/// The production implementation delegates to `AppState`; tests supply fakes.
pub trait ImportPersistence {
    fn add_auth_profile(&mut self, profile: AuthProfile);
    fn add_ssh_tunnel(&mut self, tunnel: SshTunnelProfile);
    fn add_proxy(&mut self, proxy: ProxyProfile);

    /// Insert a connection profile.
    ///
    /// Returns `ConnectionInsertResult::Ok` on success.
    /// Returns `ConnectionInsertResult::NeedsDriver` when the `driver_id` is not
    /// registered, causing the connection to be recorded in `needs_driver` and skipped.
    /// Returns `ConnectionInsertResult::ConfigFailed(reason)` when the driver is present
    /// but `build_config` fails; the connection is recorded in `config_failures` and
    /// NOT persisted with a broken placeholder config.
    fn add_connection(&mut self, profile: ConnectionProfile) -> ConnectionInsertResult;

    /// Write a secret under an explicit keyring reference.
    ///
    /// Returns `true` on success, `false` when the keyring is unavailable or
    /// rejected the write.
    fn write_secret(&self, secret_ref: &str, secret: &SecretString) -> bool;

    /// Populate the in-memory `secret_fields` for an auth profile after its secrets
    /// have been written to the keyring.
    ///
    /// Called immediately after `add_auth_profile` for newly created (non-Reuse/MapTo)
    /// profiles so they authenticate in the current session without a restart (H3 / ADR-4).
    /// The production implementation looks up the profile in the profile manager and
    /// sets `secret_fields` directly on the in-memory copy.
    fn hydrate_auth_secret_fields(
        &mut self,
        auth_id: uuid::Uuid,
        fields: HashMap<String, SecretString>,
    );
}

/// Run the pure `dbflux_portability::apply()` and persist the resulting
/// `ImportActions` through `deps`.
///
/// This is the final step of the import pipeline.  It is SEPARATE from the
/// portability crate's `apply()` because persistence requires access to the
/// driver registry (to rebuild the real `DbConfig`) and the OS keyring (for
/// `SecretManager::set_by_ref`), neither of which the pure-logic crate owns.
///
/// Persistence order: auth profiles → SSH tunnels → proxy profiles →
/// connections → secrets.  This order ensures FK-like references exist before
/// they are pointed to.
///
/// Any connection whose `driver_id` is not registered by `deps.add_connection`
/// is skipped and recorded in `ImportOutcome::needs_driver`; the import
/// continues for all remaining entities.
pub fn apply_import(
    parsed: &ParsedBundle,
    plan: &ImportPlan,
    choices: &ResolutionChoices,
    deps: &mut dyn ImportPersistence,
) -> Result<ImportOutcome, dbflux_portability::PortabilityError> {
    let actions = dbflux_portability::import::apply(parsed, plan, choices)?;
    let outcome = persist_import_actions(actions, deps);
    Ok(outcome)
}

/// Persist `ImportActions` through the `ImportPersistence` seam.
///
/// Separated from `apply_import` so tests can call it directly with a
/// pre-built `ImportActions` without needing a full `ParsedBundle`.
///
/// # Hydration (H3 / ADR-4)
///
/// After writing auth-profile secrets to the keyring, this function populates the
/// in-memory `secret_fields` on newly created auth profiles via `hydrate_auth_secret_fields`.
/// This allows the imported profile to authenticate in the current session without restart.
/// Reuse/MapTo profiles are excluded — they retain their destination credentials (ADR-6 / H2).
///
/// # Secret-write filtering
///
/// Secrets are written only for connections that were successfully persisted.
/// Connections recorded in `needs_driver` or `config_failures` are skipped, and
/// their `connection_secret_ref`-keyed entries are filtered out of `secret_writes`
/// to prevent orphaned keyring entries for profiles that were never inserted.
pub fn persist_import_actions(
    actions: ImportActions,
    deps: &mut dyn ImportPersistence,
) -> ImportOutcome {
    let mut outcome = ImportOutcome::default();

    // Record kind and dangling-ref failures reported by the pure apply() step.
    for (name, driver_id) in actions.kind_failures {
        outcome
            .config_failures
            .push((name, format!("missing or unparseable `kind` field (driver: {driver_id})")));
    }
    for name in actions.unresolved_ref_connections {
        outcome.unresolved_refs.push(name);
    }

    // Collect (new_auth_id, field_name) -> secret pairs for in-session hydration.
    // We capture these before writing so we can populate secret_fields post-write.
    let mut auth_secret_by_id: HashMap<uuid::Uuid, HashMap<String, SecretString>> =
        HashMap::new();

    // Stage: identify which auth secret_writes belong to the new auth profiles.
    // Auth secret refs use the format `dbflux:auth:<uuid>:<field>`.
    for (ref_str, secret) in &actions.secret_writes {
        if let Some(stripped) = ref_str.strip_prefix("dbflux:auth:")
            && let Some((uuid_str, field)) = stripped.split_once(':')
            && let Ok(auth_id) = uuid::Uuid::parse_str(uuid_str)
        {
            auth_secret_by_id
                .entry(auth_id)
                .or_default()
                .insert(field.to_string(), secret.clone());
        }
    }

    for auth in actions.auth_profiles {
        let name = auth.name.clone();
        let auth_id = auth.id;
        deps.add_auth_profile(auth);
        outcome.succeeded.push(name);

        // H3 / ADR-4: hydrate in-memory secret_fields for the newly added profile.
        if let Some(fields) = auth_secret_by_id.get(&auth_id) {
            deps.hydrate_auth_secret_fields(auth_id, fields.clone());
        }
    }

    for ssh in actions.ssh_tunnels {
        let name = ssh.name.clone();
        deps.add_ssh_tunnel(ssh);
        outcome.succeeded.push(name);
    }

    for proxy in actions.proxies {
        let name = proxy.name.clone();
        deps.add_proxy(proxy);
        outcome.succeeded.push(name);
    }

    // Track UUIDs of skipped connections to filter their secret writes below.
    let mut skipped_conn_ids: std::collections::HashSet<uuid::Uuid> =
        std::collections::HashSet::new();

    for conn in actions.connections {
        let name = conn.name.clone();
        let driver_id = conn.driver_id().to_string();
        let conn_id = conn.id;

        match deps.add_connection(conn) {
            ConnectionInsertResult::Ok => {
                outcome.succeeded.push(name);
            }
            ConnectionInsertResult::NeedsDriver => {
                outcome.needs_driver.push((name, driver_id));
                skipped_conn_ids.insert(conn_id);
            }
            ConnectionInsertResult::ConfigFailed(reason) => {
                outcome.config_failures.push((name, reason));
                skipped_conn_ids.insert(conn_id);
            }
        }
    }

    // Precompute the exact `connection_secret_ref` string for each skipped id so the
    // filter below uses an exact prefix match rather than a loose substring search.
    // `connection_secret_ref` produces the canonical `"dbflux:conn:<uuid>"` prefix that
    // all connection-scoped secret refs begin with.
    let skipped_conn_refs: std::collections::HashSet<String> = skipped_conn_ids
        .iter()
        .map(dbflux_core::connection_secret_ref)
        .collect();

    for (secret_ref, secret) in actions.secret_writes {
        // Skip secrets belonging to connections that were not persisted.
        let is_skipped = skipped_conn_refs
            .iter()
            .any(|prefix| secret_ref.starts_with(prefix.as_str()));

        if is_skipped {
            continue;
        }

        if !deps.write_secret(&secret_ref, &secret) {
            outcome
                .secret_failures
                .push(("(secret)".to_string(), secret_ref));
        }
    }

    outcome
}

// ---------------------------------------------------------------------------
// Phase 4.1 — M7: MapTo candidate computation
// ---------------------------------------------------------------------------

/// Return all destination profiles of the same `ConflictKind` as the given conflict,
/// suitable for the "Map to another" picker in the import wizard.
///
/// MapTo (choose a different destination profile) is distinct from Reuse (keep the
/// conflicting one). This function returns ALL same-kind profiles at the destination,
/// not only the conflicting one. An empty list means only Reuse or CreateNew are
/// available (ADR-3 / M7 / R-WIZ-3).
pub fn mapto_candidates(
    kind: dbflux_portability::ConflictKind,
    dest: &OwnedDestSnapshot,
) -> Vec<(uuid::Uuid, String)> {
    match kind {
        dbflux_portability::ConflictKind::AuthProfile => dest
            .auth_profiles
            .iter()
            .map(|a| (a.id, a.name.clone()))
            .collect(),
        dbflux_portability::ConflictKind::SshTunnel => dest
            .ssh_tunnels
            .iter()
            .map(|s| (s.id, s.name.clone()))
            .collect(),
        dbflux_portability::ConflictKind::Proxy => dest
            .proxies
            .iter()
            .map(|p| (p.id, p.name.clone()))
            .collect(),
        dbflux_portability::ConflictKind::Connection => dest
            .connections
            .iter()
            .map(|c| (c.id, c.name.clone()))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// Phase 4.2 — C3: ConfirmSummary — computed once at plan time for stable rendering
// ---------------------------------------------------------------------------

/// Summary of entities to be imported, computed at plan time.
///
/// The wizard renders from this value rather than from the live `parsed_bundle`,
/// so the Confirm step shows correct counts even after `parsed_bundle` is consumed
/// by `do_apply` (ADR-3 / C3 / R-WIZ-1).
#[derive(Debug, Clone, Default)]
pub struct ConfirmSummary {
    /// Number of connections to be imported (including new + reuse + conflict-resolved).
    pub connection_count: usize,
    /// Number of auth profiles to be imported or reused.
    pub auth_profile_count: usize,
    /// Number of SSH tunnel profiles to be imported or reused.
    pub ssh_tunnel_count: usize,
    /// Number of proxy profiles to be imported or reused.
    pub proxy_count: usize,
    /// Number of required resolutions the user must supply before applying.
    pub required_resolution_count: usize,
    /// Number of connection conflicts detected (Reuse/CreateNew/MapTo choices pending).
    pub conflict_count: usize,
    /// True when any connection in the bundle targets a driver not in the registry.
    /// The wizard can surface a note about installing the relevant driver.
    pub has_driver_not_installed: bool,
}

/// Compute a `ConfirmSummary` from the plan and bundle.
///
/// Called immediately after `plan()` returns so the wizard can render the Confirm
/// step from a stable snapshot rather than the live `ParsedBundle`.
pub fn confirm_summary(
    parsed: &dbflux_portability::ParsedBundle,
    plan: &dbflux_portability::ImportPlan,
    registered_drivers: &std::collections::HashSet<String>,
) -> ConfirmSummary {
    let has_driver_not_installed = parsed
        .bundle
        .connections
        .iter()
        .any(|c| !registered_drivers.contains(&c.driver_id));

    ConfirmSummary {
        connection_count: parsed.bundle.connections.len(),
        auth_profile_count: parsed.bundle.auth_profiles.len(),
        ssh_tunnel_count: parsed.bundle.ssh_tunnels.len(),
        proxy_count: parsed.bundle.proxies.len(),
        required_resolution_count: plan.required_resolutions.len(),
        conflict_count: plan.conflicts.len(),
        has_driver_not_installed,
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use dbflux_core::secrecy::SecretString;
    use dbflux_core::{
        AuthProfile, Connection, ConnectionProfile, DbConfig, DbError, DbKind, DriverFormDef,
        DriverMetadata, ExportFieldHint, FormValues, ProxyProfile, SecretStore, SshTunnelProfile,
    };
    use dbflux_portability::{AwsRef, FieldHintResolver, SecretReader};
    use uuid::Uuid;

    // -----------------------------------------------------------------------
    // Fake driver — implements only the methods needed for export tests.
    // Required methods that are irrelevant here are marked `unimplemented!()`.
    // -----------------------------------------------------------------------

    fn empty_form_def() -> DriverFormDef {
        DriverFormDef { tabs: vec![] }
    }

    struct FakeDriver {
        /// When `Some`, returned for any field whose ID starts with `"secret_"`.
        secret_field_hint: Option<ExportFieldHint>,
        form_def: DriverFormDef,
    }

    impl FakeDriver {
        fn new_with_hint(hint: ExportFieldHint) -> Self {
            Self {
                secret_field_hint: Some(hint),
                form_def: empty_form_def(),
            }
        }

        fn new_default() -> Self {
            Self {
                secret_field_hint: None,
                form_def: empty_form_def(),
            }
        }
    }

    impl dbflux_core::DbDriver for FakeDriver {
        fn kind(&self) -> DbKind {
            DbKind::SQLite
        }

        fn metadata(&self) -> &DriverMetadata {
            unimplemented!("FakeDriver::metadata not needed in export tests")
        }

        fn form_definition(&self) -> &DriverFormDef {
            &self.form_def
        }

        fn driver_key(&self) -> String {
            "fake".to_string()
        }

        fn build_config(&self, _values: &FormValues) -> Result<DbConfig, DbError> {
            unimplemented!("FakeDriver::build_config not needed in export tests")
        }

        fn extract_values(&self, _config: &DbConfig) -> FormValues {
            FormValues::default()
        }

        fn export_field_hint(&self, field_id: &str, _values: &FormValues) -> ExportFieldHint {
            match &self.secret_field_hint {
                Some(hint) if field_id.starts_with("secret_") => *hint,
                _ => ExportFieldHint::Include,
            }
        }

        fn connect_with_secrets(
            &self,
            _profile: &ConnectionProfile,
            _password: Option<&SecretString>,
            _ssh_secret: Option<&SecretString>,
        ) -> Result<Box<dyn Connection>, DbError> {
            unimplemented!("FakeDriver::connect_with_secrets not needed in export tests")
        }

        fn test_connection(&self, _profile: &ConnectionProfile) -> Result<(), DbError> {
            unimplemented!("FakeDriver::test_connection not needed in export tests")
        }
    }

    // -----------------------------------------------------------------------
    // Fake SecretStore
    // -----------------------------------------------------------------------

    struct FakeSecretStore {
        secrets: HashMap<String, String>,
    }

    impl FakeSecretStore {
        fn with(pairs: &[(&str, &str)]) -> Self {
            Self {
                secrets: pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            }
        }

        fn empty() -> Self {
            Self {
                secrets: HashMap::new(),
            }
        }
    }

    impl SecretStore for FakeSecretStore {
        fn is_available(&self) -> bool {
            true
        }

        fn get(&self, secret_ref: &str) -> Result<Option<SecretString>, DbError> {
            Ok(self
                .secrets
                .get(secret_ref)
                .map(|v| SecretString::from(v.clone())))
        }

        fn set(&self, _secret_ref: &str, _value: &SecretString) -> Result<(), DbError> {
            Ok(())
        }

        fn delete(&self, _secret_ref: &str) -> Result<(), DbError> {
            Ok(())
        }
    }

    fn make_store_arc(store: FakeSecretStore) -> Arc<RwLock<Box<dyn SecretStore>>> {
        Arc::new(RwLock::new(Box::new(store)))
    }

    // -----------------------------------------------------------------------
    // Profile constructors (no Default impl on these types)
    // -----------------------------------------------------------------------

    fn make_profile(driver_id: &str) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            "test",
            DbConfig::External {
                kind: DbKind::SQLite,
                values: FormValues::default(),
            },
        );
        p.driver_id = Some(driver_id.to_string());
        p
    }

    fn make_auth_profile(id: Uuid) -> AuthProfile {
        AuthProfile {
            id,
            name: "test-auth".to_string(),
            provider_id: "test-provider".to_string(),
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
            enabled: true,
            read_only: false,
            dangling_origin: None,
        }
    }

    fn make_ssh_tunnel(id: Uuid) -> SshTunnelProfile {
        let mut s = SshTunnelProfile::new(
            "test-ssh",
            dbflux_core::SshTunnelConfig {
                host: "bastion.example.com".to_string(),
                port: 22,
                user: "ec2-user".to_string(),
                auth_method: dbflux_core::SshAuthMethod::Password,
            },
        );
        s.id = id;
        s
    }

    fn make_proxy(id: Uuid) -> ProxyProfile {
        ProxyProfile {
            id,
            name: "test-proxy".to_string(),
            kind: dbflux_core::ProxyKind::Socks5,
            host: "proxy.example.com".to_string(),
            port: 1080,
            auth: dbflux_core::ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        }
    }

    // -----------------------------------------------------------------------
    // AppFieldHintResolver tests
    // -----------------------------------------------------------------------

    #[test]
    fn hint_resolver_delegates_to_driver_without_branching_on_id() {
        let mut drivers: HashMap<String, Arc<dyn dbflux_core::DbDriver>> = HashMap::new();
        drivers.insert(
            "fake".to_string(),
            Arc::new(FakeDriver::new_with_hint(ExportFieldHint::RequiredOnImport)),
        );

        let resolver = AppFieldHintResolver::new(drivers);
        let profile = make_profile("fake");
        let values = FormValues::default();

        let hint = resolver.hint(&profile, "secret_password", &values);
        assert_eq!(hint, ExportFieldHint::RequiredOnImport);
    }

    #[test]
    fn hint_resolver_returns_include_for_non_secret_fields() {
        let mut drivers: HashMap<String, Arc<dyn dbflux_core::DbDriver>> = HashMap::new();
        drivers.insert("fake".to_string(), Arc::new(FakeDriver::new_default()));

        let resolver = AppFieldHintResolver::new(drivers);
        let profile = make_profile("fake");
        let values = FormValues::default();

        let hint = resolver.hint(&profile, "host", &values);
        assert_eq!(hint, ExportFieldHint::Include);
    }

    #[test]
    fn hint_resolver_falls_back_to_include_for_unknown_driver() {
        let resolver = AppFieldHintResolver::new(HashMap::new());
        let profile = make_profile("totally-unknown-driver");
        let values = FormValues::default();

        let hint = resolver.hint(&profile, "secret_password", &values);
        assert_eq!(hint, ExportFieldHint::Include);
    }

    #[test]
    fn hint_resolver_falls_back_to_include_when_driver_id_is_none() {
        let resolver = AppFieldHintResolver::new(HashMap::new());
        let mut profile = make_profile("fake");
        profile.driver_id = None;
        let values = FormValues::default();

        let hint = resolver.hint(&profile, "secret_password", &values);
        assert_eq!(hint, ExportFieldHint::Include);
    }

    #[test]
    fn hint_resolver_dispatches_independently_per_driver() {
        // Two different drivers, each with a distinct export hint behavior.
        // This verifies the resolver calls the driver generically and does not
        // branch on driver_id strings.
        let mut drivers: HashMap<String, Arc<dyn dbflux_core::DbDriver>> = HashMap::new();
        drivers.insert(
            "driver-a".to_string(),
            Arc::new(FakeDriver::new_with_hint(ExportFieldHint::RequiredOnImport)),
        );
        drivers.insert(
            "driver-b".to_string(),
            Arc::new(FakeDriver::new_with_hint(ExportFieldHint::Secret)),
        );

        let resolver = AppFieldHintResolver::new(drivers);
        let values = FormValues::default();

        let hint_a = resolver.hint(&make_profile("driver-a"), "secret_key", &values);
        let hint_b = resolver.hint(&make_profile("driver-b"), "secret_key", &values);

        assert_eq!(hint_a, ExportFieldHint::RequiredOnImport);
        assert_eq!(hint_b, ExportFieldHint::Secret);
    }

    // -----------------------------------------------------------------------
    // AppSecretReader tests
    // -----------------------------------------------------------------------

    #[test]
    fn secret_reader_returns_value_for_known_ref() {
        use dbflux_core::secrecy::ExposeSecret;

        let store = FakeSecretStore::with(&[("dbflux/conn/profile-1/password", "s3cr3t")]);
        let reader = AppSecretReader::new(make_store_arc(store));

        let result = reader.read("dbflux/conn/profile-1/password");
        assert!(result.is_some());
        assert_eq!(result.unwrap().expose_secret(), "s3cr3t");
    }

    #[test]
    fn secret_reader_returns_none_for_missing_ref() {
        let store = FakeSecretStore::empty();
        let reader = AppSecretReader::new(make_store_arc(store));

        let result = reader.read("dbflux/conn/nonexistent/password");
        assert!(result.is_none());
    }

    #[test]
    fn secret_reader_never_panics_on_poisoned_lock() {
        // Poison the RwLock by panicking inside a write guard from another thread.
        let arc: Arc<RwLock<Box<dyn SecretStore>>> = make_store_arc(FakeSecretStore::empty());
        let arc2 = arc.clone();

        let _ = std::thread::spawn(move || {
            let _guard = arc2.write().unwrap();
            panic!("poisoning the lock intentionally");
        })
        .join();

        // After poisoning, AppSecretReader must return None rather than panic.
        let reader = AppSecretReader { store: arc };
        let result = reader.read("any/key");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // build_export_graph tests
    // -----------------------------------------------------------------------

    #[test]
    fn build_export_graph_includes_all_provided_entities() {
        let conn_id = Uuid::new_v4();
        let auth_id = Uuid::new_v4();
        let ssh_id = Uuid::new_v4();
        let proxy_id = Uuid::new_v4();

        let mut profile = make_profile("fake");
        profile.id = conn_id;

        let inputs = ExportInputs {
            connections_with_values: vec![(profile, FormValues::default())],
            auth_profiles: vec![make_auth_profile(auth_id)],
            aws_references: vec![AwsRef {
                provider_id: "aws-sso".to_string(),
                name: "dev-account".to_string(),
            }],
            ssh_tunnels: vec![make_ssh_tunnel(ssh_id)],
            proxies: vec![make_proxy(proxy_id)],
        };

        let graph = build_export_graph(&inputs);

        assert_eq!(graph.connections.len(), 1);
        assert_eq!(graph.connections[0].profile.id, conn_id);

        assert_eq!(graph.auth_profiles.len(), 1);
        assert_eq!(graph.auth_profiles[0].id, auth_id);

        assert_eq!(graph.aws_references.len(), 1);
        assert_eq!(graph.aws_references[0].name, "dev-account");

        assert_eq!(graph.ssh_tunnels.len(), 1);
        assert_eq!(graph.ssh_tunnels[0].id, ssh_id);

        assert_eq!(graph.proxies.len(), 1);
        assert_eq!(graph.proxies[0].id, proxy_id);
    }

    #[test]
    fn build_export_graph_with_no_side_entities() {
        let mut profile = make_profile("fake");
        let conn_id = Uuid::new_v4();
        profile.id = conn_id;

        let inputs = ExportInputs {
            connections_with_values: vec![(profile, FormValues::default())],
            auth_profiles: vec![],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
        };

        let graph = build_export_graph(&inputs);

        assert_eq!(graph.connections.len(), 1);
        assert_eq!(graph.auth_profiles.len(), 0);
        assert_eq!(graph.aws_references.len(), 0);
        assert_eq!(graph.ssh_tunnels.len(), 0);
        assert_eq!(graph.proxies.len(), 0);
    }

    #[test]
    fn build_export_graph_with_empty_inputs_is_empty() {
        let inputs = ExportInputs {
            connections_with_values: vec![],
            auth_profiles: vec![],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
        };

        let graph = build_export_graph(&inputs);

        assert_eq!(graph.connections.len(), 0);
        assert_eq!(graph.auth_profiles.len(), 0);
        assert_eq!(graph.aws_references.len(), 0);
        assert_eq!(graph.ssh_tunnels.len(), 0);
        assert_eq!(graph.proxies.len(), 0);
    }

    #[test]
    fn build_export_graph_only_includes_explicitly_passed_entities() {
        // The function does NOT auto-discover transitive references — that
        // responsibility belongs to the UI layer before calling build_export_graph.
        // Only the entities present in `inputs` appear in the graph.
        let conn_id = Uuid::new_v4();
        let included_auth = Uuid::new_v4();
        let excluded_auth = Uuid::new_v4();

        let mut profile = make_profile("fake");
        profile.id = conn_id;

        let inputs = ExportInputs {
            connections_with_values: vec![(profile, FormValues::default())],
            auth_profiles: vec![make_auth_profile(included_auth)],
            aws_references: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
        };

        let graph = build_export_graph(&inputs);

        assert_eq!(graph.auth_profiles.len(), 1);
        assert_eq!(graph.auth_profiles[0].id, included_auth);
        assert!(!graph.auth_profiles.iter().any(|a| a.id == excluded_auth));
    }

    // -----------------------------------------------------------------------
    // ImportPersistence tests (T5.1 — import orchestration, TDD first)
    // -----------------------------------------------------------------------

    use super::{ImportPersistence, persist_import_actions};
    use dbflux_portability::ImportActions;

    /// Minimal fake implementation of `ImportPersistence` for unit tests.
    struct FakePersistence {
        drivers: std::collections::HashSet<String>,
        auth_count: usize,
        ssh_count: usize,
        proxy_count: usize,
        conn_names: Vec<String>,
        /// `None` keys succeed; `Some(false)` simulates keyring-locked failure.
        secret_outcomes: HashMap<String, bool>,
    }

    impl FakePersistence {
        fn with_drivers(drivers: &[&str]) -> Self {
            Self {
                drivers: drivers.iter().map(|s| s.to_string()).collect(),
                auth_count: 0,
                ssh_count: 0,
                proxy_count: 0,
                conn_names: Vec::new(),
                secret_outcomes: HashMap::new(),
            }
        }

        fn all_drivers() -> Self {
            let mut s = Self::with_drivers(&[]);
            s.drivers.insert("*".to_string());
            s
        }

        fn with_keyring_failure(mut self, secret_ref: &str) -> Self {
            self.secret_outcomes.insert(secret_ref.to_string(), false);
            self
        }
    }

    impl ImportPersistence for FakePersistence {
        fn add_auth_profile(&mut self, _profile: AuthProfile) {
            self.auth_count += 1;
        }

        fn add_ssh_tunnel(&mut self, _tunnel: SshTunnelProfile) {
            self.ssh_count += 1;
        }

        fn add_proxy(&mut self, _proxy: ProxyProfile) {
            self.proxy_count += 1;
        }

        fn add_connection(&mut self, profile: ConnectionProfile) -> ConnectionInsertResult {
            let driver_id = profile.driver_id().to_string();
            if !self.drivers.contains("*") && !self.drivers.contains(&driver_id) {
                return ConnectionInsertResult::NeedsDriver;
            }
            self.conn_names.push(profile.name.clone());
            ConnectionInsertResult::Ok
        }

        fn write_secret(&self, secret_ref: &str, _secret: &SecretString) -> bool {
            self.secret_outcomes
                .get(secret_ref)
                .copied()
                .unwrap_or(true)
        }

        fn hydrate_auth_secret_fields(
            &mut self,
            _auth_id: uuid::Uuid,
            _fields: HashMap<String, SecretString>,
        ) {
            // No-op for tests that don't verify hydration.
        }
    }

    fn make_import_actions_empty() -> ImportActions {
        ImportActions {
            connections: vec![],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        }
    }

    fn make_conn_profile(name: &str, driver_id: &str) -> ConnectionProfile {
        let mut p = ConnectionProfile::new(
            name,
            dbflux_core::DbConfig::External {
                kind: dbflux_core::DbKind::SQLite,
                values: FormValues::default(),
            },
        );
        p.driver_id = Some(driver_id.to_string());
        p
    }

    #[test]
    fn persist_empty_actions_returns_empty_outcome() {
        let mut deps = FakePersistence::all_drivers();
        let outcome = persist_import_actions(make_import_actions_empty(), &mut deps);

        assert!(outcome.succeeded.is_empty());
        assert!(outcome.secret_failures.is_empty());
        assert!(outcome.needs_driver.is_empty());
    }

    #[test]
    fn persist_auth_ssh_proxy_are_inserted_before_connections() {
        let mut deps = FakePersistence::all_drivers();

        let auth = AuthProfile {
            id: Uuid::new_v4(),
            name: "TestAuth".to_string(),
            provider_id: "test-provider".to_string(),
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
            enabled: true,
            read_only: false,
            dangling_origin: None,
        };
        let ssh = SshTunnelProfile::new(
            "TestSSH",
            dbflux_core::SshTunnelConfig {
                host: "bastion.example.com".to_string(),
                port: 22,
                user: "ec2-user".to_string(),
                auth_method: dbflux_core::SshAuthMethod::Password,
            },
        );
        let proxy = ProxyProfile {
            id: Uuid::new_v4(),
            name: "TestProxy".to_string(),
            kind: dbflux_core::ProxyKind::Http,
            host: "proxy.example.com".to_string(),
            port: 3128,
            auth: dbflux_core::ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        };
        let conn = make_conn_profile("TestConn", "postgres");

        let actions = ImportActions {
            connections: vec![conn],
            auth_profiles: vec![auth],
            ssh_tunnels: vec![ssh],
            proxies: vec![proxy],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert_eq!(deps.auth_count, 1);
        assert_eq!(deps.ssh_count, 1);
        assert_eq!(deps.proxy_count, 1);
        assert_eq!(deps.conn_names.len(), 1);
        assert_eq!(deps.conn_names[0], "TestConn");
        assert!(outcome.needs_driver.is_empty());
        assert_eq!(outcome.succeeded.len(), 4);
    }

    #[test]
    fn persist_unknown_driver_skips_connection_and_records_needs_driver() {
        let mut deps = FakePersistence::with_drivers(&["postgres"]);
        let conn_unknown = make_conn_profile("UnknownConn", "totally-unknown-driver");

        let actions = ImportActions {
            connections: vec![conn_unknown],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert_eq!(outcome.needs_driver.len(), 1);
        assert_eq!(outcome.needs_driver[0].0, "UnknownConn");
        assert_eq!(outcome.needs_driver[0].1, "totally-unknown-driver");
        assert!(outcome.succeeded.is_empty());
        assert!(deps.conn_names.is_empty());
    }

    #[test]
    fn persist_known_driver_inserts_connection_into_succeeded() {
        let mut deps = FakePersistence::with_drivers(&["postgres"]);
        let conn = make_conn_profile("ProdPG", "postgres");

        let actions = ImportActions {
            connections: vec![conn],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(outcome.succeeded[0], "ProdPG");
        assert!(outcome.needs_driver.is_empty());
    }

    #[test]
    fn persist_secret_write_failure_is_recorded_not_silently_lost() {
        let mut deps =
            FakePersistence::all_drivers().with_keyring_failure("dbflux:conn:aaaa-bbbb:password");

        let actions = ImportActions {
            connections: vec![],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![(
                "dbflux:conn:aaaa-bbbb:password".to_string(),
                SecretString::from("s3cr3t".to_string()),
            )],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert_eq!(outcome.secret_failures.len(), 1);
        assert_eq!(
            outcome.secret_failures[0].1,
            "dbflux:conn:aaaa-bbbb:password"
        );
    }

    #[test]
    fn persist_successful_secret_write_does_not_appear_in_failures() {
        let mut deps = FakePersistence::all_drivers();

        let actions = ImportActions {
            connections: vec![],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![(
                "dbflux:conn:1111-2222:password".to_string(),
                SecretString::from("ok".to_string()),
            )],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert!(outcome.secret_failures.is_empty());
    }

    #[test]
    fn persist_reuse_does_not_insert_new_entity_and_unknown_driver_does_not_block_others() {
        let mut deps = FakePersistence::with_drivers(&["postgres"]);

        let conn_known = make_conn_profile("KnownConn", "postgres");
        let conn_unknown = make_conn_profile("UnknownConn", "some-external-driver");

        let actions = ImportActions {
            connections: vec![conn_known, conn_unknown],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(outcome.needs_driver.len(), 1);
        assert_eq!(deps.conn_names.len(), 1);
        assert_eq!(deps.conn_names[0], "KnownConn");
    }

    /// `persist_import_actions` must carry the `DbConfig::External` variant
    /// (carrying form values) to `add_connection` so the app-layer rebuild can
    /// call `build_config(values)` with the real driver rather than
    /// `extract_values(placeholder_config)`.
    ///
    /// This test verifies that a connection profile emitted by the portability
    /// crate (with `DbConfig::External`) arrives at the persistence seam with
    /// `driver_id()` intact — not silently rewritten to `"postgres"`.
    #[test]
    fn persist_connection_driver_id_is_preserved_through_seam() {
        struct DriverIdRecorder {
            recorded_driver_ids: Vec<String>,
            recorded_config_is_external: Vec<bool>,
        }

        impl ImportPersistence for DriverIdRecorder {
            fn add_auth_profile(&mut self, _: AuthProfile) {}
            fn add_ssh_tunnel(&mut self, _: SshTunnelProfile) {}
            fn add_proxy(&mut self, _: ProxyProfile) {}

            fn add_connection(&mut self, profile: ConnectionProfile) -> ConnectionInsertResult {
                self.recorded_driver_ids
                    .push(profile.driver_id().to_string());
                let is_external = matches!(profile.config, dbflux_core::DbConfig::External { .. });
                self.recorded_config_is_external.push(is_external);
                ConnectionInsertResult::Ok
            }

            fn write_secret(&self, _: &str, _: &SecretString) -> bool {
                true
            }

            fn hydrate_auth_secret_fields(
                &mut self,
                _auth_id: uuid::Uuid,
                _fields: HashMap<String, SecretString>,
            ) {
            }
        }

        let mut recorder = DriverIdRecorder {
            recorded_driver_ids: Vec::new(),
            recorded_config_is_external: Vec::new(),
        };

        let mut mysql_profile = ConnectionProfile::new(
            "MySQL Prod",
            dbflux_core::DbConfig::External {
                kind: dbflux_core::DbKind::SQLite,
                values: {
                    let mut v = FormValues::default();
                    v.insert("host".to_string(), "mysql.example.com".to_string());
                    v.insert("port".to_string(), "3306".to_string());
                    v
                },
            },
        );
        mysql_profile.driver_id = Some("mysql".to_string());

        let actions = ImportActions {
            connections: vec![mysql_profile],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        persist_import_actions(actions, &mut recorder);

        assert_eq!(recorder.recorded_driver_ids.len(), 1);
        assert_eq!(
            recorder.recorded_driver_ids[0], "mysql",
            "driver_id must arrive at the persistence seam as 'mysql', \
             not rewritten to 'postgres'"
        );
        assert!(
            recorder.recorded_config_is_external[0],
            "config must be DbConfig::External so the app layer can call \
             build_config(values) with the real driver"
        );
    }

    // -----------------------------------------------------------------------
    // Finding #4 — build_config failure is recorded, not silently persisted
    // -----------------------------------------------------------------------

    /// A persistence implementation that reports ConfigFailed for the "bad-driver" id.
    struct FailingConfigPersistence {
        inserted_names: Vec<String>,
    }

    impl ImportPersistence for FailingConfigPersistence {
        fn add_auth_profile(&mut self, _: AuthProfile) {}
        fn add_ssh_tunnel(&mut self, _: SshTunnelProfile) {}
        fn add_proxy(&mut self, _: ProxyProfile) {}

        fn add_connection(&mut self, profile: ConnectionProfile) -> ConnectionInsertResult {
            let driver_id = profile.driver_id().to_string();
            if driver_id == "bad-driver" {
                return ConnectionInsertResult::ConfigFailed(
                    "required field 'host' is missing".to_string(),
                );
            }
            self.inserted_names.push(profile.name.clone());
            ConnectionInsertResult::Ok
        }

        fn write_secret(&self, _: &str, _: &SecretString) -> bool {
            true
        }

        fn hydrate_auth_secret_fields(
            &mut self,
            _auth_id: uuid::Uuid,
            _fields: HashMap<String, SecretString>,
        ) {
        }
    }

    #[test]
    fn persist_config_failure_is_not_persisted_and_not_counted_as_succeeded() {
        let mut deps = FailingConfigPersistence {
            inserted_names: Vec::new(),
        };

        let mut profile = make_conn_profile("BadConfig", "bad-driver");
        profile.id = Uuid::parse_str("aaaaaaaa-0000-0000-0000-000000000001").unwrap();

        let actions = ImportActions {
            connections: vec![profile],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        assert!(
            outcome.succeeded.is_empty(),
            "config-failure connection must NOT appear in succeeded"
        );
        assert!(
            outcome.needs_driver.is_empty(),
            "config-failure must NOT appear in needs_driver"
        );
        assert_eq!(
            outcome.config_failures.len(),
            1,
            "config-failure must be recorded in config_failures"
        );
        assert_eq!(outcome.config_failures[0].0, "BadConfig");
        assert!(deps.inserted_names.is_empty(), "must NOT be persisted");
    }

    // -----------------------------------------------------------------------
    // Finding #8 — skipped connection secrets are not written to keyring
    // -----------------------------------------------------------------------

    /// Mutable-tracking version for this test.
    struct TrackingPersistence {
        registered: std::collections::HashSet<String>,
        written_secret_refs: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ImportPersistence for TrackingPersistence {
        fn add_auth_profile(&mut self, _: AuthProfile) {}
        fn add_ssh_tunnel(&mut self, _: SshTunnelProfile) {}
        fn add_proxy(&mut self, _: ProxyProfile) {}

        fn add_connection(&mut self, profile: ConnectionProfile) -> ConnectionInsertResult {
            if !self.registered.contains(&profile.driver_id().to_string()) {
                ConnectionInsertResult::NeedsDriver
            } else {
                ConnectionInsertResult::Ok
            }
        }

        fn write_secret(&self, secret_ref: &str, _: &SecretString) -> bool {
            self.written_secret_refs
                .lock()
                .unwrap()
                .push(secret_ref.to_string());
            true
        }

        fn hydrate_auth_secret_fields(
            &mut self,
            _auth_id: uuid::Uuid,
            _fields: HashMap<String, SecretString>,
        ) {
        }
    }

    #[test]
    fn persist_skipped_connection_secrets_are_not_written() {
        use dbflux_core::connection_secret_ref;

        let skipped_id = Uuid::parse_str("bbbbbbbb-0000-0000-0000-000000000002").unwrap();
        let ok_id = Uuid::parse_str("cccccccc-0000-0000-0000-000000000003").unwrap();

        let mut skipped_conn = make_conn_profile("SkippedConn", "unknown-driver");
        skipped_conn.id = skipped_id;

        let mut ok_conn = make_conn_profile("OkConn", "postgres");
        ok_conn.id = ok_id;

        let skipped_secret_ref = connection_secret_ref(&skipped_id);
        let ok_secret_ref = connection_secret_ref(&ok_id);

        let written = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let mut deps = TrackingPersistence {
            registered: ["postgres".to_string()].into_iter().collect(),
            written_secret_refs: written.clone(),
        };

        let actions = ImportActions {
            connections: vec![skipped_conn, ok_conn],
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![
                (
                    skipped_secret_ref.clone(),
                    SecretString::from("skipped-secret".to_string()),
                ),
                (
                    ok_secret_ref.clone(),
                    SecretString::from("ok-secret".to_string()),
                ),
            ],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let outcome = persist_import_actions(actions, &mut deps);

        let written_refs = written.lock().unwrap();

        assert!(
            !written_refs.contains(&skipped_secret_ref),
            "secret for skipped connection must NOT be written to keyring"
        );
        assert!(
            written_refs.contains(&ok_secret_ref),
            "secret for successfully persisted connection must be written to keyring"
        );
        assert_eq!(outcome.needs_driver.len(), 1);
        assert_eq!(outcome.succeeded.len(), 1);
    }

    // --- Phase 2.8: ExportReport::skipped_connections for unregistered drivers (R-ROB-2 / M5) ---

    #[test]
    fn export_report_skipped_connections_can_be_populated() {
        let mut report = dbflux_portability::ExportReport::default();

        report
            .skipped_connections
            .push(("Prod DB".to_string(), "unknown-driver".to_string()));

        assert_eq!(
            report.skipped_connections.len(),
            1,
            "skipped_connections must be recordable"
        );
        assert_eq!(report.skipped_connections[0].0, "Prod DB");
        assert_eq!(report.skipped_connections[0].1, "unknown-driver");
        assert!(
            report.warnings.is_empty(),
            "skipped connection must not also appear as a warning unless explicitly added"
        );
    }

    // -----------------------------------------------------------------------
    // Phase 4.1 — mapto_candidates (M7 / R-WIZ-3)
    // -----------------------------------------------------------------------

    #[test]
    fn mapto_candidates_auth_profile_returns_all_dest_auth_profiles() {
        let auth1 = make_auth_profile(Uuid::new_v4());
        let auth2 = make_auth_profile(Uuid::new_v4());

        let dest = OwnedDestSnapshot {
            auth_profiles: vec![auth1.clone(), auth2.clone()],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let candidates = super::mapto_candidates(dbflux_portability::ConflictKind::AuthProfile, &dest);

        assert_eq!(candidates.len(), 2, "must return all auth profiles");
        let ids: Vec<_> = candidates.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&auth1.id));
        assert!(ids.contains(&auth2.id));
    }

    #[test]
    fn mapto_candidates_ssh_tunnel_returns_all_dest_tunnels() {
        let ssh1 = make_ssh_tunnel(Uuid::new_v4());
        let ssh2 = make_ssh_tunnel(Uuid::new_v4());

        let dest = OwnedDestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![ssh1.clone(), ssh2.clone()],
            proxies: vec![],
            connections: vec![],
        };

        let candidates = super::mapto_candidates(dbflux_portability::ConflictKind::SshTunnel, &dest);

        assert_eq!(candidates.len(), 2);
        let ids: Vec<_> = candidates.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&ssh1.id));
        assert!(ids.contains(&ssh2.id));
    }

    #[test]
    fn mapto_candidates_empty_dest_returns_empty_list() {
        let dest = OwnedDestSnapshot {
            auth_profiles: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            connections: vec![],
        };

        let candidates = super::mapto_candidates(dbflux_portability::ConflictKind::AuthProfile, &dest);
        assert!(
            candidates.is_empty(),
            "empty dest must yield empty candidate list"
        );
    }

    // -----------------------------------------------------------------------
    // Phase 4.2 — ConfirmSummary (C3 / R-WIZ-1)
    // -----------------------------------------------------------------------

    #[test]
    fn confirm_summary_counts_bundle_entities() {
        use dbflux_portability::bundle::{
            AuthEntry, Bundle, BundleMeta, CURRENT_FORMAT_VERSION, ConnectionEntry, EncryptionMode,
            SshEntry, SshAuthMethodKind,
        };
        use dbflux_portability::ParsedBundle;

        let bundle = Bundle {
            bundle: BundleMeta {
                format_version: CURRENT_FORMAT_VERSION,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                dbflux_version: "0.7.0-dev.0".to_string(),
                encryption: EncryptionMode::None,
            },
            drivers: vec![],
            connections: vec![
                ConnectionEntry {
                    local_id: "c1".to_string(),
                    name: "Conn1".to_string(),
                    driver_id: "postgres".to_string(),
                    fields: Default::default(),
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
                    kind: Some("Postgres".to_string()),
                    uri_secret_fields: vec![],
                },
                ConnectionEntry {
                    local_id: "c2".to_string(),
                    name: "Conn2".to_string(),
                    driver_id: "unknown-driver".to_string(),
                    fields: Default::default(),
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
                    kind: Some("MongoDB".to_string()),
                    uri_secret_fields: vec![],
                },
            ],
            auth_profiles: vec![AuthEntry {
                local_id: "a1".to_string(),
                name: "MySSO".to_string(),
                provider_id: "aws-sso".to_string(),
                enabled: true,
                fields: Default::default(),
                secret_field_names: vec![],
                required_refs: vec![],
            }],
            ssh_tunnels: vec![SshEntry {
                local_id: "s1".to_string(),
                name: "Bastion".to_string(),
                host: "bastion.example.com".to_string(),
                port: 22,
                user: "ec2-user".to_string(),
                auth_method: SshAuthMethodKind::Password,
                key_embedded: false,
                required_refs: vec![],
            }],
            proxies: vec![],
            secrets: None,
        };

        let parsed = ParsedBundle {
            bundle,
            decrypted_secrets: None,
        };

        let plan = dbflux_portability::ImportPlan {
            conflicts: vec![],
            required_resolutions: vec![],
        };

        let registered: std::collections::HashSet<String> = ["postgres".to_string()].into_iter().collect();
        let summary = super::confirm_summary(&parsed, &plan, &registered);

        assert_eq!(summary.connection_count, 2);
        assert_eq!(summary.auth_profile_count, 1);
        assert_eq!(summary.ssh_tunnel_count, 1);
        assert_eq!(summary.proxy_count, 0);
        assert_eq!(summary.conflict_count, 0);
        assert_eq!(summary.required_resolution_count, 0);
        assert!(
            summary.has_driver_not_installed,
            "unknown-driver must trigger has_driver_not_installed"
        );
    }

    // -----------------------------------------------------------------------
    // Phase 4.3 — H3: hydrate_auth_secret_fields (R-INT-2)
    // -----------------------------------------------------------------------

    #[test]
    fn hydrate_auth_secret_fields_is_called_for_new_auth_profiles() {
        // After persist_import_actions, the hydrate_auth_secret_fields seam must
        // be called for newly created auth profiles (H3 / ADR-4 / R-INT-2).
        struct HydrationTracker {
            hydrated: HashMap<uuid::Uuid, HashMap<String, SecretString>>,
        }

        impl ImportPersistence for HydrationTracker {
            fn add_auth_profile(&mut self, _p: AuthProfile) {}
            fn add_ssh_tunnel(&mut self, _: SshTunnelProfile) {}
            fn add_proxy(&mut self, _: ProxyProfile) {}
            fn add_connection(&mut self, _: ConnectionProfile) -> ConnectionInsertResult {
                ConnectionInsertResult::Ok
            }
            fn write_secret(&self, _: &str, _: &SecretString) -> bool {
                true
            }
            fn hydrate_auth_secret_fields(
                &mut self,
                auth_id: uuid::Uuid,
                fields: HashMap<String, SecretString>,
            ) {
                self.hydrated.insert(auth_id, fields);
            }
        }

        let auth_id = Uuid::new_v4();
        let auth_secret_ref = dbflux_core::auth_field_secret_ref(&auth_id, "token");

        let auth = AuthProfile {
            id: auth_id,
            name: "SSO Profile".to_string(),
            provider_id: "aws-sso".to_string(),
            fields: HashMap::new(),
            secret_fields: HashMap::new(),
            enabled: true,
            read_only: false,
            dangling_origin: None,
        };

        let actions = ImportActions {
            auth_profiles: vec![auth],
            connections: vec![],
            ssh_tunnels: vec![],
            proxies: vec![],
            secret_writes: vec![(
                auth_secret_ref.clone(),
                SecretString::from("hydrated-secret".to_string()),
            )],
            kind_failures: vec![],
            unresolved_ref_connections: vec![],
        };

        let mut tracker = HydrationTracker {
            hydrated: HashMap::new(),
        };

        persist_import_actions(actions, &mut tracker);

        assert!(
            tracker.hydrated.contains_key(&auth_id),
            "hydrate_auth_secret_fields must be called for the new auth profile"
        );
        let fields = &tracker.hydrated[&auth_id];
        assert!(
            fields.contains_key("token"),
            "the token field must be hydrated; got: {:?}",
            fields.keys().collect::<Vec<_>>()
        );
    }
}
