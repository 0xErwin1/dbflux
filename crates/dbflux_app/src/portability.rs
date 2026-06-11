//! Connection-export bridge between `dbflux_portability` and `AppState`.
//!
//! This module owns the two seam implementations that the portability crate
//! requires from the app layer — `FieldHintResolver` and `SecretReader` — and
//! the function that assembles an `ExportGraph` from AppState data.
//!
//! # Testability contract
//!
//! All public items in this module depend only on plain data or on trait objects
//! that can be satisfied by fakes in unit tests.  No GPUI `Entity` or `Context`
//! types appear here, which is what allows the `dbflux_app` test binary to
//! compile and run these tests (unlike `dbflux_ui_windows`, whose GPUI proc-macro
//! expansion causes rustc to SIGSEGV during test-binary compilation).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    AuthProfile, ConnectionProfile, DbDriver, ExportFieldHint, FormValues, ProxyProfile,
    SecretStore, SshTunnelProfile,
};
use dbflux_portability::{
    AwsRef, ConnectionWithValues, ExportGraph, FieldHintResolver, SecretReader,
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
}
