mod global_scope;

use super::*;

use dbflux_core::auth::{AuthProfile, AuthSessionState, UrlCallback};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_storage::repositories::connection_profile_hook_bindings::ConnectionProfileHookBindingDto;
use dbflux_storage::repositories::connection_profile_hooks::ConnectionProfileHookDto;
use dbflux_storage::repositories::connection_profiles::ConnectionProfileDto;
use dbflux_storage::repositories::hook_definitions::HookDefinitionDto;
use dbflux_storage::repositories::services::ServiceDto;
use dbflux_test_support::{
    FakeAuthProviderRpcConfig, FakeAuthProviderRpcServer, FakeAuthRpcResult,
};

#[test]
#[cfg(feature = "clickhouse")]
fn clickhouse_is_available_to_mcp_when_feature_enabled() {
    assert_eq!(
        str_to_db_kind("ClickHouse"),
        Some(dbflux_core::DbKind::ClickHouse)
    );

    let registry = build_driver_registry();
    let driver = registry
        .get("clickhouse")
        .expect("clickhouse driver must be registered");
    assert_eq!(driver.driver_key(), "builtin:clickhouse");
}

struct ReflectingAuthProvider {
    provider_id: &'static str,
    reflected: Vec<AuthProfile>,
}

#[async_trait::async_trait]
impl DynAuthProvider for ReflectingAuthProvider {
    fn provider_id(&self) -> &str {
        self.provider_id
    }

    fn display_name(&self) -> &str {
        "Reflecting Test Provider"
    }

    fn form_def(&self) -> &dbflux_core::AuthFormDef {
        static FORM: std::sync::OnceLock<dbflux_core::AuthFormDef> = std::sync::OnceLock::new();
        FORM.get_or_init(|| dbflux_core::AuthFormDef { tabs: vec![] })
    }

    async fn validate_session(
        &self,
        _profile: &AuthProfile,
    ) -> Result<AuthSessionState, dbflux_core::DbError> {
        Ok(AuthSessionState::LoginRequired)
    }

    async fn login(
        &self,
        _profile: &AuthProfile,
        _url_callback: UrlCallback,
    ) -> Result<dbflux_core::auth::AuthSession, dbflux_core::DbError> {
        Err(dbflux_core::DbError::connection_failed("not supported"))
    }

    async fn resolve_credentials(
        &self,
        _profile: &AuthProfile,
    ) -> Result<dbflux_core::auth::ResolvedCredentials, dbflux_core::DbError> {
        Ok(dbflux_core::auth::ResolvedCredentials::default())
    }

    fn reflect_profiles(&self) -> Vec<AuthProfile> {
        self.reflected.clone()
    }
}

fn reflecting_registry(
    provider_id: &'static str,
    reflected: Vec<AuthProfile>,
) -> HashMap<String, Arc<dyn DynAuthProvider>> {
    HashMap::from([(
        provider_id.to_string(),
        Arc::new(ReflectingAuthProvider {
            provider_id,
            reflected,
        }) as Arc<dyn DynAuthProvider>,
    )])
}

#[test]
fn auth_profile_manager_includes_reflected_profile_without_stored_row() {
    let mut reflected = AuthProfile::new("dev", "aws-sso", HashMap::new());
    reflected.read_only = true;
    let reflected_id = reflected.id;
    let registry = reflecting_registry("aws-sso", vec![reflected]);

    let manager = build_auth_profile_manager(Vec::new(), &registry);

    assert!(
        manager
            .items
            .iter()
            .any(|profile| profile.id == reflected_id)
    );
}

#[test]
fn auth_profile_manager_replaces_stored_row_of_reflecting_provider() {
    let mut reflected = AuthProfile::new("dev", "aws-sso", HashMap::new());
    reflected.read_only = true;
    let reflected_id = reflected.id;
    let registry = reflecting_registry("aws-sso", vec![reflected]);

    let stale_stored = AuthProfile::new("dev", "aws-sso", HashMap::new());
    let other_stored = AuthProfile::new("rpc", "rpc-auth", HashMap::new());
    let other_stored_id = other_stored.id;

    let manager = build_auth_profile_manager(vec![stale_stored, other_stored], &registry);

    let ids: Vec<_> = manager.items.iter().map(|profile| profile.id).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&reflected_id));
    assert!(ids.contains(&other_stored_id));
}

fn temp_runtime() -> (tempfile::TempDir, StorageRuntime) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let runtime = StorageRuntime::for_path(temp_dir.path().join("dbflux.db")).expect("runtime");
    (temp_dir, runtime)
}

fn insert_profile(runtime: &StorageRuntime, name: &str) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    let mut dto = ConnectionProfileDto::new(id, name.to_string());
    dto.driver_id = Some("postgres".to_string());
    dto.kind = Some("Postgres".to_string());
    runtime
        .connection_profiles()
        .insert(&dto)
        .expect("insert test profile");
    id
}

#[test]
fn load_profiles_keeps_inline_hooks_and_resolves_global_hook_bindings() {
    let (_temp_dir, runtime) = temp_runtime();
    let inline_profile_id = insert_profile(&runtime, "inline");
    let bound_profile_id = insert_profile(&runtime, "bound");
    let mixed_profile_id = insert_profile(&runtime, "mixed");

    let profiles_repo = runtime.connection_profiles();
    let inline_hook = ConnectionProfileHookDto::new_command(
        inline_profile_id.to_string(),
        "pre_connect".to_string(),
        0,
        "refresh-token".to_string(),
    );
    profiles_repo
        .hooks()
        .insert(&inline_hook)
        .expect("insert inline hook");
    profiles_repo
        .hook_args()
        .insert_batch(&inline_hook.id, &["--renew".to_string()])
        .expect("insert inline hook args");
    profiles_repo
        .hook_envs()
        .insert_batch(
            &inline_hook.id,
            &HashMap::from([("TOKEN_SOURCE".to_string(), "keyring".to_string())]),
        )
        .expect("insert inline hook environment");
    let mixed_inline_hook = ConnectionProfileHookDto::new_command(
        mixed_profile_id.to_string(),
        "pre_connect".to_string(),
        0,
        "inline-refresh".to_string(),
    );
    profiles_repo
        .hooks()
        .insert(&mixed_inline_hook)
        .expect("insert mixed inline hook");

    let global_hook_id = uuid::Uuid::new_v4().to_string();
    let global_hook = dbflux_core::ConnectionHook {
        enabled: true,
        kind: dbflux_core::HookKind::Command {
            command: "vpn-connect".to_string(),
            args: vec!["--profile".to_string(), "work".to_string()],
        },
        cwd: None,
        env: HashMap::from([("VPN_MODE".to_string(), "work".to_string())]),
        inherit_env: true,
        env_denylist: vec!["DBFLUX_PASSWORD".to_string()],
        timeout_ms: Some(5_000),
        execution_mode: dbflux_core::HookExecutionMode::Blocking,
        ready_signal: None,
        on_failure: dbflux_core::HookFailureMode::Disconnect,
    };
    let mut definition = HookDefinitionDto::new(
        uuid::Uuid::parse_str(&global_hook_id).expect("global hook id"),
        "Connect VPN".to_string(),
        "Blocking".to_string(),
    );
    definition.kind_json = Some(serde_json::to_string(&global_hook.kind).expect("hook JSON"));
    definition.env_denylist = global_hook.env_denylist.clone();
    definition.timeout_ms = global_hook.timeout_ms.map(|value| value as i64);
    definition.on_failure = "Disconnect".to_string();
    runtime
        .hook_definitions()
        .insert(&definition)
        .expect("insert global hook");
    runtime
        .hook_definitions()
        .set_env(&global_hook_id, &global_hook.env)
        .expect("insert global hook environment");
    profiles_repo
        .hook_bindings()
        .insert(&ConnectionProfileHookBindingDto::new(
            bound_profile_id.to_string(),
            global_hook_id.clone(),
            "post_connect".to_string(),
            0,
        ))
        .expect("insert hook binding");
    profiles_repo
        .hook_bindings()
        .insert(&ConnectionProfileHookBindingDto::new(
            mixed_profile_id.to_string(),
            global_hook_id,
            "pre_connect".to_string(),
            0,
        ))
        .expect("insert mixed profile hook binding");

    let (profiles, profile_load_errors) = load_profiles(&runtime).expect("load profiles");
    assert!(profile_load_errors.is_empty());
    let inline_profile = profiles
        .iter()
        .find(|profile| profile.id == inline_profile_id)
        .expect("inline profile loaded");
    let inline_hook = inline_profile
        .hooks
        .as_ref()
        .expect("inline hooks loaded")
        .pre_connect
        .first()
        .expect("inline pre-connect hook loaded");
    match &inline_hook.kind {
        dbflux_core::HookKind::Command { command, args } => {
            assert_eq!(command, "refresh-token");
            assert_eq!(args, &["--renew"]);
        }
        other => panic!("expected command hook, got {other:?}"),
    }
    assert_eq!(
        inline_hook.env.get("TOKEN_SOURCE").map(String::as_str),
        Some("keyring")
    );

    let bound_profile = profiles
        .iter()
        .find(|profile| profile.id == bound_profile_id)
        .expect("bound profile loaded");
    let bound_hook = bound_profile
        .hooks
        .as_ref()
        .expect("resolved global hook loaded")
        .post_connect
        .first()
        .expect("post-connect binding resolved");
    match &bound_hook.kind {
        dbflux_core::HookKind::Command { command, args } => {
            assert_eq!(command, "vpn-connect");
            assert_eq!(args, &["--profile", "work"]);
        }
        other => panic!("expected command hook, got {other:?}"),
    }
    assert_eq!(
        bound_hook.env.get("VPN_MODE").map(String::as_str),
        Some("work")
    );
    assert_eq!(bound_hook.timeout_ms, Some(5_000));
    assert_eq!(bound_hook.env_denylist, ["DBFLUX_PASSWORD"]);

    let mixed_profile = profiles
        .iter()
        .find(|profile| profile.id == mixed_profile_id)
        .expect("mixed profile loaded");
    let mixed_hooks = mixed_profile.hooks.as_ref().expect("mixed hooks loaded");
    assert_eq!(mixed_hooks.pre_connect.len(), 1);
    assert!(matches!(
        &mixed_hooks.pre_connect[0].kind,
        dbflux_core::HookKind::Command { command, .. } if command == "vpn-connect"
    ));
}

#[test]
fn load_profiles_scopes_invalid_hook_errors_to_affected_profile() {
    let (_temp_dir, runtime) = temp_runtime();
    let invalid_profile_id = insert_profile(&runtime, "invalid-hook");
    let valid_profile_id = insert_profile(&runtime, "valid-profile");
    let invalid_hook = ConnectionProfileHookDto::new_command(
        invalid_profile_id.to_string(),
        "unknown_phase".to_string(),
        0,
        "will-not-run".to_string(),
    );
    runtime
        .connection_profiles()
        .hooks()
        .insert(&invalid_hook)
        .expect("insert malformed hook");

    let (profiles, profile_load_errors) = load_profiles(&runtime).expect("load profiles");

    assert_eq!(profiles.len(), 2);
    assert!(
        profiles
            .iter()
            .any(|profile| profile.id == valid_profile_id)
    );
    assert!(
        profiles
            .iter()
            .any(|profile| profile.id == invalid_profile_id)
    );
    assert!(
        profile_load_errors
            .get(&invalid_profile_id)
            .is_some_and(|error| error.contains("unknown phase"))
    );
    assert!(!profile_load_errors.contains_key(&valid_profile_id));
}

fn insert_auth_provider_service(runtime: &StorageRuntime, socket_id: &str) {
    let repo = runtime.services();

    repo.insert(&ServiceDto {
        socket_id: socket_id.to_string(),
        enabled: true,
        command: None,
        startup_timeout_ms: None,
        service_kind: "auth_provider".to_string(),
        api_family: Some("auth_provider_rpc".to_string()),
        api_major: Some(1),
        api_minor: Some(0),
        created_at: String::new(),
        updated_at: String::new(),
    })
    .expect("insert service");
}

#[tokio::test]
async fn build_auth_provider_registry_loads_persisted_external_provider() {
    let (_temp_dir, runtime) = temp_runtime();
    insert_auth_provider_service(&runtime, "mcp-auth");

    let profile = AuthProfile::new("rpc profile", "rpc-auth", HashMap::new());
    let profile_id = profile.id;

    let mut credentials = dbflux_ipc::ResolvedCredentialsDto {
        fields: HashMap::new(),
        secret_fields: HashMap::new(),
    };
    credentials
        .fields
        .insert("region".to_string(), "us-east-1".to_string());
    credentials
        .secret_fields
        .insert("token".to_string(), "secret-token".to_string());

    let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
        socket_id: "mcp-auth".to_string(),
        provider_id: "rpc-auth".to_string(),
        display_name: "RPC Auth".to_string(),
        form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
        capabilities: dbflux_core::auth::AuthProviderCapabilities {
            login: dbflux_core::auth::AuthProviderLoginCapabilities {
                supported: true,
                verification_url_progress: true,
            },
            edit: None,
        },
        supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
        expected_connections: 5,
        validate_session: FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
        validate_session_sequence: Vec::new(),
        login_progress: Some(Some("https://verify.example".to_string())),
        login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
            provider_id: "rpc-auth".to_string(),
            profile_id,
            expires_at: None,
            session_data: None,
        }),
        resolve_credentials: FakeAuthRpcResult::Ok(credentials),
        fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "fetch_dynamic_options not configured".to_string(),
            retriable: false,
        }),
        expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty()),
    })
    .expect("start fake auth provider server");

    let direct_probe = dbflux_ipc::RpcAuthProvider::probe("mcp-auth", None)
        .expect("direct probe should succeed before registry build");
    assert_eq!(direct_probe.provider_id(), "rpc-auth");

    let services = dbflux_storage::load_service_configs(&runtime);
    let registry = build_auth_provider_registry(&services);
    let provider = registry.get("rpc-auth").expect("registered provider");

    let state = provider
        .validate_session(&profile)
        .await
        .expect("validate session");
    assert!(matches!(state, AuthSessionState::Valid { .. }));

    let callback_url = Arc::new(std::sync::Mutex::new(None));
    let callback_url_for_login = callback_url.clone();
    let session = provider
        .login(
            &profile,
            Box::new(move |url| {
                *callback_url_for_login.lock().expect("callback lock") = url;
            }) as UrlCallback,
        )
        .await
        .expect("login");

    assert_eq!(session.provider_id, "rpc-auth");
    assert_eq!(
        callback_url.lock().expect("callback lock").as_deref(),
        Some("https://verify.example")
    );

    let resolved = provider
        .resolve_credentials(&profile)
        .await
        .expect("resolve credentials");
    assert_eq!(
        resolved.fields.get("region").map(String::as_str),
        Some("us-east-1")
    );
    assert_eq!(
        resolved
            .secret_fields
            .get("token")
            .map(|value| value.expose_secret()),
        Some("secret-token")
    );

    server.wait().expect("fake server join");
}

#[test]
fn build_auth_provider_registry_skips_incompatible_persisted_service() {
    let mut services = vec![ServiceConfig {
        socket_id: "bad-auth".to_string(),
        enabled: true,
        command: None,
        args: Vec::new(),
        env: HashMap::new(),
        startup_timeout_ms: None,
        kind: RpcServiceKind::AuthProvider,
        api_contract: Some(dbflux_core::ServiceRpcApiContract::new("driver_rpc", 1, 1)),
    }];

    let registry = build_auth_provider_registry(&services);
    assert!(!registry.contains_key("bad-auth"));

    services[0].api_contract = Some(dbflux_core::ServiceRpcApiContract::new(
        "auth_provider_rpc",
        2,
        0,
    ));

    let registry = build_auth_provider_registry(&services);
    assert!(!registry.contains_key("bad-auth"));
}

fn unreachable_service(socket_id: &str, kind: RpcServiceKind) -> ServiceConfig {
    ServiceConfig {
        socket_id: socket_id.to_string(),
        enabled: true,
        command: None,
        args: Vec::new(),
        env: HashMap::new(),
        startup_timeout_ms: None,
        kind,
        api_contract: None,
    }
}

#[test]
fn secret_provider_service_is_not_treated_as_auth_provider() {
    let socket_id = format!("mcp-secret-{}", uuid::Uuid::new_v4().simple());
    let services = vec![unreachable_service(
        &socket_id,
        RpcServiceKind::SecretProvider,
    )];

    let auth_registry = build_auth_provider_registry(&services);
    let external_auth_ids: Vec<&String> = auth_registry
        .keys()
        .filter(|provider_id| !provider_id.starts_with("aws-"))
        .collect();

    assert!(
        external_auth_ids.is_empty(),
        "secret-provider services must never be probed as auth providers: {external_auth_ids:?}"
    );
}

#[test]
fn auth_and_driver_services_are_not_treated_as_secret_providers() {
    let services = vec![
        unreachable_service("mcp-auth-only", RpcServiceKind::AuthProvider),
        unreachable_service("mcp-driver-only", RpcServiceKind::Driver),
    ];

    assert!(build_secret_provider_registry(&services).is_empty());
}

#[test]
fn build_secret_provider_registry_skips_incompatible_and_unreachable_services() {
    let mut incompatible = unreachable_service("bad-secret", RpcServiceKind::SecretProvider);
    incompatible.api_contract = Some(dbflux_core::ServiceRpcApiContract::new(
        "auth_provider_rpc",
        1,
        0,
    ));
    let unreachable = unreachable_service(
        &format!("mcp-secret-{}", uuid::Uuid::new_v4().simple()),
        RpcServiceKind::SecretProvider,
    );

    assert!(validate_secret_provider_contract(&incompatible).is_err());
    assert!(build_secret_provider_registry(&[incompatible, unreachable]).is_empty());
}

#[tokio::test]
async fn rpc_auth_provider_login_without_progress_reports_none_callback() {
    let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
        socket_id: "no-progress-auth".to_string(),
        provider_id: "rpc-auth".to_string(),
        display_name: "RPC Auth".to_string(),
        form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
        capabilities: dbflux_core::auth::AuthProviderCapabilities {
            login: dbflux_core::auth::AuthProviderLoginCapabilities {
                supported: true,
                verification_url_progress: false,
            },
            edit: None,
        },
        supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
        expected_connections: 2,
        validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
        validate_session_sequence: Vec::new(),
        login_progress: None,
        login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
            provider_id: "rpc-auth".to_string(),
            profile_id: uuid::Uuid::nil(),
            expires_at: None,
            session_data: None,
        }),
        resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "unused".to_string(),
            retriable: false,
        }),
        fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "fetch_dynamic_options not configured".to_string(),
            retriable: false,
        }),
        expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty()),
    })
    .expect("start fake auth provider server");

    let provider = dbflux_ipc::RpcAuthProvider::probe("no-progress-auth", None)
        .expect("probe provider without progress url");
    let profile = AuthProfile::new("rpc profile", "rpc-auth", HashMap::new());

    let callback_url = Arc::new(std::sync::Mutex::new(Some("unexpected".to_string())));
    let callback_url_for_login = callback_url.clone();

    provider
        .login(
            &profile,
            Box::new(move |url| {
                *callback_url_for_login.lock().expect("callback lock") = url;
            }) as UrlCallback,
        )
        .await
        .expect("login without progress");

    assert!(callback_url.lock().expect("callback lock").is_none());
    server.wait().expect("fake server join");
}

#[tokio::test]
async fn rpc_auth_provider_login_refreshes_session_after_successful_generic_flow() {
    let profile = AuthProfile::new("rpc profile", "rpc-auth", HashMap::new());
    let profile_id = profile.id;

    let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
        socket_id: "refreshing-auth".to_string(),
        provider_id: "rpc-auth".to_string(),
        display_name: "RPC Auth".to_string(),
        form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
        capabilities: dbflux_core::auth::AuthProviderCapabilities {
            login: dbflux_core::auth::AuthProviderLoginCapabilities {
                supported: true,
                verification_url_progress: true,
            },
            edit: None,
        },
        supported_versions: dbflux_ipc::auth_provider_rpc_supported_versions().to_vec(),
        expected_connections: 4,
        validate_session: FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
        validate_session_sequence: vec![
            FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
            FakeAuthRpcResult::Ok(AuthSessionState::Valid { expires_at: None }),
        ],
        login_progress: Some(Some("https://verify.example/device".to_string())),
        login: FakeAuthRpcResult::Ok(dbflux_ipc::AuthSessionDto {
            provider_id: "rpc-auth".to_string(),
            profile_id,
            expires_at: None,
            session_data: None,
        }),
        resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "unused".to_string(),
            retriable: false,
        }),
        fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "fetch_dynamic_options not configured".to_string(),
            retriable: false,
        }),
        expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty()),
    })
    .expect("start refreshing fake auth provider server");

    let provider = dbflux_ipc::RpcAuthProvider::probe("refreshing-auth", None)
        .expect("probe provider with login capability");

    let initial_state = provider
        .validate_session(&profile)
        .await
        .expect("initial validate_session");
    assert!(matches!(initial_state, AuthSessionState::LoginRequired));

    let callback_url = Arc::new(std::sync::Mutex::new(None));
    let callback_url_for_login = callback_url.clone();

    let session = provider
        .login(
            &profile,
            Box::new(move |url| {
                *callback_url_for_login.lock().expect("callback lock") = url;
            }) as UrlCallback,
        )
        .await
        .expect("generic login should succeed");

    assert_eq!(session.provider_id, "rpc-auth");
    assert_eq!(session.profile_id, profile_id);
    assert_eq!(
        callback_url.lock().expect("callback lock").as_deref(),
        Some("https://verify.example/device")
    );

    let refreshed_state = provider
        .validate_session(&profile)
        .await
        .expect("refreshed validate_session");
    assert!(matches!(
        refreshed_state,
        AuthSessionState::Valid { expires_at: None }
    ));

    server.wait().expect("fake server join");
}

#[test]
fn build_auth_provider_registry_skips_version_mismatch_probe_failure() {
    let (_temp_dir, runtime) = temp_runtime();
    insert_auth_provider_service(&runtime, "version-mismatch-auth");

    let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
        socket_id: "version-mismatch-auth".to_string(),
        provider_id: "rpc-auth".to_string(),
        display_name: "RPC Auth".to_string(),
        form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
        capabilities: dbflux_core::auth::AuthProviderCapabilities::default(),
        supported_versions: vec![dbflux_ipc::ProtocolVersion::new(2, 0)],
        expected_connections: 1,
        validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
        validate_session_sequence: Vec::new(),
        login_progress: None,
        login: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "unused".to_string(),
            retriable: false,
        }),
        resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "unused".to_string(),
            retriable: false,
        }),
        fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "fetch_dynamic_options not configured".to_string(),
            retriable: false,
        }),
        expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty()),
    })
    .expect("start mismatch fake auth provider server");

    let services = dbflux_storage::load_service_configs(&runtime);
    let registry = build_auth_provider_registry(&services);

    assert!(!registry.contains_key("rpc-auth"));
    server.wait().expect("fake server join");
}

#[test]
fn build_auth_provider_registry_accepts_legacy_v1_0_auth_provider_probe() {
    let (_temp_dir, runtime) = temp_runtime();
    insert_auth_provider_service(&runtime, "legacy-auth");

    let server = FakeAuthProviderRpcServer::start(FakeAuthProviderRpcConfig {
        socket_id: "legacy-auth".to_string(),
        provider_id: "legacy-rpc-auth".to_string(),
        display_name: "Legacy RPC Auth".to_string(),
        form_definition: dbflux_core::AuthFormDef { tabs: vec![] },
        capabilities: dbflux_core::auth::AuthProviderCapabilities::default(),
        supported_versions: vec![dbflux_ipc::ProtocolVersion::new(1, 0)],
        expected_connections: 1,
        validate_session: FakeAuthRpcResult::Ok(AuthSessionState::LoginRequired),
        validate_session_sequence: Vec::new(),
        login_progress: None,
        login: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "unused".to_string(),
            retriable: false,
        }),
        resolve_credentials: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "unused".to_string(),
            retriable: false,
        }),
        fetch_dynamic_options: FakeAuthRpcResult::Err(dbflux_ipc::AuthProviderRpcError {
            code: dbflux_ipc::auth_provider_protocol::AuthProviderRpcErrorCode::UnsupportedMethod,
            message: "fetch_dynamic_options not configured".to_string(),
            retriable: false,
        }),
        expected_auth_token: std::env::var(dbflux_ipc::AUTH_PROVIDER_RPC_AUTH_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty()),
    })
    .expect("start legacy fake auth provider server");

    let services = dbflux_storage::load_service_configs(&runtime);
    let registry = build_auth_provider_registry(&services);

    let provider = registry
        .get("legacy-rpc-auth")
        .expect("legacy provider must register");
    assert_eq!(provider.provider_id(), "legacy-rpc-auth");
    assert!(!provider.capabilities().login.supported);

    server.wait().expect("fake server join");
}

fn runtime_for_governance_tests(file_name: &str) -> McpRuntime {
    let path = dbflux_audit::temp_sqlite_path(&format!(
        "{}_{}.sqlite",
        file_name.trim_end_matches(".sqlite"),
        uuid::Uuid::new_v4()
    ));
    let audit =
        dbflux_audit::AuditService::new_sqlite(&path).expect("audit service should initialize");
    McpRuntime::new(
        audit,
        Box::new(dbflux_approval::InMemoryPendingExecutionStore::default()),
    )
}

#[test]
fn apply_governance_settings_rejects_client_with_empty_id() {
    use dbflux_core::{GovernanceSettings, TrustedClientConfig};

    let mut runtime = runtime_for_governance_tests("governance-empty-client-id.sqlite");

    let governance = GovernanceSettings {
        mcp_enabled_by_default: false,
        trusted_clients: vec![TrustedClientConfig {
            id: String::new(),
            name: "bad client".to_string(),
            issuer: None,
            active: true,
        }],
        roles: vec![],
        policies: vec![],
    };

    let result = apply_governance_settings(&mut runtime, &governance);
    assert!(result.is_err(), "empty client id must cause an Err");
    assert!(
        result
            .unwrap_err()
            .contains("failed to load trusted client"),
        "error message must identify the failed operation"
    );
}

#[test]
fn apply_governance_settings_rejects_role_with_empty_id() {
    use dbflux_core::{GovernanceSettings, PolicyRoleConfig};

    let mut runtime = runtime_for_governance_tests("governance-empty-role-id.sqlite");

    let governance = GovernanceSettings {
        mcp_enabled_by_default: false,
        trusted_clients: vec![],
        roles: vec![PolicyRoleConfig {
            id: String::new(),
            policy_ids: vec![],
        }],
        policies: vec![],
    };

    let result = apply_governance_settings(&mut runtime, &governance);
    assert!(result.is_err(), "empty role id must cause an Err");
    assert!(
        result.unwrap_err().contains("failed to load role"),
        "error message must identify the failed operation"
    );
}

#[test]
fn apply_governance_settings_rejects_policy_with_empty_id() {
    use dbflux_core::{GovernanceSettings, ToolPolicyConfig};

    let mut runtime = runtime_for_governance_tests("governance-empty-policy-id.sqlite");

    let governance = GovernanceSettings {
        mcp_enabled_by_default: false,
        trusted_clients: vec![],
        roles: vec![],
        policies: vec![ToolPolicyConfig {
            id: String::new(),
            allowed_tools: vec![],
            allowed_classes: vec![],
            approval_classes: vec![],
        }],
    };

    let result = apply_governance_settings(&mut runtime, &governance);
    assert!(result.is_err(), "empty policy id must cause an Err");
    assert!(
        result.unwrap_err().contains("failed to load policy"),
        "error message must identify the failed operation"
    );
}

#[test]
fn apply_governance_settings_accepts_well_formed_entries() {
    use dbflux_core::{
        GovernanceSettings, PolicyRoleConfig, ToolPolicyConfig, TrustedClientConfig,
    };

    let mut runtime = runtime_for_governance_tests("governance-well-formed.sqlite");

    let governance = GovernanceSettings {
        mcp_enabled_by_default: true,
        trusted_clients: vec![TrustedClientConfig {
            id: "claude".to_string(),
            name: "Claude".to_string(),
            issuer: None,
            active: true,
        }],
        roles: vec![PolicyRoleConfig {
            id: "custom/analyst".to_string(),
            policy_ids: vec!["read-only".to_string()],
        }],
        policies: vec![ToolPolicyConfig {
            id: "read-only".to_string(),
            allowed_tools: vec!["select_data".to_string()],
            allowed_classes: vec![],
            approval_classes: vec![],
        }],
    };

    let result = apply_governance_settings(&mut runtime, &governance);
    assert!(
        result.is_ok(),
        "well-formed governance settings must succeed"
    );

    let clients = runtime
        .list_trusted_clients()
        .expect("list trusted clients");
    assert!(clients.iter().any(|c| c.id == "claude"));

    let roles = runtime.list_roles().expect("list roles");
    assert!(roles.iter().any(|r| r.id == "custom/analyst"));

    let policies = runtime.list_policies().expect("list policies");
    assert!(policies.iter().any(|p| p.id == "read-only"));
}

#[test]
fn load_profiles_reads_the_persisted_read_only_flag() {
    let runtime = StorageRuntime::in_memory().expect("in-memory storage runtime");
    let repo = runtime.connection_profiles();

    let locked_id = uuid::Uuid::new_v4();
    let mut locked = dbflux_storage::repositories::connection_profiles::ConnectionProfileDto::new(
        locked_id,
        "locked".to_string(),
    );
    locked.kind = Some("Turso".to_string());
    locked.read_only = true;
    repo.insert(&locked).expect("insert read-only profile");

    let writable_id = uuid::Uuid::new_v4();
    let mut writable = dbflux_storage::repositories::connection_profiles::ConnectionProfileDto::new(
        writable_id,
        "writable".to_string(),
    );
    writable.kind = Some("Turso".to_string());
    repo.insert(&writable).expect("insert writable profile");

    let (profiles, profile_load_errors) = load_profiles(&runtime).expect("load profiles");
    assert!(profile_load_errors.is_empty());
    let find = |id| {
        profiles
            .iter()
            .find(|profile| profile.id == id)
            .expect("loaded profile")
    };

    assert!(find(locked_id).read_only_flag);
    assert!(!find(writable_id).read_only_flag);
}

#[test]
fn turso_headless_config_uses_the_same_empty_url_default() {
    assert_eq!(str_to_db_kind("Turso"), Some(dbflux_core::DbKind::Turso));
    assert!(matches!(
        default_db_config_for_kind(dbflux_core::DbKind::Turso),
        dbflux_core::DbConfig::Turso { ref url } if url.is_empty()
    ));
}
