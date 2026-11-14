use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use dbflux_core::DbError;
use dbflux_core::auth::ResolvedCredentials;
use dbflux_core::values::{DynSecretProvider, ResolveContext};
use interprocess::local_socket::traits::Listener;
use interprocess::local_socket::{ListenerNonblockingMode, ListenerOptions};
use secrecy::{ExposeSecret, SecretString};

use super::client::sanitize_provider_message;
use super::*;
use crate::auth_provider_protocol::ResolvedCredentialsDto;
use crate::envelope::{
    ProtocolVersion, RpcApiFamily, SECRET_PROVIDER_RPC_SUPPORTED_VERSIONS,
    SECRET_PROVIDER_RPC_V1_0, SECRET_PROVIDER_RPC_VERSION, negotiate_highest_mutual_version,
};
use crate::framing;
use crate::socket::secret_provider_socket_name;

const SECRET_VALUE: &str = "s3cr3t-value-never-printed";

fn manifest() -> SecretProviderManifest {
    SecretProviderManifest {
        manifest_version: 1,
        provider_id: "test-vault".to_string(),
        display_name: "Test Vault".to_string(),
        capabilities: SecretProviderCapabilities {
            get_secret: true,
            json_key: true,
        },
        auth_profile_dependency: AuthProfileDependency::None,
        cache_ttl_seconds: None,
        request_timeout_ms: None,
    }
}

fn credentials() -> ResolvedCredentials {
    let mut credentials = ResolvedCredentials::default();
    credentials
        .fields
        .insert("region".to_string(), "eu-west-1".to_string());
    credentials.secret_fields.insert(
        "token".to_string(),
        SecretString::from("credential-token".to_string()),
    );
    credentials
}

fn failure_message(error: DbError) -> String {
    match error {
        DbError::ValueResolutionFailed(message) => message,
        other => panic!("expected ValueResolutionFailed, got {other:?}"),
    }
}

// --- protocol ---------------------------------------------------------------

#[test]
fn hello_response_round_trips_through_postcard() {
    let envelope = SecretProviderResponseEnvelope::new(
        SECRET_PROVIDER_RPC_VERSION,
        0,
        SecretProviderResponseBody::Hello(SecretProviderHelloResponse {
            server_name: "fake".to_string(),
            server_version: "1.0.0".to_string(),
            selected_version: SECRET_PROVIDER_RPC_V1_0,
            manifest: manifest(),
        }),
    );

    let bytes = postcard::to_allocvec(&envelope).expect("serialize");
    let decoded: SecretProviderResponseEnvelope = postcard::from_bytes(&bytes).expect("decode");

    match decoded.body {
        SecretProviderResponseBody::Hello(hello) => assert_eq!(hello.manifest, manifest()),
        other => panic!("unexpected body {other:?}"),
    }
}

#[test]
fn get_secret_request_round_trips_with_credentials() {
    let envelope = SecretProviderRequestEnvelope::new(
        SECRET_PROVIDER_RPC_VERSION,
        7,
        SecretProviderRequestBody::GetSecret(GetSecretRequest {
            locator: "db/prod".to_string(),
            json_key: Some("password".to_string()),
            credentials: Some(ResolvedCredentialsDto::from(&credentials())),
        }),
    );

    let bytes = postcard::to_allocvec(&envelope).expect("serialize");
    let decoded: SecretProviderRequestEnvelope = postcard::from_bytes(&bytes).expect("decode");

    assert_eq!(decoded.request_id, 7);
    let SecretProviderRequestBody::GetSecret(request) = decoded.body else {
        panic!("expected GetSecret");
    };
    assert_eq!(request.locator, "db/prod");
    assert_eq!(request.json_key.as_deref(), Some("password"));
    let credentials = request.credentials.expect("credentials");
    assert_eq!(credentials.secret_fields["token"], "credential-token");
}

#[test]
fn error_body_round_trips_every_code() {
    let codes = [
        SecretProviderRpcErrorCode::NotFound,
        SecretProviderRpcErrorCode::AccessDenied,
        SecretProviderRpcErrorCode::Locked,
        SecretProviderRpcErrorCode::InvalidLocator,
        SecretProviderRpcErrorCode::Unavailable,
        SecretProviderRpcErrorCode::Timeout,
        SecretProviderRpcErrorCode::UnsupportedMethod,
        SecretProviderRpcErrorCode::VersionMismatch,
        SecretProviderRpcErrorCode::InvalidRequest,
        SecretProviderRpcErrorCode::Internal,
    ];

    for code in codes {
        let error = SecretProviderRpcError {
            code,
            message: "message".to_string(),
            retriable: true,
        };
        let bytes = postcard::to_allocvec(&SecretProviderResponseBody::Error(error.clone()))
            .expect("serialize");
        let decoded: SecretProviderResponseBody = postcard::from_bytes(&bytes).expect("decode");
        let SecretProviderResponseBody::Error(decoded) = decoded else {
            panic!("expected Error");
        };
        assert_eq!(decoded, error);
    }
}

#[test]
fn manifest_json_uses_snake_case_dependency_and_defaults_to_none() {
    let mut required = manifest();
    required.auth_profile_dependency = AuthProfileDependency::Required;
    let json = serde_json::to_string(&required).expect("serialize");
    assert!(json.contains("\"auth_profile_dependency\":\"required\""));

    let minimal = r#"{"manifest_version":1,"provider_id":"x","display_name":"X","capabilities":{"get_secret":true}}"#;
    let parsed: SecretProviderManifest = serde_json::from_str(minimal).expect("parse");
    assert_eq!(parsed.auth_profile_dependency, AuthProfileDependency::None);
    assert!(!parsed.capabilities.json_key);
    assert_eq!(parsed.request_timeout_ms, None);
}

#[test]
fn debug_of_secret_body_redacts_the_value() {
    let body = SecretProviderResponseBody::Secret {
        value: SECRET_VALUE.to_string(),
    };
    let envelope = SecretProviderResponseEnvelope::new(SECRET_PROVIDER_RPC_VERSION, 1, body);

    let printed = format!("{envelope:?}");

    assert!(!printed.contains(SECRET_VALUE), "{printed}");
    assert!(printed.contains("[REDACTED]"));
}

#[test]
fn debug_of_request_redacts_credentials_and_auth_token() {
    let request = SecretProviderRequestBody::GetSecret(GetSecretRequest {
        locator: "db/prod".to_string(),
        json_key: None,
        credentials: Some(ResolvedCredentialsDto::from(&credentials())),
    });
    let hello = SecretProviderRequestBody::Hello(SecretProviderHelloRequest {
        client_name: "dbflux".to_string(),
        client_version: "0".to_string(),
        supported_versions: vec![],
        auth_token: Some("auth-token-value".to_string()),
    });

    let printed = format!("{request:?} {hello:?}");

    assert!(!printed.contains("credential-token"), "{printed}");
    assert!(!printed.contains("eu-west-1"), "{printed}");
    assert!(!printed.contains("auth-token-value"), "{printed}");
    assert!(printed.contains("db/prod"));
}

// --- manifest validation ----------------------------------------------------

#[test]
fn manifest_rejects_unsupported_version() {
    let mut candidate = manifest();
    candidate.manifest_version = 2;
    assert_eq!(
        validate_manifest(&candidate, &BUILT_IN_SECRET_PROVIDER_IDS),
        Err(ManifestError::UnsupportedManifestVersion(2))
    );
}

#[test]
fn manifest_rejects_empty_provider_id() {
    let mut candidate = manifest();
    candidate.provider_id = String::new();
    assert_eq!(
        validate_manifest(&candidate, &[]),
        Err(ManifestError::EmptyProviderId)
    );
}

#[test]
fn manifest_rejects_provider_id_longer_than_64() {
    let mut candidate = manifest();
    candidate.provider_id = "a".repeat(64);
    assert!(validate_manifest(&candidate, &[]).is_ok());

    candidate.provider_id = "a".repeat(65);
    assert_eq!(
        validate_manifest(&candidate, &[]),
        Err(ManifestError::ProviderIdTooLong(65))
    );
}

#[test]
fn manifest_rejects_provider_id_outside_allowed_characters() {
    for invalid in ["Vault", "vault id", "vault/x", "vault:x"] {
        let mut candidate = manifest();
        candidate.provider_id = invalid.to_string();
        assert_eq!(
            validate_manifest(&candidate, &[]),
            Err(ManifestError::InvalidProviderId(invalid.to_string())),
            "{invalid}"
        );
    }

    let mut candidate = manifest();
    candidate.provider_id = "vault.v1_a-b".to_string();
    assert!(validate_manifest(&candidate, &[]).is_ok());
}

#[test]
fn manifest_rejects_built_in_provider_ids() {
    for reserved in BUILT_IN_SECRET_PROVIDER_IDS {
        let mut candidate = manifest();
        candidate.provider_id = reserved.to_string();
        assert_eq!(
            validate_manifest(&candidate, &BUILT_IN_SECRET_PROVIDER_IDS),
            Err(ManifestError::ReservedProviderId(reserved.to_string()))
        );
    }
}

#[test]
fn manifest_rejects_blank_display_name() {
    for blank in ["", "   "] {
        let mut candidate = manifest();
        candidate.display_name = blank.to_string();
        assert_eq!(
            validate_manifest(&candidate, &[]),
            Err(ManifestError::EmptyDisplayName)
        );
    }
}

#[test]
fn manifest_rejects_missing_get_secret_capability() {
    let mut candidate = manifest();
    candidate.capabilities.get_secret = false;
    assert_eq!(
        validate_manifest(&candidate, &[]),
        Err(ManifestError::MissingGetSecretCapability)
    );
}

#[test]
fn manifest_errors_render_readable_messages() {
    let message = ManifestError::ReservedProviderId("aws-ssm".to_string()).to_string();
    assert_eq!(message, "secret-provider id 'aws-ssm' is already in use");
}

#[test]
fn manifest_normalizes_request_timeout_boundaries() {
    let cases = [
        (None, 15_000),
        (Some(0), 1),
        (Some(1), 1),
        (Some(60_000), 60_000),
        (Some(60_001), 60_000),
    ];

    for (input, expected_ms) in cases {
        let mut candidate = manifest();
        candidate.request_timeout_ms = input;
        let validated = validate_manifest(&candidate, &[]).expect("valid");
        assert_eq!(
            validated.request_timeout,
            Duration::from_millis(expected_ms),
            "{input:?}"
        );
    }
}

#[test]
fn manifest_maps_cache_ttl_seconds() {
    let mut candidate = manifest();
    assert_eq!(
        validate_manifest(&candidate, &[]).expect("valid").cache_ttl,
        None
    );

    candidate.cache_ttl_seconds = Some(0);
    assert_eq!(
        validate_manifest(&candidate, &[]).expect("valid").cache_ttl,
        Some(Duration::ZERO)
    );

    candidate.cache_ttl_seconds = Some(30);
    assert_eq!(
        validate_manifest(&candidate, &[]).expect("valid").cache_ttl,
        Some(Duration::from_secs(30))
    );
}

// --- version negotiation ----------------------------------------------------

#[test]
fn secret_family_negotiates_highest_mutual_version() {
    assert_eq!(
        negotiate_highest_mutual_version(
            RpcApiFamily::SecretProviderRpc,
            &SECRET_PROVIDER_RPC_SUPPORTED_VERSIONS,
            &[ProtocolVersion::new(1, 0), ProtocolVersion::new(1, 5)],
        ),
        Some(SECRET_PROVIDER_RPC_V1_0)
    );
    assert_eq!(
        negotiate_secret_provider_version(&[ProtocolVersion::new(2, 0)]),
        None
    );
}

#[test]
fn sanitize_strips_control_characters_and_truncates() {
    let message = format!("bad\u{1b}[31m\nline{}", "x".repeat(600));
    let sanitized = sanitize_provider_message(&message);

    assert!(!sanitized.chars().any(char::is_control));
    assert_eq!(sanitized.chars().count(), 512);
    assert!(sanitized.starts_with("bad[31mline"));
}

#[test]
fn source_lost_classification_matches_codes_only() {
    for code in ["Locked", "Unavailable", "Transport"] {
        let error = DbError::ValueResolutionFailed(format!("[vault/rpc] GetSecret ({code}): gone"));
        assert!(is_source_lost_error(&error), "{code}");
    }

    let not_found =
        DbError::ValueResolutionFailed("[vault/rpc] GetSecret (NotFound): Locked".into());
    assert!(!is_source_lost_error(&not_found));
}

// --- client over a real local socket ----------------------------------------

type GetSecretHandler = dyn Fn(&GetSecretRequest) -> SecretProviderResponseBody + Send + Sync;

struct FakeServer {
    socket_id: String,
    get_secret_requests: Arc<Mutex<Vec<GetSecretRequest>>>,
}

impl FakeServer {
    fn start(
        manifest: SecretProviderManifest,
        selected_version: ProtocolVersion,
        handler: Box<GetSecretHandler>,
    ) -> Self {
        let socket_id = format!("sp-test-{}", uuid::Uuid::new_v4().simple());
        let name = secret_provider_socket_name(&socket_id).expect("socket name");
        let listener = ListenerOptions::new()
            .name(name)
            .nonblocking(ListenerNonblockingMode::Neither)
            .create_sync()
            .expect("listener");

        let get_secret_requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&get_secret_requests);
        let handler: Arc<GetSecretHandler> = Arc::from(handler);

        thread::spawn(move || {
            loop {
                let Ok(stream) = listener.accept() else {
                    continue;
                };
                let manifest = manifest.clone();
                let recorded = Arc::clone(&recorded);
                let handler = Arc::clone(&handler);

                thread::spawn(move || {
                    serve_connection(stream, &manifest, selected_version, &recorded, &*handler);
                });
            }
        });

        Self {
            socket_id,
            get_secret_requests,
        }
    }

    #[allow(clippy::result_large_err)]
    fn probe(&self) -> Result<RpcSecretProvider, DbError> {
        RpcSecretProvider::probe(&self.socket_id, None, &BUILT_IN_SECRET_PROVIDER_IDS)
    }

    fn recorded_requests(&self) -> Vec<GetSecretRequest> {
        self.get_secret_requests.lock().expect("lock").clone()
    }
}

fn serve_connection(
    mut stream: interprocess::local_socket::Stream,
    manifest: &SecretProviderManifest,
    selected_version: ProtocolVersion,
    recorded: &Mutex<Vec<GetSecretRequest>>,
    handler: &GetSecretHandler,
) {
    // Liveness probes connect and close without sending a request.
    let Ok(request) = framing::recv_msg::<_, SecretProviderRequestEnvelope>(&mut stream) else {
        return;
    };

    let body = match request.body {
        SecretProviderRequestBody::Hello(_) => {
            SecretProviderResponseBody::Hello(SecretProviderHelloResponse {
                server_name: "fake".to_string(),
                server_version: "0.0.0".to_string(),
                selected_version,
                manifest: manifest.clone(),
            })
        }
        SecretProviderRequestBody::GetSecret(get_secret) => {
            recorded.lock().expect("lock").push(get_secret.clone());
            handler(&get_secret)
        }
    };

    let response =
        SecretProviderResponseEnvelope::new(request.protocol_version, request.request_id, body);
    if let Err(error) = framing::send_msg(&mut stream, &response) {
        eprintln!("fake server could not answer: {error}");
    }
}

fn answer_with_secret() -> Box<GetSecretHandler> {
    Box::new(|_| SecretProviderResponseBody::Secret {
        value: SECRET_VALUE.to_string(),
    })
}

#[tokio::test]
async fn get_secret_returns_the_provider_value() {
    let server = FakeServer::start(manifest(), SECRET_PROVIDER_RPC_V1_0, answer_with_secret());
    let provider = server.probe().expect("probe");

    assert_eq!(DynSecretProvider::provider_id(&provider), "test-vault");
    assert_eq!(DynSecretProvider::display_name(&provider), "Test Vault");
    assert_eq!(provider.cache_ttl(), None);

    let value = provider
        .get_secret("db/prod", Some("password"))
        .await
        .expect("secret");

    assert_eq!(value.expose_secret(), SECRET_VALUE);
    let requests = server.recorded_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].locator, "db/prod");
    assert_eq!(requests[0].json_key.as_deref(), Some("password"));
}

#[tokio::test]
async fn provider_not_found_maps_to_redacted_typed_failure() {
    let server = FakeServer::start(
        manifest(),
        SECRET_PROVIDER_RPC_V1_0,
        Box::new(|request| {
            SecretProviderResponseBody::Error(SecretProviderRpcError {
                code: SecretProviderRpcErrorCode::NotFound,
                message: format!("no secret at {}\u{1b}[0m", request.locator),
                retriable: false,
            })
        }),
    );
    let provider = server.probe().expect("probe");

    let error = provider
        .get_secret("db/missing", None)
        .await
        .expect_err("must fail");
    let message = failure_message(error);

    assert!(
        message.starts_with("[test-vault/rpc] GetSecret (NotFound): no secret at db/missing"),
        "{message}"
    );
    assert!(message.contains("[correlation id: "), "{message}");
    assert!(!message.contains('\u{1b}'), "{message}");
    assert!(!message.contains(SECRET_VALUE));
}

#[tokio::test]
async fn locked_provider_failure_carries_recovery_hint() {
    let server = FakeServer::start(
        manifest(),
        SECRET_PROVIDER_RPC_V1_0,
        Box::new(|_| {
            SecretProviderResponseBody::Error(SecretProviderRpcError {
                code: SecretProviderRpcErrorCode::Locked,
                message: "vault is locked".to_string(),
                retriable: true,
            })
        }),
    );
    let provider = server.probe().expect("probe");

    let error = provider
        .get_secret("db/prod", None)
        .await
        .expect_err("fail");

    assert!(is_source_lost_error(&error));
    let message = failure_message(error);
    assert!(
        message.contains("Unlock or restart the secret provider, then reconnect"),
        "{message}"
    );
}

#[tokio::test]
async fn request_timeout_returns_typed_failure_in_bounded_time() {
    let mut slow = manifest();
    slow.request_timeout_ms = Some(50);
    let server = FakeServer::start(
        slow,
        SECRET_PROVIDER_RPC_V1_0,
        Box::new(|_| {
            thread::sleep(Duration::from_millis(1_500));
            SecretProviderResponseBody::Secret {
                value: SECRET_VALUE.to_string(),
            }
        }),
    );
    let provider = server.probe().expect("probe");

    let started = Instant::now();
    let error = provider
        .get_secret("db/prod", None)
        .await
        .expect_err("timeout");
    let elapsed = started.elapsed();

    assert!(elapsed < Duration::from_millis(1_000), "{elapsed:?}");
    let message = failure_message(error);
    assert!(
        message.starts_with("[test-vault/rpc] GetSecret (Timeout): no response within 50 ms"),
        "{message}"
    );
    assert!(message.contains("[correlation id: "));
}

#[test]
fn request_timeout_works_without_a_tokio_runtime() {
    let mut slow = manifest();
    slow.request_timeout_ms = Some(50);
    let server = FakeServer::start(
        slow,
        SECRET_PROVIDER_RPC_V1_0,
        Box::new(|_| {
            thread::sleep(Duration::from_millis(1_500));
            SecretProviderResponseBody::Secret {
                value: SECRET_VALUE.to_string(),
            }
        }),
    );
    let provider = server.probe().expect("probe");

    let started = Instant::now();
    let error =
        futures::executor::block_on(provider.get_secret("db/prod", None)).expect_err("timeout");

    assert!(started.elapsed() < Duration::from_millis(1_000));
    assert!(failure_message(error).contains("GetSecret (Timeout)"));
}

#[test]
fn get_secret_works_without_a_tokio_runtime() {
    let server = FakeServer::start(
        manifest(),
        SECRET_PROVIDER_RPC_V1_0,
        Box::new(|_| SecretProviderResponseBody::Secret {
            value: SECRET_VALUE.to_string(),
        }),
    );
    let provider = server.probe().expect("probe");

    let secret = futures::executor::block_on(provider.get_secret("db/prod", None)).expect("secret");

    assert_eq!(secret.expose_secret(), SECRET_VALUE);
}

#[tokio::test]
async fn dropping_the_future_does_not_block() {
    let server = FakeServer::start(
        manifest(),
        SECRET_PROVIDER_RPC_V1_0,
        Box::new(|_| {
            thread::sleep(Duration::from_millis(1_500));
            SecretProviderResponseBody::Secret {
                value: SECRET_VALUE.to_string(),
            }
        }),
    );
    let provider = server.probe().expect("probe");

    let started = Instant::now();
    let cancelled = tokio::time::timeout(
        Duration::from_millis(50),
        provider.get_secret("db/prod", None),
    )
    .await;

    assert!(cancelled.is_err());
    assert!(started.elapsed() < Duration::from_millis(1_000));
}

#[tokio::test]
async fn required_dependency_without_credentials_fails_before_rpc() {
    let mut required = manifest();
    required.auth_profile_dependency = AuthProfileDependency::Required;
    let server = FakeServer::start(required, SECRET_PROVIDER_RPC_V1_0, answer_with_secret());
    let provider = server.probe().expect("probe");

    let error = provider
        .get_secret("db/prod", None)
        .await
        .expect_err("fail");

    let message = failure_message(error);
    assert!(
        message.starts_with("[test-vault/rpc] GetSecret (MissingCredentials):"),
        "{message}"
    );
    assert!(server.recorded_requests().is_empty());
}

#[tokio::test]
async fn unsupported_json_key_fails_before_rpc() {
    let mut no_json_key = manifest();
    no_json_key.capabilities.json_key = false;
    let server = FakeServer::start(no_json_key, SECRET_PROVIDER_RPC_V1_0, answer_with_secret());
    let provider = server.probe().expect("probe");

    let error = provider
        .get_secret("db/prod", Some("password"))
        .await
        .expect_err("fail");

    let message = failure_message(error);
    assert!(
        message.starts_with("[test-vault/rpc] GetSecret (InvalidRequest):"),
        "{message}"
    );
    assert!(server.recorded_requests().is_empty());

    provider
        .get_secret("db/prod", None)
        .await
        .expect("no key works");
    assert_eq!(server.recorded_requests().len(), 1);
}

#[tokio::test]
async fn credentials_are_forwarded_only_when_declared() {
    let undeclared = FakeServer::start(manifest(), SECRET_PROVIDER_RPC_V1_0, answer_with_secret());
    let provider = undeclared.probe().expect("probe");
    let credentials = credentials();
    let context = ResolveContext {
        credentials: Some(&credentials),
        auth_session: None,
        profile_name: "test",
    };
    provider
        .get_secret_in_context("db/prod", None, &context)
        .await
        .expect("secret");
    assert!(undeclared.recorded_requests()[0].credentials.is_none());

    let mut optional = manifest();
    optional.auth_profile_dependency = AuthProfileDependency::Optional;
    let declared = FakeServer::start(optional, SECRET_PROVIDER_RPC_V1_0, answer_with_secret());
    let provider = declared.probe().expect("probe");
    assert_eq!(provider.provider_id(), "test-vault");
    provider
        .get_secret_in_context("db/prod", None, &context)
        .await
        .expect("secret");
    provider.get_secret("db/prod", None).await.expect("secret");

    let requests = declared.recorded_requests();
    let forwarded = requests[0].credentials.as_ref().expect("forwarded");
    assert_eq!(forwarded.fields["region"], "eu-west-1");
    assert_eq!(forwarded.secret_fields["token"], "credential-token");
    assert!(requests[1].credentials.is_none());
}

#[tokio::test]
async fn transport_failure_maps_to_transport_code_with_hint() {
    let server = FakeServer::start(manifest(), SECRET_PROVIDER_RPC_V1_0, answer_with_secret());
    let probed = server.probe().expect("probe");

    // The same provider pointed at a socket nobody listens on, as after a crash.
    let missing_socket = format!("sp-missing-{}", uuid::Uuid::new_v4().simple());
    let detached = probed.with_socket_id_for_test(&missing_socket);
    let error = detached
        .get_secret("db/prod", None)
        .await
        .expect_err("fail");

    assert!(is_source_lost_error(&error));
    let message = failure_message(error);
    assert!(
        message.starts_with("[test-vault/rpc] GetSecret (Transport):"),
        "{message}"
    );
    assert!(message.contains("Unlock or restart the secret provider, then reconnect"));
    assert!(message.contains("[correlation id: "));
}

#[test]
fn probe_without_server_fails_to_connect() {
    let missing_socket = format!("sp-missing-{}", uuid::Uuid::new_v4().simple());

    let error = RpcSecretProvider::probe(&missing_socket, None, &[])
        .err()
        .expect("no server");

    assert!(matches!(error, DbError::ConnectionFailed(_)), "{error:?}");
}

#[test]
fn probe_rejects_invalid_manifest() {
    let mut invalid = manifest();
    invalid.capabilities.get_secret = false;
    let server = FakeServer::start(invalid, SECRET_PROVIDER_RPC_V1_0, answer_with_secret());

    let error = server.probe().err().expect("must reject");

    assert!(
        error
            .to_string()
            .contains("does not declare the get_secret capability"),
        "{error}"
    );
}

#[test]
fn probe_rejects_reserved_provider_id() {
    let mut reserved = manifest();
    reserved.provider_id = "aws-secrets-manager".to_string();
    let server = FakeServer::start(reserved, SECRET_PROVIDER_RPC_V1_0, answer_with_secret());

    let error = server.probe().err().expect("must reject");

    assert!(error.to_string().contains("is already in use"), "{error}");
}

#[test]
fn probe_rejects_unsupported_selected_version() {
    let server = FakeServer::start(manifest(), ProtocolVersion::new(2, 0), answer_with_secret());

    let error = server.probe().err().expect("must reject");

    assert!(
        error
            .to_string()
            .contains("unsupported protocol version 2.0"),
        "{error}"
    );
}

#[test]
fn managed_launch_requires_an_explicit_command() {
    let env = std::collections::HashMap::new();

    assert_eq!(
        RpcSecretProvider::build_launch_config("vault", None, &[], &env, None).expect("ok"),
        None
    );
    assert!(
        RpcSecretProvider::build_launch_config("vault", None, &["--x".to_string()], &env, None)
            .is_err()
    );
    assert!(
        RpcSecretProvider::build_launch_config("vault", Some("bin"), &[], &env, Some(0)).is_err()
    );

    let launch = RpcSecretProvider::build_launch_config("vault", Some(" bin "), &[], &env, None)
        .expect("ok")
        .expect("launch");
    assert_eq!(launch.program, "bin");
    assert_eq!(launch.startup_timeout, Duration::from_millis(5_000));
}

#[test]
fn shutdown_without_managed_hosts_stops_nothing() {
    assert_eq!(shutdown_managed_secret_provider_hosts(), 0);
}
