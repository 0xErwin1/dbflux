mod resolve;

use std::sync::Arc;

use secrecy::SecretString;
use tokio::sync::watch;

use crate::access::{AccessHandle, AccessKind, AccessManager};
use crate::auth::{
    AuthProfile, AuthSession, AuthSessionState, DynAuthProvider, ResolvedCredentials,
};
use crate::connection::profile::ConnectionProfile;
use crate::values::{CompositeValueResolver, ResolveContext};
use crate::{CancelToken, DbError};

use std::fmt;

pub use resolve::resolve_profile_values;

pub type StateSender = watch::Sender<PipelineState>;
pub type StateWatcher = watch::Receiver<PipelineState>;

/// Create a `(StateSender, StateWatcher)` pair for observing pipeline progress.
pub fn pipeline_state_channel() -> (StateSender, StateWatcher) {
    watch::channel(PipelineState::Idle)
}

// ---------------------------------------------------------------------------
// PipelineState
// ---------------------------------------------------------------------------

/// Observable state of the connect pipeline.
#[derive(Debug, Clone)]
pub enum PipelineState {
    Idle,
    Authenticating {
        provider_name: String,
    },
    WaitingForLogin {
        provider_name: String,
        verification_url: Option<String>,
    },
    ResolvingValues {
        total: usize,
        resolved: usize,
    },
    OpeningAccess {
        method_label: String,
    },
    Connecting {
        driver_name: String,
    },
    FetchingSchema,
    Connected,
    Failed {
        stage: String,
        error: String,
    },
    Cancelled,
}

impl fmt::Display for PipelineState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Authenticating { provider_name } => {
                write!(f, "Authenticating ({provider_name})")
            }
            Self::WaitingForLogin { provider_name, .. } => {
                write!(f, "Waiting for login ({provider_name})")
            }
            Self::ResolvingValues { total, resolved } => {
                write!(f, "Resolving values ({resolved}/{total})")
            }
            Self::OpeningAccess { method_label } => {
                write!(f, "Opening access ({method_label})")
            }
            Self::Connecting { driver_name } => {
                write!(f, "Connecting ({driver_name})")
            }
            Self::FetchingSchema => write!(f, "Fetching schema"),
            Self::Connected => write!(f, "Connected"),
            Self::Failed { stage, error } => {
                write!(f, "Failed at {stage}: {error}")
            }
            Self::Cancelled => write!(f, "Cancelled"),
        }
    }
}

// ---------------------------------------------------------------------------
// PipelineError
// ---------------------------------------------------------------------------

/// Error with pipeline stage context.
#[derive(Debug)]
pub struct PipelineError {
    pub stage: String,
    pub source: DbError,
}

impl PipelineError {
    pub fn auth(source: DbError) -> Self {
        Self {
            stage: "authentication".to_string(),
            source,
        }
    }

    pub fn resolve(source: DbError) -> Self {
        Self {
            stage: "value_resolution".to_string(),
            source,
        }
    }

    pub fn access(source: DbError) -> Self {
        Self {
            stage: "access".to_string(),
            source,
        }
    }

    pub fn connect(source: DbError) -> Self {
        Self {
            stage: "driver_connect".to_string(),
            source,
        }
    }

    pub fn schema(source: DbError) -> Self {
        Self {
            stage: "schema_fetch".to_string(),
            source,
        }
    }

    pub fn cancelled() -> Self {
        Self {
            stage: "cancelled".to_string(),
            source: DbError::Cancelled,
        }
    }
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pipeline failed at {}: {}", self.stage, self.source)
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

// ---------------------------------------------------------------------------
// PipelineInput / PipelineOutput
// ---------------------------------------------------------------------------

/// Everything the pipeline needs to run the pre-connect stages.
///
/// The actual driver connect and schema fetch happen in the caller
/// (workspace), since they need driver-specific types that `dbflux_core`
/// doesn't own.
pub struct PipelineInput {
    pub profile: ConnectionProfile,
    pub auth_provider: Option<Box<dyn DynAuthProvider>>,
    pub auth_profile: Option<AuthProfile>,
    pub resolver: CompositeValueResolver,
    pub access_manager: Arc<dyn AccessManager>,
    pub cancel: CancelToken,
}

/// Successful result of the pre-connect pipeline stages.
pub struct PipelineOutput {
    /// Profile with all ValueRefs resolved and config fields patched.
    pub resolved_profile: ConnectionProfile,

    /// Resolved password extracted from ValueRefs (if any).
    pub resolved_password: Option<SecretString>,

    /// Auth session if SSO/cloud authentication was performed.
    pub auth_session: Option<AuthSession>,

    /// RAII handle for tunnel lifetime (SSH, proxy, SSM).
    /// Drop this to tear down the tunnel.
    pub access_handle: AccessHandle,
}

// ---------------------------------------------------------------------------
// run_pipeline
// ---------------------------------------------------------------------------

/// Run the connect pipeline pre-connect stages to completion.
///
/// Returns `PipelineOutput` on success. The `state_tx` channel emits
/// state changes that the UI observes via `StateWatcher`.
///
/// This function runs on a background executor. The caller spawns it via
/// `cx.background_executor().spawn(...)` and polls the watcher.
///
/// Stages:
/// 1. **Authenticating** — validate session, login if needed, resolve credentials
/// 2. **ResolvingValues** — resolve all `ValueRef` entries in the profile
/// 3. **OpeningAccess** — set up tunnel via `AccessManager`
///
/// Driver connect and schema fetch are handled by the caller.
pub async fn run_pipeline(
    input: PipelineInput,
    state_tx: &StateSender,
) -> Result<PipelineOutput, PipelineError> {
    let cancel = &input.cancel;

    // --- Stage 1: Authentication ---

    let (auth_session, resolved_credentials) = run_auth_stage(
        input.auth_provider.as_deref(),
        input.auth_profile.as_ref(),
        cancel,
        state_tx,
    )
    .await?;

    cancel.check_pipeline()?;

    // --- Stage 2: Resolve values ---

    let total_refs = input.profile.value_refs.len();
    let _ = state_tx.send(PipelineState::ResolvingValues {
        total: total_refs,
        resolved: 0,
    });

    let resolve_ctx = ResolveContext {
        credentials: resolved_credentials.as_ref(),
        auth_session: auth_session.as_ref(),
        profile_name: &input.profile.name,
    };

    let (resolved_profile, resolved_password) =
        resolve_profile_values(&input.profile, &input.resolver, &resolve_ctx).await?;

    let _ = state_tx.send(PipelineState::ResolvingValues {
        total: total_refs,
        resolved: total_refs,
    });

    cancel.check_pipeline()?;

    // --- Stage 3: Open access ---

    let access_kind = resolved_profile
        .access_kind
        .clone()
        .unwrap_or(AccessKind::Direct);

    let _ = state_tx.send(PipelineState::OpeningAccess {
        method_label: access_kind_label(&access_kind),
    });

    let (remote_host, remote_port) = resolved_profile
        .config
        .host_port()
        .map(|(h, p)| (h.to_string(), p))
        .unwrap_or_else(|| ("localhost".to_string(), 0));

    let access_handle = input
        .access_manager
        .open(&access_kind, &remote_host, remote_port)
        .await
        .map_err(PipelineError::access)?;

    cancel.check_pipeline()?;

    Ok(PipelineOutput {
        resolved_profile,
        resolved_password,
        auth_session,
        access_handle,
    })
}

// ---------------------------------------------------------------------------
// Auth stage
// ---------------------------------------------------------------------------

async fn run_auth_stage(
    provider: Option<&dyn DynAuthProvider>,
    auth_profile: Option<&AuthProfile>,
    cancel: &CancelToken,
    state_tx: &StateSender,
) -> Result<(Option<AuthSession>, Option<ResolvedCredentials>), PipelineError> {
    let (Some(provider), Some(profile)) = (provider, auth_profile) else {
        return Ok((None, None));
    };

    let provider_name = provider.display_name().to_string();

    let _ = state_tx.send(PipelineState::Authenticating {
        provider_name: provider_name.clone(),
    });

    let session_state = provider
        .validate_session(profile)
        .await
        .map_err(PipelineError::auth)?;

    log::debug!(
        "[pipeline] auth session state for provider '{}': {:?}",
        provider_name,
        session_state
    );

    cancel.check_pipeline()?;

    let session = match session_state {
        AuthSessionState::Valid { expires_at } => {
            log::debug!("[pipeline] session valid, skipping login flow");
            // Session is still valid — skip the browser login flow entirely.
            // Credentials are resolved in the next step and will populate
            // session.data via resolved_credentials.provider_data.
            AuthSession {
                provider_id: provider.provider_id().to_string(),
                profile_id: profile.id,
                expires_at,
                data: None,
            }
        }

        AuthSessionState::Expired | AuthSessionState::LoginRequired => {
            log::debug!("[pipeline] session expired or missing, starting login flow");

            // Send an initial WaitingForLogin with no URL — the real device URL
            // arrives asynchronously once the AWS CLI starts and prints to stdout.
            let _ = state_tx.send(PipelineState::WaitingForLogin {
                provider_name: provider_name.clone(),
                verification_url: None,
            });

            cancel.check_pipeline()?;

            // Build a callback that updates the state channel with the real URL
            // as soon as the provider discovers it (e.g. from `aws sso login` stdout).
            let state_tx_for_url = state_tx.clone();
            let provider_name_for_url = provider_name.clone();
            let url_callback = Box::new(move |url: Option<String>| {
                log::debug!("[pipeline] url_callback fired with url: {:?}", url);
                let _ = state_tx_for_url.send(PipelineState::WaitingForLogin {
                    provider_name: provider_name_for_url,
                    verification_url: url,
                });
            });

            provider
                .login(profile, url_callback)
                .await
                .map_err(PipelineError::auth)?
        }
    };

    let resolved_credentials = provider
        .resolve_credentials(profile)
        .await
        .map_err(PipelineError::auth)?;

    let mut session = session;
    if session.data.is_none() {
        session.data = resolved_credentials.provider_data.clone();
    }

    Ok((Some(session), Some(resolved_credentials)))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn access_kind_label(kind: &AccessKind) -> String {
    match kind {
        AccessKind::Direct => "Direct".to_string(),
        AccessKind::Ssh { .. } => "SSH tunnel".to_string(),
        AccessKind::Proxy { .. } => "Proxy".to_string(),
        AccessKind::Ssm { region, .. } => format!("SSM ({})", region),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{
        AuthProfile, AuthProfileConfig, AuthSession, AuthSessionState, ResolvedCredentials,
    };
    use crate::connection::profile::{ConnectionProfile, DbConfig};
    use crate::values::{SecretProvider, ValueCache, ValueRef};
    use secrecy::{ExposeSecret, SecretString};
    use std::collections::HashMap;
    use std::time::Duration;

    // -- Mock auth provider --------------------------------------------------

    struct MockAuthProvider {
        should_require_login: bool,
    }

    impl crate::auth::AuthProvider for MockAuthProvider {
        fn provider_id(&self) -> &'static str {
            "mock_auth"
        }

        fn display_name(&self) -> &'static str {
            "Mock Auth"
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            if self.should_require_login {
                Ok(AuthSessionState::LoginRequired)
            } else {
                Ok(AuthSessionState::Valid { expires_at: None })
            }
        }

        async fn login(&self, profile: &AuthProfile) -> Result<AuthSession, DbError> {
            Ok(AuthSession {
                provider_id: "mock_auth".to_string(),
                profile_id: profile.id,
                expires_at: None,
                data: None,
            })
        }

        async fn resolve_credentials(
            &self,
            _profile: &AuthProfile,
        ) -> Result<ResolvedCredentials, DbError> {
            Ok(ResolvedCredentials::default())
        }
    }

    // -- Mock access manager -------------------------------------------------

    struct MockAccessManager;

    #[async_trait::async_trait]
    impl AccessManager for MockAccessManager {
        async fn open(
            &self,
            _kind: &AccessKind,
            _remote_host: &str,
            _remote_port: u16,
        ) -> Result<AccessHandle, DbError> {
            Ok(AccessHandle::direct())
        }
    }

    // -- Mock secret provider ------------------------------------------------

    struct StubSecretProvider;

    impl SecretProvider for StubSecretProvider {
        fn provider_id(&self) -> &'static str {
            "stub"
        }

        fn display_name(&self) -> &'static str {
            "Stub"
        }

        async fn get_secret(
            &self,
            locator: &str,
            _json_key: Option<&str>,
        ) -> Result<SecretString, DbError> {
            Ok(SecretString::from(format!("resolved-{}", locator)))
        }
    }

    // -- Helpers -------------------------------------------------------------

    fn test_resolver() -> CompositeValueResolver {
        let cache = Arc::new(ValueCache::new(Duration::from_secs(60)));
        let mut resolver = CompositeValueResolver::new(cache);
        resolver.register_secret_provider(Arc::new(StubSecretProvider));
        resolver
    }

    fn test_auth_profile() -> AuthProfile {
        AuthProfile::new(
            "test-sso",
            "mock_auth",
            AuthProfileConfig::AwsSso {
                profile_name: "test".to_string(),
                region: "us-east-1".to_string(),
                sso_start_url: "https://example.awsapps.com/start".to_string(),
                sso_account_id: "123456789012".to_string(),
                sso_role_name: "TestRole".to_string(),
            },
        )
    }

    // -- Tests ---------------------------------------------------------------

    #[tokio::test]
    async fn pipeline_direct_no_auth_no_refs() {
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let (state_tx, mut state_rx) = pipeline_state_channel();

        let input = PipelineInput {
            profile,
            auth_provider: None,
            auth_profile: None,
            resolver: test_resolver(),
            access_manager: Arc::new(MockAccessManager),
            cancel: CancelToken::new(),
        };

        let output = run_pipeline(input, &state_tx).await.unwrap();

        assert!(!output.access_handle.is_tunneled());
        assert!(output.auth_session.is_none());
        assert!(output.resolved_password.is_none());

        // The watcher should have received at least the ResolvingValues and
        // OpeningAccess states before the final value.
        let final_state = state_rx.borrow_and_update().clone();
        assert!(
            matches!(final_state, PipelineState::OpeningAccess { .. }),
            "expected OpeningAccess as final broadcasted state, got: {final_state}"
        );
    }

    #[tokio::test]
    async fn pipeline_with_auth_and_value_refs() {
        let mut refs = HashMap::new();
        refs.insert("host".to_string(), ValueRef::literal("db.example.com"));
        refs.insert(
            "password".to_string(),
            ValueRef::secret("stub", "db-pass", None),
        );

        let mut profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        profile.value_refs = refs;

        let auth_profile = test_auth_profile();
        let (state_tx, _state_rx) = pipeline_state_channel();

        let input = PipelineInput {
            profile,
            auth_provider: Some(Box::new(MockAuthProvider {
                should_require_login: false,
            })),
            auth_profile: Some(auth_profile),
            resolver: test_resolver(),
            access_manager: Arc::new(MockAccessManager),
            cancel: CancelToken::new(),
        };

        let output = run_pipeline(input, &state_tx).await.unwrap();

        assert!(output.auth_session.is_some());
        assert_eq!(
            output.auth_session.as_ref().unwrap().provider_id,
            "mock_auth"
        );

        let pw = output.resolved_password.unwrap();
        assert_eq!(pw.expose_secret(), "resolved-db-pass");

        match &output.resolved_profile.config {
            DbConfig::Postgres { host, .. } => assert_eq!(host, "db.example.com"),
            other => panic!("expected Postgres config, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn pipeline_cancellation_aborts_early() {
        let profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        let (state_tx, _state_rx) = pipeline_state_channel();

        let cancel = CancelToken::new();
        cancel.cancel();

        let input = PipelineInput {
            profile,
            auth_provider: None,
            auth_profile: None,
            resolver: test_resolver(),
            access_manager: Arc::new(MockAccessManager),
            cancel,
        };

        let result = run_pipeline(input, &state_tx).await;
        match result {
            Err(err) => assert_eq!(err.stage, "cancelled"),
            Ok(_) => panic!("expected pipeline to fail with cancellation"),
        }
    }

    #[tokio::test]
    async fn pipeline_state_transitions_are_observable() {
        let mut refs = HashMap::new();
        refs.insert("host".to_string(), ValueRef::literal("db.example.com"));

        let mut profile = ConnectionProfile::new("test-pg", DbConfig::default_postgres());
        profile.value_refs = refs;

        let auth_profile = test_auth_profile();
        let (state_tx, mut state_rx) = pipeline_state_channel();

        let input = PipelineInput {
            profile,
            auth_provider: Some(Box::new(MockAuthProvider {
                should_require_login: true,
            })),
            auth_profile: Some(auth_profile),
            resolver: test_resolver(),
            access_manager: Arc::new(MockAccessManager),
            cancel: CancelToken::new(),
        };

        let output = run_pipeline(input, &state_tx).await.unwrap();
        assert!(output.auth_session.is_some());

        // After pipeline completes, the last broadcasted state should be
        // OpeningAccess (the pipeline doesn't send Connected — that's done
        // by the caller after driver connect + schema fetch).
        let last = state_rx.borrow_and_update().clone();
        assert!(
            matches!(last, PipelineState::OpeningAccess { .. }),
            "expected OpeningAccess, got: {last}"
        );
    }
}
