mod types;

use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::DbError;
use crate::driver::form::DriverFormDef;
use crate::values::CompositeValueResolver;

pub use types::*;

/// Request to fetch the available options for a `DynamicSelect` field.
///
/// Providers receive this from the UI layer when a field's options need to be
/// populated or refreshed.
#[derive(Debug, Clone)]
pub struct FetchOptionsRequest {
    /// ID of the `DynamicSelect` field whose options are being requested.
    pub field_id: String,
    /// Current values of fields listed in the target field's `depends_on`.
    pub dependencies: HashMap<String, String>,
    /// Serialized session data, if a session is active.
    pub session: Option<serde_json::Value>,
}

/// Successful response to a `FetchOptions` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchOptionsResponse {
    /// Options to populate the dropdown.
    pub options: Vec<crate::SelectOption>,
    /// How long (in seconds) the caller may cache these options.
    ///
    /// `None` means do not cache.
    pub cache_hint_seconds: Option<u32>,
}

/// Error returned when a `FetchOptions` request fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchOptionsError {
    /// The user has never logged in — no session exists.
    NeedsLogin,
    /// A session existed but is no longer valid.
    SessionExpired,
    /// Retry-eligible failure (network, 5xx, throttling, etc.).
    Transient(String),
    /// Non-retriable failure (misconfiguration, unknown field, etc.).
    Permanent(String),
}

/// Type alias for auth provider form definitions.
///
/// Auth providers reuse the same form definition type as drivers, allowing the
/// generic settings renderer to handle both without additional abstractions.
pub type AuthFormDef = DriverFormDef;

/// A profile discovered from an external source (e.g., `~/.aws/config`) that
/// can be imported into DBFlux as an `AuthProfile`.
pub struct ImportableProfile {
    pub display_name: String,
    pub provider_id: String,
    pub fields: HashMap<String, String>,
}

pub trait AuthProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    fn capabilities(&self) -> &'static AuthProviderCapabilities {
        default_auth_provider_capabilities()
    }

    fn validate_session(
        &self,
        profile: &AuthProfile,
    ) -> impl Future<Output = Result<AuthSessionState, DbError>> + Send;

    fn login(
        &self,
        profile: &AuthProfile,
    ) -> impl Future<Output = Result<AuthSession, DbError>> + Send;

    fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> impl Future<Output = Result<ResolvedCredentials, DbError>> + Send;
}

/// Callback type for surfacing a login verification URL to the UI while the
/// login process is still in progress. Called at most once per login attempt,
/// with `None` if the provider cannot determine the URL.
pub type UrlCallback = Box<dyn FnOnce(Option<String>) + Send>;

#[async_trait::async_trait]
pub trait DynAuthProvider: Send + Sync {
    fn provider_id(&self) -> &str;

    fn display_name(&self) -> &str;

    /// Returns the form definition used to render this provider's settings UI.
    ///
    fn form_def(&self) -> &AuthFormDef;

    fn capabilities(&self) -> &AuthProviderCapabilities {
        default_auth_provider_capabilities()
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError>;

    /// Perform the login flow.
    ///
    /// `url_callback` is called once the provider has determined the
    /// verification URL the user should visit (e.g. the AWS device-auth URL).
    /// Providers that cannot surface a URL call the callback with `None`.
    /// The default implementation calls the callback immediately with `None`.
    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError>;

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError>;

    /// Register provider-specific secret and parameter value providers into
    /// the connection resolver.
    ///
    /// Called after authentication succeeds, before value refs are resolved.
    /// The default is a no-op; providers that integrate with secrets backends
    /// (e.g. AWS Secrets Manager, SSM Parameter Store) override this.
    fn register_value_providers(
        &self,
        _profile: &AuthProfile,
        _session: Option<&AuthSession>,
        _resolver: &mut CompositeValueResolver,
    ) -> Result<(), DbError> {
        Ok(())
    }

    /// Return profiles discovered from external sources (e.g., `~/.aws/config`)
    /// that have not yet been imported into DBFlux.
    ///
    /// The default returns an empty list.
    fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
        vec![]
    }

    /// Called after a profile using this provider is saved.
    ///
    /// Providers can use this hook to write back data to external config files
    /// (e.g., appending an AWS profile entry to `~/.aws/config`).
    /// The default is a no-op.
    fn after_profile_saved(&self, _profile: &AuthProfile) {}

    /// Fetch the available options for a `DynamicSelect` field declared in this
    /// provider's `form_def()`.
    ///
    /// Called by the UI when the user opens the auth profile editor and for each
    /// `DynamicSelect` field whose cache is stale or invalidated. The default
    /// returns `FetchOptionsError::Permanent("not supported")`.
    async fn fetch_dynamic_options(
        &self,
        _profile: &AuthProfile,
        _request: FetchOptionsRequest,
    ) -> Result<FetchOptionsResponse, FetchOptionsError> {
        Err(FetchOptionsError::Permanent(
            "fetch_dynamic_options not implemented for this provider".to_string(),
        ))
    }
}

static EMPTY_AUTH_FORM: OnceLock<AuthFormDef> = OnceLock::new();
static DEFAULT_AUTH_PROVIDER_CAPABILITIES: OnceLock<AuthProviderCapabilities> = OnceLock::new();

fn empty_auth_form() -> &'static AuthFormDef {
    EMPTY_AUTH_FORM.get_or_init(|| AuthFormDef { tabs: vec![] })
}

fn default_auth_provider_capabilities() -> &'static AuthProviderCapabilities {
    DEFAULT_AUTH_PROVIDER_CAPABILITIES.get_or_init(AuthProviderCapabilities::default)
}

#[async_trait::async_trait]
impl<T: AuthProvider> DynAuthProvider for T {
    fn provider_id(&self) -> &str {
        AuthProvider::provider_id(self)
    }

    fn display_name(&self) -> &str {
        AuthProvider::display_name(self)
    }

    fn form_def(&self) -> &AuthFormDef {
        empty_auth_form()
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        AuthProvider::capabilities(self)
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        AuthProvider::validate_session(self, profile).await
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        // Static providers don't stream a URL; signal that immediately.
        url_callback(None);
        AuthProvider::login(self, profile).await
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        AuthProvider::resolve_credentials(self, profile).await
    }
}

pub struct SharedDynAuthProvider {
    provider: std::sync::Arc<dyn DynAuthProvider>,
}

impl SharedDynAuthProvider {
    pub fn boxed(provider: std::sync::Arc<dyn DynAuthProvider>) -> Box<dyn DynAuthProvider> {
        Box::new(Self { provider })
    }
}

#[async_trait::async_trait]
impl DynAuthProvider for SharedDynAuthProvider {
    fn provider_id(&self) -> &str {
        self.provider.provider_id()
    }

    fn display_name(&self) -> &str {
        self.provider.display_name()
    }

    fn form_def(&self) -> &AuthFormDef {
        self.provider.form_def()
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        self.provider.capabilities()
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        self.provider.validate_session(profile).await
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        self.provider.login(profile, url_callback).await
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        self.provider.resolve_credentials(profile).await
    }

    fn register_value_providers(
        &self,
        profile: &AuthProfile,
        session: Option<&AuthSession>,
        resolver: &mut CompositeValueResolver,
    ) -> Result<(), DbError> {
        self.provider
            .register_value_providers(profile, session, resolver)
    }

    fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
        self.provider.detect_importable_profiles()
    }

    fn after_profile_saved(&self, profile: &AuthProfile) {
        self.provider.after_profile_saved(profile);
    }

    async fn fetch_dynamic_options(
        &self,
        profile: &AuthProfile,
        request: FetchOptionsRequest,
    ) -> Result<FetchOptionsResponse, FetchOptionsError> {
        self.provider.fetch_dynamic_options(profile, request).await
    }
}

#[async_trait::async_trait]
impl DynAuthProvider for std::sync::Arc<dyn DynAuthProvider> {
    fn provider_id(&self) -> &str {
        self.as_ref().provider_id()
    }

    fn display_name(&self) -> &str {
        self.as_ref().display_name()
    }

    fn form_def(&self) -> &AuthFormDef {
        self.as_ref().form_def()
    }

    fn capabilities(&self) -> &AuthProviderCapabilities {
        self.as_ref().capabilities()
    }

    async fn validate_session(&self, profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
        self.as_ref().validate_session(profile).await
    }

    async fn login(
        &self,
        profile: &AuthProfile,
        url_callback: UrlCallback,
    ) -> Result<AuthSession, DbError> {
        self.as_ref().login(profile, url_callback).await
    }

    async fn resolve_credentials(
        &self,
        profile: &AuthProfile,
    ) -> Result<ResolvedCredentials, DbError> {
        self.as_ref().resolve_credentials(profile).await
    }

    fn register_value_providers(
        &self,
        profile: &AuthProfile,
        session: Option<&AuthSession>,
        resolver: &mut CompositeValueResolver,
    ) -> Result<(), DbError> {
        self.as_ref()
            .register_value_providers(profile, session, resolver)
    }

    fn detect_importable_profiles(&self) -> Vec<ImportableProfile> {
        self.as_ref().detect_importable_profiles()
    }

    fn after_profile_saved(&self, profile: &AuthProfile) {
        self.as_ref().after_profile_saved(profile);
    }

    async fn fetch_dynamic_options(
        &self,
        profile: &AuthProfile,
        request: FetchOptionsRequest,
    ) -> Result<FetchOptionsResponse, FetchOptionsError> {
        self.as_ref().fetch_dynamic_options(profile, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct StaticTestAuthProvider;

    impl AuthProvider for StaticTestAuthProvider {
        fn provider_id(&self) -> &'static str {
            "static-test"
        }

        fn display_name(&self) -> &'static str {
            "Static Test"
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            Ok(AuthSessionState::Valid { expires_at: None })
        }

        async fn login(&self, profile: &AuthProfile) -> Result<AuthSession, DbError> {
            Ok(AuthSession {
                provider_id: AuthProvider::provider_id(self).to_string(),
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

    struct LoginCapableAuthProvider;

    impl AuthProvider for LoginCapableAuthProvider {
        fn provider_id(&self) -> &'static str {
            "login-capable"
        }

        fn display_name(&self) -> &'static str {
            "Login Capable"
        }

        fn capabilities(&self) -> &'static AuthProviderCapabilities {
            static CAPABILITIES: AuthProviderCapabilities = AuthProviderCapabilities {
                login: AuthProviderLoginCapabilities {
                    supported: true,
                    verification_url_progress: true,
                },
            };

            &CAPABILITIES
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            Ok(AuthSessionState::LoginRequired)
        }

        async fn login(&self, profile: &AuthProfile) -> Result<AuthSession, DbError> {
            Ok(AuthSession {
                provider_id: AuthProvider::provider_id(self).to_string(),
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

    struct RuntimeOwnedProvider {
        provider_id: String,
        display_name: String,
        form_def: AuthFormDef,
    }

    #[async_trait::async_trait]
    impl DynAuthProvider for RuntimeOwnedProvider {
        fn provider_id(&self) -> &str {
            &self.provider_id
        }

        fn display_name(&self) -> &str {
            &self.display_name
        }

        fn form_def(&self) -> &AuthFormDef {
            &self.form_def
        }

        fn capabilities(&self) -> &AuthProviderCapabilities {
            static CAPABILITIES: AuthProviderCapabilities = AuthProviderCapabilities {
                login: AuthProviderLoginCapabilities {
                    supported: true,
                    verification_url_progress: false,
                },
            };

            &CAPABILITIES
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            Ok(AuthSessionState::LoginRequired)
        }

        async fn login(
            &self,
            profile: &AuthProfile,
            url_callback: UrlCallback,
        ) -> Result<AuthSession, DbError> {
            url_callback(Some("https://verify.example".to_string()));

            Ok(AuthSession {
                provider_id: self.provider_id.clone(),
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

    #[test]
    fn shared_dyn_auth_provider_boxes_runtime_owned_metadata() {
        let provider = Arc::new(RuntimeOwnedProvider {
            provider_id: "rpc-provider".to_string(),
            display_name: "RPC Provider".to_string(),
            form_def: AuthFormDef { tabs: vec![] },
        }) as Arc<dyn DynAuthProvider>;

        let boxed = SharedDynAuthProvider::boxed(provider);

        assert_eq!(boxed.provider_id(), "rpc-provider");
        assert_eq!(boxed.display_name(), "RPC Provider");
        assert!(boxed.form_def().tabs.is_empty());
    }

    #[test]
    fn auth_provider_defaults_capabilities_to_login_disabled() {
        let provider = StaticTestAuthProvider;

        assert_eq!(
            AuthProvider::capabilities(&provider),
            &AuthProviderCapabilities::default()
        );
        assert!(!AuthProvider::capabilities(&provider).login.supported);
        assert!(
            !AuthProvider::capabilities(&provider)
                .login
                .verification_url_progress
        );
    }

    #[test]
    fn dyn_auth_provider_wrapper_forwards_auth_provider_capabilities() {
        let provider = LoginCapableAuthProvider;

        assert!(
            <LoginCapableAuthProvider as DynAuthProvider>::capabilities(&provider)
                .login
                .supported
        );
        assert!(
            <LoginCapableAuthProvider as DynAuthProvider>::capabilities(&provider)
                .login
                .verification_url_progress
        );
    }

    #[test]
    fn shared_dyn_auth_provider_forwards_runtime_capabilities() {
        let provider = Arc::new(RuntimeOwnedProvider {
            provider_id: "rpc-provider".to_string(),
            display_name: "RPC Provider".to_string(),
            form_def: AuthFormDef { tabs: vec![] },
        }) as Arc<dyn DynAuthProvider>;

        let boxed = SharedDynAuthProvider::boxed(provider);

        assert!(boxed.capabilities().login.supported);
        assert!(!boxed.capabilities().login.verification_url_progress);
    }
}
