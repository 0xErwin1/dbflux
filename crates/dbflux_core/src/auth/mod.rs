mod types;

use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;

use crate::DbError;
use crate::driver::form::DriverFormDef;
use crate::values::CompositeValueResolver;

pub use types::*;

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
    fn provider_id(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    /// Returns the form definition used to render this provider's settings UI.
    ///
    /// The returned reference must be `'static` (typically backed by a
    /// `OnceLock`) to avoid repeated allocations during rendering.
    fn form_def(&self) -> &'static AuthFormDef;

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
}

static EMPTY_AUTH_FORM: OnceLock<AuthFormDef> = OnceLock::new();

fn empty_auth_form() -> &'static AuthFormDef {
    EMPTY_AUTH_FORM.get_or_init(|| AuthFormDef { tabs: vec![] })
}

#[async_trait::async_trait]
impl<T: AuthProvider> DynAuthProvider for T {
    fn provider_id(&self) -> &'static str {
        AuthProvider::provider_id(self)
    }

    fn display_name(&self) -> &'static str {
        AuthProvider::display_name(self)
    }

    fn form_def(&self) -> &'static AuthFormDef {
        empty_auth_form()
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
