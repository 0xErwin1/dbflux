mod types;

use std::future::Future;

use crate::DbError;

pub use types::*;

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
}

#[async_trait::async_trait]
impl<T: AuthProvider> DynAuthProvider for T {
    fn provider_id(&self) -> &'static str {
        AuthProvider::provider_id(self)
    }

    fn display_name(&self) -> &'static str {
        AuthProvider::display_name(self)
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
