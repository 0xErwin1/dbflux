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
    fn provider_id(&self) -> &str;

    fn display_name(&self) -> &str;

    /// Returns the form definition used to render this provider's settings UI.
    ///
    fn form_def(&self) -> &AuthFormDef;

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
    fn provider_id(&self) -> &str {
        AuthProvider::provider_id(self)
    }

    fn display_name(&self) -> &str {
        AuthProvider::display_name(self)
    }

    fn form_def(&self) -> &AuthFormDef {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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

        async fn validate_session(&self, _profile: &AuthProfile) -> Result<AuthSessionState, DbError> {
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
}
