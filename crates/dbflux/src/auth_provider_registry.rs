use std::sync::Arc;

use dbflux_core::DbError;
use dbflux_core::auth::{
    AuthFormDef, AuthProfile, AuthSession, AuthSessionState, DynAuthProvider, ImportableProfile,
    ResolvedCredentials, UrlCallback,
};
use dbflux_core::values::CompositeValueResolver;
use indexmap::IndexMap;

pub struct AuthProviderRegistry {
    providers: IndexMap<String, Arc<dyn DynAuthProvider>>,
}

impl AuthProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: IndexMap::new(),
        }
    }

    pub fn register(&mut self, provider: Arc<dyn DynAuthProvider>) {
        self.providers
            .insert(provider.provider_id().to_string(), provider);
    }

    pub fn get(&self, provider_id: &str) -> Option<Arc<dyn DynAuthProvider>> {
        self.providers.get(provider_id).map(Arc::clone)
    }

    pub fn providers(&self) -> impl Iterator<Item = Arc<dyn DynAuthProvider>> + '_ {
        self.providers.values().cloned()
    }
}

impl Default for AuthProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RegistryAuthProviderWrapper {
    provider: Arc<dyn DynAuthProvider>,
}

impl RegistryAuthProviderWrapper {
    pub fn boxed(provider: Arc<dyn DynAuthProvider>) -> Box<dyn DynAuthProvider> {
        Box::new(Self { provider })
    }
}

#[async_trait::async_trait]
impl DynAuthProvider for RegistryAuthProviderWrapper {
    fn provider_id(&self) -> &'static str {
        self.provider.provider_id()
    }

    fn display_name(&self) -> &'static str {
        self.provider.display_name()
    }

    fn form_def(&self) -> &'static AuthFormDef {
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
