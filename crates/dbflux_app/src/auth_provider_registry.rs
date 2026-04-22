use std::sync::Arc;

use dbflux_core::auth::{
    DynAuthProvider, SharedDynAuthProvider,
};
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

pub type RegistryAuthProviderWrapper = SharedDynAuthProvider;
