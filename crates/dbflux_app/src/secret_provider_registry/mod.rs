use std::sync::Arc;

use dbflux_core::values::{CompositeValueResolver, DynSecretProvider};
use indexmap::IndexMap;

#[cfg(test)]
mod tests;

/// External secret providers discovered from RPC services, keyed by provider id.
#[derive(Clone, Default)]
pub struct SecretProviderRegistry {
    providers: IndexMap<String, Arc<dyn DynSecretProvider>>,
}

impl SecretProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, provider: Arc<dyn DynSecretProvider>) {
        self.providers
            .insert(provider.provider_id().to_string(), provider);
    }

    pub fn contains(&self, provider_id: &str) -> bool {
        self.providers.contains_key(provider_id)
    }

    pub fn providers(&self) -> impl Iterator<Item = Arc<dyn DynSecretProvider>> + '_ {
        self.providers.values().cloned()
    }

    /// Provider ids paired with display names, sorted by display name for pickers.
    pub fn list(&self) -> Vec<(String, String)> {
        let mut entries: Vec<(String, String)> = self
            .providers
            .values()
            .map(|provider| {
                (
                    provider.provider_id().to_string(),
                    provider.display_name().to_string(),
                )
            })
            .collect();

        entries.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        entries
    }

    pub fn register_into(&self, resolver: &mut CompositeValueResolver) {
        for provider in self.providers() {
            resolver.register_secret_provider(provider);
        }
    }
}
