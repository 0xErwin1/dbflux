use std::sync::Arc;
use std::time::Duration;

use dbflux_core::DbError;
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::{CompositeValueResolver, DynSecretProvider, ValueCache};

use super::SecretProviderRegistry;

struct NamedSecretProvider {
    provider_id: &'static str,
    display_name: &'static str,
}

#[async_trait::async_trait]
impl DynSecretProvider for NamedSecretProvider {
    fn provider_id(&self) -> &str {
        self.provider_id
    }

    fn display_name(&self) -> &str {
        self.display_name
    }

    async fn get_secret(
        &self,
        _locator: &str,
        _json_key: Option<&str>,
    ) -> Result<SecretString, DbError> {
        Ok(SecretString::from("unused".to_string()))
    }
}

fn registry_with(entries: &[(&'static str, &'static str)]) -> SecretProviderRegistry {
    let mut registry = SecretProviderRegistry::new();
    for (provider_id, display_name) in entries {
        registry.register(Arc::new(NamedSecretProvider {
            provider_id,
            display_name,
        }));
    }
    registry
}

#[test]
fn list_is_sorted_by_display_name() {
    let registry = registry_with(&[("zeta", "Alpha Vault"), ("alpha", "Zeta Vault")]);

    assert_eq!(
        registry.list(),
        vec![
            ("zeta".to_string(), "Alpha Vault".to_string()),
            ("alpha".to_string(), "Zeta Vault".to_string()),
        ]
    );
}

#[test]
fn contains_reports_registered_ids() {
    let registry = registry_with(&[("vault", "Vault")]);

    assert!(registry.contains("vault"));
    assert!(!registry.contains("aws-ssm"));
}

#[test]
fn register_into_makes_providers_resolvable() {
    let registry = registry_with(&[("vault", "Vault"), ("keeper", "Keeper")]);
    let mut resolver =
        CompositeValueResolver::new(Arc::new(ValueCache::new(Duration::from_secs(60))));

    registry.register_into(&mut resolver);

    assert!(resolver.has_secret_provider("vault"));
    assert!(resolver.has_secret_provider("keeper"));
    assert!(!resolver.has_secret_provider("aws-secrets-manager"));
}
