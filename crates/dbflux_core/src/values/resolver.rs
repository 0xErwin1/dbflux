use std::collections::HashMap;
use std::sync::Arc;

use secrecy::{ExposeSecret, SecretString};

use crate::DbError;
use crate::auth::ResolvedCredentials;
use crate::values::cache::{CacheKey, CachedValue, ValueCache};
use crate::values::{
    DynParameterProvider, DynSecretProvider, ResolveContext, ResolvedValue, ValueOrigin, ValueRef,
};

/// Resolves `ValueRef` values by dispatching to registered providers.
pub struct CompositeValueResolver {
    secret_providers: HashMap<String, Arc<dyn DynSecretProvider>>,
    parameter_providers: HashMap<String, Arc<dyn DynParameterProvider>>,
    cache: Arc<ValueCache>,
}

impl CompositeValueResolver {
    pub fn new(cache: Arc<ValueCache>) -> Self {
        Self {
            secret_providers: HashMap::new(),
            parameter_providers: HashMap::new(),
            cache,
        }
    }

    pub fn register_secret_provider(&mut self, provider: Arc<dyn DynSecretProvider>) {
        let id = provider.provider_id().to_string();
        self.secret_providers.insert(id, provider);
    }

    pub fn register_parameter_provider(&mut self, provider: Arc<dyn DynParameterProvider>) {
        let id = provider.provider_id().to_string();
        self.parameter_providers.insert(id, provider);
    }

    pub fn available_secret_providers(&self) -> Vec<(&str, &str)> {
        self.secret_providers
            .values()
            .map(|p| (p.provider_id(), p.display_name()))
            .collect()
    }

    pub fn available_parameter_providers(&self) -> Vec<(&str, &str)> {
        self.parameter_providers
            .values()
            .map(|p| (p.provider_id(), p.display_name()))
            .collect()
    }

    pub fn cache(&self) -> &ValueCache {
        &self.cache
    }

    /// Resolve a single `ValueRef` to a concrete value.
    pub async fn resolve(
        &self,
        value_ref: &ValueRef,
        ctx: &ResolveContext<'_>,
    ) -> Result<ResolvedValue, DbError> {
        match value_ref {
            ValueRef::Literal { value } => {
                Ok(ResolvedValue::new(value.clone(), ValueOrigin::Literal))
            }

            ValueRef::Env { key } => {
                let value = std::env::var(key).map_err(|_| {
                    DbError::value_resolution_failed(format!(
                        "Environment variable '{}' not set",
                        key
                    ))
                })?;

                Ok(ResolvedValue::new(
                    value,
                    ValueOrigin::EnvVar { name: key.clone() },
                ))
            }

            ValueRef::Secret {
                provider,
                locator,
                json_key,
            } => self.resolve_secret(provider, locator, json_key).await,

            ValueRef::Parameter {
                provider,
                name,
                json_key,
            } => self.resolve_parameter(provider, name, json_key).await,

            ValueRef::Auth { field } => {
                let credentials = ctx.credentials.ok_or_else(|| {
                    DbError::value_resolution_failed(
                        "Auth field reference requires resolved credentials",
                    )
                })?;

                let value = resolve_auth_field(credentials, field)?;

                Ok(ResolvedValue::new(
                    value,
                    ValueOrigin::AuthCredential {
                        field: field.clone(),
                    },
                ))
            }
        }
    }

    /// Resolve multiple `ValueRef`s concurrently, returning a map of field
    /// name to resolved value. Fails on the first resolution error, tagging
    /// the error with the field name that failed.
    pub async fn resolve_all(
        &self,
        refs: &HashMap<String, ValueRef>,
        ctx: &ResolveContext<'_>,
    ) -> Result<HashMap<String, ResolvedValue>, DbError> {
        let futures: Vec<_> = refs
            .iter()
            .map(|(field, value_ref)| {
                let field = field.clone();
                async move {
                    let value = self.resolve(value_ref, ctx).await.map_err(|e| {
                        DbError::value_resolution_failed(format!(
                            "Failed to resolve field '{}': {}",
                            field, e
                        ))
                    })?;
                    Ok::<_, DbError>((field, value))
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;

        Ok(results.into_iter().collect())
    }

    async fn resolve_secret(
        &self,
        provider: &str,
        locator: &str,
        json_key: &Option<String>,
    ) -> Result<ResolvedValue, DbError> {
        let cache_key = CacheKey::new(provider, locator, json_key.clone());

        if let Some(cached) = self.cache.get(&cache_key) {
            let value = match cached {
                CachedValue::Plain(v) => v,
                CachedValue::Secret(s) => s.expose_secret().to_string(),
            };

            return Ok(ResolvedValue::new(
                value,
                ValueOrigin::SecretProvider {
                    provider: provider.to_string(),
                    locator_summary: locator.to_string(),
                },
            ));
        }

        let secret_provider = self.secret_providers.get(provider).ok_or_else(|| {
            DbError::value_resolution_failed(format!(
                "Secret provider '{}' not registered",
                provider
            ))
        })?;

        let secret = secret_provider
            .get_secret(locator, json_key.as_deref())
            .await?;

        let value_str = secret.expose_secret().to_string();

        self.cache.put(
            cache_key,
            CachedValue::Secret(SecretString::from(value_str.clone())),
        );

        Ok(ResolvedValue::new(
            value_str,
            ValueOrigin::SecretProvider {
                provider: provider.to_string(),
                locator_summary: locator.to_string(),
            },
        ))
    }

    async fn resolve_parameter(
        &self,
        provider: &str,
        name: &str,
        json_key: &Option<String>,
    ) -> Result<ResolvedValue, DbError> {
        let cache_key = CacheKey::new(provider, name, json_key.clone());

        if let Some(cached) = self.cache.get(&cache_key) {
            let value = match cached {
                CachedValue::Plain(v) => v,
                CachedValue::Secret(s) => s.expose_secret().to_string(),
            };

            return Ok(ResolvedValue::new(
                value,
                ValueOrigin::ParameterProvider {
                    provider: provider.to_string(),
                    name: name.to_string(),
                },
            ));
        }

        let param_provider = self.parameter_providers.get(provider).ok_or_else(|| {
            DbError::value_resolution_failed(format!(
                "Parameter provider '{}' not registered",
                provider
            ))
        })?;

        let value = param_provider
            .get_parameter(name, json_key.as_deref())
            .await?;

        self.cache.put(cache_key, CachedValue::Plain(value.clone()));

        Ok(ResolvedValue::new(
            value,
            ValueOrigin::ParameterProvider {
                provider: provider.to_string(),
                name: name.to_string(),
            },
        ))
    }
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
fn extract_json_field(
    parameter_value: &str,
    key: &str,
    parameter_name: &str,
) -> Result<String, DbError> {
    let parsed: serde_json::Value = serde_json::from_str(parameter_value).map_err(|err| {
        DbError::ValueResolutionFailed(format!(
            "Parameter '{}' is not valid JSON (json_key '{}' requested): {}",
            parameter_name, key, err
        ))
    })?;

    let field = parsed.get(key).ok_or_else(|| {
        DbError::ValueResolutionFailed(format!(
            "Field '{}' not found in parameter '{}'",
            key, parameter_name
        ))
    })?;

    let value = match field {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };

    Ok(value)
}

fn resolve_auth_field(credentials: &ResolvedCredentials, field: &str) -> Result<String, DbError> {
    match field {
        "access_key_id" => credentials
            .access_key_id
            .clone()
            .ok_or_else(|| DbError::value_resolution_failed("access_key_id not available")),

        "secret_access_key" => credentials
            .secret_access_key
            .as_ref()
            .map(|s| s.expose_secret().to_string())
            .ok_or_else(|| DbError::value_resolution_failed("secret_access_key not available")),

        "session_token" => credentials
            .session_token
            .as_ref()
            .map(|s| s.expose_secret().to_string())
            .ok_or_else(|| DbError::value_resolution_failed("session_token not available")),

        "region" => credentials
            .region
            .clone()
            .ok_or_else(|| DbError::value_resolution_failed("region not available")),

        other => credentials.extra.get(other).cloned().ok_or_else(|| {
            DbError::value_resolution_failed(format!(
                "Auth credential field '{}' not available",
                other
            ))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::{ParameterProvider, SecretProvider};
    use std::time::Duration;

    struct MockSecretProvider;

    impl SecretProvider for MockSecretProvider {
        fn provider_id(&self) -> &'static str {
            "mock"
        }

        fn display_name(&self) -> &'static str {
            "Mock Secrets"
        }

        fn get_secret(
            &self,
            locator: &str,
            _json_key: Option<&str>,
        ) -> impl std::future::Future<Output = Result<SecretString, DbError>> + Send {
            let locator = locator.to_string();
            async move {
                if locator == "not-found" {
                    return Err(DbError::value_resolution_failed("secret not found"));
                }
                Ok(SecretString::from(format!("secret-value-for-{}", locator)))
            }
        }
    }

    struct MockParameterProvider;

    impl ParameterProvider for MockParameterProvider {
        fn provider_id(&self) -> &'static str {
            "mock"
        }

        fn display_name(&self) -> &'static str {
            "Mock Params"
        }

        fn get_parameter(
            &self,
            name: &str,
            json_key: Option<&str>,
        ) -> impl std::future::Future<Output = Result<String, DbError>> + Send {
            let name = name.to_string();
            let json_key = json_key.map(ToString::to_string);

            async move {
                let value = if name == "json-param" {
                    r#"{"host":"db.example.com","port":5432}"#.to_string()
                } else {
                    format!("param-value-for-{}", name)
                };

                match json_key {
                    Some(key) => extract_json_field(&value, &key, &name),
                    None => Ok(value),
                }
            }
        }
    }

    fn test_resolver() -> CompositeValueResolver {
        let cache = Arc::new(ValueCache::new(Duration::from_secs(60)));
        let mut resolver = CompositeValueResolver::new(cache);
        resolver.register_secret_provider(Arc::new(MockSecretProvider));
        resolver.register_parameter_provider(Arc::new(MockParameterProvider));
        resolver
    }

    fn empty_ctx() -> ResolveContext<'static> {
        ResolveContext::default()
    }

    #[tokio::test]
    async fn resolve_literal() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::literal("hello"), &empty_ctx())
            .await
            .unwrap();
        assert_eq!(result.expose_secret(), "hello");
    }

    #[tokio::test]
    async fn resolve_env_var() {
        unsafe { std::env::set_var("TEST_DBFLUX_VAR", "env_value") };
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::env("TEST_DBFLUX_VAR"), &empty_ctx())
            .await
            .unwrap();
        assert_eq!(result.expose_secret(), "env_value");
        unsafe { std::env::remove_var("TEST_DBFLUX_VAR") };
    }

    #[tokio::test]
    async fn resolve_missing_env_var_fails() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::env("NONEXISTENT_VAR_12345"), &empty_ctx())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn resolve_secret_delegates_to_provider() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::secret("mock", "db-creds", None), &empty_ctx())
            .await
            .unwrap();
        assert_eq!(result.expose_secret(), "secret-value-for-db-creds");
    }

    #[tokio::test]
    async fn resolve_secret_uses_cache() {
        let resolver = test_resolver();
        let vr = ValueRef::secret("mock", "cached-secret", None);

        let _ = resolver.resolve(&vr, &empty_ctx()).await.unwrap();

        let result = resolver.resolve(&vr, &empty_ctx()).await.unwrap();
        assert_eq!(result.expose_secret(), "secret-value-for-cached-secret");
    }

    #[tokio::test]
    async fn resolve_unknown_provider_fails() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(
                &ValueRef::secret("nonexistent", "locator", None),
                &empty_ctx(),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn resolve_parameter_delegates_to_provider() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::parameter("mock", "db-port"), &empty_ctx())
            .await
            .unwrap();
        assert_eq!(result.expose_secret(), "param-value-for-db-port");
    }

    #[tokio::test]
    async fn resolve_parameter_json_field_delegates_to_provider() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(
                &ValueRef::parameter_with_key("mock", "json-param", Some("host".to_string())),
                &empty_ctx(),
            )
            .await
            .unwrap();

        assert_eq!(result.expose_secret(), "db.example.com");
    }

    #[tokio::test]
    async fn resolve_auth_field_with_credentials() {
        let resolver = test_resolver();
        let creds = ResolvedCredentials {
            access_key_id: Some("AKIATEST".to_string()),
            region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        let ctx = ResolveContext {
            credentials: Some(&creds),
            auth_session: None,
            profile_name: "test",
        };
        let result = resolver
            .resolve(&ValueRef::auth("access_key_id"), &ctx)
            .await
            .unwrap();
        assert_eq!(result.expose_secret(), "AKIATEST");
    }

    #[tokio::test]
    async fn resolve_auth_field_without_credentials_fails() {
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::auth("access_key_id"), &empty_ctx())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn resolve_all_concurrent() {
        unsafe { std::env::set_var("TEST_RESOLVE_ALL_VAR", "env_val") };
        let resolver = test_resolver();

        let mut refs = HashMap::new();
        refs.insert("host".to_string(), ValueRef::literal("localhost"));
        refs.insert("port".to_string(), ValueRef::env("TEST_RESOLVE_ALL_VAR"));
        refs.insert(
            "password".to_string(),
            ValueRef::secret("mock", "db-pass", None),
        );

        let result = resolver.resolve_all(&refs, &empty_ctx()).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result["host"].expose_secret(), "localhost");
        assert_eq!(result["port"].expose_secret(), "env_val");
        assert_eq!(
            result["password"].expose_secret(),
            "secret-value-for-db-pass"
        );

        unsafe { std::env::remove_var("TEST_RESOLVE_ALL_VAR") };
    }

    #[tokio::test]
    async fn resolve_all_failure_includes_field_name() {
        let resolver = test_resolver();

        let mut refs = HashMap::new();
        refs.insert("host".to_string(), ValueRef::literal("localhost"));
        refs.insert(
            "password".to_string(),
            ValueRef::secret("mock", "not-found", None),
        );

        let result = resolver.resolve_all(&refs, &empty_ctx()).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("password"));
    }
}
