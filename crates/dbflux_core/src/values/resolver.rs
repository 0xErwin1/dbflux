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

    pub fn has_secret_provider(&self, provider_id: &str) -> bool {
        self.secret_providers.contains_key(provider_id)
    }

    pub fn invalidate_secret_provider(&self, provider_id: &str) {
        self.cache.invalidate_provider(provider_id);
    }

    pub fn cache(&self) -> &ValueCache {
        &self.cache
    }

    /// Synchronously reads the environment variable behind a
    /// [`ValueRef::Env`] ref.
    ///
    /// Shared with the synchronous switch-database path so both consumers use
    /// the same lookup and the same errors. An unset, empty or non-Unicode
    /// variable fails with its own message, and no message carries a value.
    pub(crate) fn resolve_env_ref(key: &str) -> Result<String, DbError> {
        match std::env::var(key) {
            Ok(value) if value.is_empty() => Err(DbError::value_resolution_failed(format!(
                "Environment variable '{}' is empty",
                key
            ))),
            Ok(value) => Ok(value),
            Err(std::env::VarError::NotPresent) => Err(DbError::value_resolution_failed(format!(
                "Environment variable '{}' not set",
                key
            ))),
            Err(std::env::VarError::NotUnicode(_)) => Err(DbError::value_resolution_failed(
                format!("Environment variable '{}' is not valid Unicode", key),
            )),
        }
    }

    /// Synchronously reads the dotenv key behind a [`ValueRef::Dotenv`] ref.
    ///
    /// Shared with the synchronous switch-database path.
    pub(crate) fn resolve_dotenv_ref(path: &str, key: &str) -> Result<String, DbError> {
        crate::values::read_dotenv_value(path, key).map(|value| value.expose_secret().to_string())
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
                let value = Self::resolve_env_ref(key)?;

                Ok(ResolvedValue::new(
                    value,
                    ValueOrigin::EnvVar { name: key.clone() },
                ))
            }

            ValueRef::Dotenv { path, key } => {
                let value = Self::resolve_dotenv_ref(path, key)?;

                Ok(ResolvedValue::new(
                    value,
                    ValueOrigin::DotenvFile {
                        path: path.clone(),
                        key: key.clone(),
                    },
                ))
            }

            ValueRef::Secret {
                provider,
                locator,
                json_key,
            } => self.resolve_secret(provider, locator, json_key, ctx).await,

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
        ctx: &ResolveContext<'_>,
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

        // A failed fetch means the source may have been disconnected or locked again, so no
        // value this provider served earlier may outlive it in this resolver's cache.
        let secret = match secret_provider
            .get_secret_in_context(locator, json_key.as_deref(), ctx)
            .await
        {
            Ok(secret) => secret,
            Err(error) => {
                self.cache.invalidate_provider(provider);
                return Err(error);
            }
        };

        let value_str = secret.expose_secret().to_string();

        let cached_value = CachedValue::Secret(SecretString::from(value_str.clone()));

        match secret_provider.cache_ttl() {
            None => self.cache.put(cache_key, cached_value),
            Some(ttl) if ttl.is_zero() => {}
            Some(ttl) => {
                self.cache
                    .put_with_ttl(cache_key, cached_value, ttl.min(self.cache.ttl()))
            }
        }

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
    if let Some(value) = credentials.fields.get(field) {
        return Ok(value.clone());
    }

    if let Some(secret) = credentials.secret_fields.get(field) {
        return Ok(secret.expose_secret().to_string());
    }

    Err(DbError::value_resolution_failed(format!(
        "Auth credential field '{}' is not available from the resolved credentials",
        field
    )))
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

    struct CountingSecretProvider {
        id: &'static str,
        cache_ttl: Option<Duration>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl CountingSecretProvider {
        fn new(id: &'static str, cache_ttl: Option<Duration>) -> Self {
            Self {
                id,
                cache_ttl,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl DynSecretProvider for CountingSecretProvider {
        fn provider_id(&self) -> &str {
            self.id
        }

        fn display_name(&self) -> &str {
            "Counting"
        }

        fn cache_ttl(&self) -> Option<Duration> {
            self.cache_ttl
        }

        async fn get_secret(
            &self,
            locator: &str,
            _json_key: Option<&str>,
        ) -> Result<SecretString, DbError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(SecretString::from(format!("value-for-{}", locator)))
        }
    }

    fn counting_resolver(
        cache_ttl: Duration,
        providers: &[Arc<CountingSecretProvider>],
    ) -> CompositeValueResolver {
        let mut resolver = CompositeValueResolver::new(Arc::new(ValueCache::new(cache_ttl)));
        for provider in providers {
            resolver.register_secret_provider(provider.clone());
        }
        resolver
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
        let before = std::env::var_os("TEST_DBFLUX_VAR");
        if crate::isolated_env::run_in_isolated_fixture(
            "values::resolver::tests::resolve_env_var",
            &[("TEST_DBFLUX_VAR", std::ffi::OsStr::new("env_value"))],
        ) {
            let after = std::env::var_os("TEST_DBFLUX_VAR");
            assert!(
                after == before,
                "fixture must preserve the caller's process environment"
            );
            return;
        }
        let resolver = test_resolver();
        let result = resolver
            .resolve(&ValueRef::env("TEST_DBFLUX_VAR"), &empty_ctx())
            .await
            .unwrap();
        assert_eq!(result.expose_secret(), "env_value");
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
    async fn missing_env_var_error_names_the_variable() {
        let resolver = test_resolver();
        let Err(DbError::ValueResolutionFailed(message)) = resolver
            .resolve(&ValueRef::env("NONEXISTENT_VAR_12345"), &empty_ctx())
            .await
        else {
            panic!("missing variable must fail with a value resolution error");
        };
        assert_eq!(
            message,
            "Environment variable 'NONEXISTENT_VAR_12345' not set"
        );
    }

    #[tokio::test]
    async fn empty_env_var_fails_with_its_own_error() {
        if crate::isolated_env::run_in_isolated_fixture(
            "values::resolver::tests::empty_env_var_fails_with_its_own_error",
            &[("TEST_DBFLUX_EMPTY_VAR", std::ffi::OsStr::new(""))],
        ) {
            return;
        }
        let resolver = test_resolver();
        let Err(DbError::ValueResolutionFailed(message)) = resolver
            .resolve(&ValueRef::env("TEST_DBFLUX_EMPTY_VAR"), &empty_ctx())
            .await
        else {
            panic!("empty variable must fail with a value resolution error");
        };
        assert_eq!(
            message,
            "Environment variable 'TEST_DBFLUX_EMPTY_VAR' is empty"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_unicode_env_var_fails_without_echoing_bytes() {
        use std::os::unix::ffi::OsStrExt;

        if crate::isolated_env::run_in_isolated_fixture(
            "values::resolver::tests::non_unicode_env_var_fails_without_echoing_bytes",
            &[(
                "TEST_DBFLUX_BINARY_VAR",
                std::ffi::OsStr::from_bytes(&[b'p', 0xff, b'w']),
            )],
        ) {
            return;
        }
        let resolver = test_resolver();
        let Err(DbError::ValueResolutionFailed(message)) = resolver
            .resolve(&ValueRef::env("TEST_DBFLUX_BINARY_VAR"), &empty_ctx())
            .await
        else {
            panic!("non-Unicode variable must fail with a value resolution error");
        };
        assert_eq!(
            message,
            "Environment variable 'TEST_DBFLUX_BINARY_VAR' is not valid Unicode"
        );
    }

    #[tokio::test]
    async fn resolve_dotenv_reads_the_file_on_each_use() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("app.env");
        let write = |content: &str| {
            std::fs::write(&path, content).expect("write dotenv");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    .expect("restrict dotenv");
            }
        };
        let value_ref = ValueRef::dotenv(path.to_string_lossy(), "TOKEN");
        let resolver = test_resolver();

        write("TOKEN=first\n");
        let first = resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();
        assert_eq!(first.expose_secret(), "first");
        assert!(matches!(
            first.origin(),
            ValueOrigin::DotenvFile { key, .. } if key == "TOKEN"
        ));

        write("TOKEN=second\n");
        let second = resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();
        assert_eq!(second.expose_secret(), "second");
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
            fields: [
                ("access_key_id".to_string(), "AKIATEST".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]
            .into_iter()
            .collect(),
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
        let before = std::env::var_os("TEST_RESOLVE_ALL_VAR");
        if crate::isolated_env::run_in_isolated_fixture(
            "values::resolver::tests::resolve_all_concurrent",
            &[("TEST_RESOLVE_ALL_VAR", std::ffi::OsStr::new("env_val"))],
        ) {
            let after = std::env::var_os("TEST_RESOLVE_ALL_VAR");
            assert!(
                after == before,
                "fixture must preserve the caller's process environment"
            );
            return;
        }
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

    #[tokio::test]
    async fn secret_provider_with_zero_ttl_is_not_cached() {
        let provider = Arc::new(CountingSecretProvider::new("rpc", Some(Duration::ZERO)));
        let resolver = counting_resolver(Duration::from_secs(60), std::slice::from_ref(&provider));
        let value_ref = ValueRef::secret("rpc", "db-pass", None);

        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();
        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();

        assert_eq!(provider.calls(), 2);
    }

    #[tokio::test]
    async fn secret_provider_without_ttl_uses_cache_default() {
        let provider = Arc::new(CountingSecretProvider::new("rpc", None));
        let resolver = counting_resolver(Duration::from_secs(60), std::slice::from_ref(&provider));
        let value_ref = ValueRef::secret("rpc", "db-pass", None);

        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();
        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();

        assert_eq!(provider.calls(), 1);
    }

    #[tokio::test]
    async fn secret_provider_ttl_expires_entry() {
        let provider = Arc::new(CountingSecretProvider::new(
            "rpc",
            Some(Duration::from_millis(1)),
        ));
        let resolver = counting_resolver(Duration::from_secs(60), std::slice::from_ref(&provider));
        let value_ref = ValueRef::secret("rpc", "db-pass", None);

        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();

        assert_eq!(provider.calls(), 2);
    }

    #[tokio::test]
    async fn secret_provider_ttl_is_capped_by_cache_default() {
        let provider = Arc::new(CountingSecretProvider::new(
            "rpc",
            Some(Duration::from_secs(3600)),
        ));
        let resolver = counting_resolver(Duration::from_millis(1), std::slice::from_ref(&provider));
        let value_ref = ValueRef::secret("rpc", "db-pass", None);

        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        resolver.resolve(&value_ref, &empty_ctx()).await.unwrap();

        assert_eq!(provider.calls(), 2);
    }

    #[tokio::test]
    async fn invalidate_secret_provider_removes_only_that_provider() {
        let first = Arc::new(CountingSecretProvider::new("first", None));
        let second = Arc::new(CountingSecretProvider::new("second", None));
        let resolver = counting_resolver(Duration::from_secs(60), &[first.clone(), second.clone()]);
        let first_ref = ValueRef::secret("first", "db-pass", None);
        let second_ref = ValueRef::secret("second", "db-pass", None);

        resolver.resolve(&first_ref, &empty_ctx()).await.unwrap();
        resolver.resolve(&second_ref, &empty_ctx()).await.unwrap();
        resolver.invalidate_secret_provider("first");
        resolver.resolve(&first_ref, &empty_ctx()).await.unwrap();
        resolver.resolve(&second_ref, &empty_ctx()).await.unwrap();

        assert_eq!(first.calls(), 2);
        assert_eq!(second.calls(), 1);
    }

    struct ContextAwareSecretProvider {
        failing_locator: &'static str,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DynSecretProvider for ContextAwareSecretProvider {
        fn provider_id(&self) -> &str {
            "context"
        }

        fn display_name(&self) -> &str {
            "Context aware"
        }

        async fn get_secret(
            &self,
            _locator: &str,
            _json_key: Option<&str>,
        ) -> Result<SecretString, DbError> {
            Err(DbError::value_resolution_failed(
                "get_secret must not be called when a context is available",
            ))
        }

        async fn get_secret_in_context(
            &self,
            locator: &str,
            _json_key: Option<&str>,
            context: &ResolveContext<'_>,
        ) -> Result<SecretString, DbError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            if locator == self.failing_locator {
                return Err(DbError::value_resolution_failed("source locked"));
            }

            let token = context
                .credentials
                .and_then(|credentials| credentials.fields.get("token"))
                .cloned()
                .unwrap_or_else(|| "no-credentials".to_string());

            Ok(SecretString::from(format!("{locator}:{token}")))
        }
    }

    #[tokio::test]
    async fn resolve_secret_forwards_context_credentials_to_provider() {
        let provider = Arc::new(ContextAwareSecretProvider {
            failing_locator: "locked",
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut resolver =
            CompositeValueResolver::new(Arc::new(ValueCache::new(Duration::from_secs(60))));
        resolver.register_secret_provider(provider);
        let credentials = ResolvedCredentials {
            fields: [("token".to_string(), "session-token".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let context = ResolveContext {
            credentials: Some(&credentials),
            auth_session: None,
            profile_name: "test",
        };

        let resolved = resolver
            .resolve(&ValueRef::secret("context", "db-pass", None), &context)
            .await
            .unwrap();

        assert_eq!(resolved.expose_secret(), "db-pass:session-token");
    }

    #[tokio::test]
    async fn failed_secret_fetch_invalidates_cached_values_of_that_provider() {
        let provider = Arc::new(ContextAwareSecretProvider {
            failing_locator: "locked",
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let other = Arc::new(CountingSecretProvider::new("other", None));
        let mut resolver =
            CompositeValueResolver::new(Arc::new(ValueCache::new(Duration::from_secs(60))));
        resolver.register_secret_provider(provider.clone());
        resolver.register_secret_provider(other.clone());
        let cached_ref = ValueRef::secret("context", "db-pass", None);
        let other_ref = ValueRef::secret("other", "db-pass", None);

        resolver.resolve(&cached_ref, &empty_ctx()).await.unwrap();
        resolver.resolve(&other_ref, &empty_ctx()).await.unwrap();
        let failure = resolver
            .resolve(&ValueRef::secret("context", "locked", None), &empty_ctx())
            .await;
        resolver.resolve(&cached_ref, &empty_ctx()).await.unwrap();
        resolver.resolve(&other_ref, &empty_ctx()).await.unwrap();

        assert!(failure.is_err());
        assert_eq!(
            provider.calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "the cached value must be fetched again after the provider failed"
        );
        assert_eq!(other.calls(), 1, "other providers keep their cache");
    }

    #[test]
    fn has_secret_provider_reports_registration() {
        let provider = Arc::new(CountingSecretProvider::new("rpc", None));
        let resolver = counting_resolver(Duration::from_secs(60), &[provider]);

        assert!(resolver.has_secret_provider("rpc"));
        assert!(!resolver.has_secret_provider("missing"));
    }
}
