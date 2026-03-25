//! Profile value resolution: resolves `value_refs` from a `ConnectionProfile`
//! and patches the effective `DbConfig` fields with the resolved values.

use secrecy::SecretString;

use crate::connection::profile::{ConnectionProfile, DbConfig};
use crate::values::{CompositeValueResolver, ResolveContext, ResolvedValue};

use super::PipelineError;

/// Resolves all `value_refs` in a connection profile and returns a cloned
/// profile with patched config fields, plus the extracted password (if any).
pub async fn resolve_profile_values(
    profile: &ConnectionProfile,
    resolver: &CompositeValueResolver,
    ctx: &ResolveContext<'_>,
) -> Result<(ConnectionProfile, Option<SecretString>), PipelineError> {
    if profile.value_refs.is_empty() {
        return Ok((profile.clone(), None));
    }

    let resolved = resolver
        .resolve_all(&profile.value_refs, ctx)
        .await
        .map_err(PipelineError::resolve)?;

    let mut patched = profile.clone();
    let mut password: Option<SecretString> = None;

    for (field, value) in &resolved {
        if field == "password" {
            password = Some(SecretString::from(value.expose_secret().to_string()));
            continue;
        }

        if patch_access_kind_field(&mut patched, field, value) {
            continue;
        }

        patch_config_field(&mut patched.config, field, value);
    }

    Ok((patched, password))
}

/// Patches a single field in `ConnectionProfile::access_kind`.
///
/// Returns `true` when the field was handled by access-kind patching.
fn patch_access_kind_field(
    profile: &mut ConnectionProfile,
    field: &str,
    value: &ResolvedValue,
) -> bool {
    let Some(access_kind) = profile.access_kind.as_mut() else {
        return false;
    };

    let val = value.expose_secret();

    match access_kind {
        crate::access::AccessKind::Managed { params, .. } => match field {
            "ssm_instance_id" => {
                params.insert("instance_id".to_string(), val.to_string());
                true
            }
            "ssm_region" => {
                params.insert("region".to_string(), val.to_string());
                true
            }
            "ssm_remote_port" => {
                params.insert("remote_port".to_string(), val.to_string());
                true
            }
            _ => false,
        },
        _ => false,
    }
}

/// Patches a single field in the `DbConfig` variant with a resolved value.
/// Unknown fields are silently ignored (the pipeline may carry extra
/// metadata fields that don't map to config).
fn patch_config_field(config: &mut DbConfig, field: &str, value: &ResolvedValue) {
    let val = value.expose_secret();

    match config {
        DbConfig::Postgres {
            host,
            port,
            user,
            database,
            ..
        } => match field {
            "host" => *host = val.to_string(),
            "port" => {
                if let Ok(p) = val.parse() {
                    *port = p;
                }
            }
            "user" => *user = val.to_string(),
            "database" => *database = val.to_string(),
            _ => {}
        },

        DbConfig::MySQL {
            host,
            port,
            user,
            database,
            ..
        } => match field {
            "host" => *host = val.to_string(),
            "port" => {
                if let Ok(p) = val.parse() {
                    *port = p;
                }
            }
            "user" => *user = val.to_string(),
            "database" => *database = Some(val.to_string()),
            _ => {}
        },

        DbConfig::MongoDB {
            host,
            port,
            user,
            database,
            ..
        } => match field {
            "host" => *host = val.to_string(),
            "port" => {
                if let Ok(p) = val.parse() {
                    *port = p;
                }
            }
            "user" => *user = Some(val.to_string()),
            "database" => *database = Some(val.to_string()),
            _ => {}
        },

        DbConfig::Redis {
            host, port, user, ..
        } => match field {
            "host" => *host = val.to_string(),
            "port" => {
                if let Ok(p) = val.parse() {
                    *port = p;
                }
            }
            "user" => *user = Some(val.to_string()),
            _ => {}
        },

        DbConfig::SQLite { path, .. } => {
            if field == "path" {
                *path = val.into();
            }
        }

        DbConfig::DynamoDB {
            region,
            profile,
            endpoint,
            table,
        } => match field {
            "region" => *region = val.to_string(),
            "profile" => *profile = Some(val.to_string()),
            "endpoint" => *endpoint = Some(val.to_string()),
            "table" => *table = Some(val.to_string()),
            _ => {}
        },

        DbConfig::External { values, .. } => {
            values.insert(field.to_string(), val.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::{ValueCache, ValueRef};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::values::SecretProvider;
    use secrecy::ExposeSecret;

    struct StubSecretProvider;

    impl SecretProvider for StubSecretProvider {
        fn provider_id(&self) -> &'static str {
            "stub"
        }

        fn display_name(&self) -> &'static str {
            "Stub"
        }

        async fn get_secret(
            &self,
            locator: &str,
            _json_key: Option<&str>,
        ) -> Result<SecretString, crate::DbError> {
            Ok(SecretString::from(format!("resolved-{}", locator)))
        }
    }

    fn test_profile_with_refs(refs: HashMap<String, ValueRef>) -> ConnectionProfile {
        let mut profile = ConnectionProfile::new("test", DbConfig::default_postgres());
        profile.value_refs = refs;
        profile
    }

    fn test_resolver() -> CompositeValueResolver {
        let cache = Arc::new(ValueCache::new(Duration::from_secs(60)));
        let mut resolver = CompositeValueResolver::new(cache);
        resolver.register_secret_provider(Arc::new(StubSecretProvider));
        resolver
    }

    #[tokio::test]
    async fn resolve_empty_refs_returns_unchanged() {
        let profile = ConnectionProfile::new("test", DbConfig::default_postgres());
        let resolver = test_resolver();
        let ctx = ResolveContext::default();

        let (patched, password) = resolve_profile_values(&profile, &resolver, &ctx)
            .await
            .unwrap();

        assert!(password.is_none());

        match &patched.config {
            DbConfig::Postgres { host, .. } => assert_eq!(host, "localhost"),
            _ => panic!("expected Postgres"),
        }
    }

    #[tokio::test]
    async fn resolve_host_and_password() {
        let mut refs = HashMap::new();
        refs.insert("host".to_string(), ValueRef::literal("db.example.com"));
        refs.insert(
            "password".to_string(),
            ValueRef::secret("stub", "db-pass", None),
        );

        let profile = test_profile_with_refs(refs);
        let resolver = test_resolver();
        let ctx = ResolveContext::default();

        let (patched, password) = resolve_profile_values(&profile, &resolver, &ctx)
            .await
            .unwrap();

        match &patched.config {
            DbConfig::Postgres { host, .. } => assert_eq!(host, "db.example.com"),
            _ => panic!("expected Postgres"),
        }

        let pw = password.unwrap();
        assert_eq!(pw.expose_secret(), "resolved-db-pass");
    }

    #[tokio::test]
    async fn resolve_port_parses_as_number() {
        let mut refs = HashMap::new();
        refs.insert("port".to_string(), ValueRef::literal("5433"));

        let profile = test_profile_with_refs(refs);
        let resolver = test_resolver();
        let ctx = ResolveContext::default();

        let (patched, _) = resolve_profile_values(&profile, &resolver, &ctx)
            .await
            .unwrap();

        match &patched.config {
            DbConfig::Postgres { port, .. } => assert_eq!(*port, 5433),
            _ => panic!("expected Postgres"),
        }
    }

    #[tokio::test]
    async fn resolve_ssm_access_fields() {
        let mut refs = HashMap::new();
        refs.insert("ssm_instance_id".to_string(), ValueRef::literal("i-abc123"));
        refs.insert("ssm_region".to_string(), ValueRef::literal("us-east-1"));
        refs.insert("ssm_remote_port".to_string(), ValueRef::literal("15432"));

        let mut params = HashMap::new();
        params.insert("instance_id".to_string(), "i-old".to_string());
        params.insert("region".to_string(), "eu-west-1".to_string());
        params.insert("remote_port".to_string(), "5432".to_string());

        let mut profile = test_profile_with_refs(refs);
        profile.access_kind = Some(crate::access::AccessKind::Managed {
            provider: "aws-ssm".to_string(),
            params,
        });

        let resolver = test_resolver();
        let ctx = ResolveContext::default();

        let (patched, _) = resolve_profile_values(&profile, &resolver, &ctx)
            .await
            .unwrap();

        match patched.access_kind {
            Some(crate::access::AccessKind::Managed { provider, params }) => {
                assert_eq!(provider, "aws-ssm");
                assert_eq!(params["instance_id"], "i-abc123");
                assert_eq!(params["region"], "us-east-1");
                assert_eq!(params["remote_port"], "15432");
            }
            other => panic!("expected AccessKind::Managed, got {:?}", other),
        }
    }
}
