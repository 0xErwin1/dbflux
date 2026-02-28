use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest, SchemaLoadingStrategy,
};
use dbflux_driver_redis::RedisDriver;
use dbflux_test_support::containers;

#[test]
#[ignore = "requires Docker daemon"]
fn redis_live_connect_ping_query_and_schema() -> Result<(), dbflux_core::DbError> {
    containers::with_redis_url(|uri| {
        let driver = RedisDriver::new();
        let profile = ConnectionProfile::new(
            "live-redis",
            DbConfig::Redis {
                use_uri: true,
                uri: Some(uri),
                host: String::new(),
                port: 6379,
                user: None,
                database: Some(0),
                tls: false,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let connection =
            containers::retry_db_operation(std::time::Duration::from_secs(30), || {
                let connection = driver.connect(&profile)?;
                connection.ping()?;
                Ok(connection)
            })?;

        let result = connection.execute(&QueryRequest::new("PING"))?;
        assert!(!result.rows.is_empty() || result.text_body.is_some());

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::LazyPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let (handle, _) = connection.execute_with_handle(&QueryRequest::new("PING"))?;
        let cancel = connection.cancel(&handle);
        assert!(matches!(cancel, Err(DbError::NotSupported(_))));

        let schema = connection.schema()?;
        let _ = schema.databases();

        Ok(())
    })
}
