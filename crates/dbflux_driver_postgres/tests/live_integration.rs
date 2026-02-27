use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, QueryRequest, SchemaLoadingStrategy, SslMode,
};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;

#[test]
#[ignore = "requires Docker daemon"]
fn postgres_live_connect_ping_query_and_schema() -> Result<(), dbflux_core::DbError> {
    containers::with_postgres_url(|uri| {
        let driver = PostgresDriver::new();
        let profile = ConnectionProfile::new(
            "live-postgres",
            DbConfig::Postgres {
                use_uri: true,
                uri: Some(uri),
                host: String::new(),
                port: 5432,
                user: String::new(),
                database: "postgres".to_string(),
                ssl_mode: SslMode::Prefer,
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

        let result = connection.execute(&QueryRequest::new("SELECT 1 AS one"))?;
        assert_eq!(result.rows.len(), 1);

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::ConnectionPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let schema = connection.schema()?;
        let _ = schema.databases();

        Ok(())
    })
}
